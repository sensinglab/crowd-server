#!/usr/bin/env python3
"""
Telegraf execd processor — LoRa + WiFi uplink routing with replay support.

Handles:
  - crowding         (FPort 1): single measurement, standard routing
  - crowding_replay  (FPort 4): batch of pending measurements, expands into
                                N individual 'crowding' lines with reconstructed
                                timestamps. Uses ts_oldest from payload (real sensor
                                timestamp) if available; falls back to T_recv-based
                                reconstruction for older payloads without ts_oldest.
  - location         (FPort 2): pass-through with bucket/sensor_name injection
  - location_delta   (FPort 3): pass-through with bucket/sensor_name injection
"""

import os
import sys
import psycopg2

PG_DSN = os.environ.get("PG_DSN")

SQL_BY_DEVICE_ID = """
SELECT influxdb_bucket, sensor_name, uuid, messages_periodicity_min
FROM public.sensors
WHERE device_id_ttn = %s OR device_name_helium = %s
LIMIT 1
"""

SQL_BY_UUID = """
SELECT influxdb_bucket, sensor_name, uuid, messages_periodicity_min
FROM public.sensors
WHERE uuid = %s
LIMIT 1
"""


def escape_tag_value(value):
    if value is None:
        return None
    value = str(value)
    value = value.replace("\\", "\\\\")
    value = value.replace(" ", r"\ ")
    value = value.replace(",", r"\,")
    value = value.replace("=", r"\=")
    return value


def parse_field_value_python(raw):
    """Converte valor de field do line protocol para tipo Python nativo."""
    if raw.endswith("i") and not raw.startswith('"'):
        try:
            return int(raw[:-1])
        except ValueError:
            pass

    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]

    if raw.lower() in ("t", "true"):
        return True
    if raw.lower() in ("f", "false"):
        return False

    try:
        return float(raw)
    except ValueError:
        return raw


def parse_line_protocol(lp):
    """
    Parse de uma linha de InfluxDB line protocol.
    Retorna (measurement, tags_dict, fields_str, fields_dict, timestamp_str)
    """
    parts = lp.split(" ")

    if len(parts) < 2:
        return None, {}, "", {}, None

    head       = parts[0]
    fields_str = parts[1]
    ts_str     = parts[2] if len(parts) > 2 else None

    head_parts  = head.split(",")
    measurement = head_parts[0]

    tags = {}
    for kv in head_parts[1:]:
        if "=" in kv:
            k, v = kv.split("=", 1)
            tags[k] = v

    fields = {}
    for kv in fields_str.split(","):
        if "=" in kv:
            k, v = kv.split("=", 1)
            fields[k] = v

    return measurement, tags, fields_str, fields, ts_str


def build_tag_string(measurement, tags):
    parts = [measurement]
    for k, v in tags.items():
        parts.append(f"{k}={v}")
    return ",".join(parts)


def build_field_string(fields):
    parts = []
    for k, v in fields.items():
        parts.append(f"{k}={v}")
    return ",".join(parts)


def inject_sensor_metadata(tags, bucket, sensor_name, sensor_uuid):
    new_tags = dict(tags)
    if bucket is not None:
        new_tags["bucket"] = escape_tag_value(bucket)
    if sensor_name is not None:
        new_tags["sensor_name"] = escape_tag_value(sensor_name)
    if sensor_uuid is not None:
        new_tags["sensor_uuid"] = escape_tag_value(sensor_uuid)
    return new_tags


def lookup_sensor(cur, device_id=None, sensor_uuid=None):
    """Lookup sensor na tabela sensors do PostgreSQL."""
    if device_id:
        cur.execute(SQL_BY_DEVICE_ID, (device_id, device_id))
        return cur.fetchone()
    elif sensor_uuid:
        cur.execute(SQL_BY_UUID, (sensor_uuid,))
        return cur.fetchone()
    return None


def handle_replay(cur, tags, fields, ts_str):
    """
    Expande um replay_lora (FPort 4) em N linhas numdetections individuais
    com timestamps corretos.

    Modo principal (payload com ts_oldest):
      T[i] = ts_oldest + i * periodicity_s
      Usa o timestamp real da medição mais antiga guardado no buffer do sensor.

    Modo fallback (payload antigo sem ts_oldest, ts_oldest == 0):
      T[i] = T_recv - (batch_size - 1 - i) * periodicity_s
      Reconstrói a partir do momento de receção do TTN (menos preciso).
    """
    sys.stderr.write(f"[REPLAY-DEBUG] tags={tags}\n")
    sys.stderr.write(f"[REPLAY-DEBUG] fields={fields}\n")

    device_id = tags.get("device_id")

    if not device_id:
        sys.stderr.write("[REPLAY] Missing device_id tag — cannot route\n")
        return

    row = lookup_sensor(cur, device_id=device_id)

    if not row:
        sys.stderr.write(f"[REPLAY] Device '{device_id}' not found in sensors table\n")
        return

    bucket, sensor_name, sensor_uuid, periodicity_min = row

    if not periodicity_min:
        sys.stderr.write(
            f"[REPLAY] messages_periodicity_min NULL para '{device_id}', usando 5\n"
        )
        periodicity_min = 5

    # ── Parse fields ─────────────────────────────────────────────
    seq_oldest       = parse_field_value_python(fields.get("seq_oldest",       "0i"))
    batch_size       = parse_field_value_python(fields.get("batch_size",       "0i"))
    measurements_enc = parse_field_value_python(fields.get("measurements_enc", '""'))
    ts_oldest        = parse_field_value_python(fields.get("ts_oldest",        "0i"))
    ts_oldest        = int(ts_oldest)

    # ── Parse pipe-separated measurements ────────────────────────
    try:
        measurements = [int(x) for x in str(measurements_enc).split("|") if x]
    except (ValueError, AttributeError):
        sys.stderr.write(f"[REPLAY] Falha ao parsear measurements_enc: {measurements_enc}\n")
        return

    if not measurements:
        sys.stderr.write("[REPLAY] Lista de medições vazia\n")
        return

    actual_batch  = len(measurements)
    periodicity_s = int(periodicity_min) * 60

    # ── Determinar modo de timestamp ─────────────────────────────
    if ts_oldest > 0:
        # Modo principal: timestamp real do buffer do sensor
        use_fallback = False
        sys.stderr.write(
            f"[REPLAY] device={device_id} seq_oldest={seq_oldest} "
            f"ts_oldest={ts_oldest} batch={actual_batch} "
            f"period={periodicity_min}min [modo=ts_oldest]\n"
        )
    else:
        # Modo fallback: reconstruir a partir do T_recv do TTN
        use_fallback = True
        if ts_str:
            try:
                ts_recv_ns = int(ts_str)
            except ValueError:
                sys.stderr.write(f"[REPLAY] Timestamp inválido: {ts_str}\n")
                return
        else:
            import time
            ts_recv_ns = int(time.time() * 1_000_000_000)

        periodicity_ns = periodicity_s * 1_000_000_000
        sys.stderr.write(
            f"[REPLAY] WARNING: ts_oldest=0, usando T_recv fallback para {device_id} "
            f"batch={actual_batch} period={periodicity_min}min [modo=fallback]\n"
        )

    # ── Injetar metadata e emitir linhas ─────────────────────────
    base_tags = inject_sensor_metadata(tags, bucket, sensor_name, sensor_uuid)
    base_tags.pop("type", None)

    for i, count in enumerate(measurements):
        if use_fallback:
            # T[i] = T_recv - (batch_size - 1 - i) × periodicity
            offset_ns = (actual_batch - 1 - i) * periodicity_ns
            ts_i_ns   = ts_recv_ns - offset_ns
        else:
            # T[i] = ts_oldest + i × periodicity  (timestamps reais do sensor)
            ts_i_ns = (ts_oldest + i * periodicity_s) * 1_000_000_000

        out_tags   = dict(base_tags)
        out_fields = {"devices_detected": f"{int(count)}i"}

        tag_str   = build_tag_string("numdetections", out_tags)
        field_str = build_field_string(out_fields)

        print(f"{tag_str} {field_str} {int(ts_i_ns)}", flush=True)

        sys.stderr.write(
            f"[REPLAY]   [{i}] seq={int(seq_oldest) + i} count={count} "
            f"ts_i={int(ts_i_ns)}\n"
        )


SQL_UPDATE_SEQ_STATE = """
INSERT INTO sensor_seq_state (device_id, last_seq, last_confirmed_seq, last_seen)
VALUES (%s, %s, %s, NOW())
ON CONFLICT (device_id) DO UPDATE
  SET last_seq           = GREATEST(sensor_seq_state.last_seq, EXCLUDED.last_seq),
      last_confirmed_seq = GREATEST(sensor_seq_state.last_confirmed_seq, EXCLUDED.last_confirmed_seq),
      last_seen          = EXCLUDED.last_seen
"""

def update_seq_state_from_wifi(cur, sensor_uuid, seq):
    """Atualiza sensor_seq_state quando uma mensagem WiFi com SEQ chega."""
    try:
        cur.execute(
            "SELECT device_id_ttn FROM public.sensors WHERE uuid = %s LIMIT 1",
            (sensor_uuid,)
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return
        device_id = row[0]
        cur.execute(SQL_UPDATE_SEQ_STATE, (device_id, seq, seq))
        sys.stderr.write(
            f"[SEQ-SYNC] Updated seq_state via WiFi: device={device_id} "
            f"last_seq={seq} last_confirmed_seq={seq}\n"
        )
    except Exception as e:
        sys.stderr.write(f"[SEQ-SYNC] Error updating seq_state: {e}\n")


def handle_standard(cur, measurement, tags, fields_str, fields, ts_str):
    """Routing standard: injeta bucket/sensor_name/uuid, sync seq, passa adiante."""

    device_id   = tags.get("device_id")
    sensor_uuid = tags.get("sensor_uuid")
    technology  = tags.get("technology")

    row = lookup_sensor(cur, device_id=device_id, sensor_uuid=sensor_uuid)

    if not row:
        head = build_tag_string(measurement, tags)
        out  = f"{head} {fields_str}"
        if ts_str:
            out += f" {ts_str}"
        print(out, flush=True)
        return

    bucket, sensor_name, sensor_uuid_db, _ = row

    # ── Sync seq state APENAS para mensagens recebidas via WiFi ──
    seq_val = fields.get("seq")

    if (
        technology == "wifi"
        and seq_val is not None
        and sensor_uuid_db is not None
    ):
        try:
            update_seq_state_from_wifi(
                cur,
                sensor_uuid_db,
                int(str(seq_val).rstrip("i"))
            )
        except Exception as e:
            sys.stderr.write(
                f"[SEQ-SYNC] Error updating WiFi seq_state: {e}\n"
            )

    new_tags = inject_sensor_metadata(
        tags, bucket, sensor_name, sensor_uuid_db
    )

    fields_out = dict(fields)

    head      = build_tag_string(measurement, new_tags)
    field_str = build_field_string(fields_out)
    out       = f"{head} {field_str}"

    if ts_str:
        out += f" {ts_str}"

    print(out, flush=True)


def get_connection():
    """Cria (ou recria) ligação ao PostgreSQL."""
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    sys.stderr.write("[ROUTER] PostgreSQL connection established\n")
    return conn, cur


def ensure_connection(conn, cur):
    """Verifica se a ligação está viva; reconecta se necessário."""
    try:
        cur.execute("SELECT 1")
        cur.fetchone()
        return conn, cur
    except Exception:
        sys.stderr.write("[ROUTER] Connection lost, reconnecting...\n")
        try:
            conn.close()
        except Exception:
            pass
        return get_connection()


def main():
    if not PG_DSN:
        sys.stderr.write("PG_DSN not defined\n")
        sys.exit(1)

    conn, cur = get_connection()

    for line in sys.stdin:
        lp = line.strip()
        if not lp:
            continue

        try:
            conn, cur = ensure_connection(conn, cur)

            measurement, tags, fields_str, fields, ts_str = parse_line_protocol(lp)

            if not measurement:
                print(lp, flush=True)
                continue

            # ── Replay expansion (FPort 4) ───────────────────────
            if measurement == "replay_lora":
                handle_replay(cur, tags, fields, ts_str)
                continue

            # ── Routing standard (FPort 1, 2, 3) ────────────────
            handle_standard(cur, measurement, tags, fields_str, fields, ts_str)

        except Exception as e:
            sys.stderr.write(f"[ROUTER] Error processing line: {e}\n")
            # Tenta reconectar na próxima iteração
            try:
                conn, cur = get_connection()
            except Exception as re_err:
                sys.stderr.write(f"[ROUTER] Reconnect failed: {re_err}\n")
            print(lp, flush=True)


if __name__ == "__main__":
    main()