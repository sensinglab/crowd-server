#!/usr/bin/env python3
"""
gap_detector.py — Daemon que deteta gaps nos SEQs dos uplinks LoRa
e pede reenvio via downlink TTN.

v2: last_confirmed_seq para retry de gaps não resolvidos.
v3: + FPort 21 OTA ACK handling (grava T6 em ota_lora_log).
"""

import json
import sys
import os
import time
import base64
import struct
import logging
import datetime
import threading
import psycopg2
import paho.mqtt.client as mqtt

# ── Configuração ─────────────────────────────────────────────────

TTN_MQTT_HOST = os.environ["TTN_MQTT_HOST"]
TTN_MQTT_PORT = int(os.getenv("TTN_MQTT_PORT", "8883"))
TTN_APP_ID = os.environ["TTN_APP_ID"]
TTN_MQTT_USER = os.getenv("TTN_MQTT_USER", TTN_APP_ID + "@ttn")
TTN_MQTT_PASSWORD = os.environ["TTN_MQTT_PASSWORD"]
PG_DSN = os.environ["PG_DSN"]

BUFFER_SIZE       = 256
MAX_GAP           = 256
DEBOUNCE_SEC      = 30
RETRY_SEC         = 120     # tempo entre retries para gaps não resolvidos
DOWNLINK_FPORT    = 5
DOWNLINK_DELAY_S  = 10       # delay antes de enviar downlink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GAP] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("gap_detector")


# ── PostgreSQL helpers ───────────────────────────────────────────

def pg_connect():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    return conn


def ensure_seq_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_seq_state (
                device_id           TEXT PRIMARY KEY,
                last_seq            INTEGER NOT NULL DEFAULT -1,
                last_confirmed_seq  INTEGER NOT NULL DEFAULT -1,
                last_seen           TIMESTAMPTZ DEFAULT NOW(),
                last_downlink_at    TIMESTAMPTZ
            )
        """)
        cur.execute("""
            ALTER TABLE sensor_seq_state
            ADD COLUMN IF NOT EXISTS last_confirmed_seq INTEGER NOT NULL DEFAULT -1
        """)


def get_last_seq(conn, device_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_seq, last_confirmed_seq, last_downlink_at "
            "FROM sensor_seq_state WHERE device_id = %s",
            (device_id,)
        )
        return cur.fetchone()


def upsert_last_seq(conn, device_id, seq):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sensor_seq_state (device_id, last_seq, last_seen)
            VALUES (%s, %s, NOW())
            ON CONFLICT (device_id) DO UPDATE
            SET last_seq = EXCLUDED.last_seq,
                last_seen = NOW()
        """, (device_id, seq))


def update_confirmed_seq(conn, device_id, seq):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE sensor_seq_state SET last_confirmed_seq = %s WHERE device_id = %s",
            (seq, device_id)
        )


def update_downlink_time(conn, device_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE sensor_seq_state SET last_downlink_at = NOW() WHERE device_id = %s",
            (device_id,)
        )


# ── SEQ arithmetic ──────────────────────────────────────────────

def seq_distance(from_seq, to_seq):
    return (to_seq - from_seq) % BUFFER_SIZE


def seq_is_ahead(a, b):
    dist = (a - b) % BUFFER_SIZE
    return 0 < dist < (BUFFER_SIZE // 2)


# ── Downlink publishing ─────────────────────────────────────────

def publish_downlink(mqtt_client, device_id, last_seq_value):
    payload_bytes = struct.pack(">B", last_seq_value % BUFFER_SIZE)
    payload_b64   = base64.b64encode(payload_bytes).decode("ascii")

    downlink_msg = {
        "downlinks": [{
            "f_port":      DOWNLINK_FPORT,
            "frm_payload": payload_b64,
            "priority":    "NORMAL"
        }]
    }

    topic = f"v3/{TTN_APP_ID}@ttn/devices/{device_id}/down/push"
    result = mqtt_client.publish(topic, json.dumps(downlink_msg), qos=0)

    log.info(
        f"Downlink sent: device={device_id} last_seq={last_seq_value} "
        f"topic={topic} rc={result.rc}"
    )


def schedule_downlink(mqtt_client, pg_conn, device_id, confirmed_seq):
    """Agenda um downlink com delay para dar tempo ao sensor de entrar em escuta."""

    def _send():
        log.info(f"Waiting {DOWNLINK_DELAY_S}s before downlink to {device_id}...")
        time.sleep(DOWNLINK_DELAY_S)
        update_downlink_time(pg_conn, device_id)
        publish_downlink(mqtt_client, device_id, confirmed_seq)

    t = threading.Thread(target=_send, daemon=True)
    t.start()


# ── Uplink handler ───────────────────────────────────────────────

def handle_uplink(mqtt_client, pg_conn, device_id, decoded_payload, f_port):

    msg_type = decoded_payload.get("type", "")

    # ── FPort 1: medição normal com SEQ ──────────────────────────
    if msg_type == "numdetections" and "seq" in decoded_payload:
        incoming_seq = int(decoded_payload["seq"])

        row = get_last_seq(pg_conn, device_id)

        if row is None:
            log.info(f"New device {device_id}, initializing seq={incoming_seq}")
            upsert_last_seq(pg_conn, device_id, incoming_seq)
            update_confirmed_seq(pg_conn, device_id, incoming_seq)
            return

        stored_last_seq, last_confirmed_seq, last_downlink_at = row
        gap = seq_distance(stored_last_seq, incoming_seq)
        unresolved = seq_distance(last_confirmed_seq, stored_last_seq) > 0

        log.info(
            f"device={device_id} stored_last_seq={stored_last_seq} "
            f"last_confirmed_seq={last_confirmed_seq} "
            f"incoming_seq={incoming_seq} gap={gap} unresolved={unresolved}"
        )

        # Atualizar last_seq
        if gap > 0:
            upsert_last_seq(pg_conn, device_id, incoming_seq)

        # ── Novo gap detetado ────────────────────────────────────
        if gap > 1:
            can_send = True
            if last_downlink_at is not None:
                elapsed = (
                    datetime.datetime.now(datetime.timezone.utc) - last_downlink_at
                ).total_seconds()
                if elapsed < DEBOUNCE_SEC:
                    log.info(
                        f"Debounce active for {device_id}: "
                        f"{elapsed:.0f}s < {DEBOUNCE_SEC}s"
                    )
                    can_send = False

            if can_send and gap <= MAX_GAP:
                log.info(
                    f"Gap detected for {device_id}: {gap - 1} missing "
                    f"(SEQ {stored_last_seq + 1} to {incoming_seq - 1})"
                )
                schedule_downlink(
                    mqtt_client, pg_conn, device_id, last_confirmed_seq
                )

            elif gap > MAX_GAP:
                log.warning(
                    f"Gap too large for {device_id}: {gap} > {MAX_GAP}, skipping"
                )

        # ── Sequencial mas gap anterior não resolvido → retry ────
        elif gap == 1 and unresolved:
            if last_downlink_at is not None:
                elapsed = (
                    datetime.datetime.now(datetime.timezone.utc) - last_downlink_at
                ).total_seconds()
                if elapsed >= RETRY_SEC:
                    log.info(
                        f"Retry for {device_id}: unresolved gap "
                        f"(confirmed={last_confirmed_seq} last={stored_last_seq}) "
                        f"elapsed={elapsed:.0f}s >= {RETRY_SEC}s"
                    )
                    schedule_downlink(
                        mqtt_client, pg_conn, device_id, last_confirmed_seq
                    )
                else:
                    log.info(
                        f"Unresolved gap for {device_id}, retry not due: "
                        f"{elapsed:.0f}s < {RETRY_SEC}s"
                    )
            else:
                # Nunca foi enviado downlink para este gap
                log.info(
                    f"Unresolved gap for {device_id}, no prior downlink, sending now"
                )
                schedule_downlink(
                    mqtt_client, pg_conn, device_id, last_confirmed_seq
                )

        # ── Sequencial, sem gaps pendentes → confirmar ───────────
        elif gap == 1 and not unresolved:
            update_confirmed_seq(pg_conn, device_id, incoming_seq)

        elif gap == 0:
            log.info(f"Duplicate SEQ={incoming_seq} for {device_id}")

    # ── FPort 4: replay recebido ─────────────────────────────────
    elif msg_type == "replay_lora":
        seq_oldest = int(decoded_payload.get("seq_oldest", 0))
        batch_size = int(decoded_payload.get("batch_size", 0))

        if batch_size > 0:
            highest_seq = (seq_oldest + batch_size - 1) % BUFFER_SIZE

            row = get_last_seq(pg_conn, device_id)

            if row is None:
                upsert_last_seq(pg_conn, device_id, highest_seq)
                update_confirmed_seq(pg_conn, device_id, highest_seq)
            else:
                stored_last_seq, last_confirmed_seq, _ = row

                if seq_is_ahead(highest_seq, last_confirmed_seq) or highest_seq == stored_last_seq:
                    update_confirmed_seq(pg_conn, device_id, highest_seq)
                    log.info(
                        f"Gap resolved for {device_id}: "
                        f"last_confirmed_seq -> {highest_seq}"
                    )

                if seq_is_ahead(highest_seq, stored_last_seq):
                    upsert_last_seq(pg_conn, device_id, highest_seq)

            log.info(
                f"Replay received: device={device_id} seq_oldest={seq_oldest} "
                f"batch_size={batch_size} highest_seq={highest_seq}"
            )

    # ── FPort 21: OTA ACK (grava T6 na tabela ota_lora_log) ─────
    elif f_port == 21:
        job_id = str(decoded_payload.get("job_id", ""))
        result = decoded_payload.get("result", "unknown")
        ts_rx  = datetime.datetime.now(datetime.timezone.utc)

        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    """UPDATE ota_lora_log
                       SET ts_ack_rx = %s, ack_result = %s
                       WHERE job_id = %s""",
                    (ts_rx, result, job_id)
                )
            log.info(
                f"OTA ACK: device={device_id} job_id={job_id} "
                f"result={result} T6={ts_rx.isoformat()}"
            )
        except Exception as e:
            log.error(f"OTA ACK: falha ao gravar T6: {e}")


# ── MQTT callbacks ───────────────────────────────────────────────

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        topic = f"v3/{TTN_APP_ID}@ttn/devices/+/up"
        client.subscribe(topic, qos=0)
        log.info(f"Connected to TTN MQTT, subscribed: {topic}")
    else:
        log.error(f"MQTT connect failed rc={rc}")


def on_message(client, userdata, msg):
    pg_conn = userdata["pg_conn"]

    try:
        data = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        log.error(f"Failed to parse MQTT message: {e}")
        return

    topic_parts = msg.topic.split("/")
    if len(topic_parts) < 5:
        return

    device_id = topic_parts[3]

    uplink_msg = data.get("uplink_message", {})
    decoded    = uplink_msg.get("decoded_payload")
    f_port     = uplink_msg.get("f_port")

    if decoded is None or f_port is None:
        return

    try:
        handle_uplink(client, pg_conn, device_id, decoded, f_port)
    except Exception as e:
        log.error(f"Error handling uplink from {device_id}: {e}")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        log.warning(f"MQTT disconnected unexpectedly rc={rc}, reconnecting...")


# ── Main ─────────────────────────────────────────────────────────

def main():
    if not TTN_MQTT_PASSWORD:
        log.error("TTN_MQTT_PASSWORD not set.")
        sys.exit(1)

    pg_conn = pg_connect()
    #ensure_seq_table(pg_conn)
    log.info("PostgreSQL connected, sensor_seq_state table ready")

    client = mqtt.Client(
        client_id="gap-detector-daemon",
        userdata={"pg_conn": pg_conn}
    )

    client.username_pw_set(TTN_MQTT_USER, TTN_MQTT_PASSWORD)
    client.tls_set()
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    log.info(f"Connecting to {TTN_MQTT_HOST}:{TTN_MQTT_PORT}...")
    client.connect(TTN_MQTT_HOST, TTN_MQTT_PORT, keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        client.disconnect()
        pg_conn.close()


if __name__ == "__main__":
    main()