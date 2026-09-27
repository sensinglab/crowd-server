#!/usr/bin/env python3
import json
import select
import time
import psycopg2
import psycopg2.extensions
import paho.mqtt.client as mqtt
from datetime import datetime, timezone


import os

MQTT_HOST = os.environ["MQTT_HOST"]
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.environ["MQTT_USER"]
MQTT_PASS = os.environ["MQTT_PASS"]

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "127.0.0.1"),
    "dbname": os.getenv("PG_DB", "sensorsconfiguration"),
    "user": os.environ["PG_USER"],
    "password": os.environ["PG_PASS"],
}

CMD_TOPIC_PREFIX = "monicrowd/sensors/cmd/"
SNAP_TOPIC_PREFIX = "monicrowd/sensors/config_snapshot/"

PUBLISH_QOS = 1
PUBLISH_TIMEOUT_SEC = 3

DEBOUNCE_TYPES = {"set_config", "disable", "activate"}
NO_DEBOUNCE_TYPES = {"reboot", "shutdown"}


def ensure_mqtt_connected(mqttc: mqtt.Client) -> None:
    if mqttc.is_connected():
        return
    mqttc.reconnect()


def publish_mqtt(mqttc: mqtt.Client, topic: str, payload: dict, retain: bool = False) -> None:
    ensure_mqtt_connected(mqttc)
    info = mqttc.publish(topic, json.dumps(payload), qos=PUBLISH_QOS, retain=retain)
    info.wait_for_publish(timeout=PUBLISH_TIMEOUT_SEC)
    if not info.is_published():
        raise RuntimeError(f"MQTT publish not confirmed topic={topic}")


def mark_skipped(cur, sensor_uuid: str, command_type: str, keep_id: int):
    cur.execute("""
        UPDATE sensor_commands
        SET status='skipped',
            error='debounced (replaced by newer command)',
            sent_at=NULL
        WHERE sensor_uuid=%s
          AND command_type=%s
          AND status='pending'
          AND id <> %s
    """, (sensor_uuid, command_type, keep_id))


def fetch_latest_pending(cur, sensor_uuid: str, command_type: str):
    cur.execute("""
        SELECT id, sensor_uuid, command_type, payload
        FROM sensor_commands
        WHERE sensor_uuid=%s
          AND command_type=%s
          AND status='pending'
        ORDER BY id DESC
        LIMIT 1
    """, (sensor_uuid, command_type))
    return cur.fetchone()


def fetch_exact_pending(cur, job_id: int):
    cur.execute("""
        SELECT id, sensor_uuid, command_type, payload
        FROM sensor_commands
        WHERE id=%s AND status='pending'
    """, (job_id,))
    return cur.fetchone()


def build_config_patch(command_type: str, payload: dict) -> dict | None:
    """
    Só comandos que alteram configuração devem gerar snapshot.
    reboot/shutdown não mudam config.
    """
    ctype = (command_type or "").strip().lower()

    if ctype == "set_config":
        return payload if isinstance(payload, dict) else {}

    if ctype == "disable":
        return {"sensor": {"Status": "Disabled"}}

    if ctype == "activate":
        return {"sensor": {"Status": "Active"}}

    return None


def merge_config(cur, sensor_uuid: str, patch: dict):
    cur.execute(
        "SELECT * FROM merge_sensor_config(%s, %s::jsonb)",
        (sensor_uuid, json.dumps(patch))
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("merge_sensor_config returned no result")

    new_version, new_config = row
    return int(new_version), new_config


def main():
    mqttc = mqtt.Client()
    mqttc.username_pw_set(MQTT_USER, MQTT_PASS)
    mqttc.connect(MQTT_HOST, MQTT_PORT, 60)
    mqttc.loop_start()

    conn = psycopg2.connect(**PG_CONFIG)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()
    cur.execute("LISTEN sensor_commands_channel;")
    cur.close()

    print("[OK] command_dispatcher listening on sensor_commands_channel (with config snapshots)")

    while True:
        if select.select([conn], [], [], 10) == ([], [], []):
            continue

        conn.poll()

        while conn.notifies:
            notify = conn.notifies.pop(0)
            job_id = int(notify.payload)

            c = conn.cursor()

            try:
                row = fetch_exact_pending(c, job_id)
                if not row:
                    continue

                _id, sensor_uuid, command_type, payload = row
                command_type_norm = (command_type or "").strip().lower()

                if command_type_norm in DEBOUNCE_TYPES:
                    latest = fetch_latest_pending(c, sensor_uuid, command_type)

                    if not latest:
                        continue

                    latest_id, sensor_uuid, command_type, payload = latest
                    mark_skipped(c, sensor_uuid, command_type, latest_id)
                    _id = latest_id

                elif command_type_norm not in NO_DEBOUNCE_TYPES:
                    print(f"[WARN] unknown command_type={command_type}, sending without debounce")

                patch = build_config_patch(command_type, payload)

                new_version = None
                new_config = None

                if patch is not None:
                    new_version, new_config = merge_config(c, sensor_uuid, patch)

                cmd = {
                    "job_id": _id,
                    "type": command_type,
                    "config_version": new_version,
                    "payload": payload,
                    "ts": int(time.time())
                }

                cmd_topic = CMD_TOPIC_PREFIX + str(sensor_uuid)
                sent_ts = datetime.now(timezone.utc)            
                publish_mqtt(mqttc, cmd_topic, cmd, retain=False)

                if patch is not None:
                    snapshot_topic = SNAP_TOPIC_PREFIX + str(sensor_uuid)

                    snapshot_payload = {
                        "uuid": str(sensor_uuid),
                        "config_version": new_version,
                        "config": new_config,
                        "ts": int(time.time())
                    }

                    publish_mqtt(mqttc, snapshot_topic, snapshot_payload, retain=True)

                    print(
                        f"[OK] snapshot retained uuid={sensor_uuid} "
                        f"version={new_version}"
                    )

                c.execute("""
                    UPDATE sensor_commands
                    SET status='sent', sent_at=%s, error=NULL
                    WHERE id=%s
                """, (sent_ts, _id))

                print(
                    f"[OK] published+sent job={_id} "
                    f"uuid={sensor_uuid} type={command_type}"
                )

            except Exception as e:
                try:
                    c.execute("""
                        UPDATE sensor_commands
                        SET status='failed', error=%s
                        WHERE id=%s
                    """, (str(e), job_id))
                except Exception:
                    pass

                print(f"[ERROR] job={job_id} failed: {e}")

            finally:
                c.close()


if __name__ == "__main__":
    main()