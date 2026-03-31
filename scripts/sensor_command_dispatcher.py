#!/usr/bin/env python3
import os
import json
import select
import time
import psycopg2
import psycopg2.extensions
import paho.mqtt.client as mqtt



PG_CONFIG = {
    "host": os.getenv("PG_HOST", "127.0.0.1"),
    "dbname": os.getenv("PG_DB", "your_database"),
    "user": os.getenv("PG_USER", "your_pg_user"),
    "password": os.getenv("PG_PASS", "your_pg_password"),
}

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "your_mqtt_username")
MQTT_PASS = os.getenv("MQTT_PASS", "your_mqtt_password")

CMD_TOPIC_PREFIX = os.getenv("CMD_TOPIC_PREFIX", "monicrowd/sensors/cmd/")

PUBLISH_QOS = 1
PUBLISH_TIMEOUT_SEC = 3

# Commands that should be debounced (keep only the latest pending)
DEBOUNCE_TYPES = {"set_config", "disable", "activate"}

# Commands that should NOT be debounced
NO_DEBOUNCE_TYPES = {"reboot", "shutdown"}


def ensure_mqtt_connected(mqttc: mqtt.Client) -> None:
    if mqttc.is_connected():
        return
    mqttc.reconnect()


def publish_cmd(mqttc: mqtt.Client, topic: str, payload: dict) -> None:
    ensure_mqtt_connected(mqttc)
    info = mqttc.publish(topic, json.dumps(payload), qos=PUBLISH_QOS, retain=False)
    info.wait_for_publish(timeout=PUBLISH_TIMEOUT_SEC)
    if not info.is_published():
        raise RuntimeError("MQTT publish not confirmed (timeout)")


def mark_skipped(cur, sensor_uuid: str, command_type: str, keep_id: int):
    """
    Mark older pending commands of same type for the same sensor as skipped.
    """
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
    """
    Fetch the latest pending command for (sensor_uuid, command_type).
    """
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


def main():
    # MQTT
    mqttc = mqtt.Client()
    mqttc.username_pw_set(MQTT_USER, MQTT_PASS)
    mqttc.connect(MQTT_HOST, MQTT_PORT, 60)
    mqttc.loop_start()

    # Postgres LISTEN
    conn = psycopg2.connect(**PG_CONFIG)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("LISTEN sensor_commands_channel;")
    cur.close()

    print("[OK] command_dispatcher listening on sensor_commands_channel (with debounce)")

    while True:
        if select.select([conn], [], [], 10) == ([], [], []):
            continue

        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            job_id = int(notify.payload)

            c = conn.cursor()
            try:
                # First, fetch the job that triggered the notify
                row = fetch_exact_pending(c, job_id)
                if not row:
                    continue

                _id, sensor_uuid, command_type, payload = row
                command_type_norm = (command_type or "").strip().lower()

                # Debounce logic: if this type is debounced, always send the latest pending one
                if command_type_norm in DEBOUNCE_TYPES:
                    latest = fetch_latest_pending(c, sensor_uuid, command_type)
                    if not latest:
                        continue

                    latest_id, sensor_uuid, command_type, payload = latest

                    # Skip all other pending commands of this type for this sensor
                    mark_skipped(c, sensor_uuid, command_type, latest_id)

                    # If the notify wasn't for the latest, do nothing else (latest will be sent now anyway)
                    _id = latest_id

                elif command_type_norm not in NO_DEBOUNCE_TYPES:
                    # Unknown types: treat as debounced-safe? safer is to NOT debounce.
                    pass

                cmd = {
                    "job_id": _id,
                    "type": command_type,
                    "payload": payload,
                    "ts": int(time.time())
                }

                topic = CMD_TOPIC_PREFIX + str(sensor_uuid)

                # Publish first
                publish_cmd(mqttc, topic, cmd)

                # Mark sent
                c.execute("""
                    UPDATE sensor_commands
                    SET status='sent', sent_at=NOW(), error=NULL
                    WHERE id=%s
                """, (_id,))

                print(f"[OK] published+sent job={_id} uuid={sensor_uuid} type={command_type}")

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