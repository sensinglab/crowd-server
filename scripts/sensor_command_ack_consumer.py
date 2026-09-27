#!/usr/bin/env python3
import json
from datetime import datetime, timezone
import psycopg2
import psycopg2.extensions
import paho.mqtt.client as mqtt

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

ACK_TOPIC = "monicrowd/sensors/ack/#"

_pg_conn = None


def get_pg():
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = psycopg2.connect(**PG_CONFIG)
        _pg_conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )
    return _pg_conn


def on_message(client, userdata, msg):
    # captura o timestamp ANTES de qualquer processamento
    recv_ts = datetime.now(timezone.utc)

    try:
        payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except Exception:
        print("[WARN][ACK] invalid json")
        return

    job_id = payload.get("job_id")
    uuid = payload.get("uuid")
    result = payload.get("result")

    if not job_id or not uuid:
        print("[WARN][ACK] missing job_id/uuid")
        return

    try:
        conn = get_pg()
        with conn.cursor() as cur:
            if result == "ok":
                cur.execute("""
                    UPDATE sensor_commands
                    SET status = 'acked',
                        acked_at = %s,
                        error = NULL
                    WHERE id = %s AND status = 'sent'
                """, (recv_ts, job_id))
            else:
                cur.execute("""
                    UPDATE sensor_commands
                    SET status = 'failed',
                        error = %s
                    WHERE id = %s
                """, (payload.get("error", "unknown"), job_id))

            if cur.rowcount == 0:
                print(f"[WARN][ACK] job={job_id} no matching 'sent' row")
            else:
                print(f"[OK][ACK] job={job_id} uuid={uuid} result={result}")

    except Exception as e:
        print(f"[ERROR][PG][ACK] {e}")
        _pg_conn_reset()


def _pg_conn_reset():
    global _pg_conn
    try:
        if _pg_conn and not _pg_conn.closed:
            _pg_conn.close()
    except Exception:
        pass
    _pg_conn = None


def main():
    c = mqtt.Client()
    c.username_pw_set(MQTT_USER, MQTT_PASS)
    c.on_message = on_message
    c.connect(MQTT_HOST, MQTT_PORT, 60)
    c.subscribe(ACK_TOPIC, qos=1)
    print("[OK] ack_consumer started (persistent PG, Python-side timestamps)")
    c.loop_forever()


if __name__ == "__main__":
    main()