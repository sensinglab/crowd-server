#!/usr/bin/env python3
import os
import json
import psycopg2
import paho.mqtt.client as mqtt


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "your_mqtt_username")
MQTT_PASS = os.getenv("MQTT_PASS", "your_mqtt_password")
ACK_TOPIC = os.getenv("ACK_TOPIC", "monicrowd/sensors/ack/#")

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "127.0.0.1"),
    "dbname": os.getenv("PG_DB", "your_database"),
    "user": os.getenv("PG_USER", "your_pg_user"),
    "password": os.getenv("PG_PASS", "your_pg_password"),
}

def on_message(client, userdata, msg):
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
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()

        if result == "ok":
            cur.execute("""
                UPDATE sensor_commands
                SET status='acked', acked_at=NOW(), error=NULL
                WHERE id=%s
            """, (job_id,))
        else:
            cur.execute("""
                UPDATE sensor_commands
                SET status='failed', error=%s
                WHERE id=%s
            """, (payload.get("error", "unknown"), job_id))

        conn.commit()
        cur.close()
        conn.close()
        print(f"[OK][ACK] job={job_id} uuid={uuid} result={result}")

    except Exception as e:
        print(f"[ERROR][PG][ACK] {e}")

def main():
    c = mqtt.Client()
    c.username_pw_set(MQTT_USER, MQTT_PASS)
    c.on_message = on_message
    c.connect(MQTT_HOST, MQTT_PORT, 60)
    c.subscribe(ACK_TOPIC)
    print("[OK] ack_consumer started")
    c.loop_forever()

if __name__ == "__main__":
    main()
