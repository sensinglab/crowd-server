#!/usr/bin/env python3
import json
import time
import psycopg2
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

MQTT_TOPIC = "monicrowd/sensors/#"

PG_CONN = None

def get_pg_connection():
    global PG_CONN
    try:
        if PG_CONN is None or PG_CONN.closed != 0:
            PG_CONN = psycopg2.connect(**PG_CONFIG)
            PG_CONN.autocommit = False
        return PG_CONN
    except Exception as e:
        print(f"[ERROR][PG] Cannot connect: {e}")
        PG_CONN = None
        return None



# upsert sensor state into "sensors"

def handle_state(conn, payload):
    uuid = payload.get("uuid")

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sensors (
            uuid,
            sensor_name,
            latitude,
            longitude,
            status,
            connectivity_mode,
            power_filtration_db,
            firmware_version,
            messages_periodicity_min,
            sliding_window_min,
            device_id_ttn,
            device_name_helium,
            influxdb_bucket,
            last_seen,
            updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
        ON CONFLICT (uuid)
        DO UPDATE SET
            sensor_name = COALESCE(EXCLUDED.sensor_name, sensors.sensor_name),
            latitude = COALESCE(EXCLUDED.latitude, sensors.latitude),
            longitude = COALESCE(EXCLUDED.longitude, sensors.longitude),
            status = EXCLUDED.status,
            connectivity_mode = EXCLUDED.connectivity_mode,
            power_filtration_db = EXCLUDED.power_filtration_db,
            firmware_version = EXCLUDED.firmware_version,
            messages_periodicity_min = EXCLUDED.messages_periodicity_min,
            sliding_window_min = EXCLUDED.sliding_window_min,
            device_id_ttn = EXCLUDED.device_id_ttn,
            device_name_helium = EXCLUDED.device_name_helium,
            influxdb_bucket = EXCLUDED.influxdb_bucket,
            last_seen = NOW(),
            updated_at = NOW();
    """, (
        uuid,
        payload.get("sensor_name"),
        payload.get("latitude"),
        payload.get("longitude"),
        payload.get("status"),
        payload.get("connectivity_mode"),
        payload.get("power_filtration_db"),
        payload.get("firmware_version"),
        payload.get("messages_periodicity_min"),
        payload.get("sliding_window_min"),
        payload.get("device_id_ttn"),
        payload.get("device_name_helium"),
        payload.get("influxdb_bucket"),
    ))

    conn.commit()
    cur.close()
    print(f"[OK][STATE] upsert uuid={uuid}")


def handle_networks(conn, payload):
    uuid = payload.get("uuid")
    networks = payload.get("networks", [])

    if not isinstance(networks, list):
        print(f"[WARN][NET] networks ignored (not a list) uuid={uuid}")
        return

    cur = conn.cursor()

    cur.execute("DELETE FROM sensor_networks WHERE sensor_uuid = %s", (uuid,))

    for n in networks:
        if not isinstance(n, dict):
            continue

        cur.execute("""
            INSERT INTO sensor_networks (
                sensor_uuid, network_type, name, priority, ssid,
                dev_eui, app_eui, available, connected, updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            uuid,
            n.get("type"),
            n.get("name"),
            n.get("priority"),
            n.get("ssid"),
            n.get("dev_eui"),
            n.get("app_eui"),
            n.get("available"),
            n.get("connected"),
        ))

    conn.commit()
    cur.close()
    print(f"[OK][NET] refreshed uuid={uuid} ({len(networks)} networks)")



def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[OK][MQTT] connected")
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"[OK][MQTT] subscribed to {MQTT_TOPIC}")
    else:
        print(f"[ERROR][MQTT] connect failed rc={rc}")


def on_message(client, userdata, msg):

    try:
        payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        print(f"[WARN][MQTT] Invalid JSON topic={msg.topic}")
        return

    # Must have uuid
    uuid = payload.get("uuid")
    if not uuid:
        print(f"[WARN][MQTT] Missing uuid topic={msg.topic}")
        return

    conn = get_pg_connection()
    if conn is None:
        print("[WARN][PG] DB unavailable, message skipped")
        return

    # Route by topic
    try:
        print("[DEBUG][MQTT_TOPIC]", msg.topic)
        print("[DEBUG][MQTT_PAYLOAD]", json.dumps(payload, indent=2))
        if msg.topic.startswith("monicrowd/sensors/state"):
            handle_state(conn, payload)
        elif msg.topic.startswith("monicrowd/sensors/networks"):
            handle_networks(conn, payload)
        else:
            print(f"[INFO][MQTT] Ignored topic={msg.topic}")

    except psycopg2.Error as e:
        print(f"[ERROR][PG] Query failed uuid={uuid}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass


def main():
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASS)

    client.on_connect = on_connect
    client.on_message = on_message

    # keep retrying MQTT connect if broker is down
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            break
        except Exception as e:
            print(f"[ERROR][MQTT] connect retry in 5s: {e}")
            time.sleep(5)

    print("[OK] Sensor consumer started (state + networks)")
    client.loop_forever()


if __name__ == "__main__":
    main()
