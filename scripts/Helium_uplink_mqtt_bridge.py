#!/usr/bin/env python3
"""
ChirpStack - MQTT Bridge with Token Authentication
Publishes uplinks to: chirpstack/uplink/<deviceName>
"""
import os
from flask import Flask, request
import paho.mqtt.publish as publish
import json
import datetime

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_USER = os.getenv("MQTT_USER", "your_mqtt_username")
MQTT_PASS = os.getenv("MQTT_PASS", "your_mqtt_password")
AUTH_TOKEN = os.getenv("CHIRPSTACK_BRIDGE_TOKEN", "change_me")
LOG_FILE = os.getenv("CHIRPSTACK_BRIDGE_LOG", "/var/log/chirpstack_bridge.log")
app = Flask(__name__)

def log(message: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{ts}] {message}\n")

@app.before_request
def check_token():
    token = request.args.get("key")
    if token != AUTH_TOKEN:
        log(f"Unauthorized access attempt from {request.remote_addr}")
        return "Unauthorized", 401

@app.route("/", methods=["POST"])
def receive_uplink():
    try:
        data = request.json

        dev = (
            data.get("deviceInfo", {})
                .get("deviceName", "unknown")
                .replace(" ", "_")
        )

        # Extract type
        obj = data.get("object", {})
        msg_type = obj.get("type", "unknown")

        # Topic routing
        if msg_type == "numdetections":
            topic = f"chirpstack/uplink/{dev}/detections"
        elif msg_type == "sensorLocation":
            topic = f"chirpstack/uplink/{dev}/location"
        else:
            topic = f"chirpstack/uplink/{dev}/unknown"

        publish.single(
            topic,
            json.dumps(data),
            hostname=MQTT_BROKER,
            auth={"username": MQTT_USER, "password": MQTT_PASS}
        )

        log(f"Published to MQTT topic: {topic}")
        return "OK", 200

    except Exception as e:
        log(f"Error: {e}")
        return "Internal Server Error", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)