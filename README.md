# Crowd Server

This repository contains the server-side components of **MoniCrowd**. Sensors send crowd measurements and operational messages over Wi-Fi or LoRaWAN. The backend ingests measurements, maintains sensor configuration and state, supports remote commands, and displays results in Grafana.

## Main components

- **Sensor state:** `scripts/sensor_state_sync_consumer.py` stores sensor state and network information in PostgreSQL.
- **Remote commands:** `scripts/sensor_command_dispatcher.py` sends pending MQTT commands and publishes configuration snapshots. `scripts/sensor_command_ack_consumer.py` records acknowledgements.
- **LoRaWAN recovery:** `scripts/gap_detector.py` detects sequence gaps, requests replay through TTN, and records LoRaWAN command acknowledgements.
- **Measurement routing:** `scripts/pg_router_execd.py` maps measurements to their InfluxDB buckets, expands replay batches, and processes Wi-Fi sequence updates.
- **ChirpStack bridge:** `scripts/chirpstack_uplink_mqtt_bridge.py` forwards ChirpStack uplinks to MQTT.

## Telegraf

The configurations in `telegraf/` cover the three ingestion paths:

- `wifi_uplink.conf` for Wi-Fi measurements;
- `ttn_uplink.conf` for TTN uplinks, using `set_measurement.star`;
- `helium_uplink.conf` for Helium uplinks.

These pipelines use `pg_router_execd.py` to enrich and route measurements before they reach InfluxDB. The Grafana dashboard exports are in `grafana dashboards/`.

## Configuration

The backend requires Python 3, PostgreSQL, an MQTT broker, Telegraf, InfluxDB, and systemd. Service definitions are in `services/systemd/`. The examples in `scripts/example.env` and `telegraf/example.env` list the required environment variables. Set their real values privately on the server; the example files contain placeholders only.

The Python services read `/etc/monicrowd/server.env`. Telegraf needs its variables in its own service environment. Check the installed service paths and account before deploying these templates. The LoRaWAN OTA sender is not included in this repository.
