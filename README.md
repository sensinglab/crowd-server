# Crowd Server

This repository contains the main server-side components of the **MoniCrowd CrowdSensor** backend for ingestion, processing, storage, visualization, and alerting of data from WiFi and complementary sensors in privacy-preserving crowd sensing systems.

It supports the cloud and control layer of the system by:

- Synchronizing sensor state and network information with PostgreSQL
- Downlinking remote commands to sensors through MQTT
- Processing command acknowledgements
- Bridging ChirpStack uplinks into internal MQTT topics
- Routing measurements to the correct InfluxDB bucket
- Supporting Telegraf-based ingestion pipelines

## Main Components

### `sensor_state_sync_consumer.py`
Consumes MQTT sensor state and network messages and updates the backend PostgreSQL tables used to track deployed sensors.

### `sensor_command_dispatcher.py`
Reads pending commands from PostgreSQL and dispatches them to the appropriate sensor through MQTT.

### `sensor_command_ack_consumer.py`
Consumes MQTT acknowledgement messages from sensors and updates command execution status in PostgreSQL.

### `chirpstack_uplink_mqtt_bridge.py`
Receives HTTP uplinks from ChirpStack and republishes them into internal MQTT topics.

### `influx_bucket_router.py`
Resolves the correct InfluxDB bucket for each device by querying PostgreSQL to minimize information sent from the sensor.

## Telegraf Pipelines

- `ttn_uplink_ingestion.conf`  
  Ingests TTN uplinks and forwards decoded measurements into InfluxDB.

- `chirpstack_uplink_ingestion.conf`  
  Ingests Helium-related MQTT uplinks and forwards them into InfluxDB.

- `wifi_numdetections_ingestion.conf`  
  Ingests Wi-Fi crowd measurements uploaded through MQTT.

## Systemd Services

This repository includes systemd service definitions used to run the backend components persistently:

- `sensor-state-consumer.service`
- `command-dispatcher.service`
- `ack-consumer.service`
- `chirpstack-bridge.service`

These services are intended to start automatically on boot and restart after failures.

## Deployment Requirements

The target server should provide:

- Python 3
- PostgreSQL
- MQTT broker
- Telegraf
- InfluxDB
- systemd

## Security Note

This repository only contains sanitized code and configuration examples.  
Real MQTT credentials, database passwords, InfluxDB tokens, and API secrets were removed from the repository.
