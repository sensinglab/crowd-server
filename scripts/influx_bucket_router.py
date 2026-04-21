#!/usr/bin/env python3
import os
import sys
import psycopg2

PG_DSN = os.environ.get("PG_DSN")

SQL = """
SELECT influxdb_bucket, sensor_name, uuid
FROM public.sensors
WHERE device_id_ttn = %s
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

def main():
    if not PG_DSN:
        sys.stderr.write("PG_DSN not defined\n")
        sys.exit(1)

    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as e:
        sys.stderr.write(f"Failed to connect to PostgreSQL: {e}\n")
        sys.exit(1)

    for line in sys.stdin:
        lp = line.strip()
        if not lp:
            continue

        try:
            head, *rest = lp.split(" ", 1)

            tags = {}
            parts_head = head.split(",")
            measurement = parts_head[0]

            for kv in parts_head[1:]:
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    tags[k] = v

            device_id = tags.get("device_id")

            if not device_id:
                print(lp, flush=True)
                continue

            cur.execute(SQL, (device_id,))
            row = cur.fetchone()

            if not row:
                print(lp, flush=True)
                continue

            bucket, sensor_name, sensor_uuid = row

            new_parts = [measurement]

            for p in parts_head[1:]:
                if not (
                    p.startswith("bucket=") or
                    p.startswith("sensor_name=") or
                    p.startswith("sensor_uuid=")
                ):
                    new_parts.append(p)

            if bucket is not None:
                new_parts.insert(1, f"bucket={escape_tag_value(bucket)}")

            if sensor_name is not None:
                new_parts.append(f"sensor_name={escape_tag_value(sensor_name)}")

            if sensor_uuid is not None:
                new_parts.append(f"sensor_uuid={escape_tag_value(sensor_uuid)}")

            new_head = ",".join(new_parts)
            new_lp = new_head + (" " + rest[0] if rest else "")

            print(new_lp, flush=True)

        except Exception as e:
            sys.stderr.write(f"Error processing line: {e}\n")
            print(lp, flush=True)

if __name__ == "__main__":
    main()
