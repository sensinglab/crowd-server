#!/usr/bin/env python3
import os, sys, psycopg2

PG_DSN = os.environ.get("PG_DSN")

# Query: match device_id to TTN or Helium identifiers
SQL = """
SELECT influxdb_bucket
FROM sensorsconfiguration
WHERE device_id_ttn = %s
   OR device_name_helium = %s
LIMIT 1
"""

def main():
    if not PG_DSN:
        sys.stderr.write("PG_DSN not defined\n")
        sys.exit(1)

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    for line in sys.stdin:
        lp = line.strip()
        if not lp:
            continue

        # Extract header (measurement + tags)
        head, *rest = lp.split(" ", 1)

        # Convert tags from CSV into dict
        tags = {}
        for kv in head.split(",")[1:]:
            if "=" in kv:
                k, v = kv.split("=", 1)
                tags[k] = v

        device_id = tags.get("device_id")
        if not device_id:
            continue

        # Query DB for matching bucket
        cur.execute(SQL, (device_id, device_id))
        row = cur.fetchone()
        if not row:
            continue

        bucket = row[0]

        # Inject bucket tag
        parts = [p for p in head.split(",") if not p.startswith("bucket=")]
        parts.insert(1, f"bucket={bucket}")
        new_head = ",".join(parts)

        new_lp = new_head + (" " + rest[0] if rest else "")
        print(new_lp, flush=True)

if __name__ == "__main__":
    main()
