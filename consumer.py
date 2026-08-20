import json
import psycopg2
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:19092",
    "group.id": "redpanda-to-postgres",
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["cmapss-telemetry"])

conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="postgres", user="postgres", password="example",
)
cur = conn.cursor()

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Erreur:", msg.error())
        continue

    donnees = json.loads(msg.value().decode("utf-8"))

    colonnes = list(donnees.keys())
    noms_colonnes = ", ".join(colonnes)
    placeholders = ", ".join(f"%({c})s" for c in colonnes)

    cur.execute(
        f"""
        INSERT INTO motor ({noms_colonnes})
        VALUES ({placeholders})
        ON CONFLICT (split, dataset, engine_id, cycle) DO NOTHING
        """,
        donnees,
    )
    conn.commit()



