import time
from datetime import datetime, timezone

import psycopg2
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

conn = psycopg2.connect(
    host="localhost", port=5432,
    dbname="postgres", user="postgres", password="example",
)
cur = conn.cursor()

if not es.indices.exists(index="cmapss-telemetry"):
    es.indices.create(
        index="cmapss-telemetry",
        mappings={
            "properties": {
                "split": {"type": "keyword"},
                "dataset": {"type": "keyword"},
                "engine_id": {"type": "integer"},
                "cycle": {"type": "integer"},
                "sensor_2": {"type": "float"},
                "anomalie": {"type": "boolean"},
                "date_ingestion": {"type": "date"},
            }
        }
    )

while True:
    cur.execute("""
        WITH calcul AS (
            Select
                engine_id,
                dataset,
                split,
                sensor_2,
                cycle,
                AVG(sensor_2) OVER (PARTITION BY engine_id, dataset, split) as average,
                STDDEV(sensor_2) OVER (PARTITION BY engine_id, dataset, split) as ecart
            FROM motor
        )
        SELECT
            engine_id,
            dataset,
            split,
            sensor_2,
            cycle,
            ABS(sensor_2 - average) > 2 * ecart AS anomalie
        FROM calcul
        ORDER BY anomalie DESC, engine_id, cycle;
    """)
    lignes = cur.fetchall()
    noms_colonnes = [desc[0] for desc in cur.description]

    for ligne in lignes:
        donnees = dict(zip(noms_colonnes, ligne))
        donnees["date_ingestion"] = datetime.now(timezone.utc).isoformat()
        id_document = f"{donnees['dataset']}_{donnees['split']}_{donnees['engine_id']}_{donnees['cycle']}"
        es.index(index="cmapss-telemetry", id=id_document, document=donnees)

    time.sleep(5)