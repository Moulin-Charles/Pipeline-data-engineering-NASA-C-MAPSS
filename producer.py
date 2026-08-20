import json  # noqa: I001
import sys
import time
from pathlib import Path
from confluent_kafka import Producer

chemin_fichier = sys.argv[1] if len(sys.argv) > 1 else "CMAPSSData/train_FD001.txt"

my_files = Path(chemin_fichier).stem

split, dataset = my_files.split("_")

Naming = ["engine_id", "cycle"] + [f"setting_{i}" for i in range(1, 4)] + [f"sensor_{i}" for i in range(1, 22)]

# Configuration Kafka
producer = Producer({
    "bootstrap.servers": "redpanda:9092"
})

topic = "cmapss-telemetry"

def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur lors de l'envoi : {err}")
    else:
        print(
            f"Message envoyé dans {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"offset {msg.offset()}"
        )

with open(chemin_fichier, "r", encoding="utf-8") as fichier:
    for ligne in fichier:
        valeurs = [(valeur.strip('"')) for valeur in ligne.strip().split()]

        if len(valeurs) != 26:
            print(f"Ligne incorrecte : {len(valeurs)} valeurs")
            continue

        donnees = {"split": split, "dataset": dataset}
        for i, (nom, valeur) in enumerate(zip(Naming, valeurs)):
            donnees[nom] = int(valeur) if i < 2 else float(valeur)

        json_data = json.dumps(donnees, ensure_ascii=False)

        producer.produce(
            topic=topic,
            key=valeurs[0],
            value=json_data,
            on_delivery=delivery_report
        )

        producer.poll(0)

        time.sleep(0.2)

producer.flush()
