# Spec — modèle d'événement C-MAPSS

## Source

Dataset : NASA C-MAPSS, simulation de dégradation de moteur d'avion turbofan (pas des données réelles de vol).
Source : https://data.nasa.gov/dataset/cmapss-jet-engine-simulated-data

## Pourquoi FD001

Sous-dataset FD001 : une seule condition opérationnelle, un seul mode de panne (dégradation HPC).
Moins de facteurs à isoler pour observer une dérive capteur → le plus simple pour démarrer l'analyse.

## Forme d'un événement

26 colonnes : 1 `engine_id`, 1 `cycle`, 3 réglages opérationnels, 21 capteurs.

## Clé naturelle (unicité en base)

`(dataset, split, engine_id, cycle)`.

Nécessaire car `engine_id` seul se répète entre les sous-datasets (FD001-FD004) et entre train/test :
sans `dataset` et `split`, deux moteurs différents pourraient partager la même clé et créer une collision.

## Clé Kafka (ordre / partitionnement)

`engine_id` seul.

Rôle différent de la clé naturelle : elle sert uniquement à garantir que toutes les lignes d'un même
moteur atterrissent dans la même partition, donc dans l'ordre où elles ont été publiées (ordre temporel
de réception). Elle n'a pas besoin d'être unique globalement — l'unicité est gérée par la clé naturelle
en base, pas par la clé Kafka.
