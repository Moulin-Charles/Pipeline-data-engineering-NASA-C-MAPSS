# Notes — Étape 3 : le producteur Kafka

Résumé des concepts vus en écrivant `producer.py`. Mémo, pas spec — voir `spec_evenements.md`
pour le raisonnement propre au projet (clés, choix du dataset).

## Choix de librairie : confluent-kafka vs kafka-python

Les deux font le même métier (parler le protocole Kafka depuis Python), ce ne sont **pas** deux
outils complémentaires à utiliser ensemble — un choix entre les deux, pas une addition.

| | `kafka-python` | `confluent-kafka` |
|---|---|---|
| Implémentation | Pure Python | Wrapper autour de `librdkafka` (C) |
| Installation | Aucune dépendance système | Wheels précompilés dans la plupart des cas |
| Maintenance / perf | Moins active, plus lent | Client officiel Confluent, plus robuste |

Choisi : **confluent-kafka**, plus proche de ce qui se fait en production.

## Anatomie d'un message Kafka

Un message a **trois** zones distinctes, pas une seule :

- **Clé** (`key=`) : sert au routage/partitionnement. Chez nous, `engine_id`.
- **Valeur** (`value=`) : le contenu réel, notre JSON avec tous les champs (`dataset`, `split`,
  `engine_id`, `cycle`, réglages, capteurs — la clé naturelle complète, même si `engine_id` est
  déjà dans la clé Kafka : le consommateur lit la valeur, pas la clé brute).
- **Headers** (optionnel, pas utilisé ici) : une troisième zone clé/valeur séparée, pour des
  métadonnées de transport. Pas nécessaire quand tout rentre déjà dans le JSON.

## Typage des données

Ne pas convertir toutes les colonnes de la même façon :

- `engine_id`, `cycle` → identifiants, donc `int`. Rester en `float` (`1.0`) ou en chaîne (`"1"`)
  trahit leur rôle réel (repère de la clé naturelle, pas une mesure).
- Réglages + capteurs → `float`, ce sont des grandeurs physiques.

Un JSON avec des nombres entre guillemets (`"sensor_1": "518.67"`) oblige le consommateur à
reconvertir plus tard — autant envoyer le bon type dès le producteur.

## Simuler un flux, pas un import en masse

Publier tout le fichier d'un coup avec un `for` sans pause revient à faire un import CSV — ça ne
démontre rien de l'intérêt de Kafka (flux observable, dashboard qui se met à jour en direct).
Un `time.sleep(...)` court entre chaque `produce()` suffit ; pas besoin de respecter le vrai
espacement temporel des cycles moteur.

## API confluent-kafka : pièges retenus

- Le paramètre du callback de livraison s'appelle `on_delivery=`, pas `callback=` — nom erroné
  provoque une erreur à l'appel de `produce()`.
- `produce()` place le message dans une file interne, il ne l'envoie pas forcément tout de suite
  côté application. `producer.poll(0)` sert la file de callbacks immédiatement (utile pour voir
  les confirmations s'afficher au fur et à mesure plutôt que d'un bloc à la fin).
- `producer.flush()` en fin de script vide la file et attend que tout soit réellement parti —
  sans lui, des messages en attente peuvent être perdus à la fermeture du programme.
- La clé (`key=`) doit être une chaîne ou des octets, jamais un nombre brut (`float`/`int`) —
  sinon `produce()` lève une erreur de type.

## Vérifier avec `rpk` (CLI intégrée à l'image Redpanda)

```bash
docker compose exec redpanda rpk topic list
docker compose exec redpanda rpk topic describe <topic>
docker compose exec redpanda rpk topic consume <topic> -n 5
```

`rpk` vit **dans le conteneur** — toujours préfixer par `docker compose exec redpanda`, une
commande `rpk` tapée directement sur l'hôte Windows ne trouvera rien.

## Le piège du nombre de partitions par défaut

Un topic auto-créé par Redpanda (première publication dessus) démarre avec **1 partition** par
défaut. Avec une seule partition, le choix de clé de partition ne produit aucun effet observable
— tout atterrit au même endroit quelle que soit la clé.

Fix appliqué :
```bash
docker compose exec redpanda rpk topic create cmapss-telemetry --partitions 3
# ou, sur un topic déjà créé :
docker compose exec redpanda rpk topic add-partitions cmapss-telemetry --num 2
```

Vérification concrète que la clé fonctionne : avec 3 partitions, `engine_id=1` reste toujours
dans la même partition d'un message à l'autre, `engine_id=2` dans une autre. C'est la preuve
pratique du choix de clé fait à l'étape 1 — utile telle quelle en entretien.

**Point de vigilance pour l'étape 8** : cette commande de création de topic est manuelle, elle ne
vit dans aucun fichier du dépôt. Un `docker compose up` depuis zéro recréerait un topic à 1 seule
partition. À documenter (ou automatiser via un service "one-shot" dans `docker-compose.yml`,
avec `depends_on: condition: service_completed_successfully`) avant de considérer le projet fini.

## VS Code — rendre `producer.py` flexible

`producer.py` accepte maintenant le chemin du fichier à rejouer en argument (`sys.argv[1]`, avec
`CMAPSSData/train_FD001.txt` comme défaut) plutôt qu'un chemin codé en dur.

Dans `.vscode/launch.json`, deux types d'`inputs` pour demander une valeur à chaque lancement :

- `"type": "promptString"` : champ de texte libre, avec une valeur `default` pré-remplie.
- `"type": "pickString"` : menu déroulant avec une liste fixe (`options`), plus sûr qu'un champ
  libre quand les choix valides sont connus à l'avance (les 8 fichiers `train_FD00X`/`test_FD00X`).

Pour choisir entre différents **scripts** (pas juste un paramètre d'un même script), l'usage
standard est plusieurs `configurations` nommées dans `launch.json` (un menu déroulant apparaît
dans le panneau "Run and Debug" de VS Code) — pas un `pickString` sur le chemin du programme,
réservé au cas où tous les scripts partageraient exactement la même configuration de lancement.

## Bugs rencontrés en écrivant `producer.py`

| Bug | Cause | Fix |
|---|---|---|
| Chemin de fichier introuvable | `"\CMAPSSData\train_FD001.txt"` sans préfixe `r` — `\t` interprété comme une tabulation, pas un backslash + t | Chaîne brute (`r"..."`), slash `/`, ou `pathlib.Path` |
| `IndexError` sur la liste de noms | Liste `Naming` incomplète (3 éléments pour 26 colonnes) | Génération via `range()` plutôt que saisie manuelle |
| `TypeError` sur `key=json[0]` | `json` référence le **module** importé (`import json`), pas une variable de données | Utiliser la vraie valeur (`valeurs[0]` ou `donnees["engine_id"]`) |
| `produce()` échoue avec une clé `float` | Conversion de toutes les colonnes en `float`, y compris `engine_id`/`cycle` | Typage différencié : `int` pour les 2 premières colonnes, `float` pour le reste ; clé Kafka toujours convertie en chaîne |
| Callback jamais appelé | Mauvais nom de paramètre (`callback=` au lieu de `on_delivery=`) | Corriger le nom du paramètre |
