# Notes — Étape 4 : le consommateur et la conteneurisation

Résumé des concepts vus en écrivant `consumer.py`, `Dockerfile`, et en conteneurisant
producteur/consommateur. Mémo, pas spec — voir `spec_evenements.md` pour le raisonnement propre
au projet.

## Offsets et consumer groups (rappel conceptuel)

- Le broker garde tous les messages, peu importe qui les a lus — c'est au **consommateur** de
  retenir jusqu'où il en est, en committant un offset.
- Le `group.id` (une chaîne choisie par toi, ex: `"consumer-postgres"`) doit rester **stable dans
  le temps**, pas changer selon le fichier rejoué — c'est ce qui permet de reprendre où on
  s'était arrêté après un crash/redémarrage.
- Le carnet d'offsets est indexé par **(groupe, partition)**, pas par consommateur individuel :
  si plusieurs consommateurs partagent un groupe, Redpanda répartit les partitions entre eux ; si
  l'un disparaît, un **rebalance** réattribue ses partitions aux consommateurs restants, qui
  reprennent chacun où le carnet les indique.
- Delivery "au moins une fois" (*at-least-once*), pas "exactement une fois" : écriture en base et
  commit d'offset sont deux actions séparées, un crash entre les deux peut provoquer un
  retraitement. D'où l'importance de l'idempotence côté SQL.

## Écriture idempotente avec psycopg2

```python
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
```

- `msg.value()` renvoie des **octets** (`bytes`), pas une chaîne — `.decode("utf-8")` avant
  `json.loads()`.
- `ON CONFLICT (colonnes de la contrainte UNIQUE) DO NOTHING` : rejoue le même message deux fois
  → la deuxième insertion est silencieusement ignorée plutôt que de lever une erreur.
- **Placeholders nommés** (`%(nom)s` + passer le dictionnaire directement à `execute()`) : ne
  fonctionne que parce que les clés du JSON correspondent **exactement** aux noms de colonnes SQL
  — décision prise à l'étape 3 qui paie ici, plus besoin de construire un tuple positionnel à la
  main.
- `conn.commit()` indispensable — sans lui, rien n'est réellement écrit, la transaction reste en
  attente.

## Nuance de sécurité : f-string pour les noms de colonnes

Construire `noms_colonnes`/`placeholders` avec un f-string (pas `%s`) n'est sûr que parce que ces
noms viennent du producteur, que je contrôle entièrement — jamais d'une source externe non
fiable. Les **valeurs**, elles, passent toujours par `%(...)s` paramétré, jamais collées dans le
texte de la requête — c'est ce qui protège contre l'injection SQL. Si les clés JSON venaient
d'une source externe (API publique, etc.), il faudrait valider chaque nom contre une liste
blanche avant de l'utiliser dans le texte de la requête.

## Conteneuriser deux scripts avec un seul `Dockerfile`

Producteur et consommateur partagent les mêmes dépendances (`requirements.txt`) → un seul
`Dockerfile`, sans `CMD` fixe (la commande de lancement diffère selon le service) :

```dockerfile
FROM python:3.14
WORKDIR /Scripts_Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer.py .
COPY producer.py .
```

Puis dans `docker-compose.yml`, deux services distincts réutilisant la même image :
```yaml
producer:
  build: .
  command: python producer.py
  depends_on:
    redpanda:
      condition: service_healthy

consumer:
  build: .
  command: python consumer.py
  depends_on:
    db:
      condition: service_healthy
    redpanda:
      condition: service_healthy
```

## Pièges rencontrés

- **`WORKDIR` (conteneur) confondu avec un dossier local.** `WORKDIR /Scripts_Python` définit un
  chemin **à l'intérieur** de l'image — n'a aucun rapport avec l'organisation des fichiers sur ma
  machine. Créer un dossier local `Scripts_Python/` et y déplacer les scripts casse le build
  (`COPY` cherche les fichiers à la racine du contexte, là où vit le `Dockerfile`).
- **`COPY` a besoin de deux arguments**, source et destination, même si la destination est juste
  `.` — `COPY fichier.py` seul échoue.
- **`CMD`/`command:` dupliqués : seul le dernier compte**, que ce soit dans un `Dockerfile`
  (plusieurs `CMD`) ou un service Compose (plusieurs `command:`) — pas cumulatif, silencieusement
  ignoré.
- **Adresses internes vs externes, encore.** Une fois conteneurisés, `producer.py`/`consumer.py`
  utilisent `redpanda:9092` (pas `localhost:19092`) et `db:5432` (pas `localhost:5432`) — même
  principe que Adminer→`db` ou Kibana→`elasticsearch`, mais facile d'oublier une des deux
  connexions (Kafka **et** Postgres) en conteneurisant.
- **Config copiée-collée sans adapter la destination/le test.** Deux fois le même piège : la
  commande de démarrage Redpanda (`internal://.../external://...`) collée sur le service `db`
  (syntaxe qui n'a aucun sens pour Postgres, casse le conteneur) ; le `healthcheck`
  d'Elasticsearch (`curl http://localhost:9200/...`) collé sur `db` et `redpanda` sans changer le
  test — les deux ne passent alors **jamais** `healthy`, et tout ce qui a
  `depends_on: condition: service_healthy` dessus reste bloqué indéfiniment.
- **`env_file:` pointant vers un dossier ou un fichier inexistant** (`.venv`, ou un `.env` qui
  n'existe pas) fait échouer `docker compose up`. À supprimer si rien ne lit de variables
  d'environnement.
- **Monter un seul fichier de données plutôt que le dossier entier** limite la flexibilité — si
  `producer.py` accepte n'importe quel fichier en argument, monter tout `CMAPSSData/` (pas juste
  `train_FD001.txt`) garde cette flexibilité une fois conteneurisé.
- **`requirements.txt`, pas `requirement.txt`** — nom conventionnel exact attendu par `pip -r` et
  les `Dockerfile` standards.
- **`psycopg2` (source) vs `psycopg2-binary`** : la version binaire évite d'avoir besoin d'un
  compilateur C et des en-têtes PostgreSQL dans l'image — plus simple à conteneuriser.
