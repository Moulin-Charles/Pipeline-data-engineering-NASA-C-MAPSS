# Notes — Étape 5 : la couche SQL

Résumé des concepts vus en écrivant `sql/queries.sql`. Mémo, pas spec.

## Décision : table large, pas de normalisation

Le plan parlait de "modéliser proprement moteurs, cycles, settings et capteurs" — ce qui aurait
pu vouloir dire séparer en plusieurs tables liées par clé étrangère (`moteurs`, `cycles`,
`capteurs_valeurs` en format long). Choix fait : garder la table `motor` large (une ligne par
cycle, 21 colonnes de capteurs) — plus simple, ne casse rien côté `consumer.py`, et reste
défendable pour des capteurs fixes et connus à l'avance. Compromis assumé plutôt qu'un oubli.

## Fonctions fenêtre (`OVER`) : la notion centrale

Un `GROUP BY` **fusionne** des lignes en une seule par groupe — on perd le détail. Une fonction
fenêtre calcule un agrégat **sans fusionner** : chaque ligne reste, mais reçoit en plus un
résultat calculé sur un groupe de lignes "voisines".

```sql
AVG(sensor_2) OVER (
    PARTITION BY engine_id
    ORDER BY cycle
    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
)
```

- `PARTITION BY` : comme `GROUP BY`, mais sans fusionner — relance le calcul séparément pour
  chaque moteur (indispensable, sinon une moyenne glissante mélangerait des moteurs différents).
- `ORDER BY` (dans `OVER`) : définit l'ordre de "voisinage" — différent du `ORDER BY` final de la
  requête, qui ne fait que trier l'affichage.
- `ROWS BETWEEN ... AND CURRENT ROW` : la fenêtre elle-même (ex: 4 précédentes + la ligne
  actuelle = fenêtre glissante sur 5 cycles).

**`GROUP BY` et fonction fenêtre ne se combinent pas dans la même logique.** Vouloir garder une
ligne par cycle (fonction fenêtre) tout en fusionnant par moteur (`GROUP BY`) est contradictoire
— soit l'un, soit l'autre, pas les deux sur les mêmes colonnes.

## CTE (`WITH ... AS (...)`) : pourquoi, et le piège n°1 de cette étape

**Le piège, rencontré au moins 4 fois** : un alias défini dans un `SELECT` n'est **pas visible**
par les autres expressions du **même** `SELECT`. Toutes les colonnes d'un `SELECT` sont calculées
en une seule fois à partir des données brutes, pas séquentiellement — un alias n'est qu'une
étiquette d'affichage, pas une variable réutilisable plus bas dans la même liste.

```sql
SELECT
    prix,
    prix * 2 AS double_prix,
    double_prix + 1 AS resultat   -- ERREUR : double_prix inconnu ici
FROM produits;
```

Deux solutions : répéter le calcul complet, ou passer par une **CTE** — une étape séquentielle
séparée, dont le résultat devient une vraie colonne réutilisable ensuite :

```sql
WITH etape1 AS (
    SELECT prix, prix * 2 AS double_prix FROM produits
)
SELECT prix, double_prix, double_prix + 1 AS resultat
FROM etape1;
```

Plusieurs CTE peuvent s'enchaîner dans un même `WITH`, séparées par une virgule, chacune pouvant
réutiliser la précédente :
```sql
WITH a AS (...), b AS (...)
SELECT ... FROM b;
```
(pas de virgule après la dernière CTE, juste avant le `SELECT` final qui l'utilise.)

## `LAG()` : la valeur d'il y a N lignes

`LAG(colonne, decalage) OVER (PARTITION BY ... ORDER BY ...)` — récupère la valeur de `colonne`
`decalage` lignes avant la ligne actuelle, dans la partition/l'ordre donnés. Sert à comparer une
valeur à son propre passé (ex: dérive = moyenne actuelle − moyenne d'il y a 5 cycles).

## `RANK()` / `ROW_NUMBER()` / `DENSE_RANK()` : classer sans fusionner

- `ROW_NUMBER()` : 1, 2, 3... toujours unique, même en cas d'égalité (départage arbitraire).
- `RANK()` : égalités = même rang, mais **saute** les numéros suivants (1, 1, 3).
- `DENSE_RANK()` : égalités = même rang, **sans sauter** (1, 1, 2).

`RANK()` ne prend **aucun argument** — contrairement à `AVG()`/`MAX()`, la colonne à classer va
dans le `ORDER BY` de `OVER(...)`, pas entre les parenthèses de `RANK()`.

## `STDDEV()` et détection d'anomalie

Écart-type : mesure la dispersion des valeurs autour de leur moyenne. Convention courante :
au-delà de 2 écarts-types, une valeur est "inhabituelle" (~95% des données normales tombent dans
cet intervalle).

```sql
ABS(sensor_2 - moyenne_moteur) > 2 * ecart_type_moteur AS anomalie
```

`ABS()` gère les écarts dans les deux sens (trop haut ou trop bas). Une comparaison (`>`) est
déjà un résultat booléen en Postgres — pas besoin d'un `CASE WHEN` pour fabriquer manuellement du
texte `'True'`/`'False'`.

## Division entière : le piège du pourcentage

`cycle / MAX(cycle)` avec deux entiers fait une division **entière** en SQL — résultat tronqué
(`0`, pas `0.3`). Convertir un des deux en décimal **avant** la division avec `::numeric` :
```sql
cycle::numeric / MAX(cycle) OVER (...)
```

## Autres pièges retenus

- **Comparaisons chaînées à la Python ne marchent pas** : `0.33 < x < 0.66` n'est pas valide en
  SQL — écrire `x >= 0.33 AND x < 0.66` explicitement (ou simplifier avec `CASE WHEN` en cascade,
  qui s'arrête à la première condition vraie).
- **Virgules manquantes ou en trop** entre les colonnes d'un `SELECT`, avant un `FROM`, ou en fin
  de `GROUP BY` juste avant `ORDER BY` — la source d'erreur la plus fréquente de cette étape.
- **Valeurs texte sans guillemets** (`THEN Fin` au lieu de `THEN 'fin'`) : Postgres cherche une
  colonne nommée `Fin`, pas une chaîne de caractères.
- **`FROM table,`** avec une virgule en trop imite l'ancienne syntaxe de jointure — Postgres
  attend une deuxième table après la virgule.
