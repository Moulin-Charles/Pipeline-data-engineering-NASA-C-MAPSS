-- Moving average of Sensor 2 over 5 cycles, per motor
-- Used to smooth out measurement noise and identify a trend of deterioration
SELECT
    engine_id,
    cycle,
    sensor_2,
    AVG(sensor_2) OVER (
        PARTITION BY engine_id
        ORDER BY cycle
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS moyenne_glissante_sensor_2
FROM motor
ORDER BY engine_id, cycle;

-- Sensor 2 drift: difference between the current moving average and the moving average from 5 cycles ago
-- A difference that increases over time indicates a gradual drift, not just noise
WITH moyennes AS (
    SELECT
        engine_id,
        cycle,
        sensor_2,
        AVG(sensor_2) OVER (
            PARTITION BY engine_id
            ORDER BY cycle
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS moyenne_glissante
    FROM motor
)
SELECT
    engine_id,
    cycle,
    moyenne_glissante,
    LAG(moyenne_glissante, 5) OVER (
        PARTITION BY engine_id
        ORDER BY cycle
    ) AS moyenne_glissante_precedente,
    moyenne_glissante - LAG(moyenne_glissante, 5) OVER (
        PARTITION BY engine_id
        ORDER BY cycle
    ) AS ecart
FROM moyennes
ORDER BY engine_id, cycle;

-- Engine comparison: lifespan (max cycle reached) per engine
-- Grouped by dataset/split/engine_id together, since engine_id alone can repeat across different replayed files
SELECT
    engine_id,
    dataset,
    split,
    MAX(cycle) as CycleMax
FROM motor
GROUP BY engine_id,dataset,split
ORDER BY CycleMax;

-- Life-phase progress: percentage of life elapsed for each cycle, based on that engine's own max cycle
-- Basis for grouping cycles into early/mid/late life phases in later analysis
SELECT
    engine_id,
    dataset,
    split,
    cycle,
    MAX(cycle) OVER (PARTITION BY engine_id, dataset, split) AS cycle_max,
    cycle::numeric / MAX(cycle) OVER (PARTITION BY engine_id, dataset, split) as Pourcentage
FROM motor
ORDER BY engine_id, cycle;

-- Life-phase labeling: same percentage as above, but categorized into 'debut'/'milieu'/'fin' per row
-- Intermediate check before aggregating (see next query for the actual per-phase average)
WITH pourcentages AS (
    SELECT
        engine_id,
        dataset,
        split,
        cycle,
        cycle::numeric / MAX(cycle) OVER (PARTITION BY engine_id, dataset, split) AS pourcentage
    FROM motor
)
SELECT
    engine_id,
    dataset,
    split,
    cycle,
    pourcentage,
    CASE
        WHEN pourcentage < 0.33 THEN 'debut'
        WHEN pourcentage < 0.66 THEN 'milieu'
        ELSE 'fin'
    END AS phase_vie
FROM pourcentages
ORDER BY engine_id, cycle;

-- Life-phase aggregate: average sensor_2 reading per life phase (start/mid/end), across all engines
-- Shows whether sensor_2 trends differently as engines approach end of life
WITH pourcentages AS (
    SELECT
        engine_id, dataset, split, cycle, sensor_2,
        cycle::numeric / MAX(cycle) OVER (PARTITION BY engine_id, dataset, split) AS pourcentage
    FROM motor
),
phases AS (
    SELECT
        *,
        CASE
            WHEN pourcentage < 0.33 THEN 'debut'
            WHEN pourcentage < 0.66 THEN 'milieu'
            ELSE 'fin'
        END AS phase_vie
    FROM pourcentages
)
SELECT
    phase_vie,
    AVG(sensor_2) AS moyenne_sensor_2
FROM phases
GROUP BY phase_vie
ORDER BY phase_vie;

-- Engine ranking by lifespan: which engines lasted longest before failure
-- Uses RANK() so tied lifespans share the same rank (unlike ROW_NUMBER, which would break ties arbitrarily)
WITH cyclemax AS (
    Select
        engine_id,
        dataset,
        split,
        MAX(cycle) as CycleMax
    FROM motor
    GROUP BY engine_id,dataset,split
)
SELECT
    engine_id,
    CycleMax,
    RANK() OVER (ORDER BY CycleMax DESC) AS rank
FROM cyclemax
ORDER BY rank;

-- Sensor anomaly detection: flags cycles where sensor_2 deviates more than 2 standard deviations
-- from that engine's own average - a simple outlier check, reusable later for Elasticsearch indexing
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
    sensor_2,
    cycle,
    ABS(sensor_2 - average) > 2 * ecart AS anomalie
FROM calcul
ORDER BY anomalie DESC, engine_id, cycle;