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

-- Derive du capteur 2 : ecart entre la moyenne glissante actuelle et celle d'il y a 5 cycles
-- Un ecart qui grandit dans le temps indique une derive progressive, pas juste du bruit
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