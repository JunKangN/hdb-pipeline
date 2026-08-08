WITH transactions AS (
    SELECT * FROM {{ ref('stg_hdb_resale') }}
),

summary AS (
    SELECT
        town,
        flat_type,
        COUNT(*) AS total_transactions,
        ROUND(AVG(resale_price), 2) AS avg_price,
        ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resale_price) AS NUMERIC), 2) AS median_price,
        ROUND(AVG(price_per_sqm), 2) AS avg_price_per_sqm,
        MIN(resale_price) AS min_price,
        MAX(resale_price) AS max_price
    FROM transactions
    GROUP BY town, flat_type
)

SELECT * FROM summary
ORDER BY avg_price DESC