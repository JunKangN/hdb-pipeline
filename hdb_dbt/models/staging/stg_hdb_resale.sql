WITH source AS (
    SELECT * FROM {{ source('hdb_pipeline', 'hdb_resale_transactions') }}
),

cleaned AS (
    SELECT
        month,
        town,
        flat_type,
        block,
        street_name,
        storey_range,
        CAST(floor_area_sqm AS NUMERIC) AS floor_area_sqm,
        flat_model,
        lease_commence_date,
        remaining_lease,
        CAST(resale_price AS NUMERIC) AS resale_price,
        ROUND(
            CAST(resale_price AS NUMERIC) / NULLIF(CAST(floor_area_sqm AS NUMERIC), 0),
            2
        ) AS price_per_sqm
    FROM source
    WHERE resale_price IS NOT NULL
        AND floor_area_sqm IS NOT NULL
)

SELECT * FROM cleaned