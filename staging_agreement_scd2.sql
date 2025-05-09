
-- models/staging_agreement_scd2.sql
-- dbt model for SCD Type 2 delta load

{{ config(
    materialized='incremental',
    unique_key='AGREEMENT_IDENTIFIER',
    incremental_strategy='insert_overwrite'
) }}

WITH current_live AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY SOURCE_SYSTEM_AGREEMENT_NUMBER ORDER BY EFFECTIVE_FROM_DATE DESC) AS rn
        FROM {{ ref('history_table') }}
        WHERE EFFECTIVE_TO_DATE = '9999-12-31 23:59:59'
    )
    WHERE rn = 1
),

id_map AS (
    WITH distinct_keys AS (
        SELECT DISTINCT SOURCE_SYSTEM_AGREEMENT_NUMBER FROM {{ ref('feed_table') }}
    ),
    max_existing AS (
        SELECT COALESCE(MAX(AGREEMENT_IDENTIFIER), 0) AS max_id FROM {{ ref('history_table') }}
    ),
    new_keys AS (
        SELECT dk.SOURCE_SYSTEM_AGREEMENT_NUMBER
        FROM distinct_keys dk
        LEFT JOIN current_live cl
          ON dk.SOURCE_SYSTEM_AGREEMENT_NUMBER = cl.SOURCE_SYSTEM_AGREEMENT_NUMBER
        WHERE cl.SOURCE_SYSTEM_AGREEMENT_NUMBER IS NULL
    ),
    new_ids AS (
        SELECT
            SOURCE_SYSTEM_AGREEMENT_NUMBER,
            ROW_NUMBER() OVER (ORDER BY SOURCE_SYSTEM_AGREEMENT_NUMBER)
              + (SELECT max_id FROM max_existing) AS AGREEMENT_IDENTIFIER
        FROM new_keys
    )
    SELECT
        cl.SOURCE_SYSTEM_AGREEMENT_NUMBER,
        cl.AGREEMENT_IDENTIFIER
    FROM current_live cl
    UNION ALL
    SELECT * FROM new_ids
),

staging AS (
    SELECT
      f.SOURCE_SYSTEM_AGREEMENT_NUMBER,
      m.AGREEMENT_IDENTIFIER,
      f.EFFECTIVE_FROM_DATE,
      f.EFFECTIVE_TO_DATE,
      f.ROW_HASH,
      cl.ROW_HASH AS existing_hash,
      CASE
        WHEN cl.ROW_HASH IS NULL THEN 'INSERT'
        WHEN cl.ROW_HASH <> f.ROW_HASH THEN 'UPDATE'
        ELSE 'UNCHANGED'
      END AS ACTION
    FROM {{ ref('feed_table') }} f
    JOIN id_map m USING (SOURCE_SYSTEM_AGREEMENT_NUMBER)
    LEFT JOIN current_live cl
      ON f.SOURCE_SYSTEM_AGREEMENT_NUMBER = cl.SOURCE_SYSTEM_AGREEMENT_NUMBER
)

SELECT
  AGREEMENT_IDENTIFIER,
  SOURCE_SYSTEM_AGREEMENT_NUMBER,
  EFFECTIVE_FROM_DATE,
  EFFECTIVE_TO_DATE,
  ROW_HASH
FROM staging
WHERE ACTION IN ('INSERT', 'UPDATE')
