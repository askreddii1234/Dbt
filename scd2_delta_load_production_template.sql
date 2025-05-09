
-- SCD TYPE 2 DELTA LOAD - PRODUCTION TEMPLATE

-- Assumptions:
-- 1. Using permanent tables for `history_table`, `feed_table`, and `agreement_processing_log`
-- 2. You configure these values as needed:
--    - `@cutoff_ts`: Passed as a parameter (last processed timestamp)
--    - `_infinity_ts`: Standard infinity end date

DECLARE _infinity_ts DATETIME DEFAULT '9999-12-31 23:59:59';

-- Replace with your actual dataset and tables
DECLARE _cutoff_ts DATETIME DEFAULT (
  SELECT IFNULL(MAX(last_processed_timestamp), '2000-01-01 00:00:00')
  FROM your_dataset.agreement_processing_log
  WHERE processing_type = 'DELTA'
);

-- Step 1: Extract Current Live Rows from History
CREATE OR REPLACE TEMP TABLE current_live AS
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY SOURCE_SYSTEM_AGREEMENT_NUMBER ORDER BY EFFECTIVE_FROM_DATE DESC) AS rn
  FROM your_dataset.history_table
  WHERE EFFECTIVE_TO_DATE = _infinity_ts
)
WHERE rn = 1;

-- Step 2: Generate ID Mapping (existing + new)
CREATE OR REPLACE TEMP TABLE id_map AS
WITH distinct_keys AS (
  SELECT DISTINCT SOURCE_SYSTEM_AGREEMENT_NUMBER FROM your_dataset.feed_table
),
max_existing AS (
  SELECT COALESCE(MAX(AGREEMENT_IDENTIFIER), 0) AS max_id FROM your_dataset.history_table
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
SELECT * FROM new_ids;

-- Step 3: Join Feed to Live Data and Determine Actions
CREATE OR REPLACE TEMP TABLE staging AS
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
FROM your_dataset.feed_table f
JOIN id_map m USING (SOURCE_SYSTEM_AGREEMENT_NUMBER)
LEFT JOIN current_live cl
  ON f.SOURCE_SYSTEM_AGREEMENT_NUMBER = cl.SOURCE_SYSTEM_AGREEMENT_NUMBER;

-- Step 4: Close Existing Rows
UPDATE your_dataset.history_table h
SET EFFECTIVE_TO_DATE = DATETIME_SUB(_cutoff_ts, INTERVAL 1 SECOND)
WHERE EXISTS (
  SELECT 1 FROM staging s
  WHERE s.ACTION = 'UPDATE'
    AND h.AGREEMENT_IDENTIFIER = s.AGREEMENT_IDENTIFIER
    AND h.EFFECTIVE_TO_DATE = _infinity_ts
);

-- Step 5: Insert New/Updated Records
INSERT INTO your_dataset.history_table (
  AGREEMENT_IDENTIFIER,
  SOURCE_SYSTEM_AGREEMENT_NUMBER,
  EFFECTIVE_FROM_DATE,
  EFFECTIVE_TO_DATE,
  ROW_HASH
)
SELECT
  AGREEMENT_IDENTIFIER,
  SOURCE_SYSTEM_AGREEMENT_NUMBER,
  EFFECTIVE_FROM_DATE,
  EFFECTIVE_TO_DATE,
  ROW_HASH
FROM staging
WHERE ACTION IN ('INSERT', 'UPDATE');

-- Step 6: Log Summary
INSERT INTO your_dataset.agreement_processing_log (
  processing_type,
  last_processed_timestamp,
  record_count,
  new_records,
  changed_records,
  unchanged_records
)
SELECT
  'DELTA' AS processing_type,
  _cutoff_ts AS last_processed_timestamp,
  COUNT(*) AS record_count,
  SUM(CASE WHEN ACTION = 'INSERT' THEN 1 ELSE 0 END),
  SUM(CASE WHEN ACTION = 'UPDATE' THEN 1 ELSE 0 END),
  SUM(CASE WHEN ACTION = 'UNCHANGED' THEN 1 ELSE 0 END)
FROM staging;
