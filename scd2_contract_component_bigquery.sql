
-- SCD TYPE 2 DELTA LOAD FOR CONTRACT-COMPONENT

DECLARE _infinity_ts DATETIME DEFAULT '9999-12-31 23:59:59';
DECLARE _cutoff_ts DATETIME DEFAULT '2025-01-05 00:00:00'; -- simulate last successful run

-- STEP 1: Current Active Records from History
CREATE OR REPLACE TEMP TABLE current_live AS
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY contract_id, component_id ORDER BY effective_from_date DESC) AS rn
  FROM your_dataset.contract_component_history
  WHERE effective_to_date = _infinity_ts
)
WHERE rn = 1;

-- STEP 2: Prepare Feed with Hash Key
CREATE OR REPLACE TEMP TABLE delta_feed AS
SELECT
  f.contract_id,
  f.component_id,
  f.version,
  f.effective_from_date,
  f.effective_to_date,
  CAST(FARM_FINGERPRINT(CONCAT(CAST(f.contract_id AS STRING), '|', CAST(f.component_id AS STRING), '|', f.version)) AS INT64) AS hash_key
FROM your_dataset.component AS f
WHERE f.effective_from_date >= _cutoff_ts
   OR (f.effective_to_date <> _infinity_ts AND f.effective_to_date >= _cutoff_ts);

-- STEP 3: Classify Actions
CREATE OR REPLACE TEMP TABLE staging AS
SELECT
  f.*,
  cl.hash_key AS existing_hash,
  CASE
    WHEN cl.hash_key IS NULL THEN 'INSERT'
    WHEN cl.hash_key <> f.hash_key THEN 'UPDATE'
    ELSE 'UNCHANGED'
  END AS action
FROM delta_feed f
LEFT JOIN current_live cl
  ON f.contract_id = cl.contract_id AND f.component_id = cl.component_id;

-- STEP 4: Close Old Versions
UPDATE your_dataset.contract_component_history h
SET effective_to_date = DATETIME_SUB(_cutoff_ts, INTERVAL 1 SECOND)
WHERE EXISTS (
  SELECT 1
  FROM staging s
  WHERE s.action = 'UPDATE'
    AND h.contract_id = s.contract_id
    AND h.component_id = s.component_id
    AND h.effective_to_date = _infinity_ts
);

-- STEP 5: Insert New Records
INSERT INTO your_dataset.contract_component_history (
  contract_id,
  component_id,
  version,
  effective_from_date,
  effective_to_date,
  hash_key
)
SELECT
  contract_id,
  component_id,
  version,
  effective_from_date,
  effective_to_date,
  hash_key
FROM staging
WHERE action IN ('INSERT', 'UPDATE');
