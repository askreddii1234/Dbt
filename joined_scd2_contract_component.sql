
-- SCD TYPE 2 DELTA LOAD USING JOINED CONTRACT + COMPONENT TABLES

DECLARE _infinity_ts DATETIME DEFAULT '9999-12-31 23:59:59';
DECLARE _cutoff_ts DATETIME DEFAULT '2025-01-05 00:00:00'; -- simulate last run

-- STEP 1: Join contract + component to create source delta feed
CREATE OR REPLACE TEMP TABLE delta_feed AS
SELECT
  c.contract_id,
  c.contract_name,
  cmp.component_id,
  cmp.version,
  cmp.effective_from_date,
  cmp.effective_to_date,
  CAST(FARM_FINGERPRINT(CONCAT(CAST(c.contract_id AS STRING), '|', CAST(cmp.component_id AS STRING), '|', cmp.version, '|', c.contract_name)) AS INT64) AS hash_key
FROM your_dataset.contract AS c
JOIN your_dataset.component AS cmp
  ON c.contract_id = cmp.contract_id
WHERE cmp.effective_from_date >= _cutoff_ts
   OR (cmp.effective_to_date <> _infinity_ts AND cmp.effective_to_date >= _cutoff_ts);

-- STEP 2: Extract current active history rows
CREATE OR REPLACE TEMP TABLE current_live AS
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY contract_id, component_id ORDER BY effective_from_date DESC) AS rn
  FROM your_dataset.contract_component_history
  WHERE effective_to_date = _infinity_ts
)
WHERE rn = 1;

-- STEP 3: Classify action (INSERT / UPDATE / UNCHANGED)
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

-- STEP 4: Expire previous records if needed
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

-- STEP 5: Insert new or updated records
INSERT INTO your_dataset.contract_component_history (
  contract_id,
  contract_name,
  component_id,
  version,
  effective_from_date,
  effective_to_date,
  hash_key
)
SELECT
  contract_id,
  contract_name,
  component_id,
  version,
  effective_from_date,
  effective_to_date,
  hash_key
FROM staging
WHERE action IN ('INSERT', 'UPDATE');
