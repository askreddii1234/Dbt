
-- ROBUST SCD2 DELTA LOAD USING MERGE + LOGGING + EXCEPTION HANDLING

DECLARE _infinity_ts DATETIME DEFAULT '9999-12-31 23:59:59';
DECLARE _cutoff_ts DATETIME DEFAULT '2025-01-05 00:00:00'; -- example simulated cutoff

BEGIN
  -- STEP 1: Build Delta Feed from Joined Contract + Component
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

  -- STEP 2: Get Active History
  CREATE OR REPLACE TEMP TABLE current_live AS
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY contract_id, component_id ORDER BY effective_from_date DESC) AS rn
    FROM your_dataset.contract_component_history
    WHERE effective_to_date = _infinity_ts
  )
  WHERE rn = 1;

  -- STEP 3: Compare and Classify Actions
  CREATE OR REPLACE TEMP TABLE staging AS
  SELECT
    df.*,
    cl.hash_key AS existing_hash,
    CASE
      WHEN cl.hash_key IS NULL THEN 'INSERT'
      WHEN cl.hash_key <> df.hash_key THEN 'UPDATE'
      ELSE 'UNCHANGED'
    END AS action
  FROM delta_feed df
  LEFT JOIN current_live cl
    ON df.contract_id = cl.contract_id AND df.component_id = cl.component_id;

  -- STEP 4: MERGE (handles UPDATE to close active + INSERT for new/changed)
  MERGE your_dataset.contract_component_history AS target
  USING (
    SELECT * FROM staging WHERE action IN ('INSERT', 'UPDATE')
  ) AS source
  ON target.contract_id = source.contract_id
     AND target.component_id = source.component_id
     AND target.effective_to_date = _infinity_ts

  WHEN MATCHED AND source.action = 'UPDATE' THEN
    UPDATE SET effective_to_date = DATETIME_SUB(source.effective_from_date, INTERVAL 1 SECOND)

  WHEN NOT MATCHED BY TARGET THEN
    INSERT (
      contract_id,
      contract_name,
      component_id,
      version,
      effective_from_date,
      effective_to_date,
      hash_key
    )
    VALUES (
      source.contract_id,
      source.contract_name,
      source.component_id,
      source.version,
      source.effective_from_date,
      source.effective_to_date,
      source.hash_key
    );

  -- STEP 5: Log Summary
  INSERT INTO your_dataset.scd2_process_log (
    process_type,
    cutoff_ts,
    record_count,
    inserted,
    updated,
    unchanged,
    processed_at
  )
  SELECT
    'MERGE_SCD2',
    _cutoff_ts,
    COUNT(*) AS record_count,
    SUM(CASE WHEN action = 'INSERT' THEN 1 ELSE 0 END),
    SUM(CASE WHEN action = 'UPDATE' THEN 1 ELSE 0 END),
    SUM(CASE WHEN action = 'UNCHANGED' THEN 1 ELSE 0 END),
    CURRENT_DATETIME()
  FROM staging;

EXCEPTION WHEN ERROR THEN
  INSERT INTO your_dataset.scd2_error_log (
    error_time,
    process,
    error_message
  )
  VALUES (
    CURRENT_DATETIME(),
    'contract_component_scd2_merge',
    @@error.message
  );
END;
