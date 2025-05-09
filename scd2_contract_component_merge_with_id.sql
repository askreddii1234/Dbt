
-- ROBUST SCD2 DELTA LOAD USING MERGE + LOGGING + ID MAPPING

DECLARE _infinity_ts DATETIME DEFAULT '9999-12-31 23:59:59';
DECLARE _cutoff_ts DATETIME DEFAULT '2025-01-05 00:00:00'; -- simulated last run

BEGIN
  -- STEP 1: Join contract and component as source feed
  CREATE OR REPLACE TEMP TABLE raw_feed AS
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

  -- STEP 2: Extract active current records from history
  CREATE OR REPLACE TEMP TABLE current_live AS
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY contract_id, component_id ORDER BY effective_from_date DESC) AS rn
    FROM your_dataset.contract_component_history
    WHERE effective_to_date = _infinity_ts
  )
  WHERE rn = 1;

  -- STEP 3: Generate ID map for existing and new records
  CREATE OR REPLACE TEMP TABLE id_map AS
  WITH existing_ids AS (
    SELECT DISTINCT contract_id, component_id, scd_id
    FROM your_dataset.contract_component_history
  ),
  max_id AS (
    SELECT COALESCE(MAX(scd_id), 0) AS max_id FROM your_dataset.contract_component_history
  ),
  new_keys AS (
    SELECT rf.contract_id, rf.component_id
    FROM raw_feed rf
    LEFT JOIN existing_ids ei
      ON rf.contract_id = ei.contract_id AND rf.component_id = ei.component_id
    WHERE ei.scd_id IS NULL
  ),
  new_id_assignments AS (
    SELECT
      contract_id,
      component_id,
      ROW_NUMBER() OVER (ORDER BY contract_id, component_id)
        + (SELECT max_id FROM max_id) AS scd_id
    FROM new_keys
  )
  SELECT * FROM existing_ids
  UNION ALL
  SELECT * FROM new_id_assignments;

  -- STEP 4: Build staging with action + ID mapping
  CREATE OR REPLACE TEMP TABLE staging AS
  SELECT
    rf.*,
    idm.scd_id,
    cl.hash_key AS existing_hash,
    CASE
      WHEN cl.hash_key IS NULL THEN 'INSERT'
      WHEN cl.hash_key <> rf.hash_key THEN 'UPDATE'
      ELSE 'UNCHANGED'
    END AS action
  FROM raw_feed rf
  JOIN id_map idm
    ON rf.contract_id = idm.contract_id AND rf.component_id = idm.component_id
  LEFT JOIN current_live cl
    ON rf.contract_id = cl.contract_id AND rf.component_id = cl.component_id;

  -- STEP 5: MERGE logic
  MERGE your_dataset.contract_component_history AS target
  USING (
    SELECT * FROM staging WHERE action IN ('INSERT', 'UPDATE')
  ) AS source
  ON target.scd_id = source.scd_id AND target.effective_to_date = _infinity_ts

  WHEN MATCHED AND source.action = 'UPDATE' THEN
    UPDATE SET effective_to_date = DATETIME_SUB(source.effective_from_date, INTERVAL 1 SECOND)

  WHEN NOT MATCHED BY TARGET THEN
    INSERT (
      scd_id,
      contract_id,
      contract_name,
      component_id,
      version,
      effective_from_date,
      effective_to_date,
      hash_key
    )
    VALUES (
      source.scd_id,
      source.contract_id,
      source.contract_name,
      source.component_id,
      source.version,
      source.effective_from_date,
      source.effective_to_date,
      source.hash_key
    );

  -- STEP 6: Logging
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
    'MERGE_SCD2_WITH_ID',
    _cutoff_ts,
    COUNT(*),
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
    'contract_component_scd2_merge_with_id',
    @@error.message
  );
END;
