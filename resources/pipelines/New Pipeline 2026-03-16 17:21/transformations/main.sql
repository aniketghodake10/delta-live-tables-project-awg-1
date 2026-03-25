-- ================================================
-- STREAMING TABLES (Auto Loader from cloud files)
-- ================================================

-- streaming table: read_json_tbl2
CREATE OR REFRESH STREAMING TABLE read_json_tbl2_sql
AS SELECT * FROM STREAM read_files(
  '/Volumes/delta_live_tables_catalog/source/dlt_volume/dlt16MAR/raw/',
  format => 'json'
);

-- streaming table: read_json_tbl3
CREATE OR REFRESH STREAMING TABLE read_json_tbl3_sql
AS SELECT * FROM STREAM read_files(
  '/Volumes/delta_live_tables_catalog/source/dlt_volume/dlt16MAR/raw/',
  format => 'json'
);


-- ================================================
-- TEMPORARY STREAMING VIEW
-- ================================================

-- temporary streaming view: silver_streaming_temp_view2
CREATE OR REFRESH TEMPORARY STREAMING LIVE VIEW silver_streaming_temp_view2_sql
AS SELECT * FROM STREAM(read_json_tbl2_sql);
-- Note: STREAM keyword makes it a streaming source


-- ================================================
-- TEMPORARY BATCH VIEW
-- ================================================

-- temporary batch view: silver_batch_temp_view2
CREATE OR REFRESH TEMPORARY LIVE VIEW silver_batch_temp_view2_sql
AS SELECT * FROM read_json_tbl2_sql;


-- ================================================
-- MATERIALIZED VIEW (normal/batch read)
-- ================================================

-- materialized view: silver_normal_tbl_or_materialized_view2
CREATE OR REFRESH MATERIALIZED VIEW silver_normal_tbl_or_materialized_view2_sql
AS SELECT * FROM read_json_tbl2_sql;
-- Note: No STREAM keyword = batch/snapshot read


-- ================================================
-- EMPTY STREAMING TABLES (targets for CDC/Append flows)
-- ================================================

-- empty streaming table: silver_scd1_tbl2
CREATE OR REFRESH STREAMING TABLE silver_scd1_tbl2_sql
COMMENT 'Target for streaming data';

-- empty streaming table: silver_scd2_tbl3
CREATE OR REFRESH STREAMING TABLE silver_scd2_tbl3_sql
COMMENT 'Target for streaming data';


-- ================================================
-- CDC FLOWS (APPLY CHANGES INTO)
-- ================================================

-- scd type 1: apply_changes into silver_scd1_tbl2
APPLY CHANGES INTO silver_scd1_tbl2_sql
FROM STREAM(read_json_tbl2_sql)
KEYS (customer_id)
SEQUENCE BY order_id
STORED AS SCD TYPE 1;
-- Note: ignore_null_updates defaults to FALSE (matches your config)
-- apply_as_deletes, apply_as_truncates, except_column_list omitted = NULL/default


-- scd type 2: apply_changes into silver_scd2_tbl3
APPLY CHANGES INTO silver_scd2_tbl3_sql
FROM STREAM(read_json_tbl2_sql)
KEYS (customer_id)
SEQUENCE BY order_id
STORED AS SCD TYPE 2;


-- ================================================
-- GOLD LAYER TABLES
-- ================================================

-- materialized view (batch): gold_normal_tbl2
CREATE OR REFRESH MATERIALIZED VIEW gold_normal_tbl2_sql
AS SELECT * FROM silver_normal_tbl_or_materialized_view2_sql;

-- streaming table: gold_streaming_tbl2
CREATE OR REFRESH STREAMING TABLE gold_streaming_tbl2_sql
AS SELECT * FROM STREAM(silver_streaming_temp_view2_sql);


-- ================================================
-- APPEND FLOW - EMPTY TARGET TABLE + MULTIPLE SOURCES
-- ================================================

-- empty streaming table: append_flow_stremaing_tbl2
CREATE OR REFRESH STREAMING TABLE append_flow_stremaing_tbl2_sql
COMMENT 'Target for streaming data';

-- append flow 1
CREATE FLOW ingest_data_from_read_json_tbl2_sql
AS INSERT INTO append_flow_stremaing_tbl2_sql BY NAME
SELECT * FROM STREAM(read_json_tbl2_sql);

-- append flow 2
CREATE FLOW ingest_data_from_read_json_tbl3_sql
AS INSERT INTO append_flow_stremaing_tbl2_sql BY NAME
SELECT * FROM STREAM(read_json_tbl3_sql);