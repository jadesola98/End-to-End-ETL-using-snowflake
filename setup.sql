-- =====================================================================
-- setup.sql
-- Run this once, before the three pipeline scripts.
-- Replace every value in <angle brackets> with your own.
-- =====================================================================

-- Storage integrations need ACCOUNTADMIN (or the CREATE INTEGRATION privilege)
use role accountadmin;

-- ---------------------------------------------------------------------
-- 1. Compute, database and schemas
-- ---------------------------------------------------------------------
create warehouse if not exists ayo_warehouse
  warehouse_size = 'xsmall'
  auto_suspend   = 60
  auto_resume    = true;

create database if not exists demo;
use database demo;

create schema if not exists stg;          -- landing tables, streams and pipes
create schema if not exists raw;          -- deduplicated, upserted source data
create schema if not exists transformed;  -- dimension and fact tables, all tasks

-- ---------------------------------------------------------------------
-- 2. File format for the incoming CSV files
-- ---------------------------------------------------------------------
create or replace file format stg.csv
  type = 'csv'
  compression = 'auto'
  field_delimiter = ','
  record_delimiter = '\n'
  skip_header = 1
  field_optionally_enclosed_by = '\042'
  null_if = ('\\N');

-- ---------------------------------------------------------------------
-- 3. Storage integration: lets Snowflake read the S3 bucket through an
--    AWS IAM role, so no access keys are stored in Snowflake or in code.
--    Guide: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
-- ---------------------------------------------------------------------
create or replace storage integration s3_int
  type = external_stage
  storage_provider = 's3'
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::<your-aws-account-id>:role/<your-snowflake-access-role>'
  storage_allowed_locations = ('s3://<your-bucket-name>/');

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from this output
-- into the trust policy of the IAM role above.
desc integration s3_int;

-- ---------------------------------------------------------------------
-- 4. External stage pointing at the bucket.
--    Files are expected under:
--      s3://<your-bucket-name>/landing/customer/
--      s3://<your-bucket-name>/landing/item/
--      s3://<your-bucket-name>/landing/order/
-- ---------------------------------------------------------------------
create or replace stage stg.landing
  storage_integration = s3_int
  url = 's3://<your-bucket-name>/'
  file_format = stg.csv;

-- ---------------------------------------------------------------------
-- 5. After running the three pipeline scripts:
--    run SHOW PIPES and copy the notification_channel value (an SQS ARN).
--    In the S3 bucket, create an event notification for "All object create
--    events" on the prefix landing/ and send it to that SQS queue.
--    This is what makes the pipes load files automatically (auto_ingest).
-- ---------------------------------------------------------------------
-- show pipes in schema stg;
