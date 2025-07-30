select current_region();
create database new_db;
use database new_db;


CREATE or replace STORAGE INTEGRATION s3new_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::724068806895:role/demosf_role'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://demodata00/matches/');

  describe STORAGE INTEGRATION s3new_integration;

  --- create external stage
CREATE OR REPLACE FILE FORMAT my_new_csv_format_aws
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  DATE_FORMAT = 'YYYY/MM/DD'
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;  -- 💡 This line fixes the issue



  create or replace stage my_s3_stage
  storage_integration = s3new_integration
  url='s3://demodata00/matches/'
  file_format = my_new_csv_format_aws;


CREATE OR REPLACE TABLE cricket (
  id INT,
  season STRING,
  city STRING,
  date DATE,
  team1 STRING,
  team2 STRING,
  toss_winner STRING,
  toss_decision STRING,
  result STRING,
  dl_applied INT,
  winner STRING,
  win_by_runs INT,
  win_by_wickets INT,
  player_of_match STRING,
  venue STRING,
  umpire1 STRING,
  umpire2 STRING,
  umpire3 STRING
);



  select * from cricket;
  
-- Step 6: Create pipe for auto-ingest from S3
CREATE OR REPLACE PIPE demo_match_pipe
  AUTO_INGEST = TRUE
AS
COPY INTO cricket
FROM @my_s3_stage
FILE_FORMAT = (FORMAT_NAME = 'my_new_csv_format_aws')
ON_ERROR = 'CONTINUE'
;


ALTER PIPE demo_match_pipe REFRESH;
select * from cricket;
COPY INTO cricket
FROM @my_s3_stage
FILE_FORMAT = (FORMAT_NAME = 'my_new_csv_format_aws')
ON_ERROR = 'CONTINUE';



ALTER TABLE cricket ADD COLUMN insertion_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP;


select system$pipe_status('demo_match_pipe');
desc pipe demo_match_pipe