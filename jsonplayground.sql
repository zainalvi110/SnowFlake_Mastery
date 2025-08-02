create database json_db;


create or replace storage integration json_intergation
type= external_stage
storage_provider='s3'
storage_aws_role_arn= 'arn:aws:iam::724068806895:role/jsonrole'
enabled = true
storage_allowed_locations=('s3://jsonfileuploading/');

desc storage integration json_intergation;
--- external stage
CREATE OR REPLACE STAGE json_stage
URL = 's3://jsonfileuploading/'
STORAGE_INTEGRATION = json_intergation
FILE_FORMAT = my_json_format;

ls @json_stage

CREATE OR REPLACE FILE FORMAT my_json_format
TYPE = 'JSON';





CREATE OR REPLACE TABLE json_data00 (
    insertion_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_json VARIANT
) ENABLE_SCHEMA_EVOLUTION = TRUE;

CREATE OR REPLACE PIPE json_pipe
AUTO_INGEST = TRUE
AS
COPY INTO json_data00 (raw_json, insertion_timestamp)
FROM (
    SELECT 
        $1,                    -- JSON data
        CURRENT_TIMESTAMP()    -- Insert timestamp
    FROM @json_stage
)
FILE_FORMAT = my_json_format
ON_ERROR = 'skip_file';


 
 

COPY INTO json_data00
FROM @json_stage
FILE_FORMAT = my_json_format

ON_ERROR = 'continue';

 


-- Manually refresh if needed
ALTER PIPE json_pipe REFRESH;
SELECT * FROM json_data00;


DESC PIPE json_pipe;
select * from json_data


ls @json_stage
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE();


SELECT $1 FROM @json_stage LIMIT 1;

DROP PIPE json_pipe;
CREATE OR REPLACE PIPE json_pipe
AUTO_INGEST = TRUE
AS
COPY INTO json_data00
FROM @json_stage
FILE_FORMAT = my_json_format
ON_ERROR = 'skip_file'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


alter pipe json_pipe refresh;

CREATE TABLE properdata AS
SELECT 
  insertion_timestamp,
  raw_json:balls_faced::NUMBER AS balls_faced,
  raw_json:date::DATE AS match_date,
  raw_json:fours::NUMBER AS fours,
  raw_json:id::STRING AS player_id,
  raw_json:name::STRING AS player_name,
  raw_json:opponent::STRING AS opponent,
  raw_json:runs::NUMBER AS runs,
  raw_json:sixes::NUMBER AS sixes,
  raw_json:team::STRING AS team,
  raw_json:venue::STRING AS venue
FROM json_data00;

select * from properdata
select * from json_data00
SHOW PIPES LIKE 'json_pipe';