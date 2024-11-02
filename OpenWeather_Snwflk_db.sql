CREATE DATABASE WEATHER_API;
CREATE SCHEMA OPEN_WEATHER;


CREATE OR REPLACE TABLE WEATHER_API.OPEN_WEATHER.CITY_DATA(
id INTEGER,
city VARCHAR(50),
country VARCHAR(50),
lattitude FLOAT,
longitude FLOAT,
timezone_utc VARCHAR(255)
);


SELECT * FROM CITY_DATA;



CREATE OR REPLACE FILE FORMAT openweather_file
TYPE=CSV
FIELD_DELIMITER=','
SKIP_HEADER=1
FIELD_OPTIONALLY_ENCLOSED_BY='"'
NULL_IF=('NULL','null')
EMPTY_FIELD_AS_NULL=TRUE;


--Create a IAM user to integrate s3 with snowflake, create with external id  as 00000.
-- Once IAM role is created provide it's ARN in 'STORAGE_AWS_ROLE_ARN'

CREATE OR REPLACE STORAGE INTEGRATION openweather_storageinit
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER=S3
ENABLED=TRUE
STORAGE_AWS_ROLE_ARN='arn:aws:iam::#:role/snowflake-s3-integration-openweatherapi'
STORAGE_ALLOWED_LOCATIONS=('s3://openweather-etl-harsha');

DESC STORAGE INTEGRATION openweather_storageinit;
-- provide 'STORAGE_AWS_EXTERNAL_ID' and STORAGE_AWS_IAM_USER_ARN values that gets from above desc command's output
--in AWS IAM roles Trust relationships section at 'sts:ExternalId' and 'AWS' places respectively

-- this builds the connection but need to create stage to establish the connection to the bucket


CREATE OR REPLACE STAGE openweather_stage
URL='S3://openweather-etl-harsha/transformed/'
STORAGE_INTEGRATION=openweather_storageinit
FILE_FORMAT=openweather_file;

--test whether the data is loading into the table or not
COPY INTO CITY_DATA
FROM @openweather_citystage/city_data;

select $1 from  @openweather_citystage;
select * from city_data;

--create a snowpipe 

CREATE OR REPLACE  PIPE WEATHER_API.OPEN_WEATHER.CITY_PIPE
AUTO_INGEST=TRUE
AS
COPY INTO WEATHER_API.OPEN_WEATHER.CITY_DATA
FROM @WEATHER_API.OPEN_WEATHER.openweather_stage/city_data/;

DESC PIPE CITY_PIPE;
--now in s3bucket's properties create an event notification with SQS Queue and provide above desc commands notitication_channle value in 'Enter SQS Queue ARN'. This builds the pipe and notifies snowflake whenever there is a file in s3 bucket

--Event creation in s3 is created for the entire bucket not just to a folder in a bucket, picking the file and loading to db should be handled when creating and calling the snowpipe, mention the specific folder's path next to the stage to route to the specified file's location.
--multiple snowpipes can have the same notification_channel, so this creates error while creating a second notification event in s3 by using notification_channel(snowpipe) at 'Enter SQS Queue ARN' (notification event in s3 bucket)
-- Here used the same snowpipe and steered the route using the @stage/folder-name while creating snowpipe

--Simillarly do the same for weather data as well


CREATE OR REPLACE TABLE WEATHER_API.OPEN_WEATHER.WEATHER_DATA(
city_id INTEGER,
max_temp_c FLOAT,
min_temp_c FLOAT,
pressure_hpa INTEGER,
sea_level_hpa INTEGER,
humidity_percent INTEGER,
description STRING,
sunrise_utc STRING,
sunset_utc STRING,
timezone_utc STRING
);


CREATE OR REPLACE PIPE WEATHER_API.OPEN_WEATHER.WEATHER_PIPE
AUTO_INGEST=TRUE
AS
COPY INTO WEATHER_API.OPEN_WEATHER.weather_data
FROM @WEATHER_API.OPEN_WEATHER.openweather_stage/weather_data/;

list@openweather_weatherstage;
select $4 from @openweather_weatherstage;

SELECT * FROM weather_data;

desc pipe weather_pipe;

--create a stored procedure to truncate the tables whenver the function is called
create or replace procedure openweather_stored_prcdr()
returns string not null
language javascript
as
$$
    var code1 ="truncate table weather_api.open_weather.city_data;"
    var code2="truncate table weather_api.open_weather.weather_data;"
    sql1=snowflake.createStatement({sqlText:code1});
    sql2=snowflake.createStatement({sqlText:code2});
    var result1=sql1.execute();
    var result2=sql2.execute();
    return code1+'\n'+code2;
$$;

call openweather_stored_prcdr();

--create a task to automate the code in stored procedure by defining the frequency of code execution,
--Here weather report should be enough to generate once per day, so just before few minutes of data extract(cloudwatch's event trigger to pull data from api) this task should be schduled
create or replace task openweather_task
warehouse='compute_wh'
schedule='2 minute'
as
call openweather_stored_prcdr();

show tasks;

--alter the task to 'resume' to get started with the task, ideally it is in 'suspended' state
alter task openweather_task suspend;
select * from city_data;