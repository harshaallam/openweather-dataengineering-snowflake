# openweather-dataengineering-snowflake

This project leverages Snowflake as the data warehouse solution for the loading and processing stages, while maintaining the core architecture from the previous project [`openweather-data-etl-python-aws`](https://github.com/harshaallam/openweather-data-etl-python-aws). 

![Architecture](OpenWeather-Snowflake-architecture.png)

## Project Overview
This project is designed to get weather report of cities from OpenWeather API. Implemented Snowflake to load and manage data transformations, establishing a seamless flow from data ingestion in AWS S3 through to processing and storage in Snowflake.  

## Key Enhancements
### 1. **Optimized File Processing**
   - **Efficient File Selection**: The code now processes only the most recent file in the data pipeline, reducing duplication and maintaining a focus on the latest top 50 songs. Before each execution, any files in the `to_process/` directory are moved to `to_processed/`.


### 2. **Updated Data Loading with Snowflake Integration**
   - **Snowflake Table Creation**: Relevant tables were created within Snowflake to store the data for city and weather.
   - **S3-Snowflake Integration**: Established a Storage Integration between AWS S3 and Snowflake to streamline data movement and security.
   - **Stage and Snowpipe Setup**: Configured file format, stage, and Snowpipe within Snowflake. A notification event was added in the S3 bucket properties to trigger Snowpipe whenever a new file is uploaded, automatically loading data into Snowflake tables.

### 3. **Automated Data Truncation**
   - **Stored Procedures and Task Scheduling**: Created a stored procedure to automatically truncate the `city_data`, and `weather_data` tables before new data ingestion. The stored procedure is invoked by a Snowflake task, which is scheduled to execute a few minutes before the `openweather_data_extract` Lambda function runs.
   - **Scheduling Tips**: Ensure the Snowflake's task to be scheduled before triggering CloudWatch event's schedule for `openweather_data_extract` lambda function to maintain data consistency and avoid duplicate entries.

Refer to the following files for more details on the code:
- [`OpenWeather_Snwflk_Transform@lambda.py`](./OpenWeather_Snwflk_Transform@lambda.py) for recent file processing code.
- [`OpenWeather_Snwflk_db.sql`](./OpenWeather_Snwflk_db.sql) for Snowflake table and task setup code.
