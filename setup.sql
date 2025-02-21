-- Run the following statements to create a database, schema, and a table with data loaded from AWS S3.

CREATE DATABASE IF NOT EXISTS DASH_DB;
CREATE SCHEMA IF NOT EXISTS DASH_SCHEMA;
CREATE WAREHOUSE IF NOT EXISTS DASH_S WAREHOUSE_SIZE=SMALL;

USE DASH_DB.DASH_SCHEMA;
USE WAREHOUSE DASH_S;
  
create or replace file format csvformat  
  skip_header = 1  
  field_optionally_enclosed_by = '"'  
  type = 'CSV';  
  
create or replace stage support_tickets_data_stage  
  file_format = csvformat  
  url = 's3://sfquickstarts/sfguide_integrate_snowflake_cortex_agents_with_slack/';  
  
create or replace table SUPPORT_TICKETS (  
  ticket_id VARCHAR(60),  
  customer_name VARCHAR(60),  
  customer_email VARCHAR(60),  
  service_type VARCHAR(60),  
  request VARCHAR,  
  contact_preference VARCHAR(60)  
);  
  
copy into SUPPORT_TICKETS  
  from @support_tickets_data_stage;

-- Run the following statement to create a Snowflake managed internal stage to store the semantic model specification file.
create or replace stage DASH_SEMANTIC_MODELS encryption = (TYPE = 'SNOWFLAKE_SSE') directory = ( ENABLE = true );

-- Run the following statement to create a Snowflake managed internal stage to store the PDF documents.
 create or replace stage DASH_PDFS encryption = (TYPE = 'SNOWFLAKE_SSE') directory = ( ENABLE = true );