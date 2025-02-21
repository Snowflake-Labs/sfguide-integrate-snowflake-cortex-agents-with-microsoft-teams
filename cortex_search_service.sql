USE DASH_DB.DASH_SCHEMA;
USE WAREHOUSE DASH_S;

create or replace table parse_pdfs as 
select relative_path, SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@DASH_DB.DASH_SCHEMA.DASH_PDFS,relative_path,{'mode':'LAYOUT'}) as data
    from directory(@DASH_DB.DASH_SCHEMA.DASH_PDFS);

create or replace table parsed_pdfs as (
    with tmp_parsed as (select
        relative_path,
        SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(TO_VARIANT(data):content, 'MARKDOWN', 1800, 300) AS chunks
    from parse_pdfs where TO_VARIANT(data):content is not null)
    select
        TO_VARCHAR(c.value) as PAGE_CONTENT,
        REGEXP_REPLACE(relative_path, '\\.pdf$', '') as TITLE,
        'DASH_DB.DASH_SCHEMA.DASH_PDFS' as INPUT_STAGE,
        RELATIVE_PATH as RELATIVE_PATH
    from tmp_parsed p, lateral FLATTEN(INPUT => p.chunks) c
);

create or replace CORTEX SEARCH SERVICE DASH_DB.DASH_SCHEMA.VEHICLES_INFO
ON PAGE_CONTENT
WAREHOUSE = DASH_S
TARGET_LAG = '1 hour'
AS (
    SELECT '' AS PAGE_URL, PAGE_CONTENT, TITLE, RELATIVE_PATH
    FROM parsed_pdfs
);