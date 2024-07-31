{{ config(
    materialized='ephemeral'
) }}

with source_data as (
  SELECT * FROM {{ source('source_table_asnaf', 'asnaf_transformed_raw__stream_vwlzs_asnaf') }}
)

flattened_json_data AS (
  SELECT
    JSON_VALUE(_airbyte_data, '$.vwlzs_asnafId') AS AsnafID,
    JSON_VALUE(_airbyte_data, '$.vwlzs_AsnafRegistrationIdName') AS AsnafName,
    JSON_VALUE(_airbyte_data, '$.vwlzs_Email') AS Emel,
    JSON_VALUE(_airbyte_data, '$.vwlzs_Age') AS Age
  FROM source_data
)

SELECT * FROM flattened_json_data
