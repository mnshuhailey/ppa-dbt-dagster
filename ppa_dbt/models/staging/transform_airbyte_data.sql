with source_data as (
    select
        (_airbyte_data::json->>'vwlzs_asnafId')::text as AsnafID,
        (_airbyte_data::json->>'vwlzs_AsnafRegistrationIdName')::text as AsnafName,
        (_airbyte_data::json->>'vwlzs_Email')::text as Emel,
        (_airbyte_data::json->>'vwlzs_Age')::int as Age
    from {{ source('airbyte_internal', 'dbo_raw__stream_vwlzs_asnaf') }}
)

select * 
into dbo.asnaf_transformed
from source_data
