with raw_data as (
    select
        id,
        _airbyte_data::jsonb as json_data
    from
        {{ source('airbyte_internal', 'dbo_raw__stream_vwlzs_asnaf') }}
),
transformed_data as (
    select
        id,
        json_data->>'vwlzs_asnafId' as AsnafID,
        json_data->>'vwlzs_AsnafRegistrationIdName' as AsnafName,
        json_data->>'vwlzs_Email' as Emel
        json_data->>'vwlzs_Age' as Age
    from
        raw_data
)
select * from transformed_data

