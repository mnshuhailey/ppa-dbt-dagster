insert into {{ target('airbyte_internal', 'asnaf_transformed') }} (AsnafID, AsnafName, Emel, Age)
select
  AsnafID,
  AsnafName,
  Emel,
  Age
from
    {{ ref('asnaf_transformed') }}

