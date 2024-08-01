insert into {{ target('dbo', 'asnaf_transformed') }} (AsnafID, AsnafName, Emel, Age)
select
  AsnafID,
  AsnafName,
  Emel,
  Age
from
    {{ ref('asnaf_transformed') }}

