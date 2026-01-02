{% if execute and is_incremental() %}
    {% set query = 'select COALESCE(max(audit_updated_at), CAST(\'2000-01-01 00:00:00\' AS TIMESTAMP)) from {}'.format(this) %}
    {% set result = run_query(query).columns[0].values()[0] %}
{% endif %}

with source as (
        select * from {{ source('gades_source', 'bronze_shippers') }} source_table
        {% if is_incremental() %}
        WHERE
          source_table.updated_at > '{{ result }}'
        {% endif %}
  ),

dedup as (
      select *,
             row_number() over (
                 partition by ShipperID
                 order by updated_at desc
             ) as _rn
      from source
  )

select
    ShipperID as code_shipper,
    ShipperName as dsc_shipper_name,
    Phone as dsc_shipper_phone,
    CURRENT_TIMESTAMP AS audit_created_at,
    CURRENT_TIMESTAMP AS audit_updated_at
from dedup
where _rn = 1
    