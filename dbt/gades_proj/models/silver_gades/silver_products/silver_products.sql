{% if execute and is_incremental() %}
    {% set query = 'select COALESCE(max(audit_updated_at), CAST(\'2000-01-01 00:00:00\' AS TIMESTAMP)) from {}'.format(this) %}
    {% set result = run_query(query).columns[0].values()[0] %}
{% endif %}

with source as (
        select * from {{ source('gades_source', 'bronze_products') }} source_table
        {% if is_incremental() %}
        WHERE
          source_table.updated_at > '{{ result }}'
        {% endif %}
  ),
  
dedup as (
      select *,
             row_number() over (
                 partition by ProductID
                 order by updated_at desc
             ) as _rn
      from source
    )

  
select
  ProductID as code_product,
  ProductName as dsc_product_name,
  --SupplierID as code_supplier,
  CategoryID as code_category,
  Unit as dsc_unit,
  Price as nmb_price,
  CURRENT_TIMESTAMP AS audit_created_at,
  CURRENT_TIMESTAMP AS audit_updated_at
from dedup
where _rn = 1
    