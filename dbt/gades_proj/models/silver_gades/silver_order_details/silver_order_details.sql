{% if execute and is_incremental() %}
    {% set query = 'select COALESCE(max(audit_updated_at), CAST(\'2000-01-01 00:00:00\' AS TIMESTAMP)) from {}'.format(this) %}
    {% set result = run_query(query).columns[0].values()[0] %}
{% endif %}

with source as (
        select * from {{ source('gades_source', 'bronze_order_details') }} source_table
        {% if is_incremental() %}
        WHERE
          source_table.updated_at > '{{ result }}'
        {% endif %}
  ),

dedup as (
    select *,
            row_number() over (
                partition by OrderDetailID
                order by updated_at desc
            ) as _rn
    from source
    )

  
select
    OrderDetailID as code_order_detail,
    OrderID as code_order,
    ProductID as code_product,
    Quantity as mtr_quantity,
    p.nmb_price as mtr_product_price,
    CURRENT_TIMESTAMP AS audit_created_at,
    CURRENT_TIMESTAMP AS audit_updated_at
from dedup
left join {{ ref('silver_products') }} as p
on dedup.ProductID = p.code_product
where _rn = 1
  
    