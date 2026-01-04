{% if execute and is_incremental() %}
    {% set query = 'select COALESCE(max(audit_updated_at), CAST(\'2000-01-01 00:00:00\' AS TIMESTAMP)) from {}'.format(this) %}
    {% set result = run_query(query).columns[0].values()[0] %}
{% endif %}

with source as (
        select * from {{ source('gades_source', 'bronze_orders') }} as source_table
        {% if is_incremental() %}
        WHERE
          source_table.updated_at > '{{ result }}'
        {% endif %}
  ),

dedup as (
    select *,
            row_number() over (
                partition by OrderID
                order by updated_at desc
            ) as _rn
    from source
),

pre_join_orders as (
select
    OrderID as code_order,
    CustomerID as code_customer,
    OrderDate as dsc_order_date,
    ShipperID as code_shipper,
    created_at as audit_created_at,
    updated_at as audit_updated_at
from dedup
where _rn = 1
)

--Cada linha = um pedido
select 
    o.code_order,
    --od.code_product,
    o.code_customer,
    o.code_shipper,
    o.dsc_order_date,
    --od.mtr_quantity,
    --p.nmb_price as mrt_product_price,
    CURRENT_TIMESTAMP AS audit_created_at,
    CURRENT_TIMESTAMP AS audit_updated_at
from pre_join_orders o