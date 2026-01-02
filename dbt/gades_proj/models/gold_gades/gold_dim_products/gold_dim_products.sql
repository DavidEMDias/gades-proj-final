with source as (
    select *
    from {{ ref('silver_products') }} as source_table
    {% if is_incremental() %}
    where source_table.audit_updated_at > (
        select coalesce(max(audit_updated_at), '2000-01-01 00:00:00') 
        from {{ this }}
    )
    {% endif %}
),

dedup as (
    select *,
           row_number() over (
               partition by code_product
               order by audit_updated_at desc
           ) as _rn
    from source
)


select
    '-1' as sk_product,
    -1 as code_product,
    'N/A' as dsc_product_name,
    'N/A' as dsc_category_name,
    'N/A' as dsc_unit,
    CURRENT_TIMESTAMP as audit_created_at,
    CURRENT_TIMESTAMP as audit_updated_at

union all


select
    TO_BASE64(SHA256(CAST(source_table.code_product AS STRING))) as sk_product,
    source_table.code_product,
    source_table.dsc_product_name,
    category.dsc_category_name,
    source_table.dsc_unit,
    CURRENT_TIMESTAMP as audit_created_at,
    CURRENT_TIMESTAMP as audit_updated_at
from dedup as source_table
left join {{ref('silver_categories')}} as category
    on source_table.code_category = category.code_category
where source_table._rn = 1
