with source as (
    select *
    from {{ ref('silver_shippers') }} as source_table
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
               partition by code_shipper
               order by audit_updated_at desc
           ) as _rn
    from source
)


select
    '-1' as sk_shipper,
    -1 as code_shipper,
    'N/A' as dsc_shipper_name,
    CURRENT_TIMESTAMP as audit_created_at,
    CURRENT_TIMESTAMP as audit_updated_at

union all

select
    TO_BASE64(SHA256(CAST(source_table.code_shipper AS STRING))) as sk_shipper,
    source_table.code_shipper,
    source_table.dsc_shipper_name,
    CURRENT_TIMESTAMP as audit_created_at,
    CURRENT_TIMESTAMP as audit_updated_at
from dedup as source_table
where source_table._rn = 1
