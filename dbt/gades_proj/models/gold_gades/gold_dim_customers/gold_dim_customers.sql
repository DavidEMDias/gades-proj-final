with source as (
    select *
    from {{ ref('silver_customers') }} as source_table
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
               partition by code_customer
               order by audit_updated_at desc
           ) as _rn
    from source
)

SELECT
    '-1' AS sk_customer,
    -1 AS code_customer,
    'N/A' AS dsc_customer_name,
    'N/A' AS dsc_customer_contact_name,
    'N/A' AS dsc_customer_address,
    'N/A' AS dsc_customer_city,
    '-1' AS dsc_customer_zip_code_prefix,
    'N/A' AS dsc_customer_country,
    CURRENT_TIMESTAMP AS audit_created_at,
    CURRENT_TIMESTAMP AS audit_updated_at
    

UNION ALL

SELECT
    TO_BASE64(SHA256(CAST(source_table.code_customer AS STRING))) AS sk_customer,
    source_table.code_customer,
    source_table.dsc_customer_name,
    source_table.dsc_customer_contact_name,
    source_table.dsc_customer_address,
    source_table.dsc_customer_city,
    source_table.dsc_customer_zip_code_prefix,
    source_table.dsc_customer_country,
    CURRENT_TIMESTAMP AS audit_created_at,
    CURRENT_TIMESTAMP AS audit_updated_at
FROM dedup AS source_table
WHERE
    source_table._rn = 1