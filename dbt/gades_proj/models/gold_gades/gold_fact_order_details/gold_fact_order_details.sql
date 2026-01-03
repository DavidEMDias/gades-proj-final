{% if execute and is_incremental() %}
    {% set query = 'select COALESCE(max(audit_updated_at), CAST(\'2000-01-01 00:00:00\' AS TIMESTAMP)) from {}'.format(this) %}
    {% set result = run_query(query).columns[0].values()[0] %}
{% endif %}

WITH source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY source_table.code_order, source_table.code_product
            ORDER BY source_table.audit_updated_at DESC
        ) AS _r
    FROM {{ ref('silver_orders') }} source_table
    {% if is_incremental() %}
    WHERE source_table.audit_updated_at > '{{ result }}'
    {% endif %}
),

dedup AS (
    SELECT *
    FROM source
    WHERE _r = 1
),

metrics_per_order AS (
    SELECT
        code_order,
        SUM(mtr_quantity * mrt_product_price) AS mtr_order_total_value,
        COUNT(code_product) AS mtr_items_per_order
    FROM dedup
    GROUP BY code_order
)

SELECT
    d.code_order AS dd_code_order,
    

    TO_BASE64(SHA256(COALESCE(CAST(d.code_product as STRING), '-1'))) AS sk_product,
    TO_BASE64(SHA256(COALESCE(CAST(d.code_customer as STRING), '-1'))) AS sk_customer,
    TO_BASE64(SHA256(COALESCE(CAST(d.code_shipper as STRING), '-1'))) AS sk_shipper,
    TO_BASE64(SHA256(COALESCE(CAST(d.dsc_order_date as STRING), '-1'))) AS sk_date_order,

    -- Métricas por item
    d.mtr_quantity,
    d.mrt_product_price,
    (d.mtr_quantity * d.mrt_product_price) AS mtr_prod_total_amount,

    -- !!! Métricas por order !!! Podem ser criada uma fact_orders com estas métricas
    o.mtr_order_total_value,
    o.mtr_items_per_order,

    CURRENT_TIMESTAMP AS audit_created_at,
    CURRENT_TIMESTAMP AS audit_updated_at

FROM dedup d
LEFT JOIN metrics_per_order o
    ON d.code_order = o.code_order
