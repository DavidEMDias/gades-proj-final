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
    FROM {{ ref('silver_order_details') }} source_table
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
        SUM(mtr_quantity * mtr_product_price) AS mtr_order_total_value,
        COUNT(code_product) AS mtr_items_per_order
    FROM dedup
    GROUP BY code_order
)

SELECT
    d.code_order AS dd_code_order,
    

    TO_BASE64(SHA256(COALESCE(CAST(d.code_product as STRING), '-1'))) AS sk_product,
    TO_BASE64(SHA256(COALESCE(CAST(o.code_customer as STRING), '-1'))) AS sk_customer,
    TO_BASE64(SHA256(COALESCE(CAST(o.code_shipper as STRING), '-1'))) AS sk_shipper,
    TO_BASE64(SHA256(COALESCE(CAST(o.dsc_order_date as STRING), '-1'))) AS sk_date_order,

    -- Métricas por item
    d.mtr_quantity,
    d.mtr_product_price,
    (d.mtr_quantity * d.mtr_product_price) AS mtr_prod_total_amount,

    -- !!! Métricas por order !!! Pode ser criada uma fact_orders com estas métricas ao nivel do pedido
    om.mtr_order_total_value, -- semi-aditiva (pode ser agregada a algumas dimensoes, nao pode ser somada em outras e.g. produto, item)
    om.mtr_order_total_value / om.mtr_items_per_order as mtr_order_total_value_allocated, -- aditiva (analises por produto funcionam) 
    om.mtr_items_per_order,

    CURRENT_TIMESTAMP AS audit_created_at,
    CURRENT_TIMESTAMP AS audit_updated_at

FROM dedup d
LEFT JOIN {{ ref('silver_orders') }} AS o
    ON d.code_order = o.code_order
LEFT JOIN metrics_per_order om
    ON d.code_order = om.code_order
