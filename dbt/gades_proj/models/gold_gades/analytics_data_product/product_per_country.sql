-- No need for properties file, created in dbt_dev schema by default.
-- Will be materialized as view.
SELECT 
    dp.dsc_product_name,
    fod.mtr_product_price,
    dc.dsc_customer_country
FROM {{ ref('gold_fact_order_details') }} fod
LEFT JOIN {{ ref('gold_dim_customers') }} dc ON fod.sk_customer = dc.sk_customer
LEFT JOIN {{ ref('gold_dim_products') }} dp ON fod.sk_product = dp.sk_product