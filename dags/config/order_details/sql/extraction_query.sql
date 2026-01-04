SELECT 
    OrderDetailID,
    OrderID,
    ProductID,
    Quantity,
    created_at,
    updated_at
FROM `gades-dataeng`.order_details
WHERE
    STR_TO_DATE(updated_at, '%Y-%m-%d %H:%i:%s') > '{{last_updated}}'