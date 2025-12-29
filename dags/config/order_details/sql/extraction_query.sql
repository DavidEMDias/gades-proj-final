SELECT 
    OrderDetailID,
    OrderID,
    ProductID,
    Quantity,
    created_at,
    updated_at
FROM `gades-dataeng`.order_details
WHERE
    updated_at > '{{last_updated}}'