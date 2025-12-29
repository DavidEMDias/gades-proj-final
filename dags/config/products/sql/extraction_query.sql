SELECT 
    ProductID,
    ProductName,
    SupplierID,
    CategoryID,
    Unit,
    Price,
    created_at,
    updated_at
FROM `gades-dataeng`.products
WHERE
    updated_at > '{{last_updated}}'