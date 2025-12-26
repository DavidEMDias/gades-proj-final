SELECT 
    CustomerID,
    CustomerName,
    ContactName,
    Address,
    City,
    PostalCode,
    Country,
    created_at,
    updated_at
FROM `gades-dataeng`.customers
WHERE
    updated_at > '{{last_updated}}'