SELECT
    OrderID,
    CustomerID,
    OrderDate,
    ShipperID,
    created_at,
    updated_at
FROM `gades-dataeng`.orders
WHERE
    STR_TO_DATE(updated_at, '%Y-%m-%d %H:%i:%s') > '{{last_updated}}'