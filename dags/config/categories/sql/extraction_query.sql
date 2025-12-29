SELECT
    CategoryID,
    CategoryName,
    Description,
    created_at,
    updated_at
FROM `gades-dataeng`.categories
WHERE
    STR_TO_DATE(updated_at, '%Y-%m-%d %H:%i:%s')  > '{{last_updated}}'