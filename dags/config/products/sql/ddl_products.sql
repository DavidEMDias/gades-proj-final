CREATE OR REPLACE TABLE `GCP_PROJECT_ID.DATASET_ID.bronze_products` (
    ProductID INT64,
    ProductName STRING,
    SupplierID INT64,
    CategoryID INT64,
    Unit STRING,
    Price NUMERIC,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITION BY DATE(created_at);
