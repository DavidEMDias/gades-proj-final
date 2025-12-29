CREATE OR REPLACE TABLE `GCP_PROJECT_ID.DATASET_ID.bronze_orders` (
    OrderID INT64,
    CustomerID INT64,
    OrderDate TIMESTAMP,
    ShipperID INT64,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITION BY DATE(created_at);