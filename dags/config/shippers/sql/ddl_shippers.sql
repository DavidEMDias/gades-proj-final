CREATE OR REPLACE TABLE `GCP_PROJECT_ID.DATASET_ID.bronze_shippers` (
    ShipperID INT64,
    ShipperName STRING,
    Phone STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITION BY DATE(created_at);