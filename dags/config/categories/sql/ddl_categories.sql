CREATE OR REPLACE TABLE `GCP_PROJECT_ID.DATASET_ID.bronze_categories` 
(
    CategoryID INT64,
    CategoryName STRING,
    Description STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITION BY DATE(created_at);