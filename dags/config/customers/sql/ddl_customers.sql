CREATE OR REPLACE TABLE `GCP_PROJECT_ID.DATASET_ID.bronze_customers`
(
    CustomerID INT64,
    CustomerName STRING,
    ContactName STRING,
    Address STRING,
    City STRING,
    PostalCode STRING,
    Country STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITION BY DATE(created_at);
