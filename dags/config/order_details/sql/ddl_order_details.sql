CREATE OR REPLACE TABLE `GCP_PROJECT_ID.DATASET_ID.bronze_order_details`
(
    OrderDetailID INT64,
    OrderID INT64,
    ProductID INT64,
    Quantity INT64,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITION BY DATE(created_at);
