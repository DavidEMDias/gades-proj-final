from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


# Define the DAG
with DAG(
    "run_dbt_models",
    schedule=[
        Dataset("bronze_customers_dataset_ready"),
        Dataset("bronze_orders_dataset_ready"),
        Dataset("bronze_shippers_dataset_ready"),
        Dataset("bronze_order_details_dataset_ready"),
        Dataset("bronze_categories_dataset_ready"),
        Dataset("bronze_products_dataset_ready"),
    ],  # No regular schedule, it will be triggered by the dataset
    start_date=days_ago(1),
    catchup=False,
    tags=["dbt", "bash", "gades"],
) as dag:
    # BashOperator to run dbt inside the dbt-core container using docker exec
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            docker exec dbt sh -c "cd /opt/dbt/gades_proj && dbt run && dbt test"
        """,
        dag=dag,
    )