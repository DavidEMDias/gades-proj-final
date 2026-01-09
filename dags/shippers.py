from src.helpers import build_query, decide_next_task, check_and_create_table
from config.schemas import SCHEMA_REGISTRY
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

from datetime import datetime
import os

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

GCP_PROJECT = Variable.get("GCP_PROJECT_ID")
ENTITY = "shippers"
SCHEMA_FIELDS = SCHEMA_REGISTRY[ENTITY]
DAG_ID = f"raw_{ENTITY}"
SQL_PATH = os.path.join(os.path.dirname(__file__), f"config/{ENTITY}/sql/extraction_query.sql")
SQL_PATH_CREATION = os.path.join(os.path.dirname(__file__), f"config/{ENTITY}/sql/ddl_{ENTITY}.sql")
GCS_BUCKET = Variable.get("BRONZE_GCS_BUCKET_NAME")
BQ_DATASET = "retail_gades_bronze"
BQ_TABLE = f"bronze_{ENTITY}"


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval="00 1 * * *",
    catchup=False,
    tags=["mysql", "bigquery", "gcs"],
) as dag:

    def generate_query(**kwargs):
        query = build_query(BQ_DATASET, BQ_TABLE, SQL_PATH)
        return query


    build_query_task = PythonOperator(
        task_id="build_query",
        python_callable=generate_query,
    )

    export_to_gcs = MySQLToGCSOperator(
        task_id="extract_data",
        sql="{{ ti.xcom_pull(task_ids='build_query') }}",
        bucket=GCS_BUCKET,
        filename=f"mysql_export/result_{ENTITY}_{{{{ts_nodash}}}}.json",
        mysql_conn_id="mysql_connection",
        export_format="json",
        gzip=False
    )

    branch = BranchPythonOperator(
        task_id="check_if_data_exists",
        python_callable=decide_next_task,
        op_kwargs={
            "bucket_name": GCS_BUCKET,
            "object_name": f"mysql_export/result_{ENTITY}_{{{{ts_nodash}}}}.json",
            "gcp_conn_id": "google_cloud_default"
        }
    )

    skip_task = EmptyOperator(
        task_id="skip_load",
        outlets=[Dataset("bronze_shippers_dataset_ready")] # This indicates that this task writes to the dataset
    )

#Manually create the table
    create_table_task = PythonOperator(
        task_id="check_and_create_table",
        python_callable=check_and_create_table,
        op_kwargs={
            "project_id": GCP_PROJECT,
            "dataset_id": BQ_DATASET,
            "table_id": BQ_TABLE,
            "sql_file_path": SQL_PATH_CREATION,
        },
    )   

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_data",
        bucket=GCS_BUCKET,
        source_objects=[f"mysql_export/result_{ENTITY}_{{{{ts_nodash}}}}.json"],
        destination_project_dataset_table=f"{BQ_DATASET}.{BQ_TABLE}",
        source_format="NEWLINE_DELIMITED_JSON",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        gcp_conn_id="google_cloud_default",
        autodetect=False, #remover para false se quiser construir o ddl manualmente
        schema_fields=SCHEMA_FIELDS, #Vantanges: Schema do operador é consistente com a tabela, Idempotente, seguro para produção
                                    #Não depende de autodetect, evitando mudanças inesperadas
        outlets=[Dataset("bronze_shippers_dataset_ready")] # This indicates that this task writes to the dataset
    )

    build_query_task >> export_to_gcs  >> branch
    branch >> create_table_task >> load_to_bigquery
    branch >> skip_task



