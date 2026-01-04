from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pathlib import Path
import os
from google.api_core.exceptions import NotFound, Conflict



def build_query(bq_dataset, bq_table, sql_path, gcp_conn_id="google_cloud_default"):
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client()

    # Tenta obter o último updated_at da tabela
    query = f"SELECT MAX(updated_at) as last_updated FROM `{bq_dataset}.{bq_table}`"
    try:
        result = client.query(query).result()
        row = next(result)
        last_updated = row.last_updated if row.last_updated else datetime(2000, 1, 1, 0, 0, 0)
        print("Last updated in BQ:", row.last_updated)
    except NotFound:
        # Se a tabela não existir
        last_updated = datetime(2000, 1, 1, 0, 0, 0)

    # Lê o SQL do ficheiro e substitui o placeholder
    with open(sql_path, "r") as f:
        raw_sql = f.read()

    final_sql = raw_sql.replace("{{last_updated}}", last_updated.strftime("%Y-%m-%d %H:%M:%S"))
    return final_sql


#def decide_next_task(bucket_name, object_name, gcp_conn_id, **kwargs):
#    hook = GCSHook(gcp_conn_id=gcp_conn_id)
#    print(object_name)
#    print(bucket_name)
#    if hook.exists(bucket_name=bucket_name, object_name=object_name):
#        return "load_data"
#    else:
#       return "skip_load"
    
def decide_next_task(bucket_name, object_name, gcp_conn_id, **kwargs):
    hook = GCSHook(gcp_conn_id=gcp_conn_id) #Cria o hook para GCS com a conexão especificada
    
    # Verifica se o bucket existe e tenta listar o seu conteúdo
    try:
        hook.list(bucket_name=bucket_name, max_results=1)  # tenta aceder ao bucket
    except NotFound:
        print(f"Bucket '{bucket_name}' doesn't exist or is not accessible.")
        return "skip_load"
    
    # Verifica se o objeto (file) existe dentro do bucket
    if hook.exists(bucket_name=bucket_name, object_name=object_name):
        print(f"Object '{object_name}' found in bucket '{bucket_name}'.")
        return "check_and_create_table" #Se o ficheiro existir, prossegue para criar a tabela e carregar os dados
    else:
        print(f"Object '{object_name}' not found in bucket '{bucket_name}'.")
        return "skip_load" 
    

def check_and_create_dataset(dataset_id: str, location='europe-southwest1', gcp_conn_id='google_cloud_default'):
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client()
    dataset_ref = client.dataset(dataset_id)

    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' already exists.")
        return
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        try:
            client.create_dataset(dataset)
            print(f"Dataset '{dataset_id}' created successfully.")
        except Conflict:
            print(f"Dataset '{dataset_id}' already exists (Conflict caught).")


def check_and_create_table(project_id: str, dataset_id: str, table_id: str, sql_file_path: str, **kwargs):
    """
    Checks if a BigQuery table exists. If not, creates it using the SQL provided.
    Designed to be used with Airflow PythonOperator.
    """

    check_and_create_dataset(dataset_id) #Create the dataset if it doesn't exist

    print("Initializing BigQuery client...")
    client = bigquery.Client.from_service_account_json('/opt/airflow/keys/gcp-key.json') #Hardcoded way to connect to Bigquery - use Airflow connections in production (see example above)
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        client.get_table(table_ref)
        print(f"Table `{dataset_id}.{table_id}` already exists in project `{project_id}`.")
    except NotFound:
        print(f"Table `{dataset_id}.{table_id}` does not exist. Creating it...")
        sql_query = Path(sql_file_path).read_text()
        sql_query = sql_query.replace("GCP_PROJECT_ID", project_id).replace("DATASET_ID", dataset_id)
        query_job = client.query(sql_query)
        query_job.result()
        print(f"Table `{dataset_id}.{table_id}` has been successfully created.")

# Order of execution BuildQuery -> ExtractData to GCS -> CheckIfDataExists (Bucket has file?) -> [SkipLoad OR (CheckAndCreateDatasetAndTable -> LoadData)]