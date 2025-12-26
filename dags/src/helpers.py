from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.api_core.exceptions import NotFound
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def build_query(bq_dataset, bq_table, sql_path, gcp_conn_id="google_cloud_default"):
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client()

    # Tenta obter o último updated_at da tabela
    query = f"SELECT MAX(updated_at) as last_updated FROM `{bq_dataset}.{bq_table}`"
    try:
        result = client.query(query).result()
        row = next(result)
        last_updated = row.last_updated if row.last_updated else datetime(2000, 1, 1, 0, 0, 0)
    except NotFound:
        # Se a tabela não existir
        last_updated = datetime(2000, 1, 1, 0, 0, 0)

    # Lê o SQL do ficheiro e substitui o placeholder
    with open(sql_path, "r") as f:
        raw_sql = f.read()

    final_sql = raw_sql.replace("{{last_updated}}", last_updated.strftime("%Y-%m-%d %H:%M:%S"))
    return final_sql


def decide_next_task(bucket_name, object_name, gcp_conn_id, **kwargs):
    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    print(object_name)
    print(bucket_name)
    if hook.exists(bucket_name=bucket_name, object_name=object_name):
        return "load_data"
    else:
        return "skip_load"