

from datetime import timedelta, datetime


from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator



GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="dbproject-406721"
GS_PATH = "playstore/"
BUCKET_NAME = 'historical_play'
STAGING_DATASET = "playstore_staging_dataset"
DATASET = "playstore_dataset"
LOCATION = "us-west4"

default_args = {
    'owner': 'DB project',
    'depends_on_past': False,
    'email_on_failure':'projectdb6@gmail.com',
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

with DAG('PlayStoreDag', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )


    load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
        )    
    
    load_dataset_dbproject = GCSToBigQueryOperator(
        task_id = 'load_dataset_dbproject',
        bucket = BUCKET_NAME,
        source_objects = ['playstore/dbproject.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_dbproject',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
                {"name": "App_Name","type": "STRING","mode": "NULLABLE" },
                {"name": "App_Id","type": "STRING","mode": "NULLABLE"},
                {"name": "Category","type": "STRING","mode": "NULLABLE"},
                {"name": "Rating","type": "FLOAT","mode": "NULLABLE"},
                {"name": "Rating_Count","type": "FLOAT","mode": "NULLABLE"},
                {"name": "Installs","type": "INTEGER","mode": "NULLABLE"},
                {"name": "Free","type": "BOOLEAN","mode": "NULLABLE"},
                {"name": "Price","type": "FLOAT","mode": "NULLABLE"},
                {"name": "Developer","type": "STRING","mode": "NULLABLE"},
                {"name": "Released","type": "DATE","mode": "NULLABLE"},
                {"name": "Last_Updated","type": "DATE","mode": "NULLABLE"},
                {"name": "Content_Rating","type": "STRING","mode": "NULLABLE"},
                {"name": "Ad_Supported","type": "BOOLEAN","mode": "NULLABLE"},
                {"name": "In_App_Purchases","type": "BOOLEAN","mode": "NULLABLE"}])


    check_dataset_dbproject = BigQueryCheckOperator(
    task_id='check_dataset_dbproject',
    use_legacy_sql=False,
    location=LOCATION,
    sql=f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_dbproject`'
)

   
    create_D_Table = DummyOperator(
        task_id = 'Create_D_Table',
        dag = dag
        )

    create_D_app_dimensions = BigQueryOperator(
        task_id = 'create_D_app_dimensions',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_app_dimensions.sql'
        )
    
    create_D_developer_dimension = BigQueryOperator(
        task_id = 'create_D_developer_dimension',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_developer_dimension.sql'
        )
    
    create_D_content_data_dimension = BigQueryOperator(
        task_id = 'create_D_content_data_dimension',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_content_data_dimension.sql'
        )
    
    
    check_D_app_dimensions = BigQueryCheckOperator(
        task_id = 'check_D_app_dimensions',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.D_app_dimensions`'
        ) 

    check_D_developer_dimension = BigQueryCheckOperator(
        task_id = 'check_D_developer_dimension',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.D_developer_dimension`'
        ) 
    
    check_D_content_data_dimension = BigQueryCheckOperator(
        task_id = 'check_D_content_data_dimension',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.D_content_data_dimension`'
        ) 

    create_F_fact_table = BigQueryOperator(
        task_id = 'create_F_fact_table',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/F_fact_table.sql'
        )
    
    check_F_fact_table = BigQueryCheckOperator(
        task_id = 'check_F_fact_table',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.F_fact_table`'
        ) 

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
start_pipeline >> load_staging_dataset

load_staging_dataset >> [load_dataset_dbproject]

load_dataset_dbproject >> check_dataset_dbproject

[check_dataset_dbproject] >> create_D_Table

create_D_Table >> [create_D_app_dimensions,create_D_developer_dimension,create_D_content_data_dimension]

create_D_app_dimensions >> check_D_app_dimensions
create_D_developer_dimension >> check_D_developer_dimension
create_D_content_data_dimension >>  check_D_content_data_dimension

[check_D_app_dimensions,check_D_developer_dimension,check_D_content_data_dimension] >> create_F_fact_table

create_F_fact_table >> check_F_fact_table >> finish_pipeline
