from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from datetime import datetime
import pandas as pd


def get_df_from_db(sql, db, db_conn_id):
    
    if db == 'redshift':
        hook = RedshiftSQLHook(redshift_conn_id=db_conn_id)
    elif db == 'mysql':
        hook = MySqlHook(mysql_conn_id=db_conn_id)
    
    # Execute the query and fetch results
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    
    # Convert the records to a pandas DataFrame
    df = pd.DataFrame(records, columns=columns)
    
    return df


def get_processing_log(content_owner_id, report_type_id):
    
    # Define the SQL query
    sql = f"""SELECT * from yt_reporting_api.youtube_reporting_api_download_log 
    WHERE content_owner_id = '{content_owner_id}'
    AND report_type_id = '{report_type_id}'
    """

    processing_log_df = get_df_from_db(sql=sql, db='redshift', db_conn_id='redshift-datalake-prod')
    
    return processing_log_df


def get_all_channels(network, source_table):

    # Define the SQL query to fetch data from MySQL
    all_channels_sql = f"""
    SELECT 
        channel_uc_id
    FROM content.channel_yts
    WHERE network = '{network}'
    AND is_active = 1
    """

    # Convert the MySQL data to a pandas DataFrame
    all_channels_df = get_df_from_db(sql=all_channels_sql, db='mysql', db_conn_id='airflow-prod-aurora')

    print(f'{len(all_channels_df)} channels found for {network} network.')

    return all_channels_df


def get_report(content_owner_id, content_owner_name, report_type_id, mode, network, **kwargs):
    
    processing_log_df = get_processing_log(content_owner_id=content_owner_id, report_type_id=report_type_id)
    print(f"Processing log for {report_type_id}:")
    print(processing_log_df) 
    all_channels_df = get_all_channels(network=network, source_table='content.channel_yts')


content_owners = [
    {'content_owner_id': 'REHwCa7vymY0q5XcdZYwTA',
     'content_owner_name': 'ONErpm_Entertainment',
     'network':'ent',
     'reports': [
         {'content_owner_video_metadata_a3': 'overwrite'},
        #  {'content_owner_asset_combined_a2': 'append'},
     ]
    },
]

#Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 7, 30),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def ingest_and_process_yt_reporting_api():
    #Define tasks
    @task()
    def task_handler(task_name, content_owner_id, content_owner_name, report_type_id, mode, network):
        return PythonOperator(
            task_id=task_name,
            python_callable=get_report,
            op_kwargs={"content_owner_id":content_owner_id,
                    "content_owner_name":content_owner_name,
                    "report_type_id":report_type_id,
                    "mode":mode,
                    "network":network
            },
            dag=dag
        )

#Instantiate the DAG
ingest_and_process_yt_reporting_api()