from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime, timedelta
import os
from google.oauth2 import service_account
import google.auth.transport.requests
from googleapiclient.discovery import build
import requests as req
import shutil
import time
import pandas as pd



postgres_conn_id = 'redshift-datalake-prod'

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

    processing_log_df = get_df_from_db(sql=sql, db='redshift', db_conn_id=postgres_conn_id)
    
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
    

default_args = {
    'owner': 'marco.borges',
    'depends_on_past': True,
    'start_date': datetime(2024, 7, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 120,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_and_process_yt_reporting_api_v5',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval='0 8,12,16 * * *',
)

content_owners = [
    {'content_owner_id': 'REHwCa7vymY0q5XcdZYwTA',
     'content_owner_name': 'ONErpm_Entertainment',
     'network':'ent',
     'reports': [
         {'content_owner_video_metadata_a3': 'overwrite'},
        #  {'content_owner_asset_combined_a2': 'append'},
     ]
    },
    # {'content_owner_id': 'dBRZrw2tL2B5Ea8d2Ppi-Q',
    #  'content_owner_name': 'Radar_Records',
    #  'network':'rad',
    #  'reports': [
    #      {'content_owner_video_metadata_a3': 'overwrite'},
    #     #  {'content_owner_asset_combined_a2': 'append'},
    #  ]
    # },
    # {'content_owner_id': '8w5PQWQ9tPtlcssjKVTiiQ',
    #  'content_owner_name': 'ONErpm_Network',
    #  'network':'net',
    #  'reports': [
    #      {'content_owner_video_metadata_a3': 'overwrite'},
    #     #  {'content_owner_asset_combined_a2': 'append'},
    #  ]
    # },
    # {'content_owner_id': 'PG62VcKnD59-xTW1YXoViw',
    #  'content_owner_name': 'ONErpm_Mexico',
    #  'network':'mex',
    #  'reports': [
    #     {'content_owner_video_metadata_a3': 'overwrite'},
    #     # {'content_owner_asset_combined_a2': 'append'},
    #  ]
    # },
    # {'content_owner_id': 'QPYCxq2FSZXvIgKan3YKUQ',
    #  'content_owner_name': 'ONErpm_USA',
    #  'network':'usa',
    #  'reports': [
    #      {'content_owner_video_metadata_a3': 'overwrite'},
    #     #  {'content_owner_asset_combined_a2': 'append'},
    #  ]
    # },
    # {'content_owner_id': 'kJfnW3Y9uxJqzYe9Y5GLlw',
    #  'content_owner_name': 'ONErpm_Light',
    #  'network':'sei',
    #  'reports': [
    #      {'content_owner_video_metadata_a3': 'overwrite'},
    #     #  {'content_owner_asset_combined_a2': 'append'},
    #  ]
    # },
    # {'content_owner_id': 'MjJIpM4kvNAqcoybvrUirA',
    #  'content_owner_name': 'ONErpm_Music_Light',
    #  'network':'mli',
    #  'reports': [
    #      {'content_owner_video_metadata_a3': 'overwrite'},
    #     #  {'content_owner_asset_combined_a2': 'append'},
    #  ]
    # },
    # {'content_owner_id': 'DqnxVdH2kFLeUnYYBP53ug',
    #  'content_owner_name': 'ONErpm',
    #  'network':'1r',
    #  'reports': [
    #      {'content_owner_video_metadata_a3': 'overwrite'},
    #     #  {'content_owner_asset_combined_a2': 'append'},
    #  ]
    # }
]

previous_task = None
for content_owner in content_owners:
    for report in content_owner['reports']:

        extract_reports = task_handler(task_name=f"{content_owner['content_owner_name']}_{list(report.keys())[0]}",
                                        content_owner_id=content_owner['content_owner_id'],
                                        content_owner_name=content_owner['content_owner_name'],
                                        report_type_id=list(report.keys())[0],
                                        mode=list(report.values())[0],
                                        network=content_owner.get('network', '')
                                    )

        if previous_task:
            previous_task >> extract_reports

        previous_task = extract_reports
