from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Connection
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import paramiko
import boto3
import tempfile
import logging
import time



# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logical_date = "{{ ds }}"
AWS_CONN_ID = "aws_marco"

# Retrieve AWS access key and secret
aws_conn = Connection.get_connection_from_secrets(AWS_CONN_ID)
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password

# Redshift connection
postgres_conn_id = 'redshift-datalake-prod'

def get_quarter(logical_date):
    # Parse the logical date string to datetime object
    logical_date = datetime.strptime(logical_date, "%Y-%m-%d")

    # Get year and quarter
    year = logical_date.year
    quarter = (logical_date.month - 1) // 3 + 1

    # Return quarter in the format "YYYYQ#"
    return quarter, f"{year}Q{quarter}"


def get_year(logical_date):
    # Parse the logical date string to datetime object
    logical_date = datetime.strptime(logical_date, "%Y-%m-%d")

    # Get year and quarter
    year = logical_date.year

    # Return quarter in the format "YYYYQ#"
    return f"{year}"


def get_sftp_connection():
    """
    Fetches YouTube Encore SFTP Private Key from an S3 path. And returns its connection object.
    """
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_key = 'resources/youtube_encore_sftp_private_key.ppk'
    s3_bucket = '1r-live-airflow'

    with tempfile.NamedTemporaryFile() as temp_file:
        print(f"Downloading YouTube Encore SFTP Private Key from S3 bucket {s3_bucket} to {temp_file.name}")
        s3_hook.get_conn().download_file(s3_bucket, s3_key, temp_file.name)
        print("YouTube Encore SFTP Private Key file downloaded successfully.")

        # SFTP connection parameters
        sftp_host = 'ytmusicpublishing.com'
        sftp_port = 22
        sftp_username = 'onerpm'
        # Establish SFTP connection
        private_key = paramiko.RSAKey.from_private_key_file(temp_file.name)
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_username, pkey=private_key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        return sftp, transport
    

def ingest_data(remote_file_path, s3_bucket, s3_key):
    sftp, transport = get_sftp_connection()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # Check if the file exists in the S3 destination
    s3_key_exists = False
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        s3_key_exists = True
        print(f"File {s3_key} already exists in S3 bucket {s3_bucket}. Skipping download.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"File {s3_key} does not exist in S3 bucket {s3_bucket}. Proceeding with upload.")
        else:
            raise
        
    if not s3_key_exists:

        with tempfile.NamedTemporaryFile() as temp_file:
            try:
                sftp.get(remote_file_path, temp_file.name)
                print(f'Started upload of the file {remote_file_path} to {s3_bucket}/{s3_key}')
                s3_client.upload_file(Filename=temp_file.name, Bucket=s3_bucket, Key=s3_key)
            except FileNotFoundError:
                print(f"Remote file {remote_file_path} does not exist.")

    # Close connections
    sftp.close()
    transport.close()


def trigger_glue_job(
    job_name,
    input_s3_path,
    output_s3_path
):
    session = AwsGenericHook(aws_conn_id=AWS_CONN_ID)
    
    # Get a client in the same region as the Glue job
    boto3_session = session.get_session(
        region_name='us-east-1',
    )
    
    # Trigger the job using its name
    client = boto3_session.client('glue')

    args = {
    "--input_s3_path": input_s3_path,
    "--output_s3_path": output_s3_path,
}

    response = client.start_job_run(
        JobName=job_name,
        Arguments=args,
    )

    job_run_id = response['JobRunId']
    
    # Monitor the status of the Glue Job
    while True:
        response = client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response['JobRun']['JobRunState']

        logger.info(f"Glue Job '{job_name}' status: {status}")
        
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break  # Job has completed
        else:
            # Wait for some time before checking again
            time.sleep(60)


def main(name, filename, logical_date, target, **kwargs):
    quarter_number, quarter = get_quarter(logical_date)
    year = get_year(logical_date)
    remote_file_path = f'from_YouTube/{quarter}/{name}'
    sftp, transport = get_sftp_connection()
    try:
        files = sftp.listdir(remote_file_path)
    except FileNotFoundError:
        print(f"Remote file {remote_file_path} does not exist.")
        return True # end of task
    
    filename = filename + f'_{year}-Q{quarter_number}_BR_1of1'
    print(f"Looking for file {filename} in {remote_file_path}")
    
    file_found = False
    for file in files:
        print(f'Checking file {file}: {filename in file}')
        if filename in file:
            remote_file_path = remote_file_path+"/"+file
            file_found = True
            break

    if not file_found:
        print(f"File {filename} not found in {remote_file_path}. Exiting task.")
        return True

    ingest_data(remote_file_path=remote_file_path, s3_bucket='1r-data-lake', s3_key=f'youtube_encore/report_name={name}/quarter={quarter}/{file}')

    # Check if the file exists in the S3 destination
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    output_s3_path=f's3://1r-data-lake/youtube_encore/parquet/report_name={name}/quarter={quarter}/'
    prefix = f'youtube_encore/parquet/report_name={name}/quarter={quarter}/'

    response = s3_client.list_objects_v2(Bucket='1r-data-lake', Prefix=prefix)
    if 'Contents' in response:
        print(f"File {output_s3_path} already exists in S3 bucket 1r-data-lake. Skipping Glue run.")
        # Path exists, and there are objects under it
    else:
        print(f"File {output_s3_path} does not exist in S3 bucket 1r-data-lake. Proceeding with Glue run.")
        trigger_glue_job(job_name='YouTubeEncoreTSVToParquet',  
                        input_s3_path=f's3://1r-data-lake/youtube_encore/report_name={name}/quarter={quarter}/{file}', 
                        output_s3_path=output_s3_path)

    if target:
        print(f"Copying asset from {output_s3_path} to Redshift into table {target}")
        S3ToRedshiftOperator(
        task_id="copy_asset_to_redshift",
        schema=target.split('.')[0],  # Target Redshift schema
        table=target.split('.')[1],    # Target Redshift table
        s3_bucket='1r-data-lake',
        s3_key=output_s3_path.replace('s3://1r-data-lake/', ''),
        redshift_conn_id=postgres_conn_id,
        copy_options=[
            "PARQUET"
        ],
        dag=dag,
        ).execute(context=kwargs)

default_args = {
    'owner': 'marco.borges',
    'depends_on_past': True,
    'start_date': datetime(2024, 2, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'extract_sftp_youtube_encore_v17',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@weekly",
)

config_list = [
    {'name':'HardwareAudioTier-Music',
        'filename':'DSR_ONErpm-BR-publishing_YouTube_Usage-HardwareAudioTier-Music-All',
        'target':None,
    },
    {'name':'SubscriptionPremium-Music',
        'filename':'DSR_ONErpm-BR-publishing_YouTube_Usage-SubscriptionPremium-Music-All',
        'target':None,
    },
    {'name':'SubscriptionMusic-Music',
        'filename':'DSR_ONErpm-BR-publishing_YouTube_Usage-SubscriptionMusic-Music-All',
        'target':None,
    },
    {'name':'AdSupport-Music',
        'filename':'DSR_ONErpm-BR-publishing_YouTube_Usage-AdSupport-Music-All',
        'target':'ingest.youtube_encore_usage_ad_support',
    },
]

tasks = []

for config in config_list:

    data_ingestion = PythonOperator(
    task_id=f"extract_{config['name']}",
    python_callable=main,
    op_kwargs={"name":config['name'], 
               "filename":config['filename'],
               "target":config['target'],
               "logical_date":logical_date,
            },
    dag=dag,
    trigger_rule='all_success',
    provide_context=True, 
    )
    tasks.append(data_ingestion)

for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]
    