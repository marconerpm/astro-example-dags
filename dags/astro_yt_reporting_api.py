from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
from google.oauth2 import service_account
import google.auth.transport.requests
from googleapiclient.discovery import build
import requests as req
import shutil
import time


AWS_CONN_ID = "aws_marco"


def get_authenticated_service(token=None):

    credentials_json = Variable.get("ONERPM-LEGACY-CREDENTIALS", deserialize_json=True)

    print("Creating credentials object from the downloaded file.")
    credentials = service_account.Credentials.from_service_account_info(
        credentials_json,
        scopes=[
            'https://www.googleapis.com/auth/yt-analytics-monetary.readonly',
            'https://www.googleapis.com/auth/yt-analytics.readonly'
        ]
    )
    
    if token:
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        print("Credentials refreshed successfully.")
        return credentials.token
    else:   
        print("Building YouTube Reporting API service.")
        return build('youtubereporting', 'v1', credentials=credentials)


def list_reporting_jobs(youtube_reporting, onBehalfOfContentOwner, includeSystemManaged=True):
    """
    Lists reporting jobs for the YouTube Reporting API.

    This function uses the `jobs().list` method of the YouTube Reporting API to retrieve a list of reporting jobs based on the specified parameters.

    Args:
        youtube_reporting (googleapiclient.discovery.Resource): An authenticated service object for the YouTube Reporting API.
        onBehalfOfContentOwner (str): The content owner for which the API request is being made.
        includeSystemManaged (bool, optional): Whether to include system-managed jobs in the results. Default is True.

    Returns:
        dict: A dictionary containing the list of reporting jobs.
    """
    print("Listing reporting jobs...")
    results = youtube_reporting.jobs().list(onBehalfOfContentOwner=onBehalfOfContentOwner,
                                             includeSystemManaged=includeSystemManaged).execute()
    print("Reporting jobs listed successfully.")
    
    return results['jobs']


def retrieve_reports(youtube_reporting, jobId, onBehalfOfContentOwner):
    """
    Retrieves reports for a specific reporting job from the YouTube Reporting API.

    This function uses the `reports().list` method of the YouTube Reporting API to retrieve a list of reports for a specific reporting job based on the specified parameters.

    Args:
        youtube_reporting (googleapiclient.discovery.Resource): An authenticated service object for the YouTube Reporting API.
        jobId (str): The ID of the reporting job for which to retrieve reports.
        onBehalfOfContentOwner (str): The content owner for which the API request is being made. If specified, only reports for this content owner will be returned.

    Returns:
        list: A list of reports for the specified reporting job.
    """
    print(f"Retrieving reports for job ID {jobId} on behalf of content owner {onBehalfOfContentOwner}...")
    results = youtube_reporting.jobs().reports().list(jobId=jobId,
                                                       onBehalfOfContentOwner=onBehalfOfContentOwner).execute()
    reports = results.get('reports')
    print(f"Reports retrieved successfully: {reports}")
    
    return reports


def upload_file_to_s3(filename, bucket_name, s3_key, aws_conn_id=AWS_CONN_ID):
    """
    Uploads a file to an S3 bucket.

    Args:
        filename (str): The name of the file to upload.
        bucket_name (str): The name of the S3 bucket.
        s3_key (str): The path within the bucket to upload the file to.
        AWS_CONN_ID (str, optional): The Airflow connection ID for the AWS credentials. Default is AWS_CONN_ID.
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_key = f"{s3_key}/{filename}"
    s3_hook.load_file(
        filename=f'{filename}',
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"File uploaded to S3: {s3_key}")


def check_file_exists_in_s3(bucket_name, file_key, aws_conn_id='aws_default'):
    """
    Checks if a specific file exists within an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key (path) of the file in the S3 bucket.
        aws_conn_id (str, optional): The Airflow connection ID for the AWS credentials. Default is 'aws_default'.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    exists = s3_hook.check_for_key(bucket_name=bucket_name, key=file_key)

    return exists


def get_report(content_owner, network):
    """
    Retrieves and upload to S3 the reports a content owner using the YouTube Reporting API.

    Args:
        content_owner (str): The content owner for which to retrieve the report.
    """
    youtube_reporting = get_authenticated_service()
    print(f"{50*'=-'}\nYou just authenticated to Youtube Reporting API\n{50*'=-'}")
    jobs = list_reporting_jobs(youtube_reporting,
                                    onBehalfOfContentOwner=content_owner, includeSystemManaged=True)
    
    allowed_reports = [
        'content_owner_active_claims_a1',
    ]

    job_count = 0
    for job in jobs:
        
        report_type = job['reportTypeId']

        if report_type in allowed_reports:

            available_reports = retrieve_reports(youtube_reporting, jobId=job["id"], onBehalfOfContentOwner=content_owner)

            if available_reports:
                
                # report_count = 0
                for available_report in available_reports:
                    # report_count+=1
                    # if report_count == 8:
                    #     break

                    report_create_time_w_dash = available_report['createTime'][0:10]
                    report_start_date_wo_dash = available_report['startTime'][0:10].replace('-', '')
                    filename=f"{report_type}.csv.gz" 
                    s3_key_input=f"youtube_reporting_api/network={network}/report_type={report_type}/report_start_date={report_start_date_wo_dash}"

                    if not check_file_exists_in_s3('1r-data-lake', f'{s3_key_input}/{filename}'):

                        print(f"The report {report_type} created at {report_create_time_w_dash} is going to be downloaded.")

                        url = available_report["downloadUrl"]
                        token = get_authenticated_service(token=True)
                        headers = {
                            "Authorization": f"Bearer {token}",
                            "Accept-Encoding": "gzip"
                        }

                        with req.get(url, headers=headers, stream=True) as r:
                            with open(filename, 'wb') as f:
                                shutil.copyfileobj(r.raw, f)
                    
                        print(f'Report downloaded and stored in the file: {filename}')

                        # Upload file to data lake
                        upload_file_to_s3(filename=filename, bucket_name="1r-data-lake"
                                            , s3_key=s3_key_input)
                        
                        # Remove the temporary file
                        try:
                            os.remove(filename)
                            print(f'{filename} has been deleted from Airflow server.')
                        except Exception as e:
                            print(e)

                        print(f'Esse é o {job_count}o job.')
                        job_count+=1

                        time.sleep(10)
                        print(f"The report {report_type} created at {report_create_time_w_dash} has been downloaded and uploaded to 1r-data-lake/{s3_key_input}.")

                    else:
                        print(f"The report {report_type} created at {report_create_time_w_dash} is already in S3. It is in the key: {s3_key_input}")
            else:
                print(f"The report {report_type} has no available reports.")

        else:
            print(f'{report_type} is not allowed.')
    return None


def task_handler(task_name, content_owner, network):
    """
    Creates an Airflow task for fetching and processing a report from the YouTube Reporting API.

    Args:
        task_name (str): The name of the task.
        content_owner (str): The content owner for which to retrieve the report.
    Returns:
        airflow.operators.python_operator.PythonOperator: An Airflow PythonOperator instance representing the task.
    """
    return PythonOperator(
        task_id=task_name,
        python_callable=get_report,
        op_kwargs={"content_owner":content_owner,
                   "network":network,
        },
        dag=dag
    )
    

default_args = {
    'owner': 'marco.borges',
    'depends_on_past': True,
    'start_date': datetime(2024, 4, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'extract_youtube_reporting_api_v0',
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=timedelta(days=1),
)

content_owners = {
    # "REHwCa7vymY0q5XcdZYwTA":"ONErpm_Entertainment",
    "dBRZrw2tL2B5Ea8d2Ppi-Q":"Radar_Records",
    # "8w5PQWQ9tPtlcssjKVTiiQ":"ONErpm_Network",
    # "PG62VcKnD59-xTW1YXoViw":"ONErpm_Mexico",
    # "QPYCxq2FSZXvIgKan3YKUQ":"ONErpm_USA",
    # "kJfnW3Y9uxJqzYe9Y5GLlw":"ONErpm_Light",
    # "MjJIpM4kvNAqcoybvrUirA":"ONErpm_Music_Light",
    "DqnxVdH2kFLeUnYYBP53ug":"ONErpm",
    } 

tasks = []

for content_owner, network in content_owners.items():

    extract_reports = task_handler(task_name=f'extract_reports_{network}',
                                    content_owner=content_owner,
                                    network=network)
    tasks.append(extract_reports)

for task in tasks:
    task
