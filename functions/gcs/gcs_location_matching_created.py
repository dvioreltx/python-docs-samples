import datetime

import logging
from airflow import DAG
from airflow.operators import bash_operator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
# from google.cloud import bigquery_storage_v1beta1 Error with this!
from google.cloud import storage
from google.oauth2 import service_account
from os.path import basename

# Config variables

default_args = {
    'owner': 'danny',
    'depends_on_past': False,
    'email': ['dviorel@inmarket.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2020, 4, 1),
}

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'gcs_location_matching_created',
    default_args=default_args,
    description = 'Location Matching Tool',
    # Not scheduled, trigger only
    schedule_interval=None
)

# Print the dag_run's configuration, which includes information about the
# Cloud Storage object change.


# Define functions and query strings


def pre_process_file(**context):
    logging.warning('log: pre_process_file')
    logging.warning(f'log: pre_process_file with context: {context}')
    dag_run = context['dag_run']
    logging.warning(f'log: pre_process_file with dag_run: {dag_run}')
    logging.warning(f'log: pre_process_file type of dag_run: {type(dag_run)}')
    try:
        logging.warning(f'log: pre_process_file type of dag_run conf : {dag_run.conf}')
    except Exception as e:
        logging.error(f'Error a: {e}')
    try:
        logging.warning(f'log: pre_process_file type of dag_run conf name: {dag_run.conf["name"]}')
    except Exception as e:
        logging.error(f'Error b: {e}')
    # logging.warning(f'log: pre_process_file with configur: {configur}')
    # file_name = configur['name']
    # logging.warning(f'log: pre_process_file with file_name: {file_name}')


def execute_location_matching():
    logging.warning('log: execute_location_matching')


def prepare_results_table():
    logging.warning('log: prepare_results_table')


def send_email_results():
    logging.warning('log: send_email_results')


def delete_temp_data():
    logging.warning('log: delete_temp_data')


# define tasks

print_gcs_info = bash_operator.BashOperator(
    task_id='print_gcs_info',
    bash_command='echo {{ dag_run.conf }}',
    dag=dag)


pre_process_file = PythonOperator(
    task_id='pre_process_file',
    provide_context=True,
    python_callable=pre_process_file,
    dag=dag)

execute_location_matching = PythonOperator(
    task_id='execute_location_matching',
    python_callable=execute_location_matching,
    dag=dag)

prepare_results_table = PythonOperator(
    task_id='prepare_results_table',
    python_callable=prepare_results_table,
    dag=dag)

send_email_results = PythonOperator(
    task_id='send_email_results',
    python_callable=send_email_results,
    dag=dag)

delete_temp_data = PythonOperator(
    task_id='delete_temp_data',
    python_callable=delete_temp_data,
    dag=dag
)

# Setting up Dependencies
pre_process_file.set_upstream(print_gcs_info)
execute_location_matching.set_upstream(pre_process_file)
prepare_results_table.set_upstream(execute_location_matching)
send_email_results.set_upstream(prepare_results_table)
delete_temp_data.set_upstream(send_email_results)

# Read the file in a Pandas Dataframe, preprocess, save table to BigQuery, push the table_name to a Xcom variable
#    I think we also need the has_sic_code variable.
# Execute the Location Matching Algorithm,
# Create the final table name
# Send email with results
# Delete temp tables and intermediate results
# Maybe airflow variables to determine log level and drop/delete policy?
