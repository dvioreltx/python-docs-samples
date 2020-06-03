import datetime

import logging
import os
import pandas as pd

from airflow import DAG
from airflow.operators import bash_operator
from airflow.operators.python_operator import PythonOperator
from enum import Enum
from google.cloud import bigquery
# from google.cloud import bigquery_storage_v1beta1 Error with this!
from google.cloud import storage
from google.oauth2 import service_account
from os.path import basename

# Config variables
delete_intermediate_tables = False
delete_gcs_files = False
enable_trigger = True
send_email_on_error = True
data_set_original = "location_matching_file"
data_set_final = "location_matching_match"
bucket = 'location_matching'
mail_from = 'dviorel@inmarket.com'
email_error = ['dviorel@inmarket.com']
mail_user = 'dviorel@inmarket.com'
mail_password = 'ftjhmrukjcdtcpft'
expiration_days_original_table = 7
expiration_days_results_table = 30

logging.basicConfig(level=logging.DEBUG)

fail_on_error = False
mail_server = 'smtp.gmail.com'
project = "cptsrewards-hrd"
url_auth_gcp = 'https://www.googleapis.com/auth/cloud-platform'
query_states = 'SELECT state_abbr, state_name from aggdata.us_states'
query_chains = 'SELECT chain_id, name, sic_code from `inmarket-archive`.scansense.chain'
query_cities = 'select distinct city, state from (select distinct city, state from  `aggdata.' \
               'locations_no_distributors` union all select distinct city, state from `aggdata.location_geofence`)'


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

# Define functions and query strings

class LMAlgo(Enum):
    CHAIN = 1
    SIC_CODE = 2


def _sanitize_file_name(file_name):
    for c in ' -!@#$%^&*()=+{}[];:"\'?.,':
        file_name = file_name.replace(c, '_')
    return file_name


def _verify_fields(columns, validation_fields):
    column_validation_fields = []
    for required_field in validation_fields.keys():
        if "/" in required_field:
            tokens = required_field.split('/')
        else:
            tokens = [required_field]
        for token in tokens:
            if token in columns:
                column_validation_fields.append(token)
                break
    return column_validation_fields


def _clean_zip(zip_code):
    if zip_code is None:
        return None
    zip_code = str(zip_code)
    if '.' in zip_code:
        zip_code = zip_code[:zip_code.index('.')]
    if zip_code == 'nan':
        return None
    if len(zip_code) == 0:
        return zip_code
    if len(zip_code) > 5:
        return zip_code[:5]
    if len(zip_code) < 5:
        return zip_code.rjust(5, '0')
    return zip_code


def _get_state_code(state, df_states):
    try:
        if len(state) == 2:
            return state
    except TypeError as e:
        return state
    if state == 'Washington DC':
        state = 'Washington'
    match = df_states[df_states['state_name'].str.strip().str.lower() == state.strip().lower()]
    if len(match) == 0:
        return ' '
    if len(match) > 1:
        raise Exception(f'{len(match)} occurrences for state {state}!')
    return match.iloc[0]['state_abbr']


def _get_chain_name(chain_id, df_chains):
    # match = df_chains[df_chains['chain_id'] == chain_id]
    # if len(match) == 0:
    #     raise Exception(f'No chain for chain_id {chain_id}!')
    # if len(match) > 1:
    #     raise Exception(f'{len(match)} occurrences for chain_id {chain_id}!')
    # return match.iloc[0]['name']
    return chain_id


def _verify_match_df(df_match, possible_value):
    if len(df_match.index) > 0:
        return True, possible_value
    return False, None


def _verify_match(value_1_return, value_2):
    if value_1_return.lower() == value_2.lower():
        return True, value_1_return
    return False, None


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
    original_name = dag_run.conf["name"]
    logging.info(f'File created: {original_name}')
    if not enable_trigger:
        logging.warning(f'Trigger disabled! ...{original_name}')
        return
    file_full_name = original_name.replace(' ', '_').replace('-', '_')
    if file_full_name.endswith('/'):
        logging.debug(f'Folder created {original_name}, ignored')
        return
    if 'processed/' in file_full_name or 'results/' in file_full_name:
        logging.debug(f'Results folder {original_name}, ignored')
        return
    if '/' not in file_full_name:
        logging.error(f'{file_full_name} does not belong to a folder')
        return
    full_result = False
    if file_full_name.endswith('___test.txt'):
        logging.warning(f'This is a test petition so we will return full data ...{original_name}')
        full_result = True
    destination_email = file_full_name[:file_full_name.index('/')]
    if '@' not in destination_email:
        logging.error(f'{destination_email} is not a valid email ...{original_name}')
        return
    file_name = file_full_name[file_full_name.rfind('/') + 1:]
    now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    if '.' in file_name:
        file_name = file_name[:file_name.rfind('.')]
    file_name = _sanitize_file_name(file_name)
    logging.info(f'Email: {destination_email} ...{original_name}')
    try:
        raw_data = pd.read_csv(f'gs://{bucket}/{original_name}', sep='\t', encoding='utf-8')
    except ValueError:
        raw_data = pd.read_csv(f'gs://{bucket}/{original_name}', sep='\t', encoding='iso-8859-1')


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
