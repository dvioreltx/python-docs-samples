import datetime

import airflow
from airflow.operators import bash_operator


default_args = {
    'owner': 'danny',
    'depends_on_past': False,
    'email': ['dviorel@inmarket.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2020, 1, 1),
}

with airflow.DAG(
        # 'composer_sample_trigger_response_dag',
        'gcs_location_matching_created',
        default_args=default_args,
        # Not scheduled, trigger only
        schedule_interval=None) as dag:

    # Print the dag_run's configuration, which includes information about the
    # Cloud Storage object change.
    print_gcs_info = bash_operator.BashOperator(
        task_id='print_gcs_info', bash_command='echo {{ dag_run.conf }}')
