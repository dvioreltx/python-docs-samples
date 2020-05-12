import datetime
import logging
import pandas as pd
import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1

dataset = "dannyv"
table = "test_02"
project = "cptsrewards-hrd"
bucket = 'location_matching'

logging.basicConfig(level=logging.INFO)


def _verify_fields(columns, validation_fields):
    column_validation_fields = []
    for required_field in validation_fields.keys():
        if "/" in required_field:
            tokens = required_field.split('/')
        else:
            tokens = [required_field]
        found = False
        for token in tokens:
            if token in columns:
                column_validation_fields.append(token)
                found = True
                break
        if validation_fields[required_field][0] and not found:
            raise Exception(f'Field {required_field} not founded in headers!')
    return column_validation_fields


def _clean_zip(zip_code):
    if zip_code is None or len(zip_code) == 0:
        return zip_code
    if len(zip_code) > 5:
        return zip_code[:5]
    if len(zip_code) < 5:
        return zip_code.ljust(5, '0')
    return zip_code


def process_created(data, context):
    try:
        validation_fields = {'chain name/chain id/sic code/NAICS': (True, 'chain_name'),
                             'address/address full/address full (no zip)': (True, 'address'),
                             'city': (False, 'city'), 'state': (False, 'state'), 'zip': (False, 'zip')}
        file_name = data['name']
        if file_name.endswith('/'):
            logging.info(f'Folder created {file_name}, ignored')
            return
        logging.info(f'File: {file_name}')
        raw_data = pd.read_csv(f'gs://{bucket}/{file_name}', sep=',')
        column_validation_fields = _verify_fields(raw_data.keys(), validation_fields)
        selected_columns = raw_data[column_validation_fields].rename(columns=lambda name: name.replace(' ', '_').
                                                                     replace('(', '_').replace(')', '_'), inplace=False)
        # Read all US states
        credentials, your_project_id = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        bqclient = bigquery.Client(
            project=project,
            credentials=credentials
        )
        bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient(
            credentials=credentials
        )
        query_string = 'SELECT state_abbr, state_name from aggdata.us_states'
        df_states = (bqclient.query(query_string).result().to_dataframe(bqstorage_client=bqstorageclient))

        # Complete columns not present in file
        for key in validation_fields:
            if not validation_fields[key][0] and validation_fields[key][1] not in selected_columns:
                selected_columns[validation_fields[key][1]] = ''
        temp_table = f'tmp_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
        logging.info(f'Will write to table: {temp_table}')
        selected_columns.to_gbq(f'{dataset}.{temp_table}', project_id=project, progress_bar=False)
    except Exception as e:
        logging.error(f'Unexpected error {e}')


# process_created({'name': 'dviorel/Sample_1 updated.csv'}, None)
process_created({'name': 'dviorel/sample_2_small_subset_nozip.csv'}, None)
