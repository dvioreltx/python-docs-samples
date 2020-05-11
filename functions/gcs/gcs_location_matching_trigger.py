import datetime
import logging
import pandas as pd

dataset = "dannyv"
table = "test_02"
project = "cptsrewards-hrd"
bucket = 'location_matching'

logging.basicConfig(level=logging.INFO)


def _verify_fields(columns):
    # It must have chain name/chain id/sic code/NAICS, address
    # It could have city/state/zip
    # required_fields = ['chain name/chain id/sic code/NAICS', 'address']
    # optional_fields = ['city', 'state', 'zip']
    validation_fields = {'chain name/chain id/sic code/NAICS': True, 'address': True, 'city': False,
                         'state': False, 'zip': False}
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
        if validation_fields[required_field] and not found:
            raise Exception(f'Field {required_field} not founded in headers!')
    return column_validation_fields


def process_created(data, context):
    try:
        file_name = data['name']
        if file_name.endswith('/'):
            print(f'NOPFolder created {file_name}, ignored')
            logging.info(f'Folder created {file_name}, ignored')
            return
        logging.info(f'File: {file_name}')
        print(f'NOPFile: {file_name}')
        raw_data = pd.read_csv(f'gs://{bucket}/{file_name}', sep=',')
        column_validation_fields = _verify_fields(raw_data.keys())
        desired_data = raw_data[column_validation_fields]
        desired_data.rename(columns=lambda name: name.replace(' ', '_'), inplace=True)
        temp_table = f'tmp_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
        logging.info(f'Will write to table: {temp_table}')
        desired_data.to_gbq(f'{dataset}.{temp_table}', progress_bar=False)
        print(f'Rows: {len(desired_data.index)}')
    except Exception as e:
        logging.error('Error unexpected')
        print(f'Unexpected error: {e}')


# process_created({'name': 'dviorel/Sample_1 updated.csv'}, None)
process_created({'name': 'dviorel/sample_2_subset.csv'}, None)
