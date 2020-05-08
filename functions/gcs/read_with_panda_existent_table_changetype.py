# from google.cloud import storage
import pandas as pd

dataset = "dannyv"
# table = "test_01"
table = "test_02"
project = "cptsrewards-hrd"
bucket = 'location_matching'
# gcp_staging = 'inmarket-datasci'

# client = storage.Client()
# bucket = client.get_bucket('location_matching')
# blob = bucket.blob('file')
# dest_file = '/tmp/file.csv'
# blob.download_to_filename(dest_file)


def process_gcs_created(data, context):
    try:
        print(f"File: {data['name']}")
        # df = pd.read_csv(f"gs://{bucket}/{data['name']}", names=['col0', 'col1'], dtype=[str, str])
        df = pd.read_csv(f"gs://{bucket}/{data['name']}", names=['col0', 'col1'])
        # df.rename(index={0: 'col0', 1: 'col1'})
        # df = df.astype({'col0': 'string', 'col1': 'string'})
        # df = df.astype({'col0': 'string'})
        # df = df.astype({'col1': 'string'})
        # df.astype('string')
        # df = df.astype({'col0': str})
        df = df.astype(str)
        df.to_gbq(f'{dataset}.{table}', if_exists='append', progress_bar=False)
        print(f'Rows:{len(df.index)}')
    except NotImplementedError as e:
        print(f'Unexpected error: {e}')


process_gcs_created({'name': 'test_04.csv'}, None)
