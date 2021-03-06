import datetime
import logging
import pandas as pd
import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
# import google.appengine.api.mail.send_mail
# from google.appengine.api import mail

dataset = "dannyv"
project = "cptsrewards-hrd"
bucket = 'location_matching'
logging.basicConfig(level=logging.DEBUG)


def process_email(data, context):
    try:
        logging.debug(f'Started {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

        file_name = data['name']
        if file_name.endswith('/'):
            logging.info(f'Folder created {file_name}, ignored')
            return
        logging.info(f'File: {file_name}')
        sender_address = ''
        mail.send_mail(sender=sender_address,
                       to="Albert Johnson <Albert.Johnson@example.com>",
                       subject="Your account has been approved",
                       body="""Test email:
    This is the first test.
""")

        # Determine which query should run
        logging.debug(f'Finished {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    except Exception as e:
        logging.exception(f'Unexpected error {e}')


def join_test():
    letters = ['uno', 'dos', 'tres']
    joined = ' union all '.join(letters)
    print(joined)


join_test()
# process_email({'name': 'dviorel@inmarket.com/unclean_list.txt'}, None)
