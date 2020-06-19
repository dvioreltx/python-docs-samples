import datetime
import pandas as pd
import base64
import traceback
from enum import Enum
from os.path import basename

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, Disposition

from google.auth.transport.requests import Request
from google.oauth2 import id_token
import logging
import requests

IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'
client_id = '340376694278-ss8m9rl3vs610d3lfmnn42mcrk3467ns.apps.googleusercontent.com'
webserver_id = 'u5d9ecf3bb91ae6ad-tp'
dag_name = 'gcs_location_matching_created'

delete_intermediate_tables = False
delete_gcs_files = False
enable_trigger = True
send_email_on_error = True
data_set_original = "location_matching_file"

bucket = 'location_matching'
email_error = 'dviorel@inmarket.com'
expiration_days_original_table = 7
expiration_days_results_table = 30

logging.basicConfig(level=logging.DEBUG)

fail_on_error = False
project = "cptsrewards-hrd"
url_auth_gcp = 'https://www.googleapis.com/auth/cloud-platform'
query_states = 'SELECT state_abbr, state_name from aggdata.us_states'
query_chains = 'SELECT chain_id, name, sic_code from `inmarket-archive`.scansense.chain'
query_cities = 'select distinct city, state from (select distinct city, state from  `aggdata.' \
               'locations_no_distributors` union all select distinct city, state from `aggdata.location_geofence`)'


class LMAlgo(Enum):
    CHAIN = 1
    SIC_CODE = 2


def _make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.
    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.
    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 90
    logging.warning(f'Data: {kwargs}')
    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    logging.warning(f'Will ge token')
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)
    logging.warning(f'Token obtained')
    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    logging.warning(f'Will call request')
    resp = requests.request(method, url, headers={'Authorization': 'Bearer {}'.format(google_open_id_connect_token)},
                            **kwargs)
    logging.warning(f'It get response')
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(resp.status_code, resp.headers, resp.text))
    else:
        return resp.text


def _sanitize_file_name(file_name):
    for c in ' -!@#$%^&*()=+{}[];:"\'?.,':
        file_name = file_name.replace(c, '_')
    return file_name


def _send_mail(send_to, subject, body, attachments=None):
    logging.info(f'Will send email with title: {subject} to {send_to}')
    message = Mail(from_email='data-eng@inmarket.com', to_emails=f'{send_to}', subject=subject, html_content=body)
    for attachment in attachments or []:
        with open(attachment, "rb") as f:
            data = f.read()
            f.close()
            encoded_file = base64.b64encode(data).decode()
            attached_file = Attachment(FileContent(encoded_file), FileName(basename(attachment)), None,
                                       Disposition('attachment'))
        message.add_attachment(attached_file)
    sg = SendGridAPIClient('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')
    response = sg.send(message)
    logging.warning(f'Result: {response.status_code}')


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


def process_location_matching(data, context):
    destination_email = None
    cf_name = None
    try:
        logging.debug(f'Started {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        validation_fields = {'sic code': 'sic_code', 'chain name/chain id': 'chain_name',
                             'address/address full/address full (no zip)/address full (address, state, city, zip)':
                                 'address',
                             'city': 'city', 'state': 'state', 'zip': 'zip'}
        original_name = data['name']
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
        destination_email = file_full_name[:file_full_name.index('/')]
        cf_name = original_name[original_name.index('/') + 1:]
        if '@' not in destination_email:
            logging.error(f'{destination_email} is not a valid email ...{original_name}')
            return
        if not file_full_name.endswith('.txt'):
            _send_mail(destination_email, f'File format error in Location Matching Tool for "{cf_name}"',
                       f'"{cf_name}" is not a valid supported file type. Please verify the file '
                       f'format is "Tab delimited Text(.txt)" before resubmitting for matching.')
            return
        file_name = file_full_name[file_full_name.rfind('/') + 1:]
        now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        if '.' in file_name:
            file_name = file_name[:file_name.rfind('.')]
        file_name = _sanitize_file_name(file_name)
        logging.info(f'Email: {destination_email} ...{original_name}')
        try:
            raw_data = pd.read_csv(f'gs://{bucket}/{original_name}', sep='\t', encoding='utf-8')
        except Exception:
            try:
                raw_data = pd.read_csv(f'gs://{bucket}/{original_name}', sep='\t', encoding='iso-8859-1')
            except Exception as e:
                logging.error(f'Error reading file, will send email format {e} and exit function: '
                              f'{traceback.format_exc()}')
                _send_mail(destination_email, f'File format error in Location Matching Tool for "{cf_name}"',
                           f'"{cf_name}" is not a valid supported file type. Please verify the file '
                           f'format is "Tab delimited Text(.txt)" before resubmitting for matching.')
                return
        raw_data.columns = map(str.lower, raw_data.columns)
        raw_data.columns = map(str.strip, raw_data.columns)
        column_validation_fields = _verify_fields(raw_data.keys(), validation_fields)
        pre_processed_data = raw_data[column_validation_fields].\
            rename(columns=lambda name: name.replace(' ', '_').replace('(', '_').replace(')', '_').replace(',', '_'),
                   inplace=False)
        has_chain = 'chain_id' in pre_processed_data.columns or 'chain_name' in pre_processed_data.columns
        has_sic_code = 'sic_code' in pre_processed_data.columns
        has_zip = 'zip' in raw_data.columns
        has_city = 'city' in raw_data.columns
        has_state = 'state' in raw_data.columns
        if 'chain id' not in raw_data.columns and 'chain name' not in raw_data.columns \
                and 'sic code' not in raw_data.columns:
            logging.error(f'File {original_name} doesnt have required columns, will send email.')
            _send_mail(destination_email, f'Missing required fields error in Location Matching Tool for "{cf_name}"',
                       f'File "{cf_name}" must contain at least one of the following required fields - chain id, '
                       f'chain name or sic code. Please add the required field before resubmitting for matching.')
            return
        if 'address_full__no_zip_' in pre_processed_data.columns or 'address_full' in pre_processed_data.columns \
                or 'address_full__address_state_city_zip_' in pre_processed_data.columns \
                or 'address_full__address__state__city__zip_' in pre_processed_data.columns:
            has_city = True
            has_state = True
            if 'address_full' in pre_processed_data.columns or \
                    'address_full__address_state_city_zip_' in pre_processed_data.columns or \
                    'address_full__address__state__city__zip_' in pre_processed_data.columns:
                has_zip = True
        preprocessed_table = file_name.lower()
        has_multiple_chain_id = False
        has_multiple_chain_name = False
        if 'chain id' in raw_data.columns:
            # queried_df = raw_data[raw_data['chain id'].str.contains(',', na=False)]
            queried_df = raw_data[raw_data['chain id'].astype(str).str.contains(',', na=False)]
            if len(queried_df.index) > 0:
                has_multiple_chain_id = True
        if 'chain name' in raw_data.columns:
            # queried_df = raw_data[raw_data['chain name'].str.contains(';', na=False)]
            queried_df = raw_data[raw_data['chain name'].astype(str).str.contains(';', na=False)]
            if len(queried_df.index) > 0:
                has_multiple_chain_name = True
        webserver_url = ('https://' + webserver_id + '.appspot.com/api/experimental/dags/' + dag_name + '/dag_runs')
        # Make a POST request to IAP which then Triggers the DAG
        data['table'] = preprocessed_table
        data['has_sic_code'] = has_sic_code
        data['has_chain'] = has_chain
        data['has_zip'] = has_zip
        data['has_city'] = has_city
        data['has_state'] = has_state
        data['original_file_name'] = original_name
        data['destination_email'] = destination_email
        data['the_file_name'] = file_name
        data['file_name'] = cf_name
        data['has_multiple_chain_id'] = has_multiple_chain_id
        data['has_multiple_chain_name'] = has_multiple_chain_name
        _make_iap_request(webserver_url, client_id, method='POST', json={"conf": data})
        logging.warning(f'Process finished for {file_full_name}')
    except Exception as e:
        logging.exception(f'Unexpected error: {e}. Message: {traceback.format_exc()}. File: {data["name"]}')
        try:
            if destination_email is not None:
                _send_mail(destination_email, f'Error in Location Matching Tool for "{cf_name}"',
                           f'Unexpected error processing "{cf_name}". Please file an Engineering Support ticket '
                           f'(issue type —> reporting and analytics) for Data Engineering team to investigate the '
                           f'issue.')
            if send_email_on_error:
                _send_mail(email_error, f'Location Matching Tool error for "{cf_name}"',
                           f'Error processing location matching: {traceback.format_exc()}.<br />File: {data["name"]}')
        except Exception as e1:
            logging.exception(f'Unexpected error sending email {e1}: {traceback.format_exc()}')
        if fail_on_error:
            raise e


process_location_matching({'name': 'dviorel@inmarket.com/address full no zip_2___no_mv_gcs.txt'}, None)
