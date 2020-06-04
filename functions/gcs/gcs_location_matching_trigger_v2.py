import datetime
import google.auth
import pandas as pd
import pytz
import smtplib
import traceback
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
from google.cloud import storage
from os.path import basename

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
# TODO: Volver a poner locatin_matching
# bucket = 'location_matching'
bucket = 'dannyv'
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
    # Set the default timeout, if missing
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
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    logging.warning(f'It get response')
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text


def _set_table_expiration(dataset, table_name, expiration_days, bq_client):
    expiration = datetime.datetime.now(pytz.timezone('America/Los_Angeles')) + datetime.timedelta(days=expiration_days)
    table_ref = bq_client.dataset(dataset).table(table_name)
    table = bq_client.get_table(table_ref)
    table.expires = expiration
    bq_client.update_table(table, ["expires"])


def _sanitize_file_name(file_name):
    for c in ' -!@#$%^&*()=+{}[];:"\'?.,':
        file_name = file_name.replace(c, '_')
    return file_name


def _send_mail(mail_from, send_to, subject, body, attachments=None):
    assert isinstance(send_to, list)
    msg = MIMEMultipart()
    msg['From'] = mail_from
    msg['To'] = ','.join(send_to)
    msg['Subject'] = subject
    for attachment in attachments or []:
        with open(attachment, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(attachment)
            )
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(attachment)
        msg.attach(part)

    msg.attach(MIMEText(body))
    smtp = smtplib.SMTP(mail_server, port=587)
    smtp.starttls()
    smtp.login(mail_user, mail_password)
    smtp.sendmail(mail_from, send_to, msg.as_string())
    smtp.close()


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


def _add_clean_fields(table, bq_client):
    query = f""" CREATE TEMP FUNCTION cleanStr(str string, type string) RETURNS string
                              LANGUAGE js AS \"\"\"
                                return cleanStr(str, type)
                            \"\"\"
                            OPTIONS (library=['gs://javascript_lib/addr_functions.js']);
                            create or replace table {data_set_original}.{table} as 
                            select *, cleanstr(chain_name, 'chain') clean_chain, 
                            cleanstr(street_address, 'addr') clean_addr, cleanstr(city, 'city') clean_city
                            from `{data_set_original}.{table}`  
        """
    query_job = bq_client.query(query, project=project)
    query_job.result()


def _add_state_from_zip(table, bq_client):
    query = f"""create or replace table {data_set_original}.{table} as 
            select a.* except(state), z.state
            from {data_set_original}.{table} a
            left join (
                select *, ROW_NUMBER() OVER(partition by zip) as row_no from(
                select distinct state, zip from aggdata.location_geofence)
            )z 
            on a.zip = z.zip
            where z.row_no = 1
        """
    query_job = bq_client.query(query, project=project)
    query_job.result()


def _split_address_data(address_full, df_states, df_cities, include_zip, first_state):
    address_full = address_full.replace(',', ' ')
    tokens = address_full.split(' ')
    tokens = list(filter(None, tokens))
    length = len(tokens)
    if length < 3:
        raise Exception(f'Just {length} tokens founded for {address_full}, waiting 3 at less')
    zip_code = tokens[len(tokens) - 1] if include_zip else None
    state_position = len(tokens) - (1 if include_zip else 0) - 1 - (1 if first_state else 0)
    found, state = _verify_match_df(df_states[df_states['state_abbr'] == tokens[state_position].upper()],
                                    tokens[state_position])
    if not found and first_state:
        found, state = _verify_match_df(df_states[df_states['state_abbr'] == tokens[state_position - 1].upper()],
                                        tokens[state_position - 1])
    if not found:
        found, state = _verify_match_df(df_states[df_states['state_name'].str.lower() == tokens[state_position].lower()]
                                        , tokens[state_position])
    if not found and first_state:
        found, state = _verify_match_df(df_states[df_states['state_name'].str.lower() ==
                                                  tokens[state_position - 1].lower()], tokens[state_position - 1])
    if not found:
        found, state = _verify_match_df(df_states[df_states['state_name'].str.lower() == tokens[state_position - 1]
                                        .lower() + ' ' + tokens[state_position].lower()], tokens[state_position - 1] +
                                        ' ' + tokens[state_position])
    if not found:
        for index, row in df_states.iterrows():
            state_position = len(tokens) - (1 if include_zip else 2) - 1
            found, state = _verify_match(tokens[state_position], row['state_abbr'])
            if not found and first_state:
                found, state = _verify_match(tokens[state_position - 1], row['state_abbr'])
            if not found:
                found, state = _verify_match(tokens[state_position], row['state_name'])
            if not found and first_state:
                found, state = _verify_match(tokens[state_position - 1], row['state_name'])
            if not found:
                found, state = _verify_match(tokens[state_position - 1] + ' ' + tokens[state_position],
                                             row['state_abbr'])
            if not found:
                if row['state_name'].lower() in address_full.lower():
                    position = address_full.lower().rfind(row['state_name'].lower())
                    state = address_full[position:position + len(row['state_name'])]
                    found = True
            if found:
                break
    if found:
        if first_state:
            if include_zip:
                city = address_full[address_full.rfind(state) + len(state):address_full.rfind(' ')]
            else:
                city = address_full[address_full.rfind(state) + len(state):]
            address = address_full[:address_full.rfind(state)]
            state = _get_state_code(state, df_states)
        else:
            # try city with the previous token
            city_found = False
            position = address_full.rfind(state)
            sub_address = address_full[:position]
            sub_address_tokens = sub_address.split(' ')
            sub_address_tokens = list(filter(None, sub_address_tokens))
            city = sub_address_tokens[len(sub_address_tokens) - 1]
            state = _get_state_code(state, df_states)
            address = sub_address[:sub_address.rfind(' ')]
            filtered_cities = df_cities[df_cities['state'] == state]

            # 3 tokens
            if not city_found and len(sub_address_tokens) > 4:
                expected_city = sub_address_tokens[len(sub_address_tokens) - 3] + ' ' + \
                                sub_address_tokens[len(sub_address_tokens) - 2] + ' ' + \
                                sub_address_tokens[len(sub_address_tokens) - 1]
                expected_match = filtered_cities[filtered_cities['city'].str.lower() == expected_city.lower()]
                if len(expected_match.index) > 0:
                    city = expected_city
                    address = sub_address[:sub_address.rfind(expected_city)]
                    city_found = True
            # 2 tokens
            if not city_found and len(sub_address_tokens) > 3:
                expected_city = sub_address_tokens[len(sub_address_tokens) - 2] + ' ' + \
                                sub_address_tokens[len(sub_address_tokens) - 1]
                expected_match = filtered_cities[filtered_cities['city'].str.lower() == expected_city.lower()]
                if len(expected_match.index) > 0:
                    city = expected_city
                    address = sub_address[:sub_address.rfind(expected_city)]
                    city_found = True
            # 1 token
            if not city_found:
                expected_match = filtered_cities[filtered_cities['city'].str.lower() == city.lower()]
                if len(expected_match.index) > 0:
                    address = sub_address[:sub_address.rfind(city)]
                    city_found = True

    if not found:
        filtered_cities = df_cities[df_cities['state'] == state]
        for index_city, row_city in filtered_cities.iterrows():
            if row_city['city'] is not None and row_city['city'].lower() in address_full.lower():
                position = address_full.lower().rfind(row_city['city'].lower())
                city = address_full[position:position + len(row_city['city'])]
                address = address_full[:position]
                break
    if not found:
        # raise Exception(f'No data found for {address_full}')
        logging.warning(f'No data found for {address_full}')
        address = 'N/A'
        state = 'N/A'
        city = 'N/A'
        zip_code = 'N/A'
    return address, state, city, zip_code


def process_location_matching(data, context):
    try:
        logging.debug(f'Started {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        validation_fields = {'sic code': 'sic_code', 'chain name/chain id': 'chain_name',
                             'address/address full/address full (no zip)': 'address',
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
        credentials, your_project_id = google.auth.default(scopes=[url_auth_gcp])
        bq_client = bigquery.Client(project=project, credentials=credentials)
        bq_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient(credentials=credentials)
        
        # logging.warning(f'Will write raw table: {file_name}')
        raw_data.columns = map(str.lower, raw_data.columns)
        raw_data.columns = map(str.strip, raw_data.columns)
        column_validation_fields = _verify_fields(raw_data.keys(), validation_fields)
        pre_processed_data = raw_data[column_validation_fields].\
            rename(columns=lambda name: name.replace(' ', '_').replace('(', '_').replace(')', '_'), inplace=False)
        df_states = (bq_client.query(query_states).result().
                     to_dataframe(bqstorage_client=bq_storage_client))
        # Complete columns not present in file
        should_add_state_from_zip = 'state' not in pre_processed_data.columns and 'zip' in pre_processed_data.columns
        has_chain = 'chain_id' in pre_processed_data.columns or 'chain_name' in pre_processed_data.columns
        has_sic_code = 'sic_code' in pre_processed_data.columns
        # has_sic_code = has_sic_code and not has_chain
        for key in validation_fields:
            if validation_fields[key] not in pre_processed_data:
                pre_processed_data[validation_fields[key]] = None
        logging.debug(f'has_sic_code: {has_sic_code} ...{original_name}')
        if 'chain_id' in pre_processed_data.columns:
            df_chains = (bq_client.query(query_chains).
                         result().to_dataframe(bqstorage_client=bq_storage_client))
            pre_processed_data['chain'] = pre_processed_data['chain_id'].apply(lambda chain_id:
                                                                               _get_chain_name(chain_id, df_chains))

        if 'address_full__no_zip_' in pre_processed_data.columns or 'address_full' in pre_processed_data.columns \
                or 'address_full__address_state_city_zip_' in pre_processed_data.columns:
            df_cities = (bq_client.query(query_cities).
                         result().to_dataframe(bqstorage_client=bq_storage_client))
            logging.debug(f'Cities readed ...{original_name}')
            address = None
            state = None
            city = None
            zip_code = None
            logging.debug(f'Will processs {len(pre_processed_data.index)} rows ...{original_name}')
            for index, row in pre_processed_data.iterrows():
                if index % 500 == 0:
                    logging.debug(f'Row {index} of {len(pre_processed_data.index)} ...{original_name}')
                if 'address_full__no_zip_' in pre_processed_data.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full__no_zip_'], df_states,
                                                                         df_cities, False, True)
                if 'address_full' in pre_processed_data.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full'], df_states,
                                                                         df_cities, True, False)
                if 'address_full__address_state_city_zip_' in pre_processed_data.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full__address_state_city_zip_'],
                                                                         df_states, df_cities, True, True)
                pre_processed_data.at[index, 'address'] = address
                pre_processed_data.at[index, 'state'] = state
                pre_processed_data.at[index, 'city'] = city
                pre_processed_data.at[index, 'zip'] = zip_code

        pre_processed_data['zip'] = pre_processed_data['zip'].apply(lambda zip_code_lambda: _clean_zip(zip_code_lambda))
        pre_processed_data['state'] = pre_processed_data['state'].apply(lambda state_lambda:
                                                                        _get_state_code(state_lambda, df_states))
        pre_processed_data.rename(columns={'address': 'street_address'}, inplace=True)
        pre_processed_data['category'] = None
        pre_processed_data['lat'] = None
        pre_processed_data['lon'] = None
        if 'address_full' in pre_processed_data.columns:
            pre_processed_data = pre_processed_data.drop(['address_full'], axis=1)
        # preprocessed_table = f'{destination_email[:destination_email.index("@")]}_{file_name}_{now}'
        preprocessed_table = file_name.lower()
        logging.warning(f'Will write to table: {preprocessed_table} ...{original_name}')
        pre_processed_data.to_gbq(f'{data_set_original}.{preprocessed_table}', project_id=project, progress_bar=False,
                                  if_exists='replace')
        _set_table_expiration(data_set_original, preprocessed_table, expiration_days_original_table, bq_client)
        logging.warning(f'Will add clean fields: {preprocessed_table} ...{original_name}')
        _add_clean_fields(preprocessed_table, bq_client)
        if should_add_state_from_zip:
            logging.warning(f'Will add states from zip codes: {preprocessed_table} ...{original_name}')
            _add_state_from_zip(preprocessed_table, bq_client)
        # Call the DAG
        webserver_url = ('https://' + webserver_id + '.appspot.com/api/experimental/dags/'
                         + dag_name + '/dag_runs')
        # Make a POST request to IAP which then Triggers the DAG
        data['table'] = preprocessed_table
        data['destination_email'] = destination_email
        data['has_sic_code'] = has_sic_code
        data['has_chain'] = has_chain
        data['original_file_name'] = original_name
        data['full_result'] = full_result
        _make_iap_request(webserver_url, client_id, method='POST', json={"conf": data})
        storage_client = storage.Client()
        if delete_gcs_files and '___no_mv_gcs' not in file_full_name:
            source_bucket = storage_client.get_bucket(f'{bucket}')
            source_bucket.delete_blob(original_name)
        elif '___no_mv_gcs' not in file_full_name:
            source_bucket = storage_client.get_bucket(f'{bucket}')
            from_blob = source_bucket.blob(original_name)
            source_bucket.copy_blob(from_blob, source_bucket,
                                    new_name=f'processed/{destination_email}/{now}_{file_name}.txt')
            source_bucket.delete_blob(from_blob.name)
        logging.warning(f'Process finished for {file_full_name}')
    except Exception as e:
        logging.exception(f'Unexpected error {e}')
        if send_email_on_error:
            try:
                _send_mail(mail_from, email_error, 'location_matching tool error',
                           f'Error processing location matching: {traceback.format_exc()}')
            except Exception:
                pass
        if fail_on_error:
            raise e


# process_location_matching({'name': 'dviorel@inmarket.com/simple_list___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/chain id _ name both___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/address full - no zip___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/address full - no zip_curated___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/address full - no zip_curated_good___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/multiple_chain_ids___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/multiple_chain_ids_one_row___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/simple list 2.with.point!$%^&_-+=-, ___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/simple_list_ch___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/sic code match___no_mv_gcs.txt'}, None)
process_location_matching({'name': 'dviorel@inmarket.com/multiple_chain_ids_one_row_addr.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/Matching_list_nozip___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/walmart_list_with_match_issue_6___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/Multiple chain ids___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/walmart_list_with_match_issue___no_mv_gcs.txt'}, None)
# _send_mail('dviorel@inmarket.com', ['dviorel@inmarket.com'], 'My test', 'The body')
