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

logging.basicConfig(level=logging.DEBUG)


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
    zip_code = str(zip_code)
    if zip_code is None or len(zip_code) == 0:
        return zip_code
    if len(zip_code) > 5:
        return zip_code[:5]
    if len(zip_code) < 5:
        return zip_code.ljust(5, '0')
    return zip_code


def _get_state_code(state, df_states):
    try:
        if len(state) == 2:
            return state
    except Exception as e:
        return state
    if state == 'Washington DC':
        state = 'Washington'
    match = df_states[df_states['state_name'].str.strip().str.lower() == state.strip().lower()]
    if len(match) == 0:
        return ' '
    #    raise Exception(f'No abbreviation for state {state}!')
    if len(match) > 1:
        raise Exception(f'{len(match)} occurrences abbreviation for state {state}!')
    return match.iloc[0]['state_abbr']


def _get_chain_name(chain_id, df_chains):
    match = df_chains[df_chains['chain_id'] == chain_id]
    if len(match) == 0:
        raise Exception(f'No chain for chain_id {chain_id}!')
    if len(match) > 1:
        raise Exception(f'{len(match)} occurrences for chain_id {chain_id}!')
    return match.iloc[0]['name']


def _split_address_data(address_full, df_states, df_cities, include_zip, first_state):
    # remove commas
    address_full = address_full.replace(',', ' ')
    # if address_full == '6545 TRIGO RD ISLA VISTA':
    if '2400 ALABAMA ST BELLINGHAM WA 98229' in address_full:
        stop = True
    tokens = address_full.split(' ')
    tokens = list(filter(None, tokens))
    length = len(tokens)
    if length < 4:
        raise Exception(f'Just {length} tokens founded for {address_full}, waiting 4 at less')
    if include_zip:
        zip_code = tokens[len(tokens) - 1]
    else:
        zip_code = None
    # look for state
    found = False
    state = ''

    if not first_state:
        state_position = len(tokens) - (1 if include_zip else 2) - 1
        expected_match = df_states[df_states['state_abbr'].str.upper() == tokens[state_position].upper()]
        if len(expected_match.index) > 0:
            state = tokens[state_position]
            found = True
        if not found:
            expected_match = df_states[df_states['state_name'].str.upper() == tokens[state_position].upper()]
            if len(expected_match.index) > 0:
                state = tokens[state_position]
                found = True
        if not found:
            expected_match = df_states[df_states['state_name'].str.lower() == tokens[state_position - 1].lower()
                                       + ' ' + tokens[state_position].lower()]
            if len(expected_match.index) > 0:
                state = tokens[state_position - 1] + ' ' + tokens[state_position]
                found = True
    else:
        state_position = len(tokens) - (2 if include_zip else 3) - 1
        expected_match = df_states[df_states['state_abbr'].str.upper() == tokens[state_position].upper()]
        if len(expected_match.index) > 0:
            state = tokens[state_position]
            found = True
        if not found:
            expected_match = df_states[df_states['state_name'].str.upper() == tokens[state_position].upper()]
            if len(expected_match.index) > 0:
                state = tokens[state_position]
                found = True
        if not found:
            expected_match = df_states[df_states['state_name'].str.lower() == tokens[state_position - 1].lower()
                                       + ' ' + tokens[state_position].lower()]
            if len(expected_match.index) > 0:
                state = tokens[state_position - 1] + ' ' + tokens[state_position]
                found = True
    if found:
        if first_state:
            if include_zip:
                city = address_full[address_full.rfind(state) + len(state):address_full.rfind(' ')]
            else:
                city = address_full[address_full.rfind(state) + len(state):]
            address = address_full[:address_full.rfind(city)]
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

            # 3 chars
            if not city_found and len(sub_address_tokens) > 4:
                expected_city = sub_address_tokens[len(sub_address_tokens) - 3] + ' ' + \
                                sub_address_tokens[len(sub_address_tokens) - 2] + ' ' + \
                                sub_address_tokens[len(sub_address_tokens) - 1]
                expected_match = filtered_cities[filtered_cities['city'].str.lower() == expected_city.lower()]
                if len(expected_match.index) > 0:
                    city = expected_city
                    address = sub_address[:sub_address.rfind(expected_city)]
                    city_found = True
            # 2 chars
            if not city_found and len(sub_address_tokens) > 3:
                expected_city = sub_address_tokens[len(sub_address_tokens) - 2] + ' ' + \
                                sub_address_tokens[len(sub_address_tokens) - 1]
                expected_match = filtered_cities[filtered_cities['city'].str.lower() == expected_city.lower()]
                if len(expected_match.index) > 0:
                    city = expected_city
                    address = sub_address[:sub_address.rfind(expected_city)]
                    city_found = True
            # 1 char
            if not city_found:
                expected_match = filtered_cities[filtered_cities['city'].str.lower() == city.lower()]
                if len(expected_match.index) > 0:
                    address = sub_address[:sub_address.rfind(city)]
                    city_found = True

            # if not city_found:
            #     for index_city, row_city in filtered_cities.iterrows():
            #         if row_city['city'] is not None and row_city['city'].lower() in address_full.lower():
            #             position = address_full.lower().rfind(row_city['city'].lower())
            #             city = address_full[position:position + len(row_city['city'])]
            #             address = address_full[:position]
            #             break
    if not found:
        for index, row in df_states.iterrows():
            if not first_state:
                state_position = len(tokens) - (1 if include_zip else 2) - 1
                if row['state_abbr'] == 'WA':
                    stop_this = 1
                if len(tokens[state_position]) == 2 and tokens[state_position].lower() == row['state_abbr'].lower():
                    state = row['state_abbr']
                    found = True
                if tokens[state_position].lower() == row['state_name'].lower():
                    state = row['state_name']
                    found = True
                if tokens[state_position - 1].lower() + ' ' + tokens[state_position].lower() == \
                        row['state_name'].lower():
                    state = row['state_name']
                    found = True
            else:
                state_position = len(tokens) - (2 if include_zip else 3) - 1
                if len(tokens[state_position]) == 2 and tokens[state_position].upper() == row['state_abbr'].upper():
                    state = row['state_abbr']
                    found = True
                if len(tokens[state_position - 1]) == 2 and tokens[state_position - 1].upper() == row['state_abbr'].upper():
                    state = row['state_abbr']
                    found = True
                if tokens[state_position].lower() == row['state_name'].lower():
                    state = row['state_name']
                    found = True
                if tokens[state_position - 1].lower() == row['state_name'].lower():
                    state = row['state_name']
                    found = True
                if tokens[state_position - 1].lower() + ' ' + tokens[state_position].lower() == \
                        row['state_name'].lower():
                    state = row['state_name']
                    found = True
            if not found:
                if row['state_name'].lower() in address_full.lower():
                    position = address_full.lower().rfind(row['state_name'].lower())
                    # state = row['state_name']
                    state = address_full[position:position + len(row['state_name'])]
                    found = True
            if found:
                if first_state:
                    if include_zip:
                        city = address_full[address_full.rfind(state) + len(state):address_full.rfind(' ')]
                    else:
                        city = address_full[address_full.rfind(state) + len(state):]
                    address = address_full[:address_full.rfind(city)]
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

                    # 3 chars
                    if not city_found and len(sub_address_tokens) > 4:
                        expected_city = sub_address_tokens[len(sub_address_tokens) - 3] + ' ' + \
                                        sub_address_tokens[len(sub_address_tokens) - 2] + ' ' + \
                                        sub_address_tokens[len(sub_address_tokens) - 1]
                        expected_match = filtered_cities[filtered_cities['city'].str.lower() == expected_city.lower()]
                        if len(expected_match.index) > 0:
                            city = expected_city
                            address = sub_address[:sub_address.rfind(expected_city)]
                            city_found = True
                    # 2 chars
                    if not city_found and len(sub_address_tokens) > 3:
                        expected_city = sub_address_tokens[len(sub_address_tokens) - 2] + ' ' + \
                                        sub_address_tokens[len(sub_address_tokens) - 1]
                        expected_match = filtered_cities[filtered_cities['city'].str.lower() == expected_city.lower()]
                        if len(expected_match.index) > 0:
                            city = expected_city
                            address = sub_address[:sub_address.rfind(expected_city)]
                            city_found = True
                    # 1 char
                    if not city_found:
                        expected_match = filtered_cities[filtered_cities['city'].str.lower() == city.lower()]
                        if len(expected_match.index) > 0:
                            address = sub_address[:sub_address.rfind(city)]
                            city_found = True

                    # if not city_found:
                    #     for index_city, row_city in filtered_cities.iterrows():
                    #         if row_city['city'] is not None and row_city['city'].lower() in address_full.lower():
                    #             position = address_full.lower().rfind(row_city['city'].lower())
                    #             city = address_full[position:position + len(row_city['city'])]
                    #             address = address_full[:position]
                    #             break
                break
    if not found:
        raise Exception(f'No data found for {address_full}')
    return address, state, city, zip_code


def process_created(data, context):
    try:
        logging.debug(f'Started {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        validation_fields = {'chain/chain id/sic code/naics': (True, 'chain_name'),
                             'address/address full/address full (no zip)': (False, 'address'),
                             'city': (False, 'city'), 'state': (False, 'state'), 'zip': (False, 'zip')}
        file_name = data['name']
        if file_name.endswith('/'):
            logging.info(f'Folder created {file_name}, ignored')
            return
        logging.info(f'File: {file_name}')
        # raw_data = pd.read_csv(f'gs://{bucket}/{file_name}', sep=',')
        try:
            raw_data = pd.read_csv(f'gs://{bucket}/{file_name}', sep='\t')
        except ValueError:
            raw_data = pd.read_csv(f'gs://{bucket}/{file_name}', sep=',')

        logging.debug(f'Data readed {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

        raw_data.columns = map(str.lower, raw_data.columns)
        column_validation_fields = _verify_fields(raw_data.keys(), validation_fields)
        selected_columns = raw_data[column_validation_fields].rename(columns=lambda name: name.replace(' ', '_').
                                                                     replace('(', '_').replace(')', '_'), inplace=False)
        logging.debug(f'selected_columns started')
        # Read all US states
        credentials, your_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        bq_client = bigquery.Client(project=project, credentials=credentials)
        bq_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient(credentials=credentials)
        df_states = (bq_client.query('SELECT state_abbr, state_name from aggdata.us_states').result().
                     to_dataframe(bqstorage_client=bq_storage_client))

        # Complete columns not present in file
        for key in validation_fields:
            if not validation_fields[key][0] and validation_fields[key][1] not in selected_columns:
                selected_columns[validation_fields[key][1]] = ''
        if 'chain_id' in selected_columns.columns:
            df_chains = (bq_client.query('SELECT chain_id, name, sic_code from `inmarket-archive`.scansense.chain').
                         result().to_dataframe(bqstorage_client=bq_storage_client))
            selected_columns['chain'] = selected_columns['chain_id'].apply(lambda chain_id:
                                                                           _get_chain_name(chain_id, df_chains))
        logging.debug(f'Will verify {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        if 'address_full__no_zip_' in selected_columns.columns or 'address_full' in selected_columns.columns \
                or 'address_full__address_state_city_zip_' in selected_columns.columns:
            df_cities = (bq_client.query('select distinct city, state from (select '
                                         'distinct city, state from  `aggdata.locations_no_distributors` union '
                                         'all select distinct city, state from `aggdata.location_geofence`)').
                         result().to_dataframe(bqstorage_client=bq_storage_client))
            logging.debug(f'Cities readed {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            address = None
            state = None
            city = None
            zip_code = None
            logging.debug(f'Will processs {len(selected_columns.index)} rows {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            for index, row in selected_columns.iterrows():
                if index % 500 == 0:
                    logging.debug(f'Row {index} of {len(selected_columns.index)} {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
                if 'address_full__no_zip_' in selected_columns.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full__no_zip_'], df_states,
                                                                         df_cities, False, True)
                if 'address_full' in selected_columns.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full'], df_states,
                                                                         df_cities, True, False)
                if 'address_full__address_state_city_zip_' in selected_columns.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full__address_state_city_zip_'],
                                                                         df_states, df_cities, True, True)
                selected_columns.at[index, 'address'] = address
                selected_columns.at[index, 'state'] = state
                selected_columns.at[index, 'city'] = city
                selected_columns.at[index, 'zip'] = zip_code

        selected_columns['zip'] = selected_columns['zip'].apply(lambda zip_code_lambda: _clean_zip(zip_code_lambda))
        selected_columns['state'] = selected_columns['state'].apply(lambda state_lambda:
                                                                    _get_state_code(state_lambda, df_states))
        selected_columns.rename(columns={'chain': 'chain_name', 'address': 'street_address'}, inplace=True)
        selected_columns['category'] = None
        # selected_columns['sic_code'] = None
        selected_columns['lat'] = None
        selected_columns['lon'] = None
        if 'address_full' in selected_columns.columns:
            selected_columns = selected_columns.drop(['address_full'], axis=1)
        if 'sic_code' not in selected_columns.columns:
            selected_columns['sic_code'] = None
        # selected_columns = selected_columns.drop(['address_full'], axis=1)
        temp_table = f'tmp_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
        # temp_table = f'sic_code_match'
        logging.info(f'Will write to table: {temp_table}')
        selected_columns.to_gbq(f'{dataset}.{temp_table}', project_id=project, progress_bar=False)
        logging.debug(f'Finished {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    except Exception as e:
        logging.exception(f'Unexpected error {e}')


# process_created({'name': 'dviorel/Sample_1 updated.csv'}, None)
# process_created({'name': 'dviorel/sample_2_small_subset_nozip.csv'}, None)

# process_created({'name': 'dviorel/matching_list.txt'}, None)
# ###  process_created({'name': 'dviorel/store_only_zip.txt'}, None)
# process_created({'name': 'dviorel/walmart_match_issue.txt'}, None)
# process_created({'name': 'dviorel/store_full_address.txt'}, None)
# process_created({'name': 'dviorel/simple_list.txt'}, None)
process_created({'name': 'dviorel/sic_code_match.txt'}, None)
# process_created({'name': 'dviorel/match_multiple_sic_codes.txt'}, None)
