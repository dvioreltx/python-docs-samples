import datetime
import logging
import pandas as pd
import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1

dataset = "dannyv"
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
    match = df_chains[df_chains['chain_id'] == chain_id]
    if len(match) == 0:
        raise Exception(f'No chain for chain_id {chain_id}!')
    if len(match) > 1:
        raise Exception(f'{len(match)} occurrences for chain_id {chain_id}!')
    return match.iloc[0]['name']


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
                            create or replace table {dataset}.{table} as 
                            select *, cleanstr(chain_name, 'chain') clean_chain, 
                            cleanstr(street_address, 'addr') clean_addr, cleanstr(city, 'city') clean_city
                            from `{dataset}.{table}`  
        """
    query_job = bq_client.query(query, project=project)
    query_job.result()


def _run_location_matching_sic_codes(table, destination_table, bq_client):
    query = f""" #standardSQL
declare level_of_accuracy string default '';
set level_of_accuracy = 'relaxed';
create temporary function strMatchRate(str1 STRING, str2 string, type string, accuracy string) returns FLOAT64 
  language js as \"\"\"
    return scoreMatchFor(str1, str2, type, accuracy)
\"\"\"
OPTIONS (
library=['gs://javascript_lib/addr_functions.js']
);
  CREATE TABLE {dataset}.{destination_table} AS
  with
    sample as (
      select *, concat(sic_code, ',', ifnull(clean_addr, ''), ',', ifnull(clean_city,''), ',', ifnull(state,'')) store 
      from (
        select  substr(sic_code, 0 ,4) sic_code, clean_addr, 
          clean_city, state, zip
        from `{dataset}.{table}`
      )
    ),
    sic_code_array as (
      select split(sic_code) sic_arr 
      from (
        select distinct sic_code from sample
      )
    ),
    unique_sic_codes as (
      select array(
        select distinct regexp_replace(trim(x), ' ', '') from unnest(sic_codes) as x
      ) sic_codes
      from (
        select array_concat_agg(sic_arr) sic_codes from sic_code_array 
      )
    ),
    location_geofence as ( 
      select chain_name lg_chain, lat lg_lat, lon lg_lon, addr lg_addr, 
        city lg_city, state lg_state, substr(trim(zip),0,5) lg_zip, location_id,
        clean_chain clean_lg_chain, clean_addr clean_lg_addr, clean_city clean_lg_city, 
        substr(sic_code, 0 ,4) lg_sic_code
      from `aggdata.location_geofence_cleaned` 
      where substr(sic_code, 0 ,4) in unnest((select sic_codes from unique_sic_codes))  
    ),
    sample_lg_join as (
      select distinct sic_code, lg_sic_code, * except (sic_code, lg_sic_code)
      from sample join location_geofence on (clean_city = clean_lg_city or zip = lg_zip)
      where regexp_contains(sic_code, lg_sic_code)
    )
    select *,
      case
        when addr_match >= 1  then 'definitely'
        when addr_match >= .9 then 'very probably'
        when addr_match >= .8 then 'probably'
        when addr_match >= .7 then 'likely'
        when addr_match >= .6 then 'possibly'
        else                       'unlikely'
    end isa_match
    from (
      select *, row_number() over (partition by store order by addr_match desc, clean_lg_addr) ar
      from (
        select sic_code, lg_sic_code, clean_addr, clean_lg_addr, clean_city, clean_lg_city, state, lg_state, zip, 
          lg_zip, strmatchrate(clean_addr, clean_lg_addr, 'addr', 'sic_code') addr_match, store, location_id
        from sample_lg_join 
      )
    ) 
    where ar = 1; 
      """
    query_job = bq_client.query(query, project=project)
    query_job.result()


def _run_location_matching_chain_addr(table, destination_table, bq_client):
    query = f""" CREATE TEMP FUNCTION cleanStr(str string, type string) RETURNS string
  LANGUAGE js AS "return cleanStr(str, type)";
create temporary function strMatchRate(str1 STRING, str2 string, type string, city string) returns FLOAT64 
  language js as "return scoreMatchFor(str1, str2, type, city)";    
 create temporary function matchScore(chain_match float64, addr_match float64)
 returns float64
 language js as \"\"\"
var rawScore = 50*(1-chain_match) + 50*(1-addr_match);
var score =
(chain_match == 0 || addr_match == 0)  ? 100 :
chain_match >   1 && addr_match  >= .5 ? Math.min(5,  rawScore) :
chain_match >= .8 && addr_match  >= .5 ? Math.min(15, rawScore) :
addr_match  >= .8 && chain_match >= .5 ? Math.min(15, rawScore) :
chain_match >= .3 && addr_match  >= .9 ? Math.min(20, rawScore) :
rawScore;
return score;
\"\"\"
OPTIONS (
 library=['gs://javascript_lib/addr_functions.js']
);
create table {dataset}.{destination_table} as
with 
 stateAbbrs as (
   select * from `aggdata.us_states` 
 ),
 sample as ( 
   select *,  concat(chain, ',', ifnull(addr, ''), ',', ifnull(city,''), ',', ifnull(state,'')) store 
   from (
     Select chain,  addr, city, 
     case 
       when length(state) > 2 then state_abbr
       else state
     end state,
     cast(case
       when length(substr(zip,0,5)) = 3 then concat('00',zip)
       when length(substr(zip,0,5)) = 4 then concat('0',zip)     
       else substr(zip,0,5)
     end as string) zip,
     0 lat, 0 lon
     from ( 
       select chain_name chain, street_address  addr, city, state, zip      
       from `{dataset}.{table}` 
     ) a 
     left join stateAbbrs b on lower(a.state) = lower(state_name)
     where chain is not null
   )
 ),
 our_states as (
   select distinct state from sample
 ),
 aggdata as ( 
   select chain_name lg_chain, lat lg_lat, lon lg_lon, addr lg_addr, city lg_city, a.state lg_state, 
    substr(trim(zip),0,5) lg_zip, location_id     
   from `aggdata.location_geofence` a join our_states b on a.state = b.state
 ),
combined as (
  select * from sample a left join aggdata b on lg_zip = zip
),
chain_match_scores as (
  select *
  from (
    select *, 
      case 
        when lg_chain is null then 0
        else strMatchRate(lg_chain, chain, 'chain', city)
      end chain_match 
    from combined 
  )
),
chain_score_ranks as (
  select *, dense_rank() over (partition by store order by chain_match desc) chain_rank
  from chain_match_scores
),
addr_match_scores as (  
  select *, 
    case
      when lg_addr is null then -1
      else strMatchRate(lg_addr, addr, 'addr', city)       
    end addr_match,
    -1 best_distance  
  from chain_score_ranks
),
addr_score_ranks as (  
  select *, dense_rank() over (partition by store order by addr_match desc) addr_rank
  from addr_match_scores
),
combined_match_scores as (
  select *, matchScore(chain_match, addr_match) match_score
  from addr_score_ranks
),
combined_score_ranks as (
  select *,
    row_number() 
        over (partition by store order by match_score, addr_match desc, chain_match desc, best_distance) match_rank
  from combined_match_scores 
),
match_liklihood as (
  select chain_match, addr_match, match_score, match_rank, chain, 
    lg_chain, addr, lg_addr, city, lg_city, state, lg_state, lg_lat, lg_lon, location_id, store, 
    case
      when match_score <= 0  then 'A+'
      when match_score <= 10 then 'A'
      when match_score <= 20 then 'B'
      when match_score <= 30 then 'C'
      when match_score <= 75 then 'D'
      else                        'F'
    end grade,
    Case
      when match_score <= 0  then 'definitely'
      when match_score <= 10 then 'very probably'
      when match_score <= 20 then 'probably'
      when match_score <= 30 then 'likely'
      when match_score <= 75 then 'possibly'
      else                        'unlikely'
  end isa_match
  from combined_score_ranks
),
sample2 as (
  select distinct a.* from sample a join (select store from match_liklihood where match_rank = 1 
    and isa_match not in ('definitely', 'very probably', 'probably')) b on a.store = b.store 
),
combined2 as (
  select *,  
    case 
      when lg_chain is null then 0
      else strMatchRate(lg_chain, chain, 'chain', city)
    end chain_match  
    from sample2 a join aggdata b on state = lg_state and cleanStr(city, 'city') = cleanStr(lg_city, 'city')
),
chain_match_scores2 as (
  select * from combined2 where chain_match >= .2
),
chain_score_ranks2 as (
  select *, dense_rank() over (partition by store order by chain_match desc) chain_rank
  from chain_match_scores2
),
addr_match_scores2 as (  
  select *, 
    case
      when lg_addr is null then -1
      else strMatchRate(lg_addr, addr, 'addr', city)       
    end addr_match,
    -1 best_distance  
  from chain_score_ranks2
),
addr_score_ranks2 as (  
  select *, dense_rank() over (partition by store order by addr_match desc) addr_rank
  from addr_match_scores2
),
combined_match_scores2 as (
  select *, matchScore(chain_match, addr_match) match_score from addr_score_ranks2
),
combined_score_ranks2 as (
  select *,
    row_number() 
        over (partition by store order by match_score, addr_match desc, chain_match desc, best_distance) match_rank
  from combined_match_scores2 
),
match_liklihood2 as (
  select chain_match, addr_match, match_score, match_rank, chain, 
    lg_chain, addr, lg_addr, city, lg_city, state, lg_state, lg_lat, lg_lon, location_id, store, 
    case
      when match_score <= 0  then 'A+'
      when match_score <= 10 then 'A'
      when match_score <= 20 then 'B'
      when match_score <= 30 then 'C'
      when match_score <= 75 then 'D'
      else                        'F'
    end grade,
    Case
      when match_score <= 0  then 'definitely'
      when match_score <= 10 then 'very probably'
      when match_score <= 20 then 'probably'
      when match_score <= 30 then 'likely'
      when match_score <= 75 then 'possibly'
      else                        'unlikely'
  end isa_match
  from combined_score_ranks2
),
final as (
   select distinct *,
     row_number() over (partition by store order by match_score) match_rank
   from (
     select 'r1' match_round, * except (match_rank) from match_liklihood union all 
     select 'r2', * except (match_rank) from match_liklihood2
   )
)
select row_number() over () store_id, isa_match, round(100-match_score,1) match_score, grade, 
    * except (grade, isa_match, match_score, match_rank),
 cleanStr(chain, 'chain') clean_chain, cleanStr(lg_chain, 'chain') clean_lg_chain,
 cleanStr(addr,  'addr')  clean_addr,  cleanStr(lg_addr,  'addr')  clean_lg_addr
from final
where match_rank = 1 
       """
    query_job = bq_client.query(query, project=project)
    query_job.result()


def _split_address_data(address_full, df_states, df_cities, include_zip, first_state):
    address_full = address_full.replace(',', ' ')
    tokens = address_full.split(' ')
    tokens = list(filter(None, tokens))
    length = len(tokens)
    if length < 4:
        raise Exception(f'Just {length} tokens founded for {address_full}, waiting 4 at less')
    zip_code = tokens[len(tokens) - 1] if include_zip else None
    state_position = len(tokens) - (1 if include_zip else 2) - 1 - (1 if first_state else 0)
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
            state, found = _verify_match(tokens[state_position], row['state_abbr'])
            if not found and first_state:
                state, found = _verify_match(tokens[state_position - 1], row['state_abbr'])
            if not found:
                state, found = _verify_match(tokens[state_position], row['state_name'])
            if not found and first_state:
                state, found = _verify_match(tokens[state_position - 1], row['state_name'])
            if not found:
                state, found = _verify_match(tokens[state_position - 1] + ' ' + tokens[state_position],
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
        raise Exception(f'No data found for {address_full}')
    return address, state, city, zip_code


def process_created(data, context):
    try:
        logging.debug(f'Started {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        validation_fields = {'sic code': (False, 'sic_code'), 'chain name/chain id': (False, 'chain_name'),
                             'address/address full/address full (no zip)': (False, 'address'),
                             'city': (False, 'city'), 'state': (False, 'state'), 'zip': (False, 'zip')}
        file_name = data['name']
        if file_name.endswith('/'):
            logging.info(f'Folder created {file_name}, ignored')
            return
        logging.info(f'File: {file_name}')
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
                selected_columns[validation_fields[key][1]] = None
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
        # selected_columns.rename(columns={'chain': 'chain_name', 'address': 'street_address'}, inplace=True)
        selected_columns.rename(columns={'address': 'street_address'}, inplace=True)
        selected_columns['category'] = None
        selected_columns['lat'] = None
        selected_columns['lon'] = None
        if 'address_full' in selected_columns.columns:
            selected_columns = selected_columns.drop(['address_full'], axis=1)
        # temp_table = f'tmp{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
        temp_table = f'match_multiple_sic_codes'
        logging.info(f'Will write to table: {temp_table}')
        selected_columns.to_gbq(f'{dataset}.{temp_table}', project_id=project, progress_bar=False)
        logging.debug(f'will add new fields: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        _add_clean_fields(temp_table, bq_client)
        # Run location_matching
        logging.debug(f'will run location_matching: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        destination_table = temp_table + '_processed'

        _run_location_matching_sic_codes(temp_table, destination_table, bq_client)
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
#  process_created({'name': 'dviorel/sic_code_match.txt'}, None)
process_created({'name': 'dviorel/match_multiple_sic_codes.txt'}, None)
# process_created({'name': 'dviorel/chain_id_name_both.txt'}, None) # Error encoding
# process_created({'name': 'dviorel/multiple_chain_ids.txt'}, None) # no chain id for list
# process_created({'name': 'dviorel/sample_1_updated.txt'}, None) OK
# process_created({'name': 'dviorel/sample_2.txt'}, None) OK
# process_created({'name': 'dviorel/store_list_with_only_zip_code.txt'}, None) OK
# process_created({'name': 'dviorel/unclean_list.txt'}, None)
# process_created({'name': 'dviorel/store_list_full_address.txt'}, None)
# process_created({'name': 'dviorel/sic_code_and_chain_both.txt'}, None)
