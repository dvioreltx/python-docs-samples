import datetime
import logging
import os
import traceback
import pytz
import google.auth
import pandas as pd
from enum import Enum
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
from google.cloud import storage

# Config variables
default_args = {
    'owner': 'danny',
    'depends_on_past': False,
    'email': ['dviorel@inmarket.com'],
    'email_on_failure': True,
    'retries': 0,
    'start_date': datetime.datetime(2020, 6, 1)
}

dag = DAG(
    'gcs_location_matching_created',
    default_args=default_args,
    description='Location Matching Tool',
    # Not scheduled, trigger only
    schedule_interval=None
)

# Define functions and query strings
block_size = 5000
delete_intermediate_tables = False
delete_gcs_files = False
data_set_original = "location_matching_file"
data_set_final = "location_matching_match"
bucket = 'location_matching'
expiration_days_original_table = 7
expiration_days_results_table = 30
project = "cptsrewards-hrd"
url_auth_gcp = 'https://www.googleapis.com/auth/cloud-platform'
missing_required_fields_error = 'Missing required fields error'
logging.basicConfig(level=logging.DEBUG)
query_states = 'SELECT state_abbr, state_name from aggdata.us_states'
query_chains = 'SELECT chain_id, name, sic_code from `inmarket-archive`.scansense.chain'
query_cities = 'select distinct city, state from (select distinct city, state from  `aggdata.' \
               'locations_no_distributors` union all select distinct city, state from `aggdata.location_geofence`)'


class LMAlgo(Enum):
    CHAIN_ZIP = 1
    CHAIN_CITY = 2
    SIC_CODE = 3
    MULTI_CHAIN_ID = 4
    MULTI_CHAIN_NAME = 5


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
    match = df_chains[df_chains['chain_id'] == chain_id]
    if len(match) > 0:
        return match.iloc[0]['name']
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
                            select * except(chain_name), cast(chain_name AS STRING) chain_name, 
                            cleanstr(cast(chain_name AS STRING), 'chain') clean_chain, 
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
        logging.warning(f'Just {length} tokens founded for {address_full}, waiting 3 at less! it will return defaults')
        address = ''
        state = ''
        city = ''
        zip_code = '0'
        return address, state, city, zip_code
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
        logging.warning(f'No data found for {address_full}')
        address = ''
        state = ''
        city = ''
        zip_code = '0'
    return address, state, city, zip_code


def _sanitize_file_name(file_name):
    for c in ' -!@#$%^&*()=+{}[];:"\'?.,':
        file_name = file_name.replace(c, '_')
    return file_name


def _notify_error_adops(context, email_to, file_name):
    try:
        _send_mail(context, email_to, f'Error in Location Matching Tool for “{file_name}”',
                   f'Unexpected error processing “{file_name}”. Please file an Engineering Support ticket (issue '
                   f'type —> reporting and analytics) for Data Engineering team to investigate the issue.')
    except Exception as e:
        logging.exception(f'Unexpected error sending email {e}: {traceback.format_exc()}')


def _send_mail(context, send_to, subject, body, attachments=None):
    logging.info(f'It will send {subject} to {send_to}')
    email_op = EmailOperator(task_id='send_email', to=send_to, subject=subject, html_content=body, files=attachments)
    email_op.execute(context)


def send_email_results(**context):
    try:
        preprocessed_table = context['dag_run'].conf['table']
        logging.warning(f'It will send_email_results for {preprocessed_table}')
        file_name = preprocessed_table
        original_file_name = context['dag_run'].conf['original_file_name']
        file_name_email = context['dag_run'].conf['file_name']
        destination_email = context['dag_run'].conf['destination_email']
        credentials, _ = google.auth.default(scopes=[url_auth_gcp])
        bq_client = bigquery.Client(project=project, credentials=credentials)
        destination_uri = f'gs://{bucket}/results/{destination_email}/{file_name}.csv'
        logging.warning(f'log: send_email_results 02 desturi {destination_uri}')
        logging.info(f'Writing final CSV to {destination_uri} ...{original_file_name}')
        results_table = preprocessed_table + '_result'
        data_set_ref = bigquery.DatasetReference(project, data_set_original)
        table_ref = data_set_ref.table(results_table)
        extract_job = bq_client.extract_table(table_ref, destination_uri)
        extract_job.result()
        logging.warning(f'log: send_email_results 03 downloaded in {destination_uri}')
        storage_client = storage.Client()
        the_bucket = storage_client.bucket(bucket)
        file_result = f'results/{destination_email}/{preprocessed_table}.csv'
        blob = the_bucket.blob(file_result)
        temp_local_file = f'/tmp/{file_name}.csv'
        logging.warning(f'log: send_email_results 04 will download in {temp_local_file}')
        blob.download_to_filename(temp_local_file)
        logging.warning(f'log: send_email_results 05 downloaded in {temp_local_file}')
        _send_mail(context, destination_email, f'{file_name_email} matched locations ',
                   'Hello,\n\nPlease see your location results attached. '
                   f'You can check the complete results in BigQuery - {data_set_final}.{preprocessed_table};',
                   [temp_local_file])
        logging.warning(f'log: send_email_results 06 email sended')
        os.remove(temp_local_file)
        the_bucket.delete_blob(file_result)
    except Exception as e:
        logging.exception(f'Error with {e} and this traceback: {traceback.format_exc()}')
        _notify_error_adops(context, context['dag_run'].conf['destination_email'], context['dag_run'].conf['file_name'])
        raise e


def _set_table_expiration(dataset, table_name, expiration_days, bq_client):
    expiration = datetime.datetime.now(pytz.timezone('America/Los_Angeles')) + datetime.timedelta(days=expiration_days)
    table_ref = bq_client.dataset(dataset).table(table_name)
    table = bq_client.get_table(table_ref)
    table.expires = expiration
    bq_client.update_table(table, ["expires"])


def _run_location_matching(table, destination_table, bq_client, algorithm):
    query = None
    logging.warning(f'Will run location_matching with {algorithm}')
    if algorithm == LMAlgo.MULTI_CHAIN_ID or algorithm == LMAlgo.MULTI_CHAIN_NAME:
        field = 'chain_id' if algorithm == LMAlgo.MULTI_CHAIN_ID else 'chain_name'
        query = f"""#standardSQL
declare level_of_accuracy string default '';
set level_of_accuracy = 'relaxed';
create temporary function strMatchRate(str1 STRING, str2 string, type string, accuracy string) returns FLOAT64
language js as \"\"\"
return scoreMatchFor(str1, str2, type, accuracy)
\"\"\"
OPTIONS (
library=['gs://javascript_lib/addr_functions.js']
);
CREATE OR REPLACE TABLE {data_set_original}.{destination_table} AS
with
sample as (
  select  store_id, {field}, clean_addr,
          clean_city, state, zip,
          concat({field}, ',', ifnull(clean_addr, ''), ',', ifnull(clean_city,''), ',', ifnull(state,'')) store
    from `{data_set_original}.{table}`
),
chain_array as (
  select split({field}) chain_arr
  from (
    select distinct {field} from sample
  )
),
unique_chain as (
  select array(
        select distinct trim(x) from unnest(chains) as x
      ) chains
  from (
    select array_concat_agg(chain_arr) chains from chain_array
  )
),
location_geofence as (
  select chain_id lg_chain_id, chain_name lg_chain, lat lg_lat, lon lg_lon, addr lg_addr,
         city lg_city, state lg_state, substr(trim(zip),0,5) lg_zip, location_id,
         clean_chain clean_lg_chain, clean_addr clean_lg_addr, clean_city clean_lg_city,
         substr(sic_code, 0 ,4) lg_sic_code
  from `aggdata.location_geofence_cleaned`
  where cast({field} as string) in unnest((select chains from unique_chain))
    ),
sample_lg_join as (
  select *
    from sample join location_geofence on (clean_city = clean_lg_city or zip = lg_zip)
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
    select store_id, lg_chain_id, lg_chain, lg_sic_code, clean_addr, clean_lg_addr, clean_city, clean_lg_city, state, 
            lg_state, zip, lg_zip, strmatchrate(clean_addr, clean_lg_addr, 'addr', 'sic_code') addr_match, location_id, 
            store
    from sample_lg_join
  )
)
where ar = 1;
        """
    # elif algorithm == LMAlgo.CHAIN:
    elif algorithm == LMAlgo.CHAIN_ZIP or algorithm == LMAlgo.CHAIN_CITY:
        join_fields = 'lg_zip = zip' if algorithm == LMAlgo.CHAIN_ZIP else 'clean_city = clean_lg_city'
        query = f""" create temporary function strMatchRate(str1 STRING, str2 string, type string, city string) 
  returns float64 
  language js as "return scoreMatchFor(str1, str2, type, city)";    
create temporary function matchScore(chain_match float64, addr_match float64)
  returns float64
  language js as \"\"\"
    var rawScore = 50*(1-chain_match) + 50*(1-addr_match);
    var score =
    chain_match ==  0                      ? 100 :
    chain_match >=  1                      ? Math.min(20, rawScore) :
    chain_match >= .8 && addr_match  >= .5 ? Math.min(15, rawScore) :
    addr_match  >= .8 && chain_match >= .5 ? Math.min(15, rawScore) :
    chain_match >= .3 && addr_match  >= .9 ? Math.min(20, rawScore) :
    rawScore;
return score;
\"\"\"
OPTIONS (
library=['gs://javascript_lib/addr_functions.js']
);
create temp table stateAbbrs as select * from `aggdata.us_states`;
create temp table sample as 
select *,  
  concat(chain, ',', ifnull(addr, ''), ',', ifnull(city,''), ',', ifnull(state,'')) store 
from (
  select chain,  addr, city, 
    case 
      when length(state) > 2 then state_abbr
      else state
    end state,
    cast(case
      when length(substr(zip,0,5)) = 3 then concat('00',zip)
      when length(substr(zip,0,5)) = 4 then concat('0',zip)     
      else substr(zip,0,5)
    end as string) zip,
    clean_chain, clean_addr, clean_city, store_id
  from ( 
    select chain_name chain, street_address  addr, city, state, zip,
      clean_chain, clean_addr, clean_city, store_id
    #####################################
    ###     ENTER INPUT FILE HERE     ###
    #####################################
    from `{data_set_original}.{table}`  
  ) a 
  left join stateAbbrs b on lower(a.state) = lower(state_name)
  where chain is not null
)
;
create temp table our_states as 
select distinct state from sample
;
create temp table location_geofence as 
select chain_name lg_chain, lat lg_lat, lon lg_lon, 
  addr lg_addr, city lg_city, a.state lg_state, 
  substr(trim(zip),0,5) lg_zip, location_id,
  clean_chain lg_clean_chain, clean_city clean_lg_city, clean_addr lg_clean_addr
from `aggdata.location_geofence_cleaned` a 
join our_states b on a.state = b.state
;
create temp table stage1 (
store_id 	     int64,
isa_match 	   string,
match_score    float64,	
grade          string,	
chain_match    float64,	
addr_match     float64,
zip          string,	
chain          string,	
lg_chain       string,	
addr           string,
lg_addr        string,	
city           string,	
lg_city        string,	
state          string,	
lg_state       string,	
lg_lat         float64,	
lg_lon         float64,
location_id    string,	
store          string,	
clean_chain    string,	
clean_lg_chain string,	
clean_addr     string,	
clean_lg_addr  string
);
begin -- stage 1 - join sample to location_geofence on zipcodes
insert into stage1
with 
 combined as (
   select * from sample a left join location_geofence b 
    on {join_fields}
 ),
 chain_match_scores as (
   select *
   from (
     select *, 
       case 
         when lg_chain is null then 0
         else strMatchRate(lg_clean_chain, clean_chain, 'chain', city)
       end chain_match 
     from combined 
   )
 ),
 addr_match_scores as (  
   select *, 
     case
       when lg_addr is null then 0
       else strMatchRate(lg_clean_addr, clean_addr, 'addr', city)       
     end addr_match
   from chain_match_scores
 ),
 match_scores as (
   select *, 
     matchScore(chain_match, addr_match) match_score
   from addr_match_scores
 ),
 sorted_scores as (
   select *,
     row_number() over (partition by store order by match_score, addr_match desc, chain_match desc) match_rank
   from match_scores 
 ),
 best_matches as (
   select store_id, chain_match, addr_match, match_score, match_rank, zip, chain, 
     lg_chain, addr, lg_addr, city, lg_city, state, lg_state, lg_lat, lg_lon, 
     safe_cast(location_id as string) location_id, 
     store, 
     case
       when match_score <= 0  then 'A+'
       when match_score <= 10 then 'A'
       when match_score <= 20 then 'B'
       when match_score <= 30 then 'C'
       when match_score <= 75 then 'D'
       else                        'F'
     end grade,
     case
       when match_score <= 0  then 'definitely'
       when match_score <= 10 then 'very probably'
       when match_score <= 20 then 'probably'
       when match_score <= 30 then 'likely'
       when match_score <= 75 then 'possibly'
       else                        'unlikely'
   end isa_match,
     clean_chain, lg_clean_chain,
     clean_addr,  lg_clean_addr
   from sorted_scores
   where match_rank = 1
 )
select store_id, isa_match, round(100-match_score,1) match_score, grade, 
  * except (store_id, grade, isa_match, match_score, match_rank)
from best_matches
;  
end;
#####################################
###    ENTER OUTPUT FILE HERE     ###
#####################################
create or replace table 
{data_set_original}.{destination_table} 
as select * from stage1 order by store_id 
     """
    elif algorithm == LMAlgo.SIC_CODE:
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
  CREATE OR REPLACE TABLE {data_set_original}.{destination_table} AS
  with
    sample as (
      select *, concat(sic_code, ',', ifnull(clean_addr, ''), ',', ifnull(clean_city,''), ',', ifnull(state,'')) store 
      from (
        select store_id, cast(sic_code as string) sic_code, clean_addr, 
          clean_city, state, zip
        from `{data_set_original}.{table}`
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
        select store_id, lg_chain, sic_code, lg_sic_code, clean_addr, clean_lg_addr, clean_city, clean_lg_city, state, 
            lg_state, zip, lg_zip, strmatchrate(clean_addr, clean_lg_addr, 'addr', 'sic_code') addr_match, store, 
            location_id
        from sample_lg_join 
      )
    ) 
    where ar = 1; 
      """
    else:
        raise NotImplementedError(f'{algorithm} not implemented!')
    logging.warning(f'It will run {query}')
    query_job = bq_client.query(query, project=project)
    query_job.result()


def _create_final_table(original_table, location_matching_table, final_table, bq_client, algorithm):
    query = None
    if algorithm == LMAlgo.MULTI_CHAIN_ID or algorithm == LMAlgo.MULTI_CHAIN_NAME:
        query = f"""CREATE OR REPLACE TABLE {data_set_final}.{final_table} AS
                SELECT * FROM(
                    select p.store_id as store_id, '' as provided_chain, 
                    p.clean_addr as provided_address,
                    p.clean_city as provided_city,
                    p.state as provided_state,
                    -- p.zip as provided_zip, 
                    p.lg_chain as matched_chain,
                    case when p.isa_match = 'unlikely' then null else p.clean_lg_addr end as matched_address,
                    case when p.isa_match = 'unlikely' then null else p.clean_lg_city end as matched_city,
                    case when p.isa_match = 'unlikely' then null else p.lg_state end as matched_state,
                    case when p.isa_match = 'unlikely' then null else p.lg_zip end as zip, 
                    case when p.isa_match = 'unlikely' then null else p.location_id end as location_id,
                    case when p.isa_match = 'unlikely' then null else l.lat end as lat,
                    case when p.isa_match = 'unlikely' then null else l.lon end as lon,
                    p.isa_match as isa_match
                from {data_set_original}.{location_matching_table} p
                left join aggdata.location_geofence l on p.location_id = l.location_id
                union all
                    select o.store_id as store_id, o.chain_name as provided_chain,  
                    o.street_address as provided_address, o.city as provided_city, o.state as provided_state,
                    null as matched_chain, null as matched_address, null as matched_city, null as matched_state, 
                    null as zip, null as location_id, null as lat, null as lon, 'unmatched' as isa_match
                    from {data_set_original}.{original_table} o
                    where o.store_id not in(
                        select p.store_id from {data_set_original}.{location_matching_table} p
                    )
                 )order by store_id
            """
    # elif algorithm == LMAlgo.CHAIN:
    elif algorithm == LMAlgo.CHAIN_ZIP or algorithm == LMAlgo.CHAIN_CITY:
        query = f"""CREATE OR REPLACE TABLE {data_set_final}.{final_table} AS
                    SELECT * FROM(
                    select p.store_id as store_id, 
                    -- case when p.isa_match = 'unlikely' then p.chain else p.lg_chain end as chain,
                    p.chain as provided_chain,
                    -- case when p.isa_match = 'unlikely' then p.addr else p.lg_addr end as address, 
                    p.addr as provided_address,
                    -- case when p.isa_match = 'unlikely' then p.city else p.lg_city end as city, 
                    p.city as provided_city,
                    -- case when p.isa_match = 'unlikely' then p.state else p.lg_state end as state, 
                    p.state as provided_state,
                    case when p.isa_match = 'unlikely' then null else p.lg_chain end as matched_chain,
                    case when p.isa_match = 'unlikely' then null else p.lg_addr end as matched_address,  
                    case when p.isa_match = 'unlikely' then null else p.lg_city end as matched_city,
                    case when p.isa_match = 'unlikely' then null else p.lg_state end as matched_state,
                    p.zip as zip, 
                    case when p.isa_match = 'unlikely' then null else p.location_id end as location_id, 
                    case when p.isa_match = 'unlikely' then null else p.lg_lat end as lat, 
                    case when p.isa_match = 'unlikely' then null else p.lg_lon end as lon, 
                    p.isa_match as isa_match
                from {data_set_original}.{location_matching_table} p 
                union all
                    select o.store_id as store_id, o.chain_name as provided_chain, 
                    o.street_address as provided_address, o.city as provided_city, o.state as provided_state, 
                    null as matched_chain, null as matched_address, null as matched_city, null as matched_state, 
                    null as zip, null as location_id, null as lat, null as lon, 'unmatched' as isa_match
                    from {data_set_original}.{original_table} o
                    where o.store_id not in(
                        select p.store_id from {data_set_original}.{location_matching_table} p
                    )
                 )order by store_id
            """
    elif algorithm == LMAlgo.SIC_CODE:
        query = f"""CREATE OR REPLACE TABLE {data_set_final}.{final_table} AS
                    SELECT * FROM(
                    select p.store_id as store_id, null as provided_chain, 
                    p.clean_addr as provided_address,
                    p.clean_city as provided_city,
                    p.state as provided_state,
                    p.lg_chain as matched_chain,
                    -- p.zip as provided_zip, 
                    case when p.isa_match = 'unlikely' then null else p.clean_lg_addr end as matched_address,
                    case when p.isa_match = 'unlikely' then null else p.clean_lg_city end as matched_city,
                    case when p.isa_match = 'unlikely' then null else p.lg_state end as matched_state,
                    case when p.isa_match = 'unlikely' then null else p.lg_zip end as zip, 
                    case when p.isa_match = 'unlikely' then null else p.location_id end as location_id,
                    case when p.isa_match = 'unlikely' then null else l.lat end as lat,
                    case when p.isa_match = 'unlikely' then null else l.lon end as lon,
                    p.isa_match as isa_match
                from {data_set_original}.{location_matching_table} p
                left join aggdata.location_geofence l on p.location_id = l.location_id
                union all
                    select o.store_id as store_id, o.chain_name as provided_chain,  
                    o.street_address as provided_address, o.city as provided_city, o.state as provided_state,
                    null as matched_chain, null as matched_address, null as matched_city, null as matched_state, 
                    null as zip, null as location_id, null as lat, null as lon, 'unmatched' as isa_match
                    from {data_set_original}.{original_table} o
                    where o.store_id not in(
                        select p.store_id from {data_set_original}.{location_matching_table} p
                    )
                 )order by store_id
        """
    else:
        raise NotImplementedError(f'{algorithm} not expected!')
    logging.warning(f'It will run {query}')
    query_job = bq_client.query(query, project=project)
    query_job.result()
    _set_table_expiration(data_set_final, final_table, expiration_days_results_table, bq_client)
    # Also creates a results table with only non unlikely results
    results_table = final_table + '_result'
    query = f'''CREATE OR REPLACE TABLE {data_set_original}.{results_table} AS
                    select * from {data_set_final}.{final_table}
                        where isa_match not in ('unlikely', 'unmatched')
                    order by store_id
        '''
    query_job = bq_client.query(query, project=project)
    query_job.result()


def execute_location_matching(**context):
    try:
        preprocessed_table = context['dag_run'].conf['table']
        logging.info(f'execute_location_matching started for {preprocessed_table}')
        has_sic_code = context['dag_run'].conf['has_sic_code']
        has_chain = context['dag_run'].conf['has_chain']
        has_zip = context['dag_run'].conf['has_zip']
        has_city = context['dag_run'].conf['has_city']
        has_multiple_chain_id = context['dag_run'].conf['has_multiple_chain_id']
        has_multiple_chain_name = context['dag_run'].conf['has_multiple_chain_name']
        logging.info(f'has_sic_code: {has_sic_code}')
        logging.info(f'has_chain: {has_chain}')
        logging.info(f'has_zip: {has_zip}')
        logging.info(f'has_city: {has_city}')
        logging.info(f'has_multiple_chain_id: {has_multiple_chain_id}')
        logging.info(f'has_multiple_chain_name: {has_multiple_chain_name}')
        has_sic_code = has_sic_code and not has_chain
        credentials, _ = google.auth.default(scopes=[url_auth_gcp])
        bq_client = bigquery.Client(project=project, credentials=credentials)
        algorithm = LMAlgo.SIC_CODE if has_sic_code else (LMAlgo.CHAIN_ZIP if has_zip else LMAlgo.CHAIN_CITY)
        if has_multiple_chain_id:
            algorithm = LMAlgo.MULTI_CHAIN_ID
        if has_multiple_chain_name:
            algorithm = LMAlgo.MULTI_CHAIN_NAME
        logging.info(f'log: bq_client obtained, will run location_matching with {algorithm}')
        subtables = _should_split_process(bq_client, preprocessed_table, block_size, algorithm, has_zip, has_city)
        logging.info(f'Should split process: {subtables}')
        if subtables is not None:
            # Build small tables
            logging.info(f'Small tables: {subtables}')
            _build_small_tables(bq_client, preprocessed_table, subtables)
            # For each small table it runs the algorithm
            for table in subtables:
                location_matching_table = table[0] + '_lm'
                logging.info(f'Will run location_matching for: {location_matching_table}')
                _run_location_matching(table[0], location_matching_table, bq_client, algorithm)
            # Join every _lm_temp_XX result into a _lm final table
            location_matching_table = preprocessed_table + '_lm'
            logging.info(f'Will join all tables in {location_matching_table}')
            _join_results(bq_client, subtables, location_matching_table)
        else:
            # If it ran with CHAIN_ZIP, first run is with zip, and then another with CITY, the last one in blocks
            if algorithm == LMAlgo.CHAIN_ZIP:
                location_matching_table = preprocessed_table + '_first_lm'
                _run_location_matching(preprocessed_table, location_matching_table, bq_client, algorithm)
                # Build table with those rows that doesn't match
                preprocessed_table_second = f'{preprocessed_table}_second'
                query = f'''CREATE OR REPLACE TABLE {data_set_original}.{preprocessed_table_second} AS SELECT * FROM 
                        {data_set_original}.{preprocessed_table} WHERE store_id NOT IN (SELECT store_id FROM 
                        {data_set_original}.{location_matching_table} WHERE isa_match IN ('definitely', 
                        'very probably', 'probably'))
                        '''
                logging.info(f'It will create second table with:\n{query}')
                job = bq_client.query(query)
                job.result()
                # Get the small table names
                subtables = _should_split_process(bq_client, preprocessed_table_second, block_size, LMAlgo.CHAIN_CITY,
                                                  has_zip, has_city, forced=True)
                logging.warning(f'Should split process: {subtables}')
                if subtables is not None:
                    # For each small table it runs the algorithm
                    logging.warning(f'Small tables: {subtables}')
                    _build_small_tables(bq_client, preprocessed_table_second, subtables)
                    # For each small table it runs the algorithm
                    for table in subtables:
                        location_matching_table = table[0] + '_lm'
                        logging.info(f'Will run location_matching for: {location_matching_table}')
                        # Force to run with City
                        _run_location_matching(table[0], location_matching_table, bq_client, LMAlgo.CHAIN_CITY)
                    # Join every _lm_temp_XX result into a _lm final table
                    location_matching_table = preprocessed_table_second + '_lm'
                    logging.info(f'Will join all tables in {location_matching_table}')
                    _join_results(bq_client, subtables, location_matching_table)
                    logging.info(f'{location_matching_table} created!')
                else:
                    location_matching_table = preprocessed_table_second + '_lm'
                    _run_location_matching(preprocessed_table_second, location_matching_table, bq_client,
                                           LMAlgo.CHAIN_CITY)
                    logging.info(f'{location_matching_table} created!')
                # Join the_lm table with the fist _lm table
                query = f'''CREATE OR REPLACE TABLE {data_set_original}.{preprocessed_table}_lm AS
                            SELECT * FROM (SELECT * FROM {data_set_original}.{preprocessed_table}_first_lm
                            WHERE isa_match in ('definitely', 'very probably', 'probably')
                            UNION ALL SELECT * FROM {data_set_original}.{preprocessed_table}_second_lm
                            ) ORDER BY store_id 
                        '''
                logging.info(f'It will run: {query}')
                job = bq_client.query(query)
                job.result()
                logging.info(f'Created!!!')
            else:
                location_matching_table = preprocessed_table + '_lm'
                _run_location_matching(preprocessed_table, location_matching_table, bq_client, algorithm)
        logging.info(f'location_matching ended in table {location_matching_table}')
    except Exception as e:
        logging.exception(f'Error with {e} and this traceback: {traceback.format_exc()}')
        _notify_error_adops(context, context['dag_run'].conf['destination_email'], context['dag_run'].conf['file_name'])
        raise e


def _should_split_process(bq_client, source_table, group_size, algo, has_zip, has_city, forced=False):
    logging.info(f'It will ask if should split {source_table}, {algo}, {has_zip}, {has_city}, forced {forced}')
    full_source_table = f'{data_set_original}.{source_table}'
    if has_zip and not forced:
        return None
    job = bq_client.query(f'select count(*) as c from {full_source_table}')
    result = job.result()
    row_count = 0
    for row in result:
        row_count = int(row['c'])
    logging.info(f'Will ask if split {full_source_table} of {row_count} in blocks of {group_size}')
    if row_count <= group_size:
        return None
    current = 1
    index = 1
    tables_metadata = []
    while current <= row_count:
        upper_limit = (current + group_size - 1) if current + group_size - 1 <= row_count else row_count
        table_name = f'{source_table}_{index}'
        tables_metadata.append((table_name, current, upper_limit))
        current += group_size
        index += 1
    return tables_metadata


def _join_results(bq_client, small_tables, joined_table):
    logging.info(f'It will join results from {small_tables} in {joined_table}')
    query = f'CREATE OR REPLACE TABLE {data_set_original}.{joined_table} AS SELECT * FROM('
    query_tables = [f'SELECT * FROM {data_set_original}.' + item[0] + '_lm' for item in small_tables]
    query = query + ' UNION ALL '.join(query_tables) + ') ORDER BY store_id'
    logging.info(f'It will run:\n{query}')
    job = bq_client.query(query)
    job.result()


def _build_small_tables(bq_client, source_table, subtables):
    logging.info(f'It will split {source_table}')
    full_source_table = f'{data_set_original}.{source_table}'
    job = bq_client.query(f'select count(*) as c from {full_source_table}')
    result = job.result()
    row_count = 0
    for row in result:
        row_count = int(row['c'])
    logging.info(f'Splitting {full_source_table} of {row_count} in {subtables}')
    for subtable in subtables:
        query = f'CREATE OR REPLACE TABLE {data_set_original}.{subtable[0]} AS SELECT * FROM {full_source_table} ' \
                f'WHERE store_id between {subtable[1]} and {subtable[2]} order by store_id'
        logging.debug(f'It will run:\n{query}')
        job = bq_client.query(query)
        job.result()
    logging.info(f'Small tables splitted')


def prepare_results_table(**context):
    try:
        preprocessed_table = context['dag_run'].conf['table']
        logging.info(f'It will prepare results table for {preprocessed_table}')
        has_sic_code = context['dag_run'].conf['has_sic_code']
        has_chain = context['dag_run'].conf['has_chain']
        has_zip = context['dag_run'].conf['has_zip']
        has_multiple_chain_id = context['dag_run'].conf['has_multiple_chain_id']
        has_multiple_chain_name = context['dag_run'].conf['has_multiple_chain_name']
        has_sic_code = has_sic_code and not has_chain
        credentials, _ = google.auth.default(scopes=[url_auth_gcp])
        bq_client = bigquery.Client(project=project, credentials=credentials)
        algorithm = LMAlgo.SIC_CODE if has_sic_code else (LMAlgo.CHAIN_ZIP if has_zip else LMAlgo.CHAIN_CITY)
        if has_multiple_chain_id:
            algorithm = LMAlgo.MULTI_CHAIN_ID
        if has_multiple_chain_name:
            algorithm = LMAlgo.MULTI_CHAIN_NAME
        logging.info(f'Algorithm: {algorithm}')
        _create_final_table(preprocessed_table, preprocessed_table + '_lm', preprocessed_table, bq_client, algorithm)
        logging.info('Results table prepared')
    except Exception as e:
        logging.exception(f'Error {e} with traceback: {traceback.format_exc()}')
        _notify_error_adops(context, context['dag_run'].conf['destination_email'], context['dag_run'].conf['file_name'])
        raise e


def delete_temp_data(**context):
    logging.info(f'Delete_temp_data with value {delete_intermediate_tables}')
    if delete_intermediate_tables:
        preprocessed_table = context['dag_run'].conf["table"]
        has_zip = context['dag_run'].conf['has_zip']
        has_city = context['dag_run'].conf['has_city']
        has_sic_code = context['dag_run'].conf['has_sic_code']
        location_matching_table = preprocessed_table + '_lm'
        result_table = preprocessed_table + '_result'
        logging.info(f'Start to delete lm_table {location_matching_table} and results table {result_table}')
        credentials, _ = google.auth.default(scopes=[url_auth_gcp])
        bq_client = bigquery.Client(project=project, credentials=credentials)
        algo = LMAlgo.SIC_CODE if has_sic_code else (LMAlgo.CHAIN_ZIP if has_zip else LMAlgo.CHAIN_CITY)
        tables_data = _should_split_process(bq_client, preprocessed_table, block_size, algo, has_zip, has_city)
        if tables_data is not None:
            for table_data in tables_data:
                bq_client.delete_table(f'{data_set_original}.{table_data[0]}')
        if algo == LMAlgo.CHAIN_ZIP:
            # TODO: To complete alternative_tables
            pass
        # Also it ask if its 2nd pass and it should delete list of tables
        bq_client.delete_table(f'{data_set_original}.{location_matching_table}')
        bq_client.delete_table(f'{data_set_original}.{result_table}')


def pre_process_file(**context):
    try:
        original_name = context['dag_run'].conf['original_file_name']
        logging.info(f'Started for {original_name}')
        validation_fields = {'sic code': 'sic_code', 'chain name/chain id': 'chain_name',
                             'address/address full/address (full)/address full (no zip)/address full (address, state, city, zip)':
                                 'address', 'city': 'city', 'state': 'state', 'zip': 'zip'}
        file_full_name = original_name.replace(' ', '_').replace('-', '_')
        destination_email = context['dag_run'].conf['destination_email']
        file_name = context['dag_run'].conf['the_file_name']
        cf_name = original_name[original_name.index('/') + 1:]
        now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        try:
            raw_data = pd.read_csv(f'gs://{bucket}/{original_name}', sep='\t', encoding='utf-8')
        except Exception:
            try:
                raw_data = pd.read_csv(f'gs://{bucket}/{original_name}', sep='\t', encoding='iso-8859-1')
            except Exception as e:
                logging.error(f'Error reading file, will send email format {e} and exit function: '
                              f'{traceback.format_exc()}')
                _send_mail(context, destination_email, f'File format error in Location Matching Tool for "{cf_name}"',
                           f'"{cf_name}" is not a valid supported file type. Please verify the file '
                           f'format is "Tab delimited Text(.txt)" before resubmitting for matching.')
                return
        logging.info('Data read correctly')
        raw_data.columns = map(str.lower, raw_data.columns)
        raw_data.columns = map(str.strip, raw_data.columns)
        column_validation_fields = _verify_fields(raw_data.keys(), validation_fields)
        pre_processed_data = raw_data[column_validation_fields]. \
            rename(columns=lambda name: name.replace(' ', '_').replace('(', '_').replace(')', '_').replace(',', '_'),
                   inplace=False)
        if 'chain id' not in raw_data.columns and 'chain name' not in raw_data.columns \
                and 'sic code' not in raw_data.columns:
            logging.error(f'File {original_name} doesnt have required columns, will send email.')
            _send_mail(context, destination_email, f'Missing required fields error in Location Matching Tool for "{cf_name}"',
                       f'File "{cf_name}" must contain at least one of the following required fields - chain id, '
                       f'chain name or sic code. Please add the required field before resubmitting for matching.')
            raise Exception(missing_required_fields_error)
        df_states = pd.read_gbq(query_states, project_id=project, dialect='standard')
        should_add_state_from_zip = 'state' not in pre_processed_data.columns and 'zip' in pre_processed_data.columns
        has_chain = 'chain_id' in pre_processed_data.columns or 'chain_name' in pre_processed_data.columns
        has_city = 'city' in raw_data.columns
        has_zip = 'zip' in raw_data.columns
        credentials, _ = google.auth.default(scopes=[url_auth_gcp])
        bq_client = bigquery.Client(project=project, credentials=credentials)
        # Complete columns not present in file
        if 'chain_id' in pre_processed_data.columns:
            df_chains = pd.read_gbq(query_chains, project_id=project, dialect='standard')
            pre_processed_data['chain_name'] = pre_processed_data['chain_id'].apply(
                lambda chain_id: _get_chain_name(chain_id, df_chains))
        for key in validation_fields:
            if validation_fields[key] not in pre_processed_data:
                pre_processed_data[validation_fields[key]] = None
        pre_processed_data = pre_processed_data.astype({'zip': 'str'})
        logging.info('Columns not present were added')
        if 'address_full__no_zip_' in pre_processed_data.columns \
                or 'address_full' in pre_processed_data.columns \
                or 'address__full_' in pre_processed_data.columns \
                or 'address_full__address_state_city_zip_' in pre_processed_data.columns \
                or 'address_full__address__state__city__zip_' in pre_processed_data.columns:
            logging.info('It will pre-process address data')
            has_city = True
            if 'address_full' in pre_processed_data.columns or \
                    'address__full_' in pre_processed_data.columns or \
                    'address_full__address_state_city_zip_' in pre_processed_data.columns or \
                    'address_full__address__state__city__zip_' in pre_processed_data.columns:
                has_zip = True
            df_cities = pd.read_gbq(query_cities, project_id=project, dialect='standard')
            address = None
            state = None
            city = None
            zip_code = None
            logging.info(f'It will pre-process {len(pre_processed_data.index)} rows')
            for index, row in pre_processed_data.iterrows():
                if index % 500 == 0:
                    logging.info(f'Row {index} of {len(pre_processed_data.index)}')
                if 'address_full__no_zip_' in pre_processed_data.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full__no_zip_'], df_states,
                                                                         df_cities, False, False)
                if 'address_full' in pre_processed_data.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full'], df_states,
                                                                         df_cities, True, False)
                if 'address__full_' in pre_processed_data.columns:
                    address, state, city, zip_code = _split_address_data(row['address__full_'], df_states,
                                                                         df_cities, True, False)
                if 'address_full__address_state_city_zip_' in pre_processed_data.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full__address_state_city_zip_'],
                                                                         df_states, df_cities, True, True)
                if 'address_full__address__state__city__zip_' in pre_processed_data.columns:
                    address, state, city, zip_code = _split_address_data(row['address_full__address__state__city__zip_'],
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
        pre_processed_data.insert(0, 'store_id', range(1, 1 + len(pre_processed_data)))
        logging.info('Data already pre-processed')
        preprocessed_table = file_name.lower()
        if has_chain and not has_zip and not has_city:
            logging.error(f'File {original_name} doesnt have zip or city, will send email.')
            _send_mail(context, destination_email, f'Missing required fields error in Location Matching Tool for “{cf_name}”',
                       f'File {cf_name} must have at least one of the following fields: zip code or city. Please add '
                       f'the required field before resubmitting for matching. ')
            raise Exception(missing_required_fields_error)
        logging.info(f'It will write to table: {preprocessed_table}')
        pre_processed_data.to_gbq(f'{data_set_original}.{preprocessed_table}', project_id=project, progress_bar=False,
                                  if_exists='replace')
        _set_table_expiration(data_set_original, preprocessed_table, expiration_days_original_table, bq_client)
        logging.warning(f'It will add clean fields to: {preprocessed_table}')
        _add_clean_fields(preprocessed_table, bq_client)
        if should_add_state_from_zip:
            logging.info(f'It will add states from zip codes: {preprocessed_table}')
            _add_state_from_zip(preprocessed_table, bq_client)
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
        logging.warning(f'Pre-process finished for {file_full_name}')
    except Exception as e:
        logging.exception(f'Error with {e} and this traceback: {traceback.format_exc()}')
        exception_message = f'{e}'
        if not (exception_message == missing_required_fields_error):
            _notify_error_adops(context, context['dag_run'].conf['destination_email'], context['dag_run'].conf['file_name'])
        raise e


# define tasks
pre_process_file = PythonOperator(
    task_id='pre_process_file',
    provide_context=True,
    python_callable=pre_process_file,
    dag=dag)

execute_location_matching = PythonOperator(
    task_id='execute_location_matching',
    provide_context=True,
    python_callable=execute_location_matching,
    dag=dag)

prepare_results_table = PythonOperator(
    task_id='prepare_results_table',
    provide_context=True,
    python_callable=prepare_results_table,
    dag=dag)

send_email_results = PythonOperator(
    task_id='send_email_results',
    provide_context=True,
    python_callable=send_email_results,
    dag=dag)

delete_temp_data = PythonOperator(
    task_id='delete_temp_data',
    provide_context=True,
    python_callable=delete_temp_data,
    dag=dag
)

# Setting up Dependencies
execute_location_matching.set_upstream(pre_process_file)
prepare_results_table.set_upstream(execute_location_matching)
send_email_results.set_upstream(prepare_results_table)
delete_temp_data.set_upstream(send_email_results)
