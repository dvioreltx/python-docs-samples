import datetime
import logging
import google.auth
import os
import pandas as pd
import pytz
import smtplib
import traceback
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from google.cloud.bigquery import job
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1
from google.cloud import storage
from os.path import basename


delete_intermediate_tables = True
delete_gcs_files = False
enable_trigger = True
send_email_on_error = True
data_set_original = "location_matching_file"
data_set_final = "location_matching_match"
bucket = 'location_matching'
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


def _send_mail_results(destination_email, original_name, file_name, storage_client, file_result, now):
    the_bucket = storage_client.bucket(bucket)
    blob = the_bucket.blob(file_result)
    temp_local_file = f'/tmp/{file_name}.csv'
    if not os.path.isdir(f'/tmp'):
        temp_local_file = f'd:/tmp/{file_name}.csv'
    blob.download_to_filename(temp_local_file)
    file_name_email = original_name[original_name.index('/') + 1:]
    if '.' in file_name_email:
        file_name_email = file_name_email[:file_name_email.index('.')]
    _send_mail(mail_from, [destination_email], f'{file_name_email} matched locations ',
               'Hello,\n\nPlease see your location results attached.', [temp_local_file])
    os.remove(temp_local_file)
    the_bucket.delete_blob(file_result)


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
    smtp.ehlo()
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


def _set_table_expiration(dataset, table_name, expiration_days, bq_client):
    expiration = datetime.datetime.now(pytz.timezone('America/Los_Angeles')) + datetime.timedelta(days=expiration_days)
    table_ref = bq_client.dataset(dataset).table(table_name)
    table = bq_client.get_table(table_ref)
    table.expires = expiration
    bq_client.update_table(table, ["expires"])


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


def _create_final_table(table, destination_table, bq_client, algorithm, full_result):
    job_config = bigquery.QueryJobConfig(destination=f'{project}.{data_set_final}.{destination_table}')
    job_config.write_disposition = job.WriteDisposition.WRITE_TRUNCATE
    query = None
    if algorithm == LMAlgo.CHAIN:
        if not full_result:
            query = f"""select ROW_NUMBER() OVER() as row, 
                    case when p.isa_match = 'unlikely' then p.chain else p.lg_chain end as chain, 
                    case when p.isa_match = 'unlikely' then p.addr else p.lg_addr end as address, 
                    case when p.isa_match = 'unlikely' then p.city else p.lg_city end as city, 
                    case when p.isa_match = 'unlikely' then p.state else p.lg_state end as state, 
                    p.zip as zip, 
                    case when p.isa_match = 'unlikely' then null else p.location_id end as location_id, 
                    case when p.isa_match = 'unlikely' then null else p.lg_lat end as lat, 
                    case when p.isa_match = 'unlikely' then null else p.lg_lon end as lon, 
                    p.isa_match as isa_match, p.store_id as store_id
                from {data_set_original}.{table} p order by row
            """
        else:
            query = f"""select ROW_NUMBER() OVER() as row, 
                  case when p.isa_match = 'unlikely' then p.chain else p.lg_chain end as chain, 
                  case when p.isa_match = 'unlikely' then p.addr else p.lg_addr end as address, 
                  case when p.isa_match = 'unlikely' then p.city else p.lg_city end as city, 
                  case when p.isa_match = 'unlikely' then p.state else p.lg_state end as state, 
                  p.zip as zip, 
                  case when p.isa_match = 'unlikely' then null else p.location_id end as location_id, 
                  case when p.isa_match = 'unlikely' then null else p.lg_lat end as lat, 
                  case when p.isa_match = 'unlikely' then null else p.lg_lon end as lon, 
                  p.isa_match as isa_match, p.store_id as store_id, 
                p.store_id as raw_store_id, p.isa_match as raw_isa_match, p.match_score as raw_match_score, 
                p.grade as raw_grade, p.match_round as raw_match_round, p.chain_match as raw_chain_match,
                p.addr_match as raw_addr_match, p.chain as raw_chain, p.lg_chain as raw_lg_chain, p.addr as raw_addr,
                p.lg_addr as raw_lg_addr, p.city as raw_city, p.lg_city as raw_lg_city, p.state as raw_state, 
                p.lg_state as raw_lg_state, p.lg_lat as raw_lg_lat, p.lg_lon as raw_lg_lon, 
                p.location_id as raw_location_id, p.store as raw_store, p.clean_chain as raw_clean_chain, 
                p.clean_lg_chain as raw_clean_lg_chain, p.clean_addr as raw_clean_addr, 
                p.clean_lg_addr as raw_clean_lg_addr
                from {data_set_original}.{table} p  order by row
            """
    elif algorithm == LMAlgo.SIC_CODE:
        if not full_result:
            query = f"""select ROW_NUMBER() OVER() as row, '' as chain, p.clean_lg_addr as address, 
                    p.clean_lg_city as city, p.lg_state as state, p.lg_zip as zip, p.location_id as location_id,
                    l.lat as lat, l.lon as lon, p.isa_match as isa_match, '' as store_id
                from {data_set_original}.{table} p
                left join aggdata.location_geofence l on p.location_id = l.location_id
                 order by row
            """
        else:
            query = f"""select ROW_NUMBER() OVER() as row, '' as chain, p.clean_lg_addr as address, 
                    p.clean_lg_city as city, p.lg_state as state, p.lg_zip as zip, p.location_id as location_id, 
                    l.lat as lat, l.lon as lon, p.isa_match as isa_match, '' as store_id, 
                    p.sic_code as raw_sic_code, p.lg_sic_code as raw_lg_sic_code, p.clean_addr as raw_clean_addr,
                    p.clean_lg_addr as raw_clean_lg_addr, p.clean_city as raw_clean_city, 
                    p.clean_lg_city as raw_clean_lg_city, p.state as raw_state, p.lg_state as raw_lg_state,
                    p.zip as raw_zip, p.lg_zip as raw_lg_zip, p.addr_match as raw_addr_match, p.store as raw_store,
                    p.location_id as raw_location_id, p.ar as raw_ar, p.isa_match as raw_isa_match
                from {data_set_original}.{table} p
                left join aggdata.location_geofence l on p.location_id = l.location_id
                 order by row
            """
    else:
        raise NotImplementedError(f'{algorithm} not expected!')
    query_job = bq_client.query(query, project=project, job_config=job_config)
    query_job.result()
    _set_table_expiration(data_set_final, destination_table, expiration_days_results_table, bq_client)


def _run_location_matching(table, destination_table, bq_client, algorithm):
    query = None
    if algorithm == LMAlgo.CHAIN:
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
    clean_chain, clean_addr, clean_city 
  from ( 
    select chain_name chain, street_address  addr, city, state, zip,
      clean_chain, clean_addr, clean_city
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
  clean_chain lg_clean_chain, clean_city lg_clean_city, clean_addr lg_clean_addr
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
   select * from sample a left join location_geofence b on lg_zip = zip
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
   select chain_match, addr_match, match_score, match_rank, zip, chain, 
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
select row_number() over () store_id, 
  isa_match, round(100-match_score,1) match_score, grade, 
  * except (grade, isa_match, match_score, match_rank)
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
        select  substr(cast(sic_code as string), 0 ,4) sic_code, clean_addr, 
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
        select sic_code, lg_sic_code, clean_addr, clean_lg_addr, clean_city, clean_lg_city, state, lg_state, zip, 
          lg_zip, strmatchrate(clean_addr, clean_lg_addr, 'addr', 'sic_code') addr_match, store, location_id
        from sample_lg_join 
      )
    ) 
    where ar = 1; 
      """
    else:
        raise NotImplementedError(f'{algorithm} not implemented!')
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
        has_sic_code = has_sic_code and not has_chain
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
        preprocessed_table = file_name.lower().replace(' ', '_')
        logging.warning(f'Will write to table: {preprocessed_table} ...{original_name}')
        pre_processed_data.to_gbq(f'{data_set_original}.{preprocessed_table}', project_id=project, progress_bar=False,
                                  if_exists='replace')
        _set_table_expiration(data_set_original, preprocessed_table, expiration_days_original_table, bq_client)
        logging.warning(f'Will add clean fields: {preprocessed_table} ...{original_name}')
        _add_clean_fields(preprocessed_table, bq_client)
        if should_add_state_from_zip:
            logging.warning(f'Will add states from zip codes: {preprocessed_table} ...{original_name}')
            _add_state_from_zip(preprocessed_table, bq_client)
        # Run location_matching
        logging.warning(f'will run location_matching ...{original_name}')
        location_matching_table = f'{file_name}_lm'
        _run_location_matching(preprocessed_table, location_matching_table, bq_client,
                               LMAlgo.SIC_CODE if has_sic_code else LMAlgo.CHAIN)
        logging.warning(f'Location matching table: {location_matching_table} ...{original_name}')
        # TODO: Add the rows that doesn't match

        # Send email to agent
        final_table = file_name
        _create_final_table(location_matching_table, final_table, bq_client,
                            LMAlgo.SIC_CODE if has_sic_code else LMAlgo.CHAIN, full_result)
        logging.warning(f'Final table: {final_table} ...{original_name}')
        destination_uri = f'gs://{bucket}/results/{destination_email}/{file_name}.csv'
        partial_destination_uri = f'results/{destination_email}/{file_name}.csv'
        logging.info(f'Writing final CSV to {destination_uri} ...{original_name}')
        data_set_ref = bigquery.DatasetReference(project, data_set_final)
        table_ref = data_set_ref.table(final_table)
        extract_job = bq_client.extract_table(table_ref, destination_uri)
        extract_job.result()
        storage_client = storage.Client()
        logging.info(f'Will send email results to {destination_email} ...{original_name}')
        _send_mail_results(destination_email, original_name, file_name, storage_client, partial_destination_uri, now)
        if delete_intermediate_tables and '___no_del_bq' not in file_full_name:
            logging.info(f'Start to delete table {location_matching_table} ...{original_name}')
            bq_client.delete_table(f'{data_set_original}.{location_matching_table}')
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
process_location_matching({'name': 'dviorel@inmarket.com/multiple_chain_ids_one_row___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/simple_list_ch___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/Matching_list_nozip___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/walmart_list_with_match_issue_6___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/Multiple chain ids___no_mv_gcs.txt'}, None)
# process_location_matching({'name': 'dviorel@inmarket.com/walmart_list_with_match_issue___no_mv_gcs.txt'}, None)
# _send_mail('dviorel@inmarket.com', ['dviorel@inmarket.com'], 'My test', 'The body')
