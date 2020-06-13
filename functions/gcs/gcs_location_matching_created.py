import datetime

import logging
import os
from enum import Enum
import traceback
import smtplib
import pytz
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import google.auth
from google.cloud import bigquery
from google.cloud import storage
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Config variables

default_args = {
    'owner': 'danny',
    'depends_on_past': False,
    'email': ['dviorel@inmarket.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2020, 4, 1),
}

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'gcs_location_matching_created',
    default_args=default_args,
    description='Location Matching Tool',
    # Not scheduled, trigger only
    schedule_interval=None
)

# Define functions and query strings

delete_intermediate_tables = False
send_email_on_error = True
data_set_original = "location_matching_file"
data_set_final = "location_matching_match"
bucket = 'location_matching'  # TODOne: Volver a poner este bucket
# bucket = 'dannyv'
mail_from = 'dviorel@inmarket.com'
email_error = ['dviorel@inmarket.com']
mail_user = 'dviorel@inmarket.com'
mail_password = 'ftjhmrukjcdtcpft'
expiration_days_results_table = 30

logging.basicConfig(level=logging.DEBUG)

mail_server = 'smtp.gmail.com'
project = "cptsrewards-hrd"
url_auth_gcp = 'https://www.googleapis.com/auth/cloud-platform'


class LMAlgo(Enum):
    CHAIN = 1
    SIC_CODE = 2


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


def send_email_results(**context):
    logging.warning('log: send_email_results 01')
    preprocessed_table = context['dag_run'].conf['table']
    file_name = preprocessed_table
    original_file_name = context['dag_run'].conf['original_file_name']
    destination_email = context['dag_run'].conf['destination_email']
    credentials, _ = google.auth.default(scopes=[url_auth_gcp])
    bq_client = bigquery.Client(project=project, credentials=credentials)
    destination_uri = f'gs://{bucket}/results/{destination_email}/{file_name}.csv'
    logging.warning(f'log: send_email_results 02 desturi {destination_uri}')
    logging.info(f'Writing final CSV to {destination_uri} ...{original_file_name}')
    results_table = preprocessed_table + '_result'
    # data_set_ref = bigquery.DatasetReference(project, data_set_final)
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
    file_name_email = original_file_name[original_file_name.index('/') + 1:]
    if '.' in file_name_email:
        file_name_email = file_name_email[:file_name_email.index('.')]
    _send_mail(mail_from, [destination_email], f'{file_name_email} matched locations ',
               'Hello,\n\nPlease see your location results attached. '
               f'You can check the complete results in BigQuery - {data_set_final}.{preprocessed_table};',
               [temp_local_file])
    logging.warning(f'log: send_email_results 06 email sended')
    os.remove(temp_local_file)
    the_bucket.delete_blob(file_result)


def _set_table_expiration(dataset, table_name, expiration_days, bq_client):
    expiration = datetime.datetime.now(pytz.timezone('America/Los_Angeles')) + datetime.timedelta(days=expiration_days)
    table_ref = bq_client.dataset(dataset).table(table_name)
    table = bq_client.get_table(table_ref)
    table.expires = expiration
    bq_client.update_table(table, ["expires"])


def _run_location_matching(table, destination_table, bq_client, algorithm, has_zip, has_city, is_multiple_chain_id,
                           is_multiple_chain_name):
    query = None
    logging.warning(f'Will run location_matching with {algorithm} mid {is_multiple_chain_id} mcn {is_multiple_chain_name}')
    if is_multiple_chain_id or is_multiple_chain_name:
        field = 'chain_id' if is_multiple_chain_id else 'chain_name'
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
  select  ppid, {field}, clean_addr,
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
    select ppid, lg_chain_id, lg_chain, lg_sic_code, clean_addr, clean_lg_addr, clean_city, clean_lg_city, state, 
            lg_state, zip, lg_zip, strmatchrate(clean_addr, clean_lg_addr, 'addr', 'sic_code') addr_match, location_id, 
            store
    from sample_lg_join
  )
)
where ar = 1;
        """
    elif algorithm == LMAlgo.CHAIN:
        if not has_zip and not has_city:
            raise Exception('Zip OR City is required to run Location Matching Algorithm!')
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
    clean_chain, clean_addr, clean_city, ppid
  from ( 
    select chain_name chain, street_address  addr, city, state, zip,
      clean_chain, clean_addr, clean_city, ppid
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
ppid            int64,
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
    on {'lg_zip = zip' if has_zip else 'lg_clean_city = clean_city' }
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
   select ppid, chain_match, addr_match, match_score, match_rank, zip, chain, 
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
select ppid, row_number() over () store_id, 
  isa_match, round(100-match_score,1) match_score, grade, 
  * except (ppid, grade, isa_match, match_score, match_rank)
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
        select ppid, substr(cast(sic_code as string), 0 ,4) sic_code, clean_addr, 
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
        select ppid, lg_chain, sic_code, lg_sic_code, clean_addr, clean_lg_addr, clean_city, clean_lg_city, state, 
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


def _create_final_table(original_table, location_matching_table, final_table, bq_client, algorithm,
                        is_multiple_chain_id, is_multiple_chain_name):
    # job_config = bigquery.QueryJobConfig(destination=f'{project}.{data_set_final}.{destination_table}')
    # job_config.write_disposition = job.WriteDisposition.WRITE_TRUNCATE
    query = None
    if is_multiple_chain_id or is_multiple_chain_name:
        query = f"""CREATE OR REPLACE TABLE {data_set_final}.{final_table} AS
                SELECT * FROM(
                    select p.ppid as row, '' as provided_chain, p.lg_chain as matched_chain,
                    p.clean_addr as provided_address,
                    p.clean_city as provided_city,
                    p.state as provided_state,
                    -- p.zip as provided_zip, 
                    case when p.isa_match = 'unlikely' then null else p.clean_lg_addr end as matched_address,
                    case when p.isa_match = 'unlikely' then null else p.clean_lg_city end as matched_city,
                    case when p.isa_match = 'unlikely' then null else p.lg_state end as matched_state,
                    case when p.isa_match = 'unlikely' then null else p.lg_zip end as zip, 
                    case when p.isa_match = 'unlikely' then null else p.location_id end as location_id,
                    case when p.isa_match = 'unlikely' then null else l.lat end as lat,
                    case when p.isa_match = 'unlikely' then null else l.lon end as lon,
                    p.isa_match as isa_match, 
                    '' as store_id
                from {data_set_original}.{location_matching_table} p
                left join aggdata.location_geofence l on p.location_id = l.location_id
                union all
                    select o.ppid as row, o.chain_name as provided_chain, '' as matched_chain, 
                    o.street_address as provided_address, o.city as provided_city, o.state as provided_state,
                    null as matched_address, null as matched_city, null as matched_state, null as zip, 
                    null as location_id, null as lat, null as lon, 'unmatched' as isa_match, null as store_id
                    from {data_set_original}.{original_table} o
                    where o.ppid not in(
                        select p.ppid from {data_set_original}.{location_matching_table} p
                    )
                 )order by row
            """
    elif algorithm == LMAlgo.CHAIN:
        query = f"""CREATE OR REPLACE TABLE {data_set_final}.{final_table} AS
                    SELECT * FROM(
                    select ROW_NUMBER() OVER() as row, 
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
                    p.isa_match as isa_match, p.store_id as store_id
                from {data_set_original}.{location_matching_table} p 
                union all
                    select o.ppid as row, o.chain_name as provided_chain, '' as matched_chain, 
                    o.street_address as provided_address, o.city as provided_city, o.state as provided_state,
                    null as matched_address, null as matched_city, null as matched_state, null as zip, 
                    null as location_id, null as lat, null as lon, 'unmatched' as isa_match, null as store_id
                    from {data_set_original}.{original_table} o
                    where o.ppid not in(
                        select p.ppid from {data_set_original}.{location_matching_table} p
                    )
                 )order by row
            """
    elif algorithm == LMAlgo.SIC_CODE:
        query = f"""CREATE OR REPLACE TABLE {data_set_final}.{final_table} AS
                    SELECT * FROM(
                    select ROW_NUMBER() OVER() as row, '' as provided_chain, p.lg_chain as matched_chain,
                    p.clean_addr as provided_address,
                    p.clean_city as provided_city,
                    p.state as provided_state,
                    -- p.zip as provided_zip, 
                    case when p.isa_match = 'unlikely' then null else p.clean_lg_addr end as matched_address,
                    case when p.isa_match = 'unlikely' then null else p.clean_lg_city end as matched_city,
                    case when p.isa_match = 'unlikely' then null else p.lg_state end as matched_state,
                    case when p.isa_match = 'unlikely' then null else p.lg_zip end as zip, 
                    case when p.isa_match = 'unlikely' then null else p.location_id end as location_id,
                    case when p.isa_match = 'unlikely' then null else l.lat end as lat,
                    case when p.isa_match = 'unlikely' then null else l.lon end as lon,
                    p.isa_match as isa_match, 
                    '' as store_id
                from {data_set_original}.{location_matching_table} p
                left join aggdata.location_geofence l on p.location_id = l.location_id
                union all
                    select o.ppid as row, o.chain_name as provided_chain, '' as matched_chain, 
                    o.street_address as provided_address, o.city as provided_city, o.state as provided_state,
                    null as matched_address, null as matched_city, null as matched_state, null as zip, 
                    null as location_id, null as lat, null as lon, 'unmatched' as isa_match, null as store_id
                    from {data_set_original}.{original_table} o
                    where o.ppid not in(
                        select p.ppid from {data_set_original}.{location_matching_table} p
                    )
                 )order by row
        """
    else:
        raise NotImplementedError(f'{algorithm} not expected!')
    # query_job = bq_client.query(query, project=project, job_config=job_config)
    logging.warning(f'It will run {query}')
    query_job = bq_client.query(query, project=project)
    query_job.result()
    _set_table_expiration(data_set_final, final_table, expiration_days_results_table, bq_client)
    # Also creates a results table with only non unlikely results
    # job_config = bigquery.QueryJobConfig(destination=f'{project}.{data_set_original}.{table}_result')
    # job_config.write_disposition = job.WriteDisposition.WRITE_TRUNCATE
    results_table = final_table + '_result'
    query = f'''CREATE OR REPLACE TABLE {data_set_original}.{results_table} AS
                SELECT ROW_NUMBER() OVER() as row, a.*
                from(
                    select * except(row) from {data_set_final}.{final_table}
                                  where isa_match not in ('unlikely', 'unmatched')
                    order by row
                )a
                order by row
        '''
    # query_job = bq_client.query(query, project=project, job_config=job_config)
    query_job = bq_client.query(query, project=project)
    query_job.result()


def execute_location_matching(**context):
    try:
        preprocessed_table = context['dag_run'].conf['table']
        logging.warning(f'log: execute_location_matching started for {preprocessed_table}')
        has_sic_code = context['dag_run'].conf['has_sic_code']
        has_chain = context['dag_run'].conf['has_chain']
        has_zip = context['dag_run'].conf['has_zip']
        has_city = context['dag_run'].conf['has_city']
        has_multiple_chain_id = context['dag_run'].conf['has_multiple_chain_id']
        has_multiple_chain_name = context['dag_run'].conf['has_multiple_chain_name']
        logging.warning(f'has_sic_code: {has_sic_code}')
        logging.warning(f'has_chain: {has_chain}')
        logging.warning(f'has_zip: {has_zip}')
        logging.warning(f'has_city: {has_city}')
        logging.warning(f'has_multiple_chain_id: {has_multiple_chain_id}')
        logging.warning(f'has_multiple_chain_name: {has_multiple_chain_name}')
        has_sic_code = has_sic_code and not has_chain
        logging.warning(f'has_sic_code it eval: {has_sic_code}, type {type(has_sic_code)}')
        location_matching_table = preprocessed_table + '_lm'
        credentials, _ = google.auth.default(scopes=[url_auth_gcp])
        logging.warning('log: credentials readed')
        bq_client = bigquery.Client(project=project, credentials=credentials)
        logging.warning('log: bq_client obtained, will run location_matching')
        _run_location_matching(preprocessed_table, location_matching_table, bq_client,
                               LMAlgo.SIC_CODE if has_sic_code else LMAlgo.CHAIN, has_zip, has_city,
                               has_multiple_chain_id, has_multiple_chain_name)
        logging.warning(f'log: location_matching ended in table {location_matching_table}')
    except Exception as e:
        logging.exception(f'Error with {e} and this traceback: {traceback.format_exc()}')


def prepare_results_table(**context):
    try:
        logging.warning('log: prepare_results_table')
        preprocessed_table = context['dag_run'].conf['table']
        has_sic_code = context['dag_run'].conf['has_sic_code']
        has_chain = context['dag_run'].conf['has_chain']
        has_multiple_chain_id = context['dag_run'].conf['has_multiple_chain_id']
        has_multiple_chain_name = context['dag_run'].conf['has_multiple_chain_name']
        logging.warning(f'has_sic_code: {has_sic_code}')
        logging.warning(f'has_chain: {has_chain}')
        has_sic_code = has_sic_code and not has_chain
        logging.warning(f'has_sic_code it eval: {has_sic_code}, type {type(has_sic_code)}')
        credentials, _ = google.auth.default(scopes=[url_auth_gcp])
        bq_client = bigquery.Client(project=project, credentials=credentials)
        logging.warning(f'log: bq_client obtained, will create final table {preprocessed_table}')
        _create_final_table(preprocessed_table, preprocessed_table + '_lm', preprocessed_table, bq_client,
                            LMAlgo.SIC_CODE if has_sic_code else LMAlgo.CHAIN, has_multiple_chain_id,
                            has_multiple_chain_name)
        logging.warning('log: prepare_results ended')
    except Exception as e:
        logging.exception(f'Error with {e} and this traceback: {traceback.format_exc()}')


def delete_temp_data(**context):
    logging.warning('log: delete_temp_data')
    if delete_intermediate_tables:
        preprocessed_table = context['dag_run'].conf["table"]
        location_matching_table = preprocessed_table + '_lm'
        result_table = preprocessed_table + '_result'
        logging.info(f'Start to delete lm_table {location_matching_table} and results table {result_table}')
        credentials, _ = google.auth.default(scopes=[url_auth_gcp])
        bq_client = bigquery.Client(project=project, credentials=credentials)
        bq_client.delete_table(f'{data_set_original}.{location_matching_table}')
        bq_client.delete_table(f'{data_set_original}.{result_table}')


# define tasks
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
prepare_results_table.set_upstream(execute_location_matching)
send_email_results.set_upstream(prepare_results_table)
delete_temp_data.set_upstream(send_email_results)
