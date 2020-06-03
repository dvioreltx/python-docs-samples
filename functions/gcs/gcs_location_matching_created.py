import datetime

import logging
from enum import Enum
import traceback
from airflow import DAG
from airflow.operators import bash_operator
from airflow.operators.python_operator import PythonOperator
import google.auth
from google.cloud import bigquery
# from google.cloud import bigquery_storage_v1beta1 Error with this!
from google.cloud import storage
from google.oauth2 import service_account
from os.path import basename

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
    description = 'Location Matching Tool',
    # Not scheduled, trigger only
    schedule_interval=None
)

# Print the dag_run's configuration, which includes information about the
# Cloud Storage object change.


# Define functions and query strings

delete_intermediate_tables = False
send_email_on_error = True
data_set_original = "location_matching_file"
data_set_final = "location_matching_match"
# bucket = 'location_matching' TODO: Volver a poner este bucket
bucket = 'dannyv'
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


def execute_location_matching(**context):
    try:
        logging.warning('log: execute_location_matching started')
        dag_run = context['dag_run']
        preprocessed_table = dag_run.conf["table"]
        location_matching_table = preprocessed_table + '_lm'
        credentials, _ = google.auth.default(scopes=[url_auth_gcp])
        logging.warning('log: credentials readed')
        bq_client = bigquery.Client(project=project, credentials=credentials)
        logging.warning('log: bq_client obtained, will run location_matching')
        _run_location_matching(preprocessed_table, location_matching_table, bq_client, LMAlgo.CHAIN)
        logging.warning('log: locatin_matching ended')
    except Exception as e:
        logging.exception(f'Error with {e} and this traceback: {traceback.format_exc()}')



def prepare_results_table():
    logging.warning('log: prepare_results_table')


def send_email_results():
    logging.warning('log: send_email_results')


def delete_temp_data():
    logging.warning('log: delete_temp_data')


# define tasks
execute_location_matching = PythonOperator(
    task_id='execute_location_matching',
    provide_context=True,
    python_callable=execute_location_matching,
    dag=dag)

prepare_results_table = PythonOperator(
    task_id='prepare_results_table',
    python_callable=prepare_results_table,
    dag=dag)

send_email_results = PythonOperator(
    task_id='send_email_results',
    python_callable=send_email_results,
    dag=dag)

delete_temp_data = PythonOperator(
    task_id='delete_temp_data',
    python_callable=delete_temp_data,
    dag=dag
)

# Setting up Dependencies
prepare_results_table.set_upstream(execute_location_matching)
send_email_results.set_upstream(prepare_results_table)
delete_temp_data.set_upstream(send_email_results)

# Read the file in a Pandas Dataframe, preprocess, save table to BigQuery, push the table_name to a Xcom variable
#    I think we also need the has_sic_code variable.
# Execute the Location Matching Algorithm,
# Create the final table name
# Send email with results
# Delete temp tables and intermediate results
# Maybe airflow variables to determine log level and drop/delete policy?
