#standardSQL
declare level_of_accuracy string default '';
set level_of_accuracy = 'relaxed';
create temporary function strMatchRate(str1 STRING, str2 string, type string, accuracy string) returns FLOAT64
language js as \"\"\"
return scoreMatchFor(str1, str2, type, accuracy)
\"\"\"
OPTIONS (
library=['gs://javascript_lib/addr_functions.js']
);
-- REPLACE DESTINATION HERE
CREATE OR REPLACE TABLE {data_set_final}.{destination_table} AS
with
sample as (
  select *, concat(sic_code, ',', ifnull(clean_addr, ''), ',', ifnull(clean_city,''), ',', ifnull(state,'')) store
  from (
    select  substr(cast(sic_code as string), 0 ,4) sic_code, clean_addr,
            clean_city, state, zip
    -- REPLACE SOURCE HERE
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
