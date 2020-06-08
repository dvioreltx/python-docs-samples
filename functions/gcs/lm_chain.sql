create temporary function strMatchRate(str1 STRING, str2 string, type string, city string)
  returns float64
  language js as "return scoreMatchFor(str1, str2, type, city)";
create temporary function matchScore(chain_match float64, addr_match float64)
  returns float64
  language js as """
    var rawScore = 50*(1-chain_match) + 50*(1-addr_match);
    var score =
chain_match ==  0                      ? 100 :
chain_match >=  1                      ? Math.min(20, rawScore) :
chain_match >= .8 && addr_match  >= .5 ? Math.min(15, rawScore) :
addr_match  >= .8 && chain_match >= .5 ? Math.min(15, rawScore) :
chain_match >= .3 && addr_match  >= .9 ? Math.min(20, rawScore) :
rawScore;
return score;
"""
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
