CREATE TEMP FUNCTION cleanStr(str string, type string) RETURNS string
LANGUAGE js AS "return cleanStr(str, type)";
create temporary function strMatchRate(str1 STRING, str2 string, type string, city string) returns FLOAT64
language js as "return scoreMatchFor(str1, str2, type, city)";
create temporary function matchScore(chain_match float64, addr_match float64)
returns float64
language js as """
   var rawScore = 50*(1-chain_match) + 50*(1-addr_match);
   var score =
     (chain_match == 0 || addr_match == 0)  ? 100 :
     chain_match >   1 && addr_match  >= .5 ? Math.min(5,  rawScore) :
     chain_match >= .8 && addr_match  >= .5 ? Math.min(15, rawScore) :
     addr_match  >= .8 && chain_match >= .5 ? Math.min(15, rawScore) :
     chain_match >= .3 && addr_match  >= .9 ? Math.min(20, rawScore) :
     rawScore;
   return score;
 """
OPTIONS (
library=['gs://javascript_lib/addr_functions.js']
);
--create or replace table temp.petsmart_matches as
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
        #####################################
      ###     ENTER LOCATION FILE HERE  ###
#####################################
from `dannyv.matching_list`
) a
  left join stateAbbrs b on lower(a.state) = lower(state_name)
where chain is not null
)
),
our_states as (
    select distinct state from sample
  ),
aggdata as (
    select chain_name lg_chain, lat lg_lat, lon lg_lon, addr lg_addr, city lg_city, a.state lg_state, substr(trim(zip),0,5) lg_zip, location_id
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
     row_number() over (partition by store order by match_score, addr_match desc, chain_match desc, best_distance) match_rank
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
   select distinct a.* from sample a join (select store from match_liklihood where match_rank = 1 and isa_match not in ('definitely', 'very probably', 'probably')) b on a.store = b.store
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
     row_number() over (partition by store order by match_score, addr_match desc, chain_match desc, best_distance) match_rank
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
    select 'r1' match_round, * except (match_rank) from match_liklihood union all select 'r2', * except (match_rank) from match_liklihood2
    )
)
select row_number() over () store_id, isa_match, round(100-match_score,1) match_score, grade, * except (grade, isa_match, match_score, match_rank),
cleanStr(chain, 'chain') clean_chain, cleanStr(lg_chain, 'chain') clean_lg_chain,
cleanStr(addr,  'addr')  clean_addr,  cleanStr(lg_addr,  'addr')  clean_lg_addr
from final
where match_rank = 1
order by match_score desc, chain_match desc, addr_match desc, chain