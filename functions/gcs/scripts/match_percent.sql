select isa_match, count(isa_match)
from location_matching_match.address_full_no_zip___2
group BY (isa_match)
order by 2 desc
