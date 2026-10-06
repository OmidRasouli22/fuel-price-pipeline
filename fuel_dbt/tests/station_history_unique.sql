select station_id, valid_from, count(*) as n
from {{ ref('station_history') }}
group by station_id, valid_from
having count(*) > 1
