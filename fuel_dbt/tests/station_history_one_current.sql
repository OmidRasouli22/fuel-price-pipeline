select station_id, count_if(is_current) as current_rows
from {{ ref('station_history') }}
group by station_id
having count_if(is_current) <> 1
