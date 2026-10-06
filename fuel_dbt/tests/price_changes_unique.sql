select station_id, changed_at, count(*) as n
from {{ ref('price_changes') }}
group by station_id, changed_at
having count(*) > 1
