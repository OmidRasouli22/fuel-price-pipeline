select
    station_uuid as station_id,
    date as changed_at,
    date(from_utc_timestamp(date, 'Europe/Berlin')) as price_date,
    nullif(diesel, 0) as diesel,
    nullif(e5, 0) as e5,
    nullif(e10, 0) as e10
from {{ source('raw', 'history_prices') }}
where diesel > 0 or e5 > 0 or e10 > 0
