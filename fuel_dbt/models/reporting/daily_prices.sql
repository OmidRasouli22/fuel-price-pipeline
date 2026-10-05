select
    p.station_id,
    s.name,
    s.brand,
    date(from_utc_timestamp(p.fetched_at, 'Europe/Berlin')) as price_date,
    round(avg(p.diesel), 3) as avg_diesel,
    round(avg(p.e5), 3) as avg_e5,
    round(avg(p.e10), 3) as avg_e10,
    count(*) as readings
from {{ ref('prices') }} p
join {{ ref('stations') }} s using (station_id)
group by all

-- group by all groups every column that it is not an aggregate, which saves listing them again