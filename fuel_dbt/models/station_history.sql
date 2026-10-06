with daily as (
    select
        uuid as station_id,
        name,
        brand,
        street,
        house_number,
        post_code,
        city,
        latitude,
        longitude,
        to_date(regexp_extract(source_file, '([0-9]{4}-[0-9]{2}-[0-9]{2})-stations', 1)) as list_date,
        struct(name, brand, street, house_number, post_code, city) as attributes
    from {{ source('raw', 'history_stations') }}
),

flagged as (
    select
        *,
        case
            when attributes is distinct from lag(attributes) over (partition by station_id order by list_date)
            then 1 else 0
        end as is_change
    from daily
),

versioned as (
    select
        *,
        sum(is_change) over (partition by station_id order by list_date) as version
    from flagged
),

periods as (
    select
        station_id,
        name,
        brand,
        street,
        house_number,
        post_code,
        city,
        max_by(latitude, list_date) as latitude,
        max_by(longitude, list_date) as longitude,
        min(list_date) as valid_from,
        max(list_date) as last_seen
    from versioned
    group by station_id, version, name, brand, street, house_number, post_code, city
)

select
    *,
    lead(valid_from) over (partition by station_id order by valid_from) as valid_to,
    lead(valid_from) over (partition by station_id order by valid_from) is null as is_current
from periods
