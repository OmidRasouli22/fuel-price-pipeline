select
    id as station_id,
    name,
    brand,
    street,
    houseNumber as house_number,
    lpad(cast(postCode as string), 5, '0') as post_code,
    place,
    lat as latitude,
    lng as longitude
from {{ source('raw', 'prices') }}
qualify row_number() over (partition by id order by fetched_at desc) = 1
-- the qualify line keeps only the most recent row per station.Each station appears only once per fetch in the raw table;
