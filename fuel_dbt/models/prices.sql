select
    id as station_id,
    fetched_at,
    isOpen as is_open,
    diesel,
    e5,
    e10
from {{ source('raw', 'prices') }}