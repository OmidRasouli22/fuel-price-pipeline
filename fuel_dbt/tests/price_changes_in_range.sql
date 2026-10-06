{{ config(severity='warn') }}

select *
from {{ ref('price_changes') }}
where diesel not between 1 and 3
   or e5 not between 1 and 3
   or e10 not between 1 and 3
