-- Price patterns behind the charts in the README.
-- Run on Databricks after the history job. The results are saved in the
-- CSV files in this folder.
--
-- price_changes only has a row when a price changes. To get the price at
-- a given hour, every station gets one row per hour, and that row takes
-- the last price before it.

with ch as (
  select station_id,
         from_utc_timestamp(changed_at, 'Europe/Berlin') as t,
         e5, diesel
  from fuel.cleaned.price_changes
),
bounds as (select min(t) as lo, max(t) as hi from ch),
hours as (
  -- skip the first day: before a station's first change its price is unknown
  select explode(sequence(date_trunc('hour', lo) + interval 1 day,
                          date_trunc('hour', hi), interval 1 hour)) as h
  from bounds
  where date_trunc('hour', lo) + interval 1 day <= date_trunc('hour', hi)
),
grid as (
  select s.station_id, h.h as t, cast(null as double) as e5,
         cast(null as double) as diesel, 1 as is_grid
  from (select distinct station_id from ch) s cross join hours h
),
filled as (
  select station_id, t, is_grid,
         last(e5, true) over w as e5, last(diesel, true) over w as diesel
  from (select station_id, t, e5, diesel, 0 as is_grid from ch union all select * from grid)
  window w as (partition by station_id order by t, is_grid
               rows between unbounded preceding and current row)
),
by_hour as (
  select 'hour' as section, lpad(cast(hour(t) as string), 2, '0') as k,
         round(avg(e5), 3) as v1, round(avg(diesel), 3) as v2
  from filled where is_grid = 1 group by hour(t)
),
by_day as (
  select 'day' as section, cast(date(t) as string) as k,
         round(avg(e5), 3) as v1, round(avg(diesel), 3) as v2
  from filled where is_grid = 1 group by date(t)
),
by_brand as (
  select 'brand' as section, upper(trim(s.brand)) as k,
         round(avg(f.e5), 3) as v1, count(distinct f.station_id) as v2
  from filled f
  join fuel.cleaned.station_history s on f.station_id = s.station_id and s.is_current
  where f.is_grid = 1 and s.brand is not null and trim(s.brand) <> ''
  group by upper(trim(s.brand))
  having count(distinct f.station_id) >= 200
),
spread as (
  -- difference between the highest and lowest E5 price of a station in one day
  select 'spread' as section, 'e5' as k, round(avg(mx - mn), 3) as v1, count(*) as v2
  from (select station_id, date(t), max(e5) as mx, min(e5) as mn
        from filled where is_grid = 1 and e5 is not null
        group by station_id, date(t))
),
changes as (
  select 'changes_per_station_day' as section, 'all' as k,
         round(count(*) / count(distinct station_id, date(t)), 1) as v1,
         count(distinct date(t)) as v2
  from ch
),
data_range as (
  select 'range' as section,
         cast(min(date(t)) as string) || ' to ' || cast(max(date(t)) as string) as k,
         count(distinct station_id) as v1, count(*) as v2
  from ch
)
select * from data_range
union all select * from changes
union all select * from spread
union all select * from by_hour
union all select * from by_day
union all select * from by_brand
order by section, k
