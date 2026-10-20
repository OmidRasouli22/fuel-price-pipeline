# Fuel Price Pipeline

Petrol stations in Germany have to report every price change, and
Tankerkönig makes that data open. This project collects it for the
stations around Freiburg and turns it into clean tables on Databricks.

It has two parts:

- **Live prices:** every hour, the current prices of all stations within
  10 km of Freiburg.
- **History:** the full daily files from Tankerkönig, with every price
  change and the station list for each day.

## How it works

```
Tankerkönig API / history files
          |
          |  Python jobs on Databricks
          v
  Volume: fuel.raw.landing      raw JSON and CSV, never changed
          |
          |  Auto Loader (PySpark)
          v
  fuel.raw.*                    raw tables
          |
          |  dbt
          v
  fuel.cleaned.*                clean tables
  fuel.reporting.daily_prices   one row per station per day
```

The raw replies are saved first and only read later. If a model is
wrong, I can rebuild it from the saved files without asking the API
again.

Loading uses Auto Loader with `availableNow`, so every run picks up only
the new files and then stops. Checkpoints keep track of what was already
loaded.

## The tables

| Table | What it holds |
|---|---|
| `stations` | Latest name, brand and address of each station |
| `prices` | Every hourly reading of diesel, E5 and E10 |
| `price_changes` | Every price change from the history files |
| `station_history` | Station details over time, one row per version |
| `reporting.daily_prices` | Average price per station per day |

`station_history` keeps old versions of a station. When a station changes
its name, brand or address, a new row starts with `valid_from`, and the
old row gets a `valid_to`. It is built with window functions over the
daily station lists in the history files.

A price of 0 in the history means the station has no price for that fuel,
so it becomes null.

## Tests

`dbt build` runs the models and the tests together:

- station ids are not null and unique
- every price belongs to a known station
- no station has two price changes at the same time
- every station has exactly one current version in `station_history`
- prices outside 1 to 3 euros raise a warning, not an error, because
  they can be real

The same models also run on every push in GitHub Actions, without
Databricks. `tests/test_dbt_models.py` starts a local Spark session,
fills the raw tables with a small hand-made dataset, runs `dbt build` on
it, and then checks the results. For example, that a station which
changed its brand gets two versions, and that a price at 23:30 UTC
counts for the next day in Germany.

```
pip install -r requirements-test.txt
pytest
```

You need Java for Spark.

## Jobs

Both jobs are defined in `resources/` as a Databricks Asset Bundle and
run from this repo:

- `fuel_prices`: fetch, load, dbt build. Runs every hour.
- `fuel_history`: fetch the last 7 days of history, load, dbt build.
  Runs every morning at 8.

A failed run sends an email. Both schedules are paused right now.

## Running it

You need a Databricks workspace and a free API key from
[Tankerkönig](https://creativecommons.tankerkoenig.de/).

1. Run `setup/01_catalog.sql` once to create the catalog, schemas and
   volumes.
2. Put your Tankerkönig API key and login in a Databricks secret scope
   called `fuel`.
3. Deploy the jobs:

```
databricks bundle deploy
databricks bundle run fuel_prices
```

To load an older range of history:

```
python ingestion/fetch_history.py 2026-09-01 2026-09-30
```

## Stack

Databricks (Unity Catalog, Volumes, Jobs, Asset Bundles), PySpark Auto
Loader, dbt, Python.

## Data

[Tankerkönig](https://creativecommons.tankerkoenig.de/), licence
CC BY-NC-SA 4.0.
