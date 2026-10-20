"""Runs the real dbt models and tests on a small, hand-made dataset.

The raw tables are created in a local Spark session with the same columns
the ingestion jobs write on Databricks. Then `dbt build` runs on top, and
the checks below look at what came out.
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT = ROOT / "fuel_dbt"
PROFILES = ROOT / "ci"

STATION_A = "a1b2c3d4-0000-0000-0000-000000000001"
STATION_B = "a1b2c3d4-0000-0000-0000-000000000002"


def utc(*parts):
    # explicit UTC, so the tests give the same result on any machine
    return datetime(*parts, tzinfo=timezone.utc)


def history_file(day, kind):
    return f"/Volumes/fuel/raw/landing/history/{kind}/2026/10/2026-10-{day:02d}-{kind}.csv"


def live_file(day, time):
    return f"/Volumes/fuel/raw/landing/prices/2026-10-{day:02d}/{time}.json"


def make_raw_tables(spark):
    spark.sql("create database if not exists raw")

    # what load_raw.py writes: one row per station per hourly fetch
    live = []
    for day, hour, diesel, e5, e10 in [
        (1, 7, 1.659, 1.799, 1.739),
        (1, 18, 1.599, 1.749, 1.689),
        (2, 7, 1.649, 1.789, 1.729),
    ]:
        fetched_at = utc(2026, 10, day, hour, 0)
        file = live_file(day, f"{hour:02d}0000")
        live.append((STATION_A, "Tankstelle Wiehre", "ARAL", "Hauptstr.", "12", 79104,
                     "Freiburg", 47.98, 7.85, 1.2, True, diesel, e5, e10, file, fetched_at))
        # postcode with a leading zero, stored as a number by the API
        live.append((STATION_B, "Tankstelle Süd", "Shell", "Basler Str.", "3", 9123,
                     "Freiburg", 47.97, 7.83, 2.5, True, diesel + 0.02, e5 + 0.02, None, file, fetched_at))

    spark.createDataFrame(
        live,
        "id string, name string, brand string, street string, houseNumber string, "
        "postCode long, place string, lat double, lng double, dist double, isOpen boolean, "
        "diesel double, e5 double, e10 double, source_file string, fetched_at timestamp",
    ).write.mode("overwrite").saveAsTable("raw.prices")

    # what load_history.py writes from the daily price files
    history_prices = [
        (utc(2026, 10, 1, 6, 0), STATION_A, 1.659, 1.799, 1.739, 1, 1, 1, history_file(1, "prices")),
        (utc(2026, 10, 1, 21, 30), STATION_A, 1.599, 1.749, 1.689, 1, 1, 1, history_file(1, "prices")),
        # 23:30 UTC is already the next day in Germany
        (utc(2026, 10, 1, 23, 30), STATION_A, 1.589, 1.739, 1.679, 1, 1, 1, history_file(1, "prices")),
        # 0 means this station has no price for that fuel
        (utc(2026, 10, 1, 6, 0), STATION_B, 1.679, 1.819, 0.0, 1, 1, 0, history_file(1, "prices")),
        # nothing at all: must be dropped
        (utc(2026, 10, 1, 7, 0), STATION_B, 0.0, 0.0, 0.0, 0, 0, 0, history_file(1, "prices")),
    ]
    spark.createDataFrame(
        history_prices,
        "date timestamp, station_uuid string, diesel double, e5 double, e10 double, "
        "dieselchange int, e5change int, e10change int, source_file string",
    ).write.mode("overwrite").saveAsTable("raw.history_prices")

    # what load_history.py writes from the daily station lists.
    # Station A changes its brand on day 3, station B never changes.
    stations = []
    for day in (1, 2, 3, 4):
        brand = "ARAL" if day < 3 else "JET"
        stations.append((STATION_A, "Tankstelle Wiehre", brand, "Hauptstr.", "12", "79104",
                         "Freiburg", 47.98, 7.85, utc(2015, 1, 1), "{}", history_file(day, "stations")))
        stations.append((STATION_B, "Tankstelle Süd", "Shell", "Basler Str.", "3", "09123",
                         "Freiburg", 47.97, 7.83, utc(2015, 1, 1), "{}", history_file(day, "stations")))
    spark.createDataFrame(
        stations,
        "uuid string, name string, brand string, street string, house_number string, "
        "post_code string, city string, latitude double, longitude double, "
        "first_active timestamp, openingtimes_json string, source_file string",
    ).write.mode("overwrite").saveAsTable("raw.history_stations")


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    session = (
        SparkSession.builder.master("local[1]")
        .appName("fuel-dbt-tests")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    make_raw_tables(session)

    result = dbtRunner().invoke([
        "build",
        "--project-dir", str(DBT_PROJECT),
        "--profiles-dir", str(PROFILES),
        "--target-path", str(tmp_path_factory.mktemp("target")),
    ])
    if not result.success:
        failed = [
            f"{r.node.name}: {r.status} {r.message or ''}"
            for r in (result.result or [])
            if str(r.status) in ("error", "fail")
        ]
        pytest.fail(f"dbt build failed: {failed or result.exception}")

    yield session
    session.stop()


def rows(spark, query):
    return [r.asDict() for r in spark.sql(query).collect()]


def test_stations_keep_latest_row_and_fix_postcode(spark):
    result = rows(spark, "select station_id, post_code from cleaned.stations order by station_id")
    assert result == [
        {"station_id": STATION_A, "post_code": "79104"},
        {"station_id": STATION_B, "post_code": "09123"},
    ]


def test_daily_prices_average_per_day(spark):
    result = rows(spark, f"""
        select price_date, avg_diesel, readings
        from reporting.daily_prices
        where station_id = '{STATION_A}'
        order by price_date
    """)
    assert [str(r["price_date"]) for r in result] == ["2026-10-01", "2026-10-02"]
    assert float(result[0]["avg_diesel"]) == pytest.approx(1.629)
    assert result[0]["readings"] == 2


def test_price_changes_turn_zero_into_null_and_drop_empty_rows(spark):
    result = rows(spark, f"""
        select diesel, e10 from cleaned.price_changes
        where station_id = '{STATION_B}'
    """)
    assert result == [{"diesel": 1.679, "e10": None}]


def test_price_changes_use_german_date(spark):
    result = rows(spark, f"""
        select price_date from cleaned.price_changes
        where station_id = '{STATION_A}'
        order by changed_at
    """)
    # 06:00 and 21:30 UTC are 1 October in Berlin, 23:30 UTC is 2 October
    assert [str(r["price_date"]) for r in result] == ["2026-10-01", "2026-10-01", "2026-10-02"]


def test_station_history_starts_new_version_on_change(spark):
    result = rows(spark, f"""
        select brand, valid_from, valid_to, is_current
        from cleaned.station_history
        where station_id = '{STATION_A}'
        order by valid_from
    """)
    assert [(r["brand"], str(r["valid_from"]), str(r["valid_to"]), r["is_current"]) for r in result] == [
        ("ARAL", "2026-10-01", "2026-10-03", False),
        ("JET", "2026-10-03", "None", True),
    ]


def test_station_without_changes_has_one_version(spark):
    result = rows(spark, f"""
        select count(*) as n from cleaned.station_history where station_id = '{STATION_B}'
    """)
    assert result == [{"n": 1}]
