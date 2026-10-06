from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

LANDING = "/Volumes/fuel/raw/landing/history"
CHECKPOINTS = "/Volumes/fuel/raw/checkpoints"

PRICES_SCHEMA = """
    date timestamp, station_uuid string,
    diesel double, e5 double, e10 double,
    dieselchange int, e5change int, e10change int
"""

STATIONS_SCHEMA = """
    uuid string, name string, brand string, street string, house_number string,
    post_code string, city string, latitude double, longitude double,
    first_active timestamp, openingtimes_json string
"""


def load(kind, schema, table, **options):
    files = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .options(**options)
        .schema(schema)
        .load(f"{LANDING}/{kind}")
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
    (
        files.writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/history_{kind}")
        .trigger(availableNow=True)
        .toTable(table)
        .awaitTermination()
    )


load("prices", PRICES_SCHEMA, "fuel.raw.history_prices")
load("stations", STATIONS_SCHEMA, "fuel.raw.history_stations", escape='"', multiLine=True)
