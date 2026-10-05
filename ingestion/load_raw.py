from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

LANDING = "/Volumes/fuel/raw/landing/prices"
CHECKPOINT = "/Volumes/fuel/raw/checkpoints/prices"

files = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT)
    .option("cloudFiles.inferColumnTypes", True)
    .option("multiLine", True)
    .load(LANDING)
)

stations = files.select(
    F.explode("stations").alias("s"),
    F.col("_metadata.file_path").alias("source_file"),
    F.col("_metadata.file_modification_time").alias("fetched_at"),
).select("s.*", "source_file", "fetched_at")

(
    stations.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable("fuel.raw.prices")
    .awaitTermination()
)
