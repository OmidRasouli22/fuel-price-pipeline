from pyspark.sql import functions as F

# when running this script in a Databricks notebook, the SparkSession is already available as `spark`. However, when running this script as a standalone Python script, we need to create a SparkSession explicitly.
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


files = (
    spark.read.option("multiLine", True)
    .json("/Volumes/fuel/raw/landing/prices/*/*.json")
)

stations = files.select(
    F.explode("stations").alias("s"),
    F.col("_metadata.file_path").alias("source_file"),
    F.col("_metadata.file_modification_time").alias("fetched_at"),
).select("s.*", "source_file", "fetched_at")

stations.write.mode("overwrite").saveAsTable("fuel.raw.prices")
