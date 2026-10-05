from pyspark.sql import functions as F

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
