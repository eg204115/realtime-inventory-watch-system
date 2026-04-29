from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession.builder \
    .appName("ClickstreamAnalysis") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Add timestamp + watermark
events = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
).withWatermark("event_time", "10 minutes")

# Split streams
views = events.filter(col("event_type") == "view")
purchases = events.filter(col("event_type") == "purchase")

# Aggregations
view_counts = views.groupBy(
    window(col("event_time"), "10 minutes"),
    col("product_id")
).count().withColumnRenamed("count", "view_count")

purchase_counts = purchases.groupBy(
    window(col("event_time"), "10 minutes"),
    col("product_id")
).count().withColumnRenamed("count", "purchase_count")

# Join
joined = view_counts.join(
    purchase_counts,
    ["window", "product_id"],
    "left"
).fillna(0)

# Alert condition
alerts = joined.filter(
    (col("view_count") > 100) & (col("purchase_count") < 5)
)

query = alerts.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/alerts") \
    .option("checkpointLocation", "checkpoint/alerts") \
    .start()

query.awaitTermination()