from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
# Added extra config for stability on Windows
spark = SparkSession.builder \
    .appName("ClickstreamAnalysis") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema of your JSON data
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("timestamp", StringType())

# 1. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Convert Kafka value and Parse JSON
json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# 3. Convert timestamp and ADD WATERMARKING
# Watermarking is required for streaming-streaming joins to handle state
events = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
).withWatermark("event_time", "10 minutes")

# 4. Split events
views = events.filter(col("event_type") == "view")
purchases = events.filter(col("event_type") == "purchase")

# 5. Aggregation
# We perform the windowed count on both streams
view_counts = views.groupBy(
    window(col("event_time"), "10 minutes"),
    col("product_id")
).count().withColumnRenamed("count", "view_count")

purchase_counts = purchases.groupBy(
    window(col("event_time"), "10 minutes"),
    col("product_id")
).count().withColumnRenamed("count", "purchase_count")

# 6. Join data
# Both sides of the join now have watermarks through the 'events' dataframe
joined = view_counts.join(
    purchase_counts,
    ["window", "product_id"],
    "left"
).fillna(0)

# 7. Alert Condition
alerts = joined.filter(
    (col("view_count") > 100) & (col("purchase_count") < 5)
)

# 8. Output
# Stream-stream join supports append mode here
query = alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "checkpoint") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # Create Spark session
# spark = SparkSession.builder \
#     .appName("ClickstreamAnalysis") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Schema of your JSON data
# schema = StructType() \
#     .add("user_id", IntegerType()) \
#     .add("product_id", IntegerType()) \
#     .add("event_type", StringType()) \
#     .add("timestamp", StringType())

# # Read from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "clickstream") \
#     .option("startingOffsets", "latest") \
#     .load()

# # Convert Kafka value (binary → string → JSON)
# json_df = df.selectExpr("CAST(value AS STRING)")

# parsed_df = json_df.select(
#     from_json(col("value"), schema).alias("data")
# ).select("data.*")

# # Convert timestamp
# events = parsed_df.withColumn(
#     "event_time",
#     to_timestamp(col("timestamp"))
# )

# # ---------------------------------------
# # 🔹 Split events
# # ---------------------------------------
# views = events.filter(col("event_type") == "view")
# purchases = events.filter(col("event_type") == "purchase")

# # ---------------------------------------
# # 🔹 Aggregation (last 10 minutes)
# # ---------------------------------------
# view_counts = views.groupBy(
#     window(col("event_time"), "10 minutes"),
#     col("product_id")
# ).count().withColumnRenamed("count", "view_count")

# purchase_counts = purchases.groupBy(
#     window(col("event_time"), "10 minutes"),
#     col("product_id")
# ).count().withColumnRenamed("count", "purchase_count")

# # ---------------------------------------
# # 🔹 Join data
# # ---------------------------------------
# joined = view_counts.join(
#     purchase_counts,
#     ["window", "product_id"],
#     "left"
# ).fillna(0)

# # ---------------------------------------
# # 🔥 ALERT CONDITION
# # ---------------------------------------
# alerts = joined.filter(
#     (col("view_count") > 100) & (col("purchase_count") < 5)
# )

# # ---------------------------------------
# # 🖥️ OUTPUT (for testing)
# # ---------------------------------------
# query = alerts.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# query.awaitTermination()