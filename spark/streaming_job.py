import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    expr,
    from_json,
    sum,
    when,
    window
)
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType
)

# =========================================================
# LOGGING CONFIGURATION
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("ClickstreamStreamingJob")

# =========================================================
# SPARK SESSION
# =========================================================

spark = SparkSession.builder \
    .appName("EcommerceClickstreamAnalytics") \
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.postgresql:postgresql:42.7.3"
        ])
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

logger.info("Spark session created successfully")

# =========================================================
# KAFKA CONFIG
# =========================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "clickstream-events"

# =========================================================
# POSTGRES CONFIG
# =========================================================

POSTGRES_URL = "jdbc:postgresql://postgres:5432/ecommerce_analytics"

POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# =========================================================
# EVENT SCHEMA
# =========================================================

event_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# =========================================================
# READ STREAM FROM KAFKA
# =========================================================

logger.info("Reading stream from Kafka topic")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# =========================================================
# PARSE JSON DATA
# =========================================================

parsed_df = raw_df.selectExpr(
    "CAST(value AS STRING)"
).select(
    from_json(
        col("value"),
        event_schema
    ).alias("data")
).select("data.*")

# =========================================================
# TRANSFORM DATA
# =========================================================

events_df = parsed_df \
    .withColumn(
        "event_timestamp",
        col("timestamp").cast(TimestampType())
    ) \
    .drop("timestamp")

logger.info("Clickstream events parsed successfully")

# =========================================================
# WRITE RAW EVENTS TO POSTGRESQL
# =========================================================

def write_raw_events(batch_df, batch_id):

    logger.info(f"Writing raw events batch: {batch_id}")

    batch_df.write \
        .jdbc(
            url=POSTGRES_URL,
            table="clickstream_events",
            mode="append",
            properties=POSTGRES_PROPERTIES
        )

raw_events_query = events_df.writeStream \
    .foreachBatch(write_raw_events) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/raw-events") \
    .start()

logger.info("Raw events streaming query started")

# =========================================================
# WATERMARKING
# Handles late-arriving events
# =========================================================

watermarked_df = events_df.withWatermark(
    "event_timestamp",
    "5 minutes"
)

# =========================================================
# SLIDING WINDOW ANALYTICS
# Requirement:
# Views per product over last 10 mins
# Sliding every 5 mins
# =========================================================

windowed_df = watermarked_df.groupBy(
    window(
        col("event_timestamp"),
        "10 minutes",
        "5 minutes"
    ),
    col("product_id")
).agg(

    sum(
        when(
            col("event_type") == "view",
            1
        ).otherwise(0)
    ).alias("total_views"),

    sum(
        when(
            col("event_type") == "purchase",
            1
        ).otherwise(0)
    ).alias("total_purchases")

)

# =========================================================
# CALCULATE CONVERSION RATE
# =========================================================

analytics_df = windowed_df.withColumn(

    "conversion_rate",

    when(
        col("total_views") > 0,

        (
            col("total_purchases") /
            col("total_views")
        ) * 100

    ).otherwise(0)

)

# =========================================================
# FLASH SALE DETECTION
# Condition:
# views > 100 AND purchases < 5
# =========================================================

flash_sale_df = analytics_df.filter(
    (col("total_views") > 50) &
    (col("total_purchases") < 5)
).select(

    col("product_id"),

    col("total_views"),

    col("total_purchases"),

    expr("ROUND(conversion_rate, 2)")
        .alias("conversion_rate"),

    expr("""
        CONCAT(
            'FLASH SALE SUGGESTED: Product ',
            product_id,
            ' has high views but low purchases'
        )
    """).alias("alert_message"),

    col("window.start").alias("window_start"),

    col("window.end").alias("window_end"),

    current_timestamp().alias("created_at")
)

# =========================================================
# WRITE ALERTS TO POSTGRESQL
# =========================================================

def write_flash_sale_alerts(batch_df, batch_id):

    logger.info(f"Writing flash sale alerts batch: {batch_id}")

    batch_df.write \
        .jdbc(
            url=POSTGRES_URL,
            table="flash_sale_alerts",
            mode="append",
            properties=POSTGRES_PROPERTIES
        )

flash_sale_query = flash_sale_df.writeStream \
    .foreachBatch(write_flash_sale_alerts) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/flash-sale-alerts") \
    .start()

logger.info("Flash sale alert query started")

# =========================================================
# OPTIONAL CONSOLE OUTPUT
# Great for demos
# =========================================================

console_query = flash_sale_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

logger.info("Console monitoring started")

# =========================================================
# WAIT FOR TERMINATION
# =========================================================

logger.info("Streaming job is running...")

spark.streams.awaitAnyTermination()