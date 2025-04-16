from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, max, min, sum as _sum, count, window, when, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import time

# Start measuring streaming processing logic time
stream_start = time.time()
# Create Spark session
spark = SparkSession.builder \
    .appName("BTC Spark Streaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType() \
    .add("price", StringType()) \
    .add("best_bid", StringType()) \
    .add("best_ask", StringType()) \
    .add("side", StringType()) \
    .add("time", StringType()) \
    .add("trade_id", IntegerType()) \
    .add("last_size", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_prices") \
    .load()

# Parse JSON
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Type conversion
parsed = parsed.withColumn("price", col("price").cast("float")) \
               .withColumn("last_size", col("last_size").cast("float")) \
               .withColumn("time", to_timestamp(col("time")))

# Aggregations by 1-minute windows
aggregated = parsed \
    .withWatermark("time", "1 minute")\
    .groupBy(window(col("time"), "1 minute")) \
    .agg(
        avg("price").alias("avg_price"),
        max("price").alias("max_price"),
        min("price").alias("min_price"),
        _sum("last_size").alias("total_volume"),
        count("*").alias("trade_count"),
        count(when(col("side") == "buy", True)).alias("buy_count"),
        count(when(col("side") == "sell", True)).alias("sell_count")
    )

# Add analysis columns
aggregated = aggregated \
    .withColumn("price_range", col("max_price") - col("min_price")) \
    .withColumn("volatility_flag", when((col("max_price") - col("min_price")) > 50, "High").otherwise("Normal")) \
    .withColumn("market_sentiment", when(col("buy_count") > col("sell_count") * 1.2, "Bullish")
                .when(col("sell_count") > col("buy_count") * 1.2, "Bearish")
                .otherwise("Neutral")) \
    .withColumn("volume_spike", when(col("total_volume") > 100.0, "Spike").otherwise("Normal"))

# Select final columns for output
final_output = aggregated.selectExpr(
    "window.start as window_start",
    "window.end as window_end",
    "avg_price", "max_price", "min_price", "price_range",
    "volatility_flag", "total_volume", "trade_count",
    "(buy_count * 1.0) / (sell_count + 1e-5) as buy_sell_ratio",
    "market_sentiment", "volume_spike"
)

#  Add this right after defining final_output
stream_end = time.time()
print(f"Streaming Aggregation Logic Time: {stream_end - stream_start:.2f} seconds")

# Write to CSV
csv_query = final_output.writeStream \
    .format("csv") \
    .option("path", "/app/data/stream_csv/") \
    .option("header", True) \
    .option("checkpointLocation", "/app/checkpoints/stream_metrics") \
    .outputMode("append") \
    .start()

# Also write to console for debugging
console_query = final_output.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for both
csv_query.awaitTermination()
console_query.awaitTermination()