from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, sum as _sum, count, when, col, to_timestamp
import time

# Start timer
batch_start = time.time()

# Spark session with JDBC SQLite
spark = SparkSession.builder \
    .appName("BTC Batch Processor") \
    .master("local[*]") \
    .config("spark.driver.extraClassPath", "/app/sqlite-jdbc-3.36.0.3.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load from SQLite via JDBC
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlite:/app/data/crypto_prices.db") \
    .option("dbtable", "btc_prices") \
    .option("driver", "org.sqlite.JDBC") \
    .load()

# Ensure proper types
df = df.withColumn("price", col("price").cast("float")) \
       .withColumn("last_size", col("last_size").cast("float")) \
       .withColumn("time", to_timestamp(col("time")))

# Perform similar aggregation
aggregated = df.groupBy().agg(
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
    .withColumn("volume_spike", when(col("total_volume") > 100.0, "Spike").otherwise("Normal")) \
    .withColumn("buy_sell_ratio", (col("buy_count") * 1.0) / (col("sell_count") + 1e-5))


# Save output to CSV (can also be used in dashboard)
aggregated.write \
    .mode("append") \
    .option("header", True) \
    .csv("/app/data/batch_output")

# End timer
batch_end = time.time()
print(f"Batch Aggregation Logic Time: {batch_end - batch_start:.2f} seconds")