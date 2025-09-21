import os
os.environ['HADOOP_OPTS'] = '-Djava.security.auth.login.config=/dev/null'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['SPARK_USER'] = 'testuser'

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("IotDataStreaming") \
    .master("local[*]") \
    .config("spark.jars", "/Users/kabirmathur/Documents/AQI/libs/spark-sql-kafka-0-10_2.12-3.5.0.jar") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema for the incoming JSON data from Kafka
schema = StructType([
    StructField("date", StringType(), True),
    StructField("region", StringType(), True),
    StructField("AQI", DoubleType(), True),
    StructField("PM2.5", DoubleType(), True),
    StructField("PM10", DoubleType(), True),
    StructField("NO2", DoubleType(), True),
    StructField("SO2", DoubleType(), True),
    StructField("CO", DoubleType(), True),
    StructField("O3", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("hospital_visits", IntegerType(), True),
    StructField("emergency_visits", IntegerType(), True),
    StructField("mobility_index", DoubleType(), True),
    StructField("school_closures", StringType(), True),
    StructField("public_transport_usage", DoubleType(), True),
    StructField("mask_usage_rate", DoubleType(), True),
    StructField("lockdown_status", StringType(), True),
    StructField("industrial_activity", DoubleType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("construction_activity", DoubleType(), True),
    StructField("respiratory_admissions", IntegerType(), True),
    StructField("population_density", DoubleType(), True),
    StructField("green_cover_percentage", DoubleType(), True)
])

# Read from Kafka topic as a streaming DataFrame
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9095") \
    .option("subscribe", "iot-sensor-data") \
    .load()

# Deserialize the value from Kafka (binary) to a string and then to the defined schema
iot_data_df = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Add a timestamp column for windowing
iot_data_with_ts = iot_data_df.withColumn("timestamp", col("date").cast(TimestampType()))

# Perform a simple aggregation over a 1-minute tumbling window
aggregated_df = iot_data_with_ts \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("region")
    ) \
    .agg(
        {"AQI": "avg", "temperature": "avg", "humidity": "avg", "PM2.5": "avg", "PM10": "avg", "NO2": "avg", "SO2": "avg", "CO": "avg", "O3": "avg"}
    ) \
    .withColumnRenamed("avg(AQI)", "avg_aqi") \
    .withColumnRenamed("avg(temperature)", "avg_temperature") \
    .withColumnRenamed("avg(humidity)", "avg_humidity") \
    .withColumnRenamed("avg(PM2.5)", "avg_pm25") \
    .withColumnRenamed("avg(PM10)", "avg_pm10") \
    .withColumnRenamed("avg(NO2)", "avg_no2") \
    .withColumnRenamed("avg(SO2)", "avg_so2") \
    .withColumnRenamed("avg(CO)", "avg_co") \
    .withColumnRenamed("avg(O3)", "avg_o3") \
    .withColumnRenamed("window.start", "window_start") \
    .withColumnRenamed("window.end", "window_end")

# Write the aggregated data to PostgreSQL using JDBC
def main():
    """Main function to run the Spark streaming job with error handling"""
    try:
        logger.info("Starting AQI IoT Spark Streaming Job")

        # Test database connection first
        test_db_connection()

        query = aggregated_df.writeStream \
            .outputMode("update") \
            .foreachBatch(write_to_postgres) \
            .trigger(processingTime="1 minute") \
            .start()

        logger.info("Spark streaming job started successfully")
        logger.info("Monitoring for data from Kafka topic: iot-sensor-data")
        logger.info("Writing aggregated data to PostgreSQL")
        logger.info("⏱Processing new data every minute")

        # Wait for the termination of the query
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("⏹Received keyboard interrupt, stopping streaming job...")
        if 'query' in locals():
            query.stop()
        logger.info("Streaming job stopped gracefully")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in Spark streaming job: {e}")
        if 'query' in locals():
            query.stop()
        sys.exit(1)

def write_to_postgres(df, epoch_id):
    """Write batch to PostgreSQL"""
    try:
        logger.info(f"Writing batch {epoch_id} to PostgreSQL")

        df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5433/aqi_db") \
            .option("dbtable", "realtime_metrics_enhanced") \
            .option("user", "aqi_user") \
            .option("password", "aqi_password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        logger.info(f"Successfully wrote batch {epoch_id} to PostgreSQL")

    except Exception as e:
        logger.error(f"Error writing batch {epoch_id} to PostgreSQL: {e}")
        raise

def test_db_connection():
    """Test database connection before starting streaming"""
    try:
        test_df = spark.createDataFrame([("test",)], ["test_column"])

        test_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5433/aqi_db") \
            .option("dbtable", "connection_test") \
            .option("user", "aqi_user") \
            .option("password", "aqi_password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        logger.info("Database connection test successful")
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        raise


if __name__ == '__main__':
    main()
