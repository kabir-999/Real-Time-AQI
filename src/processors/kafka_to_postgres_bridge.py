#!/usr/bin/env python3

import json
import time
import psycopg2
from confluent_kafka import Consumer, KafkaError
import logging
from datetime import datetime, timezone, timedelta

# Set IST time zone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
kafka_conf = {
    'bootstrap.servers': 'localhost:9095',
    'group.id': 'postgres-writer-group',
    'auto.offset.reset': 'earliest',  # Changed from 'latest' to 'earliest'
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000,
    'session.timeout.ms': 30000,
    'heartbeat.interval.ms': 3000
}

# PostgreSQL configuration
postgres_conf = {
    'host': 'localhost',
    'port': '5433',
    'database': 'aqi_db',
    'user': 'aqi_user',
    'password': 'aqi_password'
}

def parse_aqi_data(message_value):
    """Parse AQI data from Kafka message"""
    try:
        data = json.loads(message_value.decode('utf-8'))
        return {
            'region': data.get('region', 'Unknown'),
            'aqi': float(data.get('AQI', 0)),
            'pm25': float(data.get('PM2.5', 0)),
            'pm10': float(data.get('PM10', 0)),
            'temperature': float(data.get('temperature', 0)),
            'humidity': float(data.get('humidity', 0)),
            'no2': float(data.get('NO2', 0)),
            'so2': float(data.get('SO2', 0)),
            'co': float(data.get('CO', 0)),
            'o3': float(data.get('O3', 0))
        }
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        return None

def write_to_postgres(data, conn):
    """Write aggregated data to PostgreSQL"""
    try:
        cursor = conn.cursor()

        # Skip Unknown and Test regions
        if data['region'] in ['Unknown', 'test']:
            logger.info(f"⏭️ Skipping invalid region: {data['region']}")
            return

        # Simple aggregation: group by region (you can make this more sophisticated)
        cursor.execute("""
            INSERT INTO realtime_metrics_enhanced
            (window_start, window_end, region, avg_aqi, avg_temperature, avg_humidity,
             avg_pm25, avg_pm10, avg_no2, avg_so2, avg_co, avg_o3)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            datetime.now(IST),
            datetime.now(IST),
            data['region'],
            data['aqi'],
            data['temperature'],
            data['humidity'],
            data['pm25'],
            data['pm10'],
            data['no2'],
            data['so2'],
            data['co'],
            data['o3']
        ))

        conn.commit()
        cursor.close()
        logger.info(f"✅ Written data for region: {data['region']}")

    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {e}")
        conn.rollback()

def main():
    """Main function to consume from Kafka and write to PostgreSQL"""
    logger.info("🚀 Starting Kafka to PostgreSQL Bridge")

    # Create Kafka consumer
    consumer = Consumer(kafka_conf)
    consumer.subscribe(['iot-sensor-data'])

    # Connect to PostgreSQL
    conn = psycopg2.connect(**postgres_conf)

    try:
        logger.info("📡 Listening for messages from Kafka...")

        processed_count = 0
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("Reached end of partition")
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            # Parse and process the message
            data = parse_aqi_data(msg.value())
            if data:
                write_to_postgres(data, conn)
                processed_count += 1

                # Log progress every 10 messages
                if processed_count % 10 == 0:
                    logger.info(f"Processed {processed_count} messages")

    except KeyboardInterrupt:
        logger.info("⏹Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        consumer.close()
        conn.close()
        logger.info("Shutdown complete")

if __name__ == '__main__':
    main()
