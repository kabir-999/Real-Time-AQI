from confluent_kafka import Producer, KafkaError, KafkaException
import json
import csv
import time
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka producer configuration
def create_producer():
    """Create and configure Kafka producer"""
    try:
        producer = Producer({
            'bootstrap.servers': 'localhost:9095',
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
            'enable.idempotence': True,  # Ensure exactly-once delivery
            'delivery.timeout.ms': 30000,  # 30 seconds
            'request.timeout.ms': 10000,   # 10 seconds
        })
        logger.info("✅ Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"❌ Failed to create Kafka producer: {e}")
        raise

# Delivery callback
def delivery_report(err, msg):
    """Callback function for message delivery reports"""
    if err is not None:
        logger.error(f"❌ Message delivery failed: {err}")
    else:
        logger.info(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

csv_file = "/Users/kabirmathur/Documents/AQI/data/air_quality_health_dataset.csv"

# Quick preview of CSV rows
def preview_csv():
    """Show first few rows of CSV for preview"""
    try:
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            logger.info("📄 CSV Preview (first 5 rows):")
            for i, row in enumerate(reader):
                logger.info(f"Row {i+1}: {row}")
                if i >= 4:  # Show first 5 rows
                    break
    except FileNotFoundError:
        logger.error(f"❌ CSV file not found: {csv_file}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Error reading CSV file: {e}")
        sys.exit(1)

# Main function to send data to Kafka
def send_to_kafka(producer):
    """Send CSV data to Kafka with proper error handling"""
    success_count = 0
    error_count = 0
    max_errors = 5  # Maximum consecutive errors before exiting
    consecutive_errors = 0

    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)

            # Count total rows first
            total_rows = sum(1 for _ in reader)
            logger.info(f"📊 Starting to stream {total_rows} rows to Kafka")

            # Reset file pointer
            f.seek(0)
            reader = csv.DictReader(f)

            for i, row in enumerate(reader):
                try:
                    # Convert row to JSON string
                    message = json.dumps(row).encode('utf-8')

                    # Send message to Kafka asynchronously
                    producer.produce(
                        'iot-sensor-data',
                        value=message,
                        callback=delivery_report
                    )

                    # Poll for delivery reports
                    producer.poll(0)

                    success_count += 1
                    consecutive_errors = 0  # Reset error counter

                    logger.info(f"✅ Sent row {i+1}/{total_rows}: {row.get('region', 'N/A')} - AQI: {row.get('AQI', 'N/A')}")

                    # Flush every 10 messages to ensure delivery
                    if success_count % 10 == 0:
                        producer.flush(timeout=10)
                        logger.info(f"📤 Flushed {success_count} messages to Kafka")

                    time.sleep(2)  # 2 second delay between messages

                    # For testing: exit after sending 10 rows
                    if i >= 9:
                        logger.info("📝 Sent 10 test rows. Exiting gracefully.")
                        break

                except KafkaException as e:
                    error_count += 1
                    consecutive_errors += 1
                    logger.error(f"❌ Kafka exception sending row {i+1}: {e}")

                    if consecutive_errors >= max_errors:
                        logger.error(f"🚫 Too many consecutive errors ({consecutive_errors}). Exiting.")
                        return False

                    time.sleep(5)  # Longer pause after error

                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Unexpected error sending row {i+1}: {e}")
                    return False

        # Final flush to ensure all messages are delivered
        logger.info("🔄 Final flush of remaining messages...")
        producer.flush(timeout=30)
        logger.info("✅ All messages flushed successfully")

    except FileNotFoundError:
        logger.error(f"❌ CSV file not found: {csv_file}")
        return False
    except Exception as e:
        logger.error(f"❌ Error reading CSV file: {e}")
        return False

    logger.info(f"📊 Streaming completed. Success: {success_count}, Errors: {error_count}")
    return success_count > 0


def main():
    """Main function to run the Kafka producer"""
    try:
        logger.info("🚀 Starting AQI IoT Kafka Producer")

        # Preview CSV data
        preview_csv()

        # Create producer
        producer = create_producer()

        # Send data to Kafka
        success = send_to_kafka(producer)

        if success:
            logger.info("✅ Kafka producer completed successfully")
            return 0
        else:
            logger.error("❌ Kafka producer failed")
            return 1

    except KeyboardInterrupt:
        logger.info("⏹️ Received keyboard interrupt, shutting down gracefully...")
        if 'producer' in locals():
            producer.flush(timeout=10)
        return 0
    except Exception as e:
        logger.error(f"❌ Fatal error in Kafka producer: {e}")
        return 1
    finally:
        # Cleanup
        if 'producer' in locals():
            logger.info("🧹 Cleaning up producer...")
            producer.flush(timeout=10)

if __name__ == '__main__':
    sys.exit(main())