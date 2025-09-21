#!/usr/bin/env python3

from confluent_kafka import Producer, KafkaError, KafkaException
import json
import csv
import time
import logging
import sys
import random
from datetime import datetime, timedelta, timezone

# Set IST time zone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

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
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

# Delivery callback
def delivery_report(err, msg):
    """Callback function for message delivery reports"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

csv_file = "/Users/kabirmathur/Documents/AQI/data/air_quality_health_dataset.csv"

# Quick preview of CSV rows
def preview_csv():
    """Show first few rows of CSV for preview"""
    try:
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            logger.info("CSV Preview (first 5 rows):")
            for i, row in enumerate(reader):
                logger.info(f"Row {i+1}: {row}")
                if i >= 4:  # Show first 5 rows
                    break
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        sys.exit(1)

def analyze_csv_structure():
    """Analyze CSV structure to get column types and ranges for random generation"""
    try:
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            logger.error("CSV file is empty")
            return None

        logger.info(f"Analyzing {len(rows)} rows from CSV")

        # Get column names and sample values
        sample_row = rows[0]
        column_info = {}

        for column_name, sample_value in sample_row.items():
            column_info[column_name] = {
                'sample': sample_value,
                'type': 'numeric' if sample_value.replace('.', '').replace('-', '').isdigit() else 'string'
            }

            # For numeric columns, calculate min/max from a sample of rows
            if column_info[column_name]['type'] == 'numeric':
                values = []
                for row in rows[:100]:  # Sample first 100 rows for range calculation
                    try:
                        values.append(float(row[column_name]))
                    except (ValueError, TypeError):
                        continue

                if values:
                    column_info[column_name]['min'] = min(values)
                    column_info[column_name]['max'] = max(values)
                    column_info[column_name]['avg'] = sum(values) / len(values)

        logger.info(f"Column analysis: {len(column_info)} columns detected")
        return column_info

    except Exception as e:
        logger.error(f"Error analyzing CSV: {e}")
        return None

def generate_random_row(column_info, regions_list):
    """Generate a random row based on CSV column structure"""
    row = {}

    for column_name, info in column_info.items():
        if column_name == 'region':
            # Select random region from the list
            row[column_name] = random.choice(regions_list)
        elif column_name == 'date':
            # Generate random date in 2024-2025 range
            start_date = datetime(2024, 1, 1, tzinfo=IST)
            end_date = datetime(2025, 12, 31, tzinfo=IST)
            random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
            row[column_name] = random_date.strftime('%Y-%m-%d')
        elif info['type'] == 'numeric':
            # Generate random numeric value based on original range
            if 'min' in info and 'max' in info:
                # Add some variation around the original range
                range_size = info['max'] - info['min']
                min_val = info['min'] - range_size * 0.2  # 20% below original min
                max_val = info['max'] + range_size * 0.2  # 20% above original max

                # Generate random value with some noise
                base_value = random.uniform(min_val, max_val)

                # Add small random noise (±10%)
                noise = base_value * 0.1 * random.uniform(-1, 1)
                row[column_name] = str(round(base_value + noise, 6))
            else:
                # Fallback: generate reasonable random values
                row[column_name] = str(round(random.uniform(0, 100), 2))
        else:
            # For string columns, use original value or generate similar
            if column_name in ['school_closures', 'lockdown_status']:
                row[column_name] = str(random.randint(0, 1))
            else:
                row[column_name] = info['sample']  # Use original value

    return row

# Main function to send data to Kafka
def send_to_kafka(producer):
    """Send CSV data to Kafka, then switch to random data generation"""
    success_count = 0
    error_count = 0
    max_errors = 5  # Maximum consecutive errors before exiting
    consecutive_errors = 0
    is_generating_random = False

    try:
        # Analyze CSV structure first
        column_info = analyze_csv_structure()
        if not column_info:
            logger.error("Failed to analyze CSV structure")
            return False

        # Get regions for random generation (only valid regions)
        regions_list = ['North', 'South', 'East', 'West', 'Central']
        logger.info(f"Using only valid regions: {regions_list}")

        # First pass: send CSV data
        logger.info("Starting CSV data streaming...")
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)

            # Count total rows first
            total_rows = sum(1 for _ in reader)
            logger.info(f"Starting to stream {total_rows} rows from CSV")

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

                    logger.info(f"CSV row {i+1}/{total_rows}: {row.get('region', 'N/A')} - AQI: {row.get('AQI', 'N/A')}")

                    # Flush every 10 messages to ensure delivery
                    if success_count % 10 == 0:
                        producer.flush(timeout=10)
                        logger.info(f"Flushed {success_count} messages to Kafka")

                    time.sleep(1)  # 1 second delay between messages

                except KafkaException as e:
                    error_count += 1
                    consecutive_errors += 1
                    logger.error(f"Kafka exception sending CSV row {i+1}: {e}")

                    if consecutive_errors >= max_errors:
                        logger.error(f"Too many consecutive errors ({consecutive_errors}). Exiting.")
                        return False

                    time.sleep(5)  # Longer pause after error

                except Exception as e:
                    error_count += 1
                    logger.error(f"Unexpected error sending CSV row {i+1}: {e}")
                    return False

        # Final flush for CSV data
        logger.info("Final flush of CSV messages...")
        producer.flush(timeout=30)
        logger.info("All CSV messages flushed successfully")

        # Switch to random data generation
        logger.info("CSV data completed! Starting random data generation...")
        is_generating_random = True

        # Second pass: generate and send random data indefinitely
        random_count = 0
        while True:
            try:
                # Generate random row
                random_row = generate_random_row(column_info, regions_list)
                random_count += 1

                # Convert row to JSON string
                message = json.dumps(random_row).encode('utf-8')

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

                logger.info(f"Random #{random_count}: {random_row.get('region', 'N/A')} - AQI: {random_row.get('AQI', 'N/A')}")

                # Flush every 10 messages to ensure delivery
                if success_count % 10 == 0:
                    producer.flush(timeout=10)
                    logger.info(f"Flushed {success_count} total messages to Kafka")

                time.sleep(1)  # 1 second delay between messages

            except KafkaException as e:
                error_count += 1
                consecutive_errors += 1
                logger.error(f"Kafka exception sending random data #{random_count}: {e}")

                if consecutive_errors >= max_errors:
                    logger.error(f"Too many consecutive errors ({consecutive_errors}). Exiting.")
                    return False

                time.sleep(5)  # Longer pause after error

            except Exception as e:
                error_count += 1
                logger.error(f"Unexpected error generating random data #{random_count}: {e}")
                return False

    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file}")
        return False
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return False

    logger.info(f"Streaming completed. Success: {success_count}, Errors: {error_count}")
    return success_count > 0


def main():
    """Main function to run the Kafka producer"""
    try:
        logger.info("🚀 Starting Enhanced AQI IoT Kafka Producer")
        logger.info("📋 This producer will send CSV data first, then switch to random data generation")

        # Preview CSV data
        preview_csv()

        # Create producer
        producer = create_producer()

        # Send data to Kafka (CSV first, then random)
        success = send_to_kafka(producer)

        if success:
            logger.info("Kafka producer completed successfully")
            return 0
        else:
            logger.error("Kafka producer failed")
            return 1

    except KeyboardInterrupt:
        logger.info("⏹Received keyboard interrupt, shutting down gracefully...")
        if 'producer' in locals():
            producer.flush(timeout=10)
        return 0
    except Exception as e:
        logger.error(f"Fatal error in Kafka producer: {e}")
        return 1
    finally:
        # Cleanup
        if 'producer' in locals():
            logger.info("🧹 Cleaning up producer...")
            producer.flush(timeout=10)

if __name__ == '__main__':
    sys.exit(main())
