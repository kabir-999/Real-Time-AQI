from confluent_kafka import Consumer, KafkaException
import json
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka consumer configuration
def create_consumer():
    """Create and configure Kafka consumer"""
    try:
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'iot-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 3000,
        })
        logger.info("✅ Kafka consumer created successfully")
        return consumer
    except Exception as e:
        logger.error(f"❌ Failed to create Kafka consumer: {e}")
        raise

# Main consumer function
def consume_messages():
    """Consume messages from Kafka topic"""
    consumer = create_consumer()

    try:
        # Subscribe to topic
        consumer.subscribe(['iot-sensor-data'])
        logger.info("📡 Listening for messages on topic 'iot-sensor-data'...")

        while True:
            msg = consumer.poll(1.0)  # Poll for messages with 1 second timeout

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    logger.info(f"📄 End of partition reached {msg.topic()}/{msg.partition()}")
                else:
                    logger.error(f"❌ Consumer error: {msg.error()}")
                continue

            # Process the message
            try:
                # Decode and parse JSON message
                message_value = msg.value().decode('utf-8')
                data = json.loads(message_value)

                logger.info(f"📨 Received message: Region={data.get('region', 'N/A')}, AQI={data.get('AQI', 'N/A')}, PM2.5={data.get('PM2.5', 'N/A')}")

                # For demonstration, just log the message
                # In a real application, you would process the data here
                # e.g., save to database, trigger alerts, etc.

            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse JSON message: {e}")
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("⏹️ Received keyboard interrupt, shutting down consumer...")
    except Exception as e:
        logger.error(f"❌ Fatal error in consumer: {e}")
    finally:
        # Clean up
        logger.info("🧹 Cleaning up consumer...")
        consumer.close()
        logger.info("✅ Consumer closed successfully")

if __name__ == '__main__':
    consume_messages()
