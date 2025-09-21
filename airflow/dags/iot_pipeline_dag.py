from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=1),
    'max_active_runs': 1,
}

# Define the DAG
with DAG(
    'iot_streaming_pipeline',
    default_args=default_args,
    description='A real-time IoT data streaming pipeline',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['iot', 'spark', 'kafka', 'postgres', 'streaming'],
) as dag:

    def log_pipeline_start(**context):
        """Log pipeline start information"""
        logger.info("🚀 Starting AQI IoT Streaming Pipeline")
        logger.info(f"Run ID: {context['run_id']}")

    def check_services(**context):
        """Check if required services are running"""
        import socket
        import time

        services = [
            ('kafka', 'kafka', 9092),
            ('main-postgres', 'postgres', 5432),
            ('spark-master', 'spark-master', 7077),
            ('airflow-postgres', 'airflow-postgres', 5432)
        ]

        all_services_up = True
        for service_name, host, port in services:
            max_retries = 3
            retry_delay = 2

            for attempt in range(max_retries):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(10)
                    result = sock.connect_ex((host, port))
                    sock.close()

                    if result == 0:
                        logger.info(f"✅ {service_name} service is running on {host}:{port}")
                        break
                    else:
                        if attempt < max_retries - 1:
                            logger.warning(f"⚠️ {service_name} service not accessible on {host}:{port} (attempt {attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                        else:
                            logger.error(f"❌ {service_name} service not accessible on {host}:{port} after {max_retries} attempts")
                            all_services_up = False
                except Exception as e:
                    logger.error(f"❌ Error checking {service_name}: {e}")
                    all_services_up = False
                    break

        return all_services_up

    def test_kafka_producer(**context):
        """Test Kafka producer connection"""
        try:
            logger.info("🚀 Testing Kafka producer connection...")
            from confluent_kafka import Producer
            import json

            producer = Producer({
                'bootstrap.servers': 'kafka:9092',
                'acks': 'all',
                'retries': 3,
            })

            test_data = {"test": "connection", "timestamp": str(datetime.now())}
            producer.produce('iot-sensor-data', json.dumps(test_data).encode('utf-8'))
            producer.flush()

            logger.info("✅ Kafka producer connection test successful")
            return True

        except Exception as e:
            logger.error(f"❌ Error testing producer: {e}")
            return False

    def test_database_connection(**context):
        """Test database connection"""
        try:
            logger.info("🚀 Testing database connection...")
            import psycopg2

            conn = psycopg2.connect(
                host="postgres",
                port="5432",
                database="aqi_db",
                user="aqi_user",
                password="aqi_password"
            )
            conn.close()

            logger.info("✅ Database connection test successful")
            return True

        except Exception as e:
            logger.error(f"❌ Error testing database connection: {e}")
            return False

    def log_pipeline_end(**context):
        """Log pipeline completion information"""
        logger.info("✅ AQI IoT Streaming Pipeline completed successfully")
        logger.info(f"Run ID: {context['run_id']}")

    # Task definitions
    start_logging = PythonOperator(
        task_id='log_pipeline_start',
        python_callable=log_pipeline_start,
    )

    check_services_task = PythonOperator(
        task_id='check_services',
        python_callable=check_services,
    )

    test_kafka = PythonOperator(
        task_id='test_kafka_producer',
        python_callable=test_kafka_producer,
    )

    test_database = PythonOperator(
        task_id='test_database_connection',
        python_callable=test_database_connection,
    )

    end_logging = PythonOperator(
        task_id='log_pipeline_end',
        python_callable=log_pipeline_end,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    cleanup = DummyOperator(
        task_id='cleanup',
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Define the task dependencies
    start_logging >> check_services_task >> [test_kafka, test_database] >> end_logging >> cleanup
