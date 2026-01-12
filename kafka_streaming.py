from consumer.kafka_consumer import create_consumer
from config.kafka_config import TOPIC_NAME
from producer.kafka_producer import create_producer, send_message, close_producer
from utils.logger import setup_logger

main_logger = setup_logger("kafka_main")

def main():
    main_logger.info("Kafka consumer started. Listening for messages...")
    consumer = create_consumer()
    producer = create_producer()
    try:
        while True:
            msg = consumer.poll(1.0) 
            if msg is None:
                continue
            if msg.error():
                main_logger.error(f"Consumer error: {msg.error()}")
                continue
            data = msg.value().decode('utf-8')
            main_logger.info(f"Received message: {data}")
            # Forward the consumed message
            target_topic = TOPIC_NAME.get('data_output', 'default-output-topic')
            send_message(producer, target_topic, {None: data})
            main_logger.info(f"Forwarded message to topic {target_topic}")
    except KeyboardInterrupt:
        main_logger.info("Shutting down consumer...")
    finally:
        if consumer:
            consumer.close()
        if producer:
            close_producer(producer)

if __name__ == "__main__":
    main()
