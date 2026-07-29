import json
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

class LogisticsKafkaProducer:
    def __init__(self, broker_url: str):
        self.broker_url = broker_url
        logger.info(f"Connecting to Confluent Kafka broker at {self.broker_url}...")
        
        # Initialize enterprise C-based Producer
        self.producer = Producer({
            'bootstrap.servers': self.broker_url,
            'client.id': 'logistics-producer'
        })
        logger.info("Confluent Kafka Producer initialized successfully.")

    def publish_shipment_event(self, topic: str, shipment_data: dict):
        try:
            # Confluent requires manual encoding 
            encoded_data = json.dumps(shipment_data, default=str).encode('utf-8')
            self.producer.produce(topic, value=encoded_data)
            self.producer.poll(0) # Serve delivery callbacks
            logger.debug(f"Queued message for topic '{topic}'")
        except Exception as e:
            logger.error(f"Failed to publish to Kafka: {e}")
            raise e
            
    def flush(self):
        """Forces all buffered records to be sent to the broker."""
        self.producer.flush()
        logger.info("Flushed all messages to Kafka.")