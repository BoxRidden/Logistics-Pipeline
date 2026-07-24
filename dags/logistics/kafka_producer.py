import json
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
logger.addHandler(console_handler)

class LogisticsKafkaProducer:
    def __init__(self, broker_url='localhost:9092'):
        self.conf = {
            'bootstrap.servers': broker_url,
            'client.id': 'logistics_simulator_producer'
        }
        self.producer = Producer(self.conf)

    def delivery_report(self, err, msg):
        """Callback triggered by Kafka once a message is delivered (or fails)."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def publish_shipment_event(self, topic: str, shipment_data: dict):
        """Converts the shipment dictionary to JSON and streams it to Kafka."""
        try:
            # We use the tracking_code as the Kafka Message Key to ensure 
            # updates for the same shipment stay in the correct order.
            key = shipment_data.get('tracking_code', 'UNKNOWN')
            value = json.dumps(shipment_data)

            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=value.encode('utf-8'),
                callback=self.delivery_report
            )
            # Poll handles delivery callbacks
            self.producer.poll(0) 
            
        except Exception as e:
            logger.error(f"Failed to publish to Kafka: {e}")

    def flush(self):
        """Wait for any outstanding messages to be delivered and delivery reports received."""
        logger.info("Flushing Kafka producer queue...")
        self.producer.flush()