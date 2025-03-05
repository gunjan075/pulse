from kafka import KafkaProducer
import json
from typing import Dict, Any
import logging

class MessageProducer:
    """
    Kafka producer for sending data to Kafka topics.
    """
    
    def __init__(self, bootstrap_servers: str, topic_prefix: str = "pulse"):
        """
        Initialize the Kafka producer.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            topic_prefix: Prefix for Kafka topics
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic_prefix = topic_prefix
        self.logger = logging.getLogger(self.__class__.__name__)
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            self.logger.info(f"Kafka producer connected to {bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {str(e)}")
            self.producer = None
    
    def send_message(self, source: str, message: Dict[str, Any], key: str = None) -> bool:
        """
        Send a message to a Kafka topic.
        
        Args:
            source: Data source (used to determine the topic)
            message: Message to send
            key: Message key (optional)
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        if not self.producer:
            self.logger.error("Kafka producer not initialized")
            return False
            
        topic = f"{self.topic_prefix}.{source.lower()}"
        
        try:
            future = self.producer.send(topic, key=key, value=message)
            # Wait for the message to be sent
            future.get(timeout=10)
            self.logger.debug(f"Message sent to topic {topic}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message to topic {topic}: {str(e)}")
            return False
    
    def close(self):
        """
        Close the Kafka producer.
        """
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.logger.info("Kafka producer closed") 