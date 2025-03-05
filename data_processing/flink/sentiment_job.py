from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import json
import os
import sys

class SentimentAnalysisJob:
    """
    Flink job for real-time sentiment analysis of streaming data.
    """
    
    def __init__(self, kafka_bootstrap_servers, input_topics, output_topic):
        """
        Initialize the Flink sentiment analysis job.
        
        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            input_topics: List of input Kafka topics
            output_topic: Output Kafka topic for processed data
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.input_topics = input_topics
        self.output_topic = output_topic
        
        # Initialize Flink environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(1)  # Set parallelism
        
        # Initialize Table environment
        self.settings = EnvironmentSettings.new_instance() \
            .in_streaming_mode() \
            .build()
        self.t_env = StreamTableEnvironment.create(self.env, self.settings)
        
    def create_kafka_source(self, topic, group_id="sentiment-analysis-group"):
        """
        Create a Kafka source for consuming data.
        
        Args:
            topic: Kafka topic to consume from
            group_id: Kafka consumer group ID
            
        Returns:
            FlinkKafkaConsumer: Kafka consumer
        """
        properties = {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'latest'
        }
        
        return FlinkKafkaConsumer(
            topic,
            SimpleStringSchema(),
            properties
        )
    
    def create_kafka_sink(self, topic):
        """
        Create a Kafka sink for producing data.
        
        Args:
            topic: Kafka topic to produce to
            
        Returns:
            FlinkKafkaProducer: Kafka producer
        """
        properties = {
            'bootstrap.servers': self.kafka_bootstrap_servers,
            'transaction.timeout.ms': '5000'
        }
        
        return FlinkKafkaProducer(
            topic,
            SimpleStringSchema(),
            properties
        )
    
    def analyze_sentiment(self, text):
        """
        Analyze sentiment of text.
        This is a placeholder - in production, you would use a proper sentiment model.
        
        Args:
            text: Text to analyze
            
        Returns:
            dict: Sentiment analysis result
        """
        # This is a simple placeholder - in production, use a proper model
        positive_words = ['good', 'great', 'excellent', 'amazing', 'love', 'best']
        negative_words = ['bad', 'terrible', 'awful', 'worst', 'hate', 'poor']
        
        text_lower = text.lower()
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            sentiment = 'positive'
            score = min(0.5 + (positive_count - negative_count) * 0.1, 1.0)
        elif negative_count > positive_count:
            sentiment = 'negative'
            score = max(0.5 - (negative_count - positive_count) * 0.1, 0.0)
        else:
            sentiment = 'neutral'
            score = 0.5
            
        return {
            'sentiment': sentiment,
            'score': score
        }
    
    def process_message(self, message):
        """
        Process a message from Kafka.
        
        Args:
            message: Message from Kafka
            
        Returns:
            str: Processed message as JSON string
        """
        try:
            # Parse JSON message
            data = json.loads(message)
            
            # Extract content
            content = data.get('content', '')
            
            # Analyze sentiment
            sentiment_result = self.analyze_sentiment(content)
            
            # Add sentiment to data
            data['sentiment'] = sentiment_result
            
            # Return as JSON string
            return json.dumps(data)
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            return message  # Return original message on error
    
    def run(self):
        """
        Run the Flink job.
        """
        # Create data streams for each input topic
        streams = []
        for topic in self.input_topics:
            source = self.create_kafka_source(topic)
            stream = self.env.add_source(source)
            streams.append(stream)
        
        # Union all streams if there are multiple
        if len(streams) > 1:
            combined_stream = streams[0]
            for stream in streams[1:]:
                combined_stream = combined_stream.union(stream)
        else:
            combined_stream = streams[0]
        
        # Process the stream
        processed_stream = combined_stream.map(
            lambda msg: self.process_message(msg),
            output_type=Types.STRING()
        )
        
        # Add sink
        sink = self.create_kafka_sink(self.output_topic)
        processed_stream.add_sink(sink)
        
        # Execute the job
        self.env.execute("Sentiment Analysis Job")


if __name__ == "__main__":
    # Example usage
    kafka_servers = "localhost:9092"
    input_topics = ["pulse.twitter", "pulse.reddit", "pulse.youtube", "pulse.bing"]
    output_topic = "pulse.sentiment"
    
    job = SentimentAnalysisJob(kafka_servers, input_topics, output_topic)
    job.run() 