#!/usr/bin/env python3
"""
Test script to verify Kafka connectivity and create the 'pryzm' topic
"""

import sys
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def test_kafka_connection():
    """Test Kafka connection and create topic if needed"""
    
    # Kafka configuration
    bootstrap_servers = ['localhost:9092']
    topic_name = 'pryzm'
    
    print("Testing Kafka connectivity...")
    
    try:
        # Test admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='test_admin'
        )
        
        print("✓ Successfully connected to Kafka")
        
        # Create topic
        topic = NewTopic(
            name=topic_name,
            num_partitions=1,
            replication_factor=1
        )
        
        try:
            admin_client.create_topics([topic])
            print(f"✓ Topic '{topic_name}' created successfully")
        except TopicAlreadyExistsError:
            print(f"✓ Topic '{topic_name}' already exists")
        
        # Test producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: str(v).encode('utf-8')
        )
        
        test_message = "Test message from Airflow plugin setup"
        future = producer.send(topic_name, test_message)
        result = future.get(timeout=10)
        
        print(f"✓ Test message sent to topic '{topic_name}' "
              f"(partition: {result.partition}, offset: {result.offset})")
        
        producer.close()
        
        # Test consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: m.decode('utf-8')
        )
        
        print(f"✓ Consumer connected to topic '{topic_name}'")
        consumer.close()
        
        print("\n🎉 All Kafka tests passed! Ready to use with Airflow plugin.")
        return True
        
    except Exception as e:
        print(f"❌ Kafka test failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_kafka_connection()
    sys.exit(0 if success else 1)
