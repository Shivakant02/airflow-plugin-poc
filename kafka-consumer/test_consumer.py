#!/usr/bin/env python3
"""
Simple Kafka consumer test to diagnose issues
"""

import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

def test_kafka_consumer():
    """Test Kafka consumer with detailed logging"""
    
    print("🔍 Testing Kafka consumer...")
    
    try:
        # Create consumer with different group ID to avoid offset issues
        consumer = KafkaConsumer(
            'pryzm',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',  # Start from beginning
            enable_auto_commit=True,
            group_id=f'test-consumer-{int(time.time())}',  # Unique group ID
            value_deserializer=lambda m: m.decode('utf-8'),  # Just decode, don't parse JSON yet
            consumer_timeout_ms=15000,  # 15 second timeout
            fetch_min_bytes=1,
            fetch_max_wait_ms=1000
        )
        
        print("✅ Consumer created successfully")
        print(f"📊 Consumer config: {consumer.config}")
        
        # Get topic info
        partitions = consumer.partitions_for_topic('pryzm')
        print(f"📊 Topic 'pryzm' has partitions: {partitions}")
        
        print("📥 Waiting for messages...")
        message_count = 0
        
        for message in consumer:
            message_count += 1
            print(f"\n📨 Message {message_count}:")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Key: {message.key}")
            print(f"   Value: {message.value}")
            
            # Try to parse as JSON
            try:
                json_data = json.loads(message.value)
                print(f"   Type: {json_data.get('type', 'unknown')}")
            except Exception as e:
                print(f"   JSON parse error: {e}")
            
            if message_count >= 5:  # Limit to 5 messages for testing
                break
        
        if message_count == 0:
            print("❌ No messages received within timeout")
            
            # Check if topic exists and has messages
            print("\n🔍 Checking topic details...")
            consumer.list_consumer_group_offsets()
            
        else:
            print(f"\n✅ Successfully received {message_count} messages")
        
        consumer.close()
        return message_count > 0
        
    except KafkaError as e:
        print(f"❌ Kafka Error: {e}")
        return False
    except Exception as e:
        print(f"❌ General Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_kafka_consumer()
    if success:
        print("\n🎉 Consumer test passed!")
    else:
        print("\n💥 Consumer test failed!")
