#!/usr/bin/env python3
"""
Enhanced Kafka consumer test for Avro messages with avro-python3==1.8.1
"""

import json
import sys
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Import local avro utils
try:
    from avro_utils import deserialize_message
    print("✅ Avro utilities imported successfully")
    AVRO_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ Avro not available: {e}")
    AVRO_AVAILABLE = False

def is_binary_data(data):
    """Check if data is binary (likely Avro)"""
    try:
        data.decode('utf-8')
        return False
    except UnicodeDecodeError:
        return True

def detect_message_type(key, value):
    """Detect message type from key or content"""
    if key:
        key_str = key.decode('utf-8')
        if '_task' in key_str:
            return 'task_metadata'
        elif '_dag' in key_str:
            return 'dag_metadata'
        elif '_graph' in key_str:
            return 'graph_metadata'
    
    # Try to parse as JSON to get type
    try:
        if not is_binary_data(value):
            json_data = json.loads(value.decode('utf-8'))
            return json_data.get('type')
    except:
        pass
    
    return None

def process_message(message):
    """Process a single Kafka message with Avro support"""
    key = message.key
    value = message.value
    
    print(f"\n📨 Received message:")
    print(f"   Key: {key.decode('utf-8') if key else 'None'}")
    print(f"   Value size: {len(value)} bytes")
    print(f"   Partition: {message.partition}")
    print(f"   Offset: {message.offset}")
    
    # Detect if it's binary (Avro) or text (JSON)
    is_binary = is_binary_data(value)
    print(f"   Format: {'Binary (likely Avro)' if is_binary else 'Text (likely JSON)'}")
    
    if is_binary and AVRO_AVAILABLE:
        # Try Avro deserialization
        message_type = detect_message_type(key, value)
        if message_type:
            try:
                data = deserialize_message(value, message_type)
                print(f"✅ Avro deserialization successful")
                print(f"   Type: {message_type}")
                print(f"   Data: {json.dumps(data, indent=2)}")
                return True
            except Exception as e:
                print(f"❌ Avro deserialization failed: {e}")
        
        # Try each schema type
        for schema_type in ['task_metadata', 'dag_metadata', 'graph_metadata']:
            try:
                data = deserialize_message(value, schema_type)
                print(f"✅ Avro deserialization successful with {schema_type}")
                print(f"   Data: {json.dumps(data, indent=2)}")
                return True
            except:
                continue
                
        print("❌ Failed to deserialize with any Avro schema")
    
    # Fallback to JSON
    try:
        if not is_binary:
            data = json.loads(value.decode('utf-8'))
            print(f"✅ JSON deserialization successful")
            print(f"   Data: {json.dumps(data, indent=2)}")
            return True
    except Exception as e:
        print(f"❌ JSON deserialization failed: {e}")
    
    print(f"❌ Unable to deserialize message")
    print(f"   Raw data (first 100 bytes): {value[:100]}")
    return False

def test_kafka_consumer():
    """Enhanced Kafka consumer test with Avro support"""
    print("🚀 Starting enhanced Kafka consumer test for Avro messages")
    print("=" * 60)
    
    try:
        consumer = KafkaConsumer(
            'pryzm',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            group_id='test-consumer-avro',
            value_deserializer=None,
            key_deserializer=None
        )
        print("✅ Connected to Kafka successfully")
        print("🔍 Listening for messages on topic 'pryzm'...")
        print("   Press Ctrl+C to stop")
        
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        print("   Make sure Kafka is running on localhost:9092")
        return False
    
    message_count = 0
    successful_deserializations = 0
    
    try:
        for message in consumer:
            message_count += 1
            print(f"\n{'='*20} Message #{message_count} {'='*20}")
            
            if process_message(message):
                successful_deserializations += 1
            
            # Print stats every 5 messages
            if message_count % 5 == 0:
                success_rate = (successful_deserializations / message_count) * 100
                print(f"\n📊 Stats: {successful_deserializations}/{message_count} messages processed successfully ({success_rate:.1f}%)")
            
            # Stop after 20 messages for testing
            if message_count >= 20:
                print(f"\n🏁 Stopping after {message_count} messages for testing")
                break
            
    except KeyboardInterrupt:
        print(f"\n🛑 Consumer stopped by user")
    except Exception as e:
        print(f"\n❌ Consumer error: {e}")
        return False
    finally:
        consumer.close()
        print(f"📊 Final stats: {successful_deserializations}/{message_count} messages processed successfully")
        print("✅ Consumer closed")
        
    return successful_deserializations > 0

if __name__ == "__main__":
    success = test_kafka_consumer()
    if success:
        print("🎉 Consumer test completed successfully!")
    else:
        print("❌ Consumer test failed!")
        sys.exit(1)
