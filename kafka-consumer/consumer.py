#!/usr/bin/env python3
"""
Kafka consumer to monitor messages from the 'pryzm' topic
and store them in PostgreSQL database with Avro deserialization support
"""

import json
import psycopg2
import psycopg2.extras
from datetime import datetime
from kafka import KafkaConsumer

try:
    from avro_utils import deserialize_message, get_serializer
    AVRO_AVAILABLE = True
    print("✅ Avro support available")
except ImportError as e:
    AVRO_AVAILABLE = False
    print(f"⚠️  Avro support not available: {e}")
    print("📝 Will use JSON fallback")

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

def create_table_if_not_exists():
    """Create the airflow_poc table if it doesn't exist"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS "airflow-poc" (
            id SERIAL PRIMARY KEY,
            info JSONB NOT NULL,
            type VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        
        print("✅ Table 'airflow-poc' created/verified successfully")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        raise

def insert_message_to_db(message_data, message_type):
    """Insert message data into PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO "airflow-poc" (info, type)
        VALUES (%s, %s)
        RETURNING id;
        """
        
        cursor.execute(insert_query, (json.dumps(message_data), message_type))
        record_id = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Message stored in database with ID: {record_id}")
        return record_id
        
    except Exception as e:
        print(f"❌ Error inserting to database: {e}")
        return None

def consume_pryzm_messages():
    """Consume and display messages from the pryzm topic and store in PostgreSQL"""
    
    # Create table if it doesn't exist
    create_table_if_not_exists()
    
    consumer = KafkaConsumer(
        'pryzm',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow-metadata-consumer',
        value_deserializer=None,  # We'll handle deserialization manually
        fetch_min_bytes=1,
        fetch_max_wait_ms=1000
    )
    
    print("🎯 Listening for messages on 'pryzm' topic...")
    print("💾 Messages will be stored in PostgreSQL 'airflow-poc' table")
    print("Press Ctrl+C to stop\n")
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            # Extract message details and deserialize
            raw_message = message.value
            message_key = message.key.decode('utf-8') if message.key else None
            
            # Try to deserialize with Avro first, fallback to JSON
            message_data = None
            deserialization_method = "unknown"
            
            # First, let's inspect the raw message to determine its format
            print(f"🔍 Raw message inspection:")
            print(f"   Type: {type(raw_message)}")
            print(f"   Length: {len(raw_message)} bytes")
            print(f"   First 50 chars: {str(raw_message[:50])}")
            
            # Check if message looks like JSON (starts with { or [)
            is_json_like = False
            try:
                if isinstance(raw_message, bytes):
                    decoded = raw_message.decode('utf-8')
                    is_json_like = decoded.strip().startswith(('{', '['))
                    print(f"   Looks like JSON: {is_json_like}")
                elif isinstance(raw_message, str):
                    is_json_like = raw_message.strip().startswith(('{', '['))
                    print(f"   Looks like JSON: {is_json_like}")
            except:
                print(f"   Cannot decode as UTF-8 - likely binary Avro data")
            
            if AVRO_AVAILABLE and not is_json_like:
                # Try to determine message type from key or try all schemas
                message_type = None
                if message_key:
                    if '_task' in message_key or message_key.endswith('_task'):
                        message_type = 'task_metadata'
                    elif '_dag' in message_key:
                        message_type = 'dag_metadata'
                    elif '_graph' in message_key:
                        message_type = 'graph_metadata'
                
                # Try Avro deserialization
                if message_type:
                    try:
                        message_data = deserialize_message(raw_message, message_type)
                        deserialization_method = f"Avro ({message_type})"
                        print(f"✅ Successfully deserialized with Avro ({message_type})")
                    except Exception as e:
                        print(f"⚠️  Avro deserialization failed: {e}")
                
                # If Avro failed, try all schemas
                if not message_data:
                    for schema_type in ['task_metadata', 'dag_metadata', 'graph_metadata']:
                        try:
                            message_data = deserialize_message(raw_message, schema_type)
                            deserialization_method = f"Avro ({schema_type})"
                            print(f"✅ Successfully deserialized with Avro ({schema_type})")
                            break
                        except:
                            continue
            
            # Fallback to JSON if Avro failed
            if not message_data:
                try:
                    message_data = json.loads(raw_message.decode('utf-8'))
                    deserialization_method = "JSON"
                except Exception as e:
                    print(f"❌ Both Avro and JSON deserialization failed: {e}")
                    continue
            
            message_type = message_data.get('type', 'unknown')
            
            print(f"📊 Received message #{message_count}:")
            print(f"   Key: {message_key}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print(f"   Type: {message_type}")
            print(f"   Deserialization: {deserialization_method}")
            print(f"   Value: {json.dumps(message_data, indent=2)}")
            
            # Store in PostgreSQL
            record_id = insert_message_to_db(message_data, message_type)
            
            if record_id:
                print(f"💾 Stored in database with ID: {record_id}")
            else:
                print("❌ Failed to store in database")
            
            print("-" * 80)
            
    except KeyboardInterrupt:
        print(f"\n👋 Stopping consumer... Processed {message_count} messages")
    except Exception as e:
        print(f"\n❌ Error in consumer: {e}")
    finally:
        consumer.close()
        print("🔌 Consumer connection closed")

def test_db_connection():
    """Test database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ Database connection successful!")
        print(f"   PostgreSQL version: {version[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def view_stored_data(limit=10):
    """View stored data from the database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get total count
        cursor.execute('SELECT COUNT(*) FROM "airflow-poc"')
        total_count = cursor.fetchone()['count']
        
        # Get recent records
        cursor.execute(f'''
            SELECT id, type, created_at, info
            FROM "airflow-poc" 
            ORDER BY created_at DESC 
            LIMIT {limit}
        ''')
        
        records = cursor.fetchall()
        
        print(f"\n📊 Database Summary:")
        print(f"   Total records: {total_count}")
        print(f"   Showing latest {min(limit, len(records))} records:\n")
        
        for record in records:
            print(f"ID: {record['id']} | Type: {record['type']} | Created: {record['created_at']}")
            print(f"Data: {json.dumps(record['info'], indent=2)}")
            print("-" * 60)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error viewing data: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Test mode: check database connection and view data
        print("🧪 Testing database connection...")
        if test_db_connection():
            view_stored_data()
    elif len(sys.argv) > 1 and sys.argv[1] == "--view":
        # View mode: show stored data
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        view_stored_data(limit)
    else:
        # Normal consumer mode
        consume_pryzm_messages()
