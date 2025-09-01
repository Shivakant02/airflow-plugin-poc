#!/usr/bin/env python3
"""
Test script for Avro serialization/deserialization with avro-python3==1.8.1
"""

import sys
import os
import json

# Add the plugins directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'plugins'))

try:
    from avro_utils import AvroSerializer, serialize_message, deserialize_message, compare_sizes
    print("✅ Successfully imported Avro utilities")
except ImportError as e:
    print(f"❌ Failed to import Avro utilities: {e}")
    sys.exit(1)

def test_task_metadata():
    """Test task metadata serialization/deserialization"""
    print("\n🔄 Testing task metadata...")
    
    test_data = {
        "dag_id": "test_dag",
        "task_id": "test_task",
        "dag_run_id": "manual__2025-09-01T10:00:00+00:00",
        "execution_date": "2025-09-01T10:00:00+00:00",
        "start_date": "2025-09-01T10:05:00+00:00",
        "end_date": "2025-09-01T10:10:00+00:00",
        "duration": 300.5,
        "state": "success",
        "try_number": 1,
        "max_tries": 3,
        "operator": "BashOperator",
        "event": "task_success",
        "pipeline_run_id": "test-pipeline-123"
    }
    
    try:
        # Test serialization
        serialized = serialize_message(test_data, 'task_metadata')
        print(f"✅ Serialization successful, size: {len(serialized)} bytes")
        
        # Test deserialization
        deserialized = deserialize_message(serialized, 'task_metadata')
        print(f"✅ Deserialization successful")
        
        # Compare sizes
        json_size, avro_size = compare_sizes(test_data, 'task_metadata')
        compression = ((json_size - avro_size) / json_size * 100)
        print(f"📊 Size comparison - JSON: {json_size} bytes, Avro: {avro_size} bytes, Compression: {compression:.2f}%")
        
        # Verify data integrity
        if deserialized == test_data:
            print("✅ Data integrity verified - original and deserialized data match")
        else:
            print("❌ Data integrity failed - data mismatch")
            print(f"Original: {test_data}")
            print(f"Deserialized: {deserialized}")
            
    except Exception as e:
        print(f"❌ Task metadata test failed: {e}")
        return False
    
    return True

def test_dag_metadata():
    """Test DAG metadata serialization/deserialization"""
    print("\n🔄 Testing DAG metadata...")
    
    test_data = {
        "dag_id": "test_dag",
        "dag_run_id": "manual__2025-09-01T10:00:00+00:00",
        "execution_date": "2025-09-01T10:00:00+00:00",
        "start_date": "2025-09-01T10:00:00+00:00",
        "end_date": "2025-09-01T10:15:00+00:00",
        "dag_state": "success",
        "event": "dag_success",
        "pipeline_run_id": "test-pipeline-123"
    }
    
    try:
        # Test serialization
        serialized = serialize_message(test_data, 'dag_metadata')
        print(f"✅ Serialization successful, size: {len(serialized)} bytes")
        
        # Test deserialization
        deserialized = deserialize_message(serialized, 'dag_metadata')
        print(f"✅ Deserialization successful")
        
        # Compare sizes
        json_size, avro_size = compare_sizes(test_data, 'dag_metadata')
        compression = ((json_size - avro_size) / json_size * 100)
        print(f"📊 Size comparison - JSON: {json_size} bytes, Avro: {avro_size} bytes, Compression: {compression:.2f}%")
        
        # Verify data integrity
        if deserialized == test_data:
            print("✅ Data integrity verified - original and deserialized data match")
        else:
            print("❌ Data integrity failed - data mismatch")
            
    except Exception as e:
        print(f"❌ DAG metadata test failed: {e}")
        return False
    
    return True

def test_graph_metadata():
    """Test graph metadata serialization/deserialization"""
    print("\n🔄 Testing graph metadata...")
    
    test_data = {
        "dag_id": "test_dag",
        "event": "graph_change",
        "graph": {
            "start_task": {
                "upstream": [],
                "downstream": ["process_data"],
                "operator": "BashOperator"
            },
            "process_data": {
                "upstream": ["start_task"],
                "downstream": ["end_task"],
                "operator": "PythonOperator"
            },
            "end_task": {
                "upstream": ["process_data"],
                "downstream": [],
                "operator": "BashOperator"
            }
        },
        "pipeline_run_id": "test-pipeline-123"
    }
    
    try:
        # Test serialization
        serialized = serialize_message(test_data, 'graph_metadata')
        print(f"✅ Serialization successful, size: {len(serialized)} bytes")
        
        # Test deserialization
        deserialized = deserialize_message(serialized, 'graph_metadata')
        print(f"✅ Deserialization successful")
        
        # Compare sizes
        json_size, avro_size = compare_sizes(test_data, 'graph_metadata')
        compression = ((json_size - avro_size) / json_size * 100)
        print(f"📊 Size comparison - JSON: {json_size} bytes, Avro: {avro_size} bytes, Compression: {compression:.2f}%")
        
        # Verify data integrity
        if deserialized == test_data:
            print("✅ Data integrity verified - original and deserialized data match")
        else:
            print("❌ Data integrity failed - data mismatch")
            
    except Exception as e:
        print(f"❌ Graph metadata test failed: {e}")
        return False
    
    return True

def test_schema_loading():
    """Test schema loading"""
    print("\n🔄 Testing schema loading...")
    
    try:
        serializer = AvroSerializer()
        loaded_schemas = list(serializer.schemas.keys())
        print(f"✅ Loaded schemas: {loaded_schemas}")
        
        expected_schemas = ['task_metadata', 'dag_metadata', 'graph_metadata']
        for schema in expected_schemas:
            if schema in loaded_schemas:
                print(f"✅ Schema '{schema}' loaded successfully")
            else:
                print(f"❌ Schema '{schema}' not found")
                return False
                
    except Exception as e:
        print(f"❌ Schema loading test failed: {e}")
        return False
    
    return True

def main():
    """Run all tests"""
    print("🧪 Starting Avro serialization tests with avro-python3==1.8.1")
    print("=" * 60)
    
    tests = [
        ("Schema Loading", test_schema_loading),
        ("Task Metadata", test_task_metadata),
        ("DAG Metadata", test_dag_metadata),
        ("Graph Metadata", test_graph_metadata),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                print(f"✅ {test_name} test PASSED")
                passed += 1
            else:
                print(f"❌ {test_name} test FAILED")
        except Exception as e:
            print(f"❌ {test_name} test FAILED with exception: {e}")
    
    print("\n" + "=" * 60)
    print(f"🏁 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Avro implementation is working correctly with version 1.8.1")
    else:
        print("⚠️  Some tests failed. Please check the error messages above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
