#!/usr/bin/env python3
"""
Avro serialization utilities for Airflow metadata
"""

import json
import os
import io
import avro.schema
import avro.io
from typing import Dict, Any, Optional

# Base directory for schemas
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), 'schemas')

class AvroSerializer:
    def __init__(self):
        self.schemas = {}
        self._load_schemas()
    
    def _load_schemas(self):
        """Load all Avro schemas from the schemas directory"""
        schema_files = {
            'task_metadata': 'task_metadata.avsc',
            'dag_metadata': 'dag_metadata.avsc', 
            'graph_metadata': 'graph_metadata.avsc'
        }
        
        for schema_name, filename in schema_files.items():
            schema_path = os.path.join(SCHEMA_DIR, filename)
            if os.path.exists(schema_path):
                with open(schema_path, 'r') as f:
                    schema_json = json.load(f)
                    self.schemas[schema_name] = avro.schema.parse(json.dumps(schema_json))
                print(f"✅ Loaded schema: {schema_name}")
            else:
                print(f"❌ Schema file not found: {schema_path}")
    
    def serialize(self, data: Dict[Any, Any], schema_type: str) -> bytes:
        """Serialize data using the specified Avro schema"""
        if schema_type not in self.schemas:
            raise ValueError(f"Unknown schema type: {schema_type}")
        
        schema = self.schemas[schema_type]
        
        # Convert data to match Avro schema requirements
        avro_data = self._convert_to_avro_format(data, schema_type)
        
        # Serialize
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(avro_data, encoder)
        
        return bytes_writer.getvalue()
    
    def deserialize(self, data: bytes, schema_type: str) -> Dict[Any, Any]:
        """Deserialize Avro data using the specified schema"""
        if schema_type not in self.schemas:
            raise ValueError(f"Unknown schema type: {schema_type}")
        
        schema = self.schemas[schema_type]
        
        # Deserialize
        bytes_reader = io.BytesIO(data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        
        return reader.read(decoder)
    
    def _convert_to_avro_format(self, data: Dict[Any, Any], schema_type: str) -> Dict[Any, Any]:
        """Convert data to match Avro schema format"""
        avro_data = data.copy()
        
        # Handle None values for nullable fields
        if schema_type == 'task_metadata':
            if 'pipeline_run_id' not in avro_data or avro_data['pipeline_run_id'] is None:
                avro_data['pipeline_run_id'] = None
        
        elif schema_type == 'dag_metadata':
            if 'pipeline_run_id' not in avro_data or avro_data['pipeline_run_id'] is None:
                avro_data['pipeline_run_id'] = None
        
        elif schema_type == 'graph_metadata':
            if 'pipeline_run_id' not in avro_data or avro_data['pipeline_run_id'] is None:
                avro_data['pipeline_run_id'] = None
            
            # Convert graph format if needed
            if 'graph' in avro_data and isinstance(avro_data['graph'], dict):
                converted_graph = {}
                for task_id, task_info in avro_data['graph'].items():
                    converted_graph[task_id] = {
                        'upstream': task_info.get('upstream', []),
                        'downstream': task_info.get('downstream', []),
                        'operator': task_info.get('operator', '')
                    }
                avro_data['graph'] = converted_graph
        
        return avro_data
    
    def get_message_size(self, data: Dict[Any, Any], schema_type: str) -> tuple:
        """Compare JSON vs Avro message sizes"""
        json_bytes = json.dumps(data).encode('utf-8')
        avro_bytes = self.serialize(data, schema_type)
        
        return len(json_bytes), len(avro_bytes)

# Global serializer instance
_serializer = None

def get_serializer() -> AvroSerializer:
    """Get global Avro serializer instance"""
    global _serializer
    if _serializer is None:
        _serializer = AvroSerializer()
    return _serializer

def serialize_message(data: Dict[Any, Any], message_type: str) -> bytes:
    """Convenience function to serialize a message"""
    serializer = get_serializer()
    return serializer.serialize(data, message_type)

def deserialize_message(data: bytes, message_type: str) -> Dict[Any, Any]:
    """Convenience function to deserialize a message"""
    serializer = get_serializer()
    return serializer.deserialize(data, message_type)

def compare_sizes(data: Dict[Any, Any], message_type: str) -> tuple:
    """Compare JSON vs Avro sizes"""
    serializer = get_serializer()
    return serializer.get_message_size(data, message_type)
