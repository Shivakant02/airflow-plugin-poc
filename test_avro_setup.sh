#!/bin/bash

echo "🧪 Testing Avro Implementation with avro-python3==1.8.1"
echo "=" * 60

# Check if schemas exist
echo "📁 Checking schema files..."
if [ -d "schemas" ] && [ -f "schemas/task_metadata.avsc" ] && [ -f "schemas/dag_metadata.avsc" ] && [ -f "schemas/graph_metadata.avsc" ]; then
    echo "✅ All schema files found"
else
    echo "❌ Schema files missing. Please ensure schemas directory exists with .avsc files"
    exit 1
fi

# Test Avro utilities (if available)
if [ -f "test_avro.py" ]; then
    echo ""
    echo "🔬 Running Avro utilities test..."
    python3 test_avro.py
    if [ $? -eq 0 ]; then
        echo "✅ Avro utilities test passed"
    else
        echo "❌ Avro utilities test failed"
    fi
fi

# Start services if not running
echo ""
echo "🚀 Checking and starting services..."

# Check if Docker Compose is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not found. Please install Docker first."
    exit 1
fi

# Check if services are running
if ! docker compose ps | grep -q "Up"; then
    echo "📦 Starting Docker services..."
    docker compose up -d
    echo "⏳ Waiting for services to start..."
    sleep 30
else
    echo "✅ Docker services already running"
fi

# Test consumer
echo ""
echo "🔍 Testing Kafka consumer with Avro support..."
cd kafka-consumer

# Check if Avro utils exists
if [ -f "avro_utils.py" ]; then
    echo "✅ Avro utilities found in kafka-consumer"
else
    echo "❌ Avro utilities not found in kafka-consumer directory"
    exit 1
fi

# Run the test consumer
echo "🏃 Running enhanced consumer test..."
echo "   This will consume up to 20 messages and then stop"
python3 test_consumer_avro.py

echo ""
echo "🏁 Test completed!"
echo "   Check the output above for Avro deserialization results"
echo "   Look for 'Binary (likely Avro)' messages and successful deserialization"
