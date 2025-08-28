#!/bin/bash

echo "=============================================="
echo "Airflow Plugin POC with Avro Serialization"
echo "=============================================="

# Set up Airflow directories with proper permissions
echo "Setting up Airflow directories..."

# Create directories if they don't exist
mkdir -p ./dags ./logs ./plugins ./schemas ./kafka-consumer

# Set proper ownership (Airflow runs as UID 50000 in the container)
echo "Setting proper ownership for Airflow directories..."
sudo chown -R 50000:0 ./logs
sudo chmod -R 755 ./logs

# Set environment variables for Airflow
export AIRFLOW_UID=50000

echo "Checking required dependencies..."

# Check if Docker Compose is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

if ! docker compose version &> /dev/null; then
    echo "Error: Docker Compose v2 is not available. Please install Docker Compose v2."
    exit 1
fi

echo "Initializing Airflow database..."
docker compose up airflow-init

echo "Starting all services (Airflow, Kafka, PostgreSQL, Consumer)..."
docker compose up -d

echo "Waiting for services to start..."
sleep 45

echo "Checking service status..."
docker compose ps

echo ""
echo "Verifying Avro schema files..."
if [ -f "./schemas/task_metadata.avsc" ] && [ -f "./schemas/dag_metadata.avsc" ] && [ -f "./schemas/graph_metadata.avsc" ]; then
    echo "✓ All Avro schemas found"
else
    echo "⚠ Warning: Some Avro schema files may be missing"
fi

echo ""
echo "Checking Avro dependency installation..."
docker compose exec airflow-scheduler python -c "import avro; print('✓ avro-python3 installed successfully')" 2>/dev/null || echo "⚠ avro-python3 may need installation"

echo ""
echo "=============================================="
echo "Setup Complete! Services Available:"
echo "=============================================="
echo "🌐 Airflow Web UI: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "🔧 Other Services:"
echo "   Kafka: localhost:9092"
echo "   PostgreSQL: localhost:5432"
echo ""
echo "📊 Monitor Avro Compression:"
echo "   docker compose logs airflow-scheduler | grep compression"
echo "   docker compose logs kafka-consumer"
echo ""
echo "🚀 Expected Compression: 30-47% size reduction"
echo "=============================================="
