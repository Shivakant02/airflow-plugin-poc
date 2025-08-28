#!/bin/bash

# Set up Airflow directories with proper permissions
echo "Setting up Airflow directories..."

# Create directories if they don't exist
mkdir -p ./dags ./logs ./plugins

# Set proper ownership (Airflow runs as UID 50000 in the container)
echo "Setting proper ownership for Airflow directories..."
sudo chown -R 50000:0 ./logs
sudo chmod -R 755 ./logs

# Set environment variables for Airflow
export AIRFLOW_UID=50000

echo "Initializing Airflow database..."
docker compose up airflow-init

echo "Starting Airflow services..."
docker compose up -d

echo "Waiting for services to start..."
sleep 30

echo "Checking service status..."
docker compose ps

echo ""
echo "=============================================="
echo "Airflow Setup Complete!"
echo "=============================================="
echo "Web UI: http://localhost:8080"
echo "Username: airflow"
echo "Password: airflow"
echo "=============================================="
