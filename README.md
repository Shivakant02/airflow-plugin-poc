# Airflow Plugin POC with Avro Serialization

This repository contains a Proof of Concept (POC) for Apache Airflow with custom plugins for task and DAG logging, metadata extraction, task graph visualization, and **lightweight Avro message serialization**.

## Features

- **Task Logging Plugin**: Captures detailed metadata for task lifecycle events with Avro binary serialization
- **DAG Logging Plugin**: Logs DAG run information with compressed Avro messages
- **Avro Serialization**: 30-47% message compression compared to JSON format
- **Kafka Integration**: Real-time streaming with binary Avro message support
- **PostgreSQL Consumer**: Dual-format message handling (Avro + JSON fallback)
- **Metadata Extraction**: Extracts and logs pipeline run IDs and task relationships
- **Task Graph Plugin**: Visualizes DAG task dependencies with compressed graph data
- **Sample DAGs**: Includes both success and failure scenario DAGs for testing

## Prerequisites

- Docker and Docker Compose v2
- Linux/Unix environment (tested on Ubuntu/Debian)
- sudo privileges (for setting up directory permissions)

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/Shivakant02/airflow-plugin-poc.git
cd airflow-plugin-poc
```

### 2. Run the Setup Script

The repository includes an automated setup script that handles all the initialization:

```bash
./setup.sh
```

This comprehensive script will:

- Install all required Python dependencies (including avro-python3==1.10.2)
- Create necessary directories with proper permissions
- Set up Airflow UID (50000) for container compatibility
- Initialize the Airflow database and create admin user
- Start all services (Airflow, Kafka, PostgreSQL, Consumer)
- Enable Avro binary message serialization

### 3. Access Services

Once the setup is complete, you can access:

- **Airflow Web UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`
- **Kafka**: localhost:9092 (message streaming)
- **PostgreSQL**: localhost:5432 (metadata storage)

### 4. Monitor Avro Compression

Check compression performance in real-time:

```bash
# View Avro compression statistics in Airflow logs
docker compose logs airflow-scheduler | grep -i "compression\|avro"

# Monitor Kafka consumer processing binary Avro messages
docker compose logs kafka-consumer

# Sample compression results you'll see:
# Task metadata: 37.62% compression (JSON: 462 bytes → Avro: 288 bytes)
# DAG metadata: 47.42% compression (JSON: 233 bytes → Avro: 123 bytes)
# Graph metadata: 45.83% compression (JSON: 240 bytes → Avro: 130 bytes)
```

## Project Structure

```
airflow-plugin-poc/
├── dags/                          # Airflow DAGs
│   ├── simple_success_dag.py      # Always succeeds DAG for testing
│   └── complex_failure_dag.py     # Mixed success/failure DAG
├── plugins/                       # Custom Airflow plugins
│   ├── task_logging_plugin.py     # Main logging plugin with Avro serialization
│   ├── avro_utils.py             # Avro serialization utilities
│   ├── metadata_extraction_plugin.py
│   ├── task_graph_plugin.py
│   └── dag_logging_plugin.py
├── schemas/                       # Avro schema definitions
│   ├── task_metadata.avsc        # Task execution metadata schema
│   ├── dag_metadata.avsc         # DAG run metadata schema
│   └── graph_metadata.avsc       # Task dependency graph schema
├── kafka-consumer/               # PostgreSQL consumer service
│   └── consumer.py               # Dual-format message processor (Avro/JSON)
├── logs/                         # Airflow logs directory
├── docker-compose.yaml          # Complete service orchestration
├── setup.sh                     # Automated setup script
└── README.md                    # This file
```

## Avro Serialization Benefits

This project implements Apache Avro binary serialization for all metadata messages, providing:

- **30-47% message compression** compared to JSON format
- **Binary encoding** for efficient network transmission
- **Schema evolution** support for backward compatibility
- **Type safety** with compile-time schema validation

### Compression Performance

Based on real production data:

| Message Type   | JSON Size | Avro Size | Compression |
| -------------- | --------- | --------- | ----------- |
| Task Metadata  | 462 bytes | 288 bytes | 37.62%      |
| DAG Metadata   | 233 bytes | 123 bytes | 47.42%      |
| Graph Metadata | 240 bytes | 130 bytes | 45.83%      |

### Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Airflow       │    │     Kafka       │    │   PostgreSQL    │
│   Plugins       │───▶│   (Binary       │───▶│   Consumer      │
│                 │    │    Avro)        │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Avro Schemas    │    │ Message Topics  │    │ Metadata Store  │
│ - task_metadata │    │ - task_events   │    │ - Dual Format   │
│ - dag_metadata  │    │ - dag_events    │    │   Support       │
│ - graph_metadata│    │ - graph_events  │    │ - Auto Detection│
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Message Flow

1. **Airflow Tasks Execute** → Generate metadata
2. **Plugin Captures Events** → Serialize to Avro binary
3. **Kafka Streaming** → Binary message transmission
4. **Consumer Processing** → Automatic format detection
5. **PostgreSQL Storage** → Structured metadata storage

## Available DAGs

### 1. Simple Success DAG (`simple_success_dag`)

- Always succeeds for testing plugin functionality
- Generates unique pipeline run IDs
- Demonstrates XCom usage for metadata passing
- Tasks: push_pipeline_run_id → start_task → process_data → end_task

### 2. Complex Failure DAG (`complex_failure_dag`)

- Mixed success/failure/retry scenarios
- Tests error handling and retry mechanisms
- Useful for testing failure logging capabilities

## Plugin Features

### Task Logging Plugin

- Captures task state changes (running, success, failed) with **Avro binary serialization**
- Logs task metadata including execution dates, try numbers, operators
- Extracts pipeline run IDs from XCom
- Provides detailed task graph information
- **Real-time compression statistics** showing size reduction benefits

### Avro Message Features

- **Binary serialization** for all Kafka messages
- **Automatic fallback** to JSON for compatibility
- **Schema validation** ensuring data consistency
- **Size comparison logging** for performance monitoring

### Key Logged Information

- Pipeline Run ID (from XCom)
- Task execution metadata (Avro compressed)
- DAG run information (binary format)
- Task dependencies and relationships (compressed graphs)
- Operator types and configurations
- **Message size statistics** (JSON vs Avro comparison)

## Manual Docker Commands

If you prefer to run commands manually instead of using the setup script:

```bash
# Initialize database first
docker compose up airflow-init

# Start all services
docker compose up -d

# Check service status
docker compose ps

# View logs
docker compose logs airflow-webserver
docker compose logs airflow-scheduler

# Stop services
docker compose down
```

## Troubleshooting

### Avro Schema Issues

If you encounter Avro schema loading errors:

```bash
# Check schema files exist
ls -la schemas/
# Should show: task_metadata.avsc, dag_metadata.avsc, graph_metadata.avsc

# Restart services to reload schemas
docker compose restart
```

### Message Format Debugging

Monitor message processing and format detection:

```bash
# Check Avro vs JSON message processing
docker compose logs kafka-consumer | grep -E "Avro|JSON|binary"

# View compression statistics
docker compose logs airflow-scheduler | grep compression
```

### Permission Issues

If you encounter permission errors, ensure the logs directory has proper ownership:

```bash
sudo chown -R 50000:0 ./logs
sudo chmod -R 755 ./logs
```

### Port Conflicts

If port 8080 is already in use, modify the `docker-compose.yaml` file to use a different port:

```yaml
ports:
  - "8081:8080" # Change 8080 to 8081 or any available port
```

### Database Issues

If you need to reset the database:

```bash
docker compose down
docker volume prune  # Remove all unused volumes
./setup.sh  # Run setup again
```

### Consumer Service Issues

If the Kafka consumer fails to start:

```bash
# Check consumer logs
docker compose logs kafka-consumer

# Rebuild consumer image
docker compose build kafka-consumer
docker compose up -d kafka-consumer

# Verify Kafka connectivity
docker compose exec kafka-consumer python -c "import kafka; print('Kafka client available')"
```

### Complete System Reset

For a fresh start with all Avro components:

```bash
# Stop all services
docker compose down

# Remove all volumes and images
docker system prune -af
docker volume prune -f

# Run fresh setup
./setup.sh

# Verify Avro is working
docker compose logs airflow-scheduler | grep -i avro
```

## Development

### Adding New DAGs

1. Place your DAG files in the `dags/` directory
2. Restart the scheduler: `docker compose restart airflow-scheduler`
3. The new DAGs will appear in the web UI
4. Monitor Avro serialization in logs for new task metadata

### Modifying Plugins

1. Edit plugin files in the `plugins/` directory
2. Update Avro schemas in `schemas/` if data structure changes
3. Restart Airflow services: `docker compose restart`
4. Changes will be loaded automatically

### Schema Development

To modify Avro schemas:

1. Edit `.avsc` files in the `schemas/` directory
2. Maintain backward compatibility for schema evolution
3. Test schema changes in development first
4. Restart services to reload schemas: `docker compose restart`

### Example: Adding a New Field to Task Metadata

```json
// In schemas/task_metadata.avsc
{
  "name": "new_field",
  "type": ["null", "string"],
  "default": null
}
```

## Logs and Monitoring

- **Airflow Logs**: Available in the `logs/` directory with Avro compression stats
- **Container Logs**: Use `docker compose logs <service-name>`
- **Web UI Logs**: Check individual task logs in the Airflow web interface
- **Plugin Logs**: Custom plugin logs with message size comparisons
- **Kafka Consumer Logs**: Real-time Avro/JSON message processing logs
- **Compression Metrics**: Size reduction statistics in scheduler logs

### Key Log Patterns to Monitor

```bash
# Avro compression performance
docker compose logs airflow-scheduler | grep "compression"

# Message format detection
docker compose logs kafka-consumer | grep "Detected.*message format"

# Schema loading status
docker compose logs airflow-scheduler | grep -i "avro.*schema"
```

## Support

For issues or questions:

1. Check the logs using `docker compose logs`
2. Verify all services are running with `docker compose ps`
3. Ensure proper permissions on the logs directory
4. Review the Airflow web UI for DAG and task status
