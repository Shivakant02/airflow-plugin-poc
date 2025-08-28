# Airflow Plugin POC

This repository contains a Proof of Concept (POC) for Apache Airflow with custom plugins for task and DAG logging, metadata extraction, and task graph visualization.

## Features

- **Task Logging Plugin**: Captures detailed metadata for task lifecycle events (running, success, failed)
- **DAG Logging Plugin**: Logs DAG run information and metadata
- **Metadata Extraction**: Extracts and logs pipeline run IDs and task relationships
- **Task Graph Plugin**: Visualizes DAG task dependencies and relationships
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

This script will:

- Create necessary directories with proper permissions
- Set up Airflow UID (50000) for container compatibility
- Initialize the Airflow database and create admin user
- Start all Airflow services (webserver, scheduler, postgres)

### 3. Access Airflow Web UI

Once the setup is complete, you can access the Airflow web interface:

- **URL**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

## Project Structure

```
airflow-plugin-poc/
├── dags/                          # Airflow DAGs
│   ├── simple_success_dag.py      # Always succeeds DAG for testing
│   └── complex_failure_dag.py     # Mixed success/failure DAG
├── plugins/                       # Custom Airflow plugins
│   ├── task_logging_plugin.py     # Main logging plugin
│   ├── metadata_extraction_plugin.py
│   ├── task_graph_plugin.py
│   └── dag_logging_plugin.py
├── logs/                          # Airflow logs directory
├── docker-compose.yaml           # Docker services configuration
├── setup.sh                      # Automated setup script
└── README.md                     # This file
```

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

- Captures task state changes (running, success, failed)
- Logs task metadata including execution dates, try numbers, operators
- Extracts pipeline run IDs from XCom
- Provides detailed task graph information

### Key Logged Information

- Pipeline Run ID (from XCom)
- Task execution metadata
- DAG run information
- Task dependencies and relationships
- Operator types and configurations

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

## Development

### Adding New DAGs

1. Place your DAG files in the `dags/` directory
2. Restart the scheduler: `docker compose restart airflow-scheduler`
3. The new DAGs will appear in the web UI

### Modifying Plugins

1. Edit plugin files in the `plugins/` directory
2. Restart Airflow services: `docker compose restart`
3. Changes will be loaded automatically

## Logs and Monitoring

- **Airflow Logs**: Available in the `logs/` directory
- **Container Logs**: Use `docker compose logs <service-name>`
- **Web UI Logs**: Check individual task logs in the Airflow web interface
- **Plugin Logs**: Custom plugin logs appear in the scheduler and task logs

## Support

For issues or questions:

1. Check the logs using `docker compose logs`
2. Verify all services are running with `docker compose ps`
3. Ensure proper permissions on the logs directory
4. Review the Airflow web UI for DAG and task status
