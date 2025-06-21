#!/bin/bash
set -e

# Create default .env file if it doesn't exist
if [ ! -f .env ]; then
  echo "Creating default .env file from env.example"
  if [ -f env.example ]; then
    cp env.example .env
    echo "Default .env file created. You may want to edit it with your custom settings."
  else
    echo "WARNING: env.example not found. Creating minimal .env file."
    cat > .env << EOL
# Airflow settings
AIRFLOW_UID=50000
AIRFLOW_GID=50000
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow123

# PostgreSQL settings
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Data Warehouse settings
POSTGRES_DW_USER=dataeng
POSTGRES_DW_PASSWORD=dataeng123

# Optional: Slack webhook for notifications
# SLACK_WEBHOOK_URL=https://hooks.slack.com/services/your/webhook/url
EOL
    echo "Minimal .env file created."
  fi
fi

# Load environment variables from .env file
echo "Loading environment variables from .env file"
export $(grep -v '^#' .env | xargs)

# Function to wait for service to be healthy
wait_for_service() {
  local service=$1
  local retries=${2:-30}
  local interval=${3:-2}
  
  echo "Waiting for $service to be healthy..."
  for i in $(seq 1 $retries); do
    if docker-compose ps $service | grep -q "(healthy)"; then
      echo "$service is healthy!"
      return 0
    fi
    echo "Waiting for $service to be healthy... ($i/$retries)"
    sleep $interval
  done
  
  echo "Error: $service did not become healthy within the timeout period."
  return 1
}

# Print access information
print_access_info() {
  echo ""
  echo "===================================="
  echo "ACCESS INFORMATION"
  echo "===================================="
  echo "Airflow UI: http://localhost:8080"
  echo "Airflow Username: ${AIRFLOW_USERNAME:-airflow}"
  echo "Airflow Password: ${AIRFLOW_PASSWORD:-airflow123}"
  echo ""
  echo "PostgreSQL (Source DB):"
  echo "  Host: localhost"
  echo "  Port: 5432"
  echo "  Database: movielens"
  echo "  Username: ${POSTGRES_USER:-postgres}"
  echo "  Password: ${POSTGRES_PASSWORD:-postgres}"
  echo ""
  echo "PostgreSQL (Data Warehouse):"
  echo "  Host: localhost"
  echo "  Port: 5433"
  echo "  Database: news_warehouse"
  echo "  Username: ${POSTGRES_DW_USER:-dataeng}"
  echo "  Password: ${POSTGRES_DW_PASSWORD:-dataeng123}"
  echo "===================================="
  echo ""
}

# Deploy the stack
if [ "$1" == "deploy" ] || [ "$1" == "" ]; then
  echo "Starting containers..."
  
  # Stop any running containers
  docker-compose down || true
  
  # Start the stack
  docker-compose up -d postgres postgres_dw
  
  # Wait for databases to be ready
  wait_for_service postgres 30 2
  wait_for_service postgres_dw 30 2
  
  # Start Airflow services
  docker-compose up -d airflow-webserver
  wait_for_service airflow-webserver 60 2
  
  docker-compose up -d airflow-scheduler
  wait_for_service airflow-scheduler 30 2
  
  echo "All containers are up and healthy!"
  
  # Configure Airflow connections
  if [ -n "$SLACK_WEBHOOK_URL" ]; then
    echo "Adding Slack webhook connection..."
    docker-compose exec -T airflow-webserver airflow connections add 'slack_webhook' \
      --conn-type 'http' \
      --conn-host 'https://hooks.slack.com/services' \
      --conn-password "$SLACK_WEBHOOK_URL" \
      --conn-extra '{"webhook_endpoint": "/services"}'
  fi
  
  echo "Deployment complete!"
  
  # Print access information
  print_access_info
  
  # Automatically run the pipelines unless notrigger is specified
  if [ "$2" != "notrigger" ]; then
    $0 run
  fi
  exit 0
fi

# Run the pipelines
if [ "$1" == "run" ]; then
  echo "Unpausing and running pipelines..."
  
  # Unpause DAGs
  docker-compose exec -T airflow-webserver airflow dags unpause news_pipeline_dag
  docker-compose exec -T airflow-webserver airflow dags unpause movielens_pipeline_dag
  
  # Trigger news pipeline first
  echo "Triggering news_pipeline_dag..."
  docker-compose exec -T airflow-webserver airflow dags trigger news_pipeline_dag
  
  # Wait for news pipeline to complete
  echo "Waiting for news_pipeline_dag to complete (60 seconds)..."
  sleep 60
  
  # Trigger movielens pipeline
  echo "Triggering movielens_pipeline_dag..."
  docker-compose exec -T airflow-webserver airflow dags trigger movielens_pipeline_dag
  
  # Wait for movielens pipeline to complete
  echo "Waiting for movielens_pipeline_dag to complete (120 seconds)..."
  sleep 120
  
  # Show results
  $0 results
  exit 0
fi

# Show pipeline results
if [ "$1" == "results" ]; then
  echo "=== Pipeline Results ==="
  
  # Get news pipeline results
  echo "News Pipeline Results:"
  NEWS_RUN_ID=$(docker-compose exec -T airflow-webserver airflow dags list-runs -d news_pipeline_dag -o json | python -c "import sys, json; print(json.load(sys.stdin)[0]['run_id'])")
  docker-compose exec -T airflow-webserver airflow tasks states-for-dag-run news_pipeline_dag $NEWS_RUN_ID
  
  # Get movielens pipeline results
  echo -e "\nMovieLens Pipeline Results:"
  ML_RUN_ID=$(docker-compose exec -T airflow-webserver airflow dags list-runs -d movielens_pipeline_dag -o json | python -c "import sys, json; print(json.load(sys.stdin)[0]['run_id'])")
  docker-compose exec -T airflow-webserver airflow tasks states-for-dag-run movielens_pipeline_dag $ML_RUN_ID
  
  # Get analysis results
  echo -e "\n=== Analysis Results ==="
  LOG_PATH="/opt/airflow/logs/dag_id=movielens_pipeline_dag/run_id=${ML_RUN_ID}/task_id=send_success_alert/attempt=1.log"
  docker-compose exec -T airflow-webserver bash -c "cat $LOG_PATH | grep -A 10 'MovieLens Analysis Results' || echo 'No results found. Pipeline may still be running.'"
  
  # Show database results
  echo -e "\n=== Database Results ==="
  echo "Articles in database:"
  docker-compose exec -T postgres_dw psql -U dataeng -d news_warehouse -c "SELECT COUNT(*) FROM articles;"
  
  echo -e "\nSample articles by sentiment:"
  docker-compose exec -T postgres_dw psql -U dataeng -d news_warehouse -c "SELECT source, sentiment_label, COUNT(*) FROM articles GROUP BY source, sentiment_label ORDER BY source, sentiment_label;"
  
  # Print access information again
  print_access_info
  
  exit 0
fi

# Stop the stack
if [ "$1" == "stop" ]; then
  echo "Stopping all containers..."
  docker-compose down
  echo "All containers stopped."
  exit 0
fi

# Show usage if no valid command provided
echo "Usage: $0 [deploy|run|results|stop]"
echo "  deploy  - Start all containers (default if no command provided)"
echo "  run     - Unpause and run the pipelines"
echo "  results - Show pipeline results"
echo "  stop    - Stop all containers"
exit 1 