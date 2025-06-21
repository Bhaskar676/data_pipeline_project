#!/bin/bash
set -e

# Load environment variables from .env file if it exists
if [ -f .env ]; then
  echo "Loading environment variables from .env file"
  export $(grep -v '^#' .env | xargs)
fi

# Set default values if not provided
DOCKER_REGISTRY=${DOCKER_REGISTRY:-localhost}
TAG=${TAG:-latest}
DEPLOYMENT_PATH=${DEPLOYMENT_PATH:-$(pwd)}

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

# Build and push Docker images
if [ "$1" == "build" ]; then
  echo "Building Docker images..."
  docker-compose build
  
  if [ "$2" == "push" ]; then
    echo "Pushing Docker images to registry..."
    docker-compose push
  fi
  exit 0
fi

# Deploy the stack
if [ "$1" == "deploy" ]; then
  echo "Deploying to $DEPLOYMENT_PATH..."
  
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
  
  # Configure Airflow connections
  echo "Configuring Airflow connections..."
  
  # Add Slack webhook connection if SLACK_WEBHOOK_URL is provided
  if [ -n "$SLACK_WEBHOOK_URL" ]; then
    echo "Adding Slack webhook connection..."
    docker-compose exec -T airflow-webserver airflow connections add 'slack_webhook' \
      --conn-type 'http' \
      --conn-host 'https://hooks.slack.com/services' \
      --conn-password "$SLACK_WEBHOOK_URL" \
      --conn-extra '{"webhook_endpoint": "/services"}'
  fi
  
  echo "Deployment complete!"
  exit 0
fi

# Unpause and trigger DAGs
if [ "$1" == "trigger" ]; then
  echo "Unpausing and triggering DAGs..."
  
  # Unpause DAGs
  docker-compose exec -T airflow-webserver airflow dags unpause news_pipeline_dag
  docker-compose exec -T airflow-webserver airflow dags unpause movielens_pipeline_dag
  
  # Trigger news pipeline first (since movielens depends on it)
  echo "Triggering news_pipeline_dag..."
  docker-compose exec -T airflow-webserver airflow dags trigger news_pipeline_dag
  
  # Wait for news pipeline to complete
  echo "Waiting for news_pipeline_dag to complete..."
  sleep 60
  
  # Trigger movielens pipeline
  echo "Triggering movielens_pipeline_dag..."
  docker-compose exec -T airflow-webserver airflow dags trigger movielens_pipeline_dag
  
  # Wait for movielens pipeline to complete
  echo "Waiting for movielens_pipeline_dag to complete..."
  sleep 120
  
  # Get results
  echo "=== Pipeline Results ==="
  docker-compose exec -T airflow-webserver airflow tasks states-for-dag-run movielens_pipeline_dag \
    $(docker-compose exec -T airflow-webserver airflow dags list-runs -d movielens_pipeline_dag -o json | python -c "import sys, json; print(json.load(sys.stdin)[0]['run_id'])")
  
  # Get the log file path for the send_success_alert task
  echo "=== Analysis Results ==="
  RUN_ID=$(docker-compose exec -T airflow-webserver airflow dags list-runs -d movielens_pipeline_dag -o json | python -c "import sys, json; print(json.load(sys.stdin)[0]['run_id'])")
  LOG_PATH="/opt/airflow/logs/dag_id=movielens_pipeline_dag/run_id=${RUN_ID}/task_id=send_success_alert/attempt=1.log"
  
  # Display the log content
  docker-compose exec -T airflow-webserver cat $LOG_PATH | grep -A 10 "MovieLens Analysis Results"
  
  exit 0
fi

# Show usage if no valid command provided
echo "Usage: $0 [build [push]|deploy|trigger]"
echo "  build       - Build Docker images"
echo "  build push  - Build and push Docker images to registry"
echo "  deploy      - Deploy the stack"
echo "  trigger     - Unpause and trigger DAGs"
exit 1 