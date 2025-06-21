#!/bin/bash
set -e

# Colors for better output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== MovieLens and News Pipeline CI/CD Demo ===${NC}"
echo -e "${YELLOW}This script will demonstrate the complete CI/CD setup${NC}"

# Check if Docker is running
echo -e "\n${BLUE}Checking if Docker is running...${NC}"
if ! docker info > /dev/null 2>&1; then
  echo -e "${RED}Docker is not running. Please start Docker and try again.${NC}"
  exit 1
fi
echo -e "${GREEN}Docker is running!${NC}"

# Create .env file if it doesn't exist
echo -e "\n${BLUE}Setting up environment...${NC}"
if [ ! -f .env ]; then
  echo -e "${YELLOW}Creating .env file from env.example${NC}"
  cp env.example .env
  echo -e "${GREEN}.env file created!${NC}"
else
  echo -e "${GREEN}.env file already exists!${NC}"
fi

# Make deploy.sh executable
echo -e "\n${BLUE}Making deploy.sh executable...${NC}"
chmod +x deploy.sh
echo -e "${GREEN}deploy.sh is now executable!${NC}"

# Build the Docker images
echo -e "\n${BLUE}Building Docker images...${NC}"
./deploy.sh build
echo -e "${GREEN}Docker images built successfully!${NC}"

# Deploy the stack
echo -e "\n${BLUE}Deploying the stack...${NC}"
./deploy.sh deploy
echo -e "${GREEN}Stack deployed successfully!${NC}"

# Wait for user to check the Airflow UI
echo -e "\n${YELLOW}Airflow UI is now available at http://localhost:8080${NC}"
echo -e "${YELLOW}Username: airflow (or as set in .env)${NC}"
echo -e "${YELLOW}Password: airflow123 (or as set in .env)${NC}"
echo -e "\n${BLUE}Press Enter to continue and trigger the DAGs...${NC}"
read

# Trigger the DAGs
echo -e "\n${BLUE}Triggering the DAGs...${NC}"
./deploy.sh trigger
echo -e "${GREEN}DAGs triggered successfully!${NC}"

# Generate evidence
echo -e "\n${BLUE}Generating evidence...${NC}"
mkdir -p evidence

# Take a screenshot of the Airflow UI (if xvfb and firefox are available)
if command -v xvfb-run > /dev/null && command -v firefox > /dev/null; then
  echo -e "${YELLOW}Taking screenshot of Airflow UI...${NC}"
  xvfb-run firefox -screenshot evidence/airflow_ui.png http://localhost:8080/home
  echo -e "${GREEN}Screenshot saved to evidence/airflow_ui.png${NC}"
fi

# Save the logs
echo -e "${YELLOW}Saving logs...${NC}"
docker-compose logs > evidence/docker_logs.txt
echo -e "${GREEN}Logs saved to evidence/docker_logs.txt${NC}"

# Save the DAG run information
echo -e "${YELLOW}Saving DAG run information...${NC}"
docker-compose exec -T airflow-webserver airflow dags list-runs -o json > evidence/dag_runs.json
echo -e "${GREEN}DAG run information saved to evidence/dag_runs.json${NC}"

# Save the task states
echo -e "${YELLOW}Saving task states...${NC}"
RUN_ID=$(docker-compose exec -T airflow-webserver airflow dags list-runs -d movielens_pipeline_dag -o json | python -c "import sys, json; print(json.load(sys.stdin)[0]['run_id'])")
docker-compose exec -T airflow-webserver airflow tasks states-for-dag-run movielens_pipeline_dag $RUN_ID > evidence/task_states.txt
echo -e "${GREEN}Task states saved to evidence/task_states.txt${NC}"

echo -e "\n${GREEN}Demo completed successfully!${NC}"
echo -e "${YELLOW}Evidence files are available in the 'evidence' directory${NC}"
echo -e "${BLUE}You can now explore the Airflow UI at http://localhost:8080${NC}" 