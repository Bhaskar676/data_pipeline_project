# PowerShell script for deploying and running pipelines

# Load environment variables from .env file if it exists
if (Test-Path .env) {
    Write-Host "Loading environment variables from .env file" -ForegroundColor Green
    Get-Content .env | ForEach-Object {
        $line = $_.Trim()
        if ($line -and !$line.StartsWith('#')) {
            $key, $value = $line -split '=', 2
            [Environment]::SetEnvironmentVariable($key, $value, 'Process')
        }
    }
}

# Function to wait for service to be healthy
function Wait-ForService {
    param (
        [string]$service,
        [int]$retries = 30,
        [int]$interval = 2
    )
    
    Write-Host "Waiting for $service to be healthy..." -ForegroundColor Yellow
    for ($i = 1; $i -le $retries; $i++) {
        $status = docker-compose ps $service | Select-String "(healthy)"
        if ($status) {
            Write-Host "$service is healthy!" -ForegroundColor Green
            return $true
        }
        Write-Host "Waiting for $service to be healthy... ($i/$retries)" -ForegroundColor Yellow
        Start-Sleep -Seconds $interval
    }
    
    Write-Host "Error: $service did not become healthy within the timeout period." -ForegroundColor Red
    return $false
}

# Function to print access information
function Print-AccessInfo {
    $airflowUsername = if ($env:AIRFLOW_USERNAME) { $env:AIRFLOW_USERNAME } else { "airflow" }
    $airflowPassword = if ($env:AIRFLOW_PASSWORD) { $env:AIRFLOW_PASSWORD } else { "airflow123" }
    $postgresUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "postgres" }
    $postgresPassword = if ($env:POSTGRES_PASSWORD) { $env:POSTGRES_PASSWORD } else { "postgres" }
    $postgresDwUser = if ($env:POSTGRES_DW_USER) { $env:POSTGRES_DW_USER } else { "dataeng" }
    $postgresDwPassword = if ($env:POSTGRES_DW_PASSWORD) { $env:POSTGRES_DW_PASSWORD } else { "dataeng123" }
    
    Write-Host ""
    Write-Host "====================================" -ForegroundColor Cyan
    Write-Host "ACCESS INFORMATION" -ForegroundColor Cyan
    Write-Host "====================================" -ForegroundColor Cyan
    Write-Host "Airflow UI: http://localhost:8080" -ForegroundColor White
    Write-Host "Airflow Username: $airflowUsername" -ForegroundColor White
    Write-Host "Airflow Password: $airflowPassword" -ForegroundColor White
    Write-Host ""
    Write-Host "PostgreSQL (Source DB):" -ForegroundColor White
    Write-Host "  Host: localhost" -ForegroundColor White
    Write-Host "  Port: 5432" -ForegroundColor White
    Write-Host "  Database: movielens" -ForegroundColor White
    Write-Host "  Username: $postgresUser" -ForegroundColor White
    Write-Host "  Password: $postgresPassword" -ForegroundColor White
    Write-Host ""
    Write-Host "PostgreSQL (Data Warehouse):" -ForegroundColor White
    Write-Host "  Host: localhost" -ForegroundColor White
    Write-Host "  Port: 5433" -ForegroundColor White
    Write-Host "  Database: news_warehouse" -ForegroundColor White
    Write-Host "  Username: $postgresDwUser" -ForegroundColor White
    Write-Host "  Password: $postgresDwPassword" -ForegroundColor White
    Write-Host "====================================" -ForegroundColor Cyan
    Write-Host ""
}

# Parse command line arguments
$command = $args[0]
if (-not $command) { $command = "deploy" }

# Deploy the stack
if ($command -eq "deploy") {
    Write-Host "Starting containers..." -ForegroundColor Cyan
    
    # Stop any running containers
    docker-compose down
    
    # Start the stack
    docker-compose up -d postgres postgres_dw
    
    # Wait for databases to be ready
    Wait-ForService -service "postgres" -retries 30 -interval 2
    Wait-ForService -service "postgres_dw" -retries 30 -interval 2
    
    # Start Airflow services
    docker-compose up -d airflow-webserver
    Wait-ForService -service "airflow-webserver" -retries 60 -interval 2
    
    docker-compose up -d airflow-scheduler
    Wait-ForService -service "airflow-scheduler" -retries 30 -interval 2
    
    Write-Host "All containers are up and healthy!" -ForegroundColor Green
    
    # Configure Airflow connections
    if ($env:SLACK_WEBHOOK_URL) {
        Write-Host "Adding Slack webhook connection..." -ForegroundColor Yellow
        docker-compose exec -T airflow-webserver airflow connections add 'slack_webhook' `
            --conn-type 'http' `
            --conn-host 'https://hooks.slack.com/services' `
            --conn-password "$env:SLACK_WEBHOOK_URL" `
            --conn-extra '{\"webhook_endpoint\": \"/services\"}'
    }
    
    Write-Host "Deployment complete!" -ForegroundColor Green
    
    # Print access information
    Print-AccessInfo
    
    # Automatically run the pipelines unless notrigger is specified
    if ($args[1] -ne "notrigger") {
        & $PSCommandPath run
    }
    exit 0
}

# Run the pipelines
if ($command -eq "run") {
    Write-Host "Unpausing and running pipelines..." -ForegroundColor Cyan
    
    # Unpause DAGs
    docker-compose exec -T airflow-webserver airflow dags unpause news_pipeline_dag
    docker-compose exec -T airflow-webserver airflow dags unpause movielens_pipeline_dag
    
    # Trigger news pipeline first
    Write-Host "Triggering news_pipeline_dag..." -ForegroundColor Yellow
    docker-compose exec -T airflow-webserver airflow dags trigger news_pipeline_dag
    
    # Wait for news pipeline to complete
    Write-Host "Waiting for news_pipeline_dag to complete (60 seconds)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 60
    
    # Trigger movielens pipeline
    Write-Host "Triggering movielens_pipeline_dag..." -ForegroundColor Yellow
    docker-compose exec -T airflow-webserver airflow dags trigger movielens_pipeline_dag
    
    # Wait for movielens pipeline to complete
    Write-Host "Waiting for movielens_pipeline_dag to complete (120 seconds)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 120
    
    # Show results
    & $PSCommandPath results
    exit 0
}

# Show pipeline results
if ($command -eq "results") {
    Write-Host "=== Pipeline Results ===" -ForegroundColor Cyan
    
    # Get news pipeline results
    Write-Host "News Pipeline Results:" -ForegroundColor Green
    $newsRunIdCommand = "docker-compose exec -T airflow-webserver airflow dags list-runs -d news_pipeline_dag -o json | python -c `"import sys, json; print(json.load(sys.stdin)[0]['run_id'])`""
    $newsRunId = Invoke-Expression $newsRunIdCommand
    docker-compose exec -T airflow-webserver airflow tasks states-for-dag-run news_pipeline_dag $newsRunId
    
    # Get movielens pipeline results
    Write-Host "`nMovieLens Pipeline Results:" -ForegroundColor Green
    $mlRunIdCommand = "docker-compose exec -T airflow-webserver airflow dags list-runs -d movielens_pipeline_dag -o json | python -c `"import sys, json; print(json.load(sys.stdin)[0]['run_id'])`""
    $mlRunId = Invoke-Expression $mlRunIdCommand
    docker-compose exec -T airflow-webserver airflow tasks states-for-dag-run movielens_pipeline_dag $mlRunId
    
    # Get analysis results
    Write-Host "`n=== Analysis Results ===" -ForegroundColor Cyan
    $logPath = "/opt/airflow/logs/dag_id=movielens_pipeline_dag/run_id=${mlRunId}/task_id=send_success_alert/attempt=1.log"
    docker-compose exec -T airflow-webserver bash -c "cat $logPath | grep -A 10 'MovieLens Analysis Results' || echo 'No results found. Pipeline may still be running.'"
    
    # Show database results
    Write-Host "`n=== Database Results ===" -ForegroundColor Cyan
    Write-Host "Articles in database:" -ForegroundColor Green
    docker-compose exec -T postgres_dw psql -U dataeng -d news_warehouse -c "SELECT COUNT(*) FROM articles;"
    
    Write-Host "`nSample articles by sentiment:" -ForegroundColor Green
    docker-compose exec -T postgres_dw psql -U dataeng -d news_warehouse -c "SELECT source, sentiment_label, COUNT(*) FROM articles GROUP BY source, sentiment_label ORDER BY source, sentiment_label;"
    
    # Print access information again
    Print-AccessInfo
    
    exit 0
}

# Stop the stack
if ($command -eq "stop") {
    Write-Host "Stopping all containers..." -ForegroundColor Cyan
    docker-compose down
    Write-Host "All containers stopped." -ForegroundColor Green
    exit 0
}

# Show usage if no valid command provided
Write-Host "Usage: .\deploy.ps1 [deploy|run|results|stop]" -ForegroundColor Yellow
Write-Host "  deploy  - Start all containers (default if no command provided)" -ForegroundColor White
Write-Host "  run     - Unpause and run the pipelines" -ForegroundColor White
Write-Host "  results - Show pipeline results" -ForegroundColor White
Write-Host "  stop    - Stop all containers" -ForegroundColor White
exit 1 