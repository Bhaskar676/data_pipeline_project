Write-Host "🚀 Starting News Pipeline Data Engineering Assessment" -ForegroundColor Green
Write-Host "==================================================" -ForegroundColor Green

# Check if Docker is running
try {
    docker info | Out-Null
    Write-Host "✅ Docker is running" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker is not running. Please start Docker first." -ForegroundColor Red
    exit 1
}

# Set Airflow UID for Windows
$env:AIRFLOW_UID = "50000"

Write-Host "📋 Pre-flight checks..." -ForegroundColor Yellow
Write-Host "✅ Environment variables set" -ForegroundColor Green

# Create necessary directories
New-Item -ItemType Directory -Force -Path "logs" | Out-Null
New-Item -ItemType Directory -Force -Path "plugins" | Out-Null

Write-Host "🏗️  Building and starting containers..." -ForegroundColor Yellow
docker-compose up -d

Write-Host "⏳ Waiting for services to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

Write-Host "🔍 Checking service status..." -ForegroundColor Yellow
docker-compose ps

Write-Host ""
Write-Host "🎉 Pipeline is starting up!" -ForegroundColor Green
Write-Host ""
Write-Host "📊 Access Points:" -ForegroundColor Cyan
Write-Host "   Airflow UI:  http://localhost:8080" -ForegroundColor White
Write-Host "   Username:    airflow" -ForegroundColor White
Write-Host "   Password:    airflow123" -ForegroundColor White
Write-Host ""
Write-Host "   PostgreSQL:  localhost:5433" -ForegroundColor White
Write-Host "   Username:    dataeng" -ForegroundColor White
Write-Host "   Password:    dataeng123" -ForegroundColor White
Write-Host "   Database:    news_warehouse" -ForegroundColor White
Write-Host ""
Write-Host "📝 Next Steps:" -ForegroundColor Cyan
Write-Host "   1. Open Airflow UI: http://localhost:8080" -ForegroundColor White
Write-Host "   2. Enable the 'news_pipeline_dag'" -ForegroundColor White
Write-Host "   3. Trigger a manual run or wait for scheduled execution" -ForegroundColor White
Write-Host ""
Write-Host "🛠️  Useful Commands:" -ForegroundColor Cyan
Write-Host "   View logs:        docker-compose logs -f" -ForegroundColor White
Write-Host "   Stop pipeline:    docker-compose down" -ForegroundColor White
Write-Host "   Restart:          docker-compose restart" -ForegroundColor White
Write-Host ""
Write-Host "📖 For detailed instructions, see README.md" -ForegroundColor Cyan 