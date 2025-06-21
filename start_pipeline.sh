#!/bin/bash

echo "🚀 Starting News Pipeline Data Engineering Assessment"
echo "=================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Set Airflow UID for Linux/macOS
export AIRFLOW_UID=50000

echo "📋 Pre-flight checks..."
echo "✅ Docker is running"
echo "✅ Environment variables set"

# Create necessary directories
mkdir -p logs plugins

echo "🏗️  Building and starting containers..."
docker-compose up -d

echo "⏳ Waiting for services to start..."
sleep 30

echo "🔍 Checking service status..."
docker-compose ps

echo ""
echo "🎉 Pipeline is starting up!"
echo ""
echo "📊 Access Points:"
echo "   Airflow UI:  http://localhost:8080"
echo "   Username:    airflow"
echo "   Password:    airflow123"
echo ""
echo "   PostgreSQL:  localhost:5433"
echo "   Username:    dataeng"
echo "   Password:    dataeng123"
echo "   Database:    news_warehouse"
echo ""
echo "📝 Next Steps:"
echo "   1. Open Airflow UI: http://localhost:8080"
echo "   2. Enable the 'news_pipeline_dag'"
echo "   3. Trigger a manual run or wait for scheduled execution"
echo ""
echo "🛠️  Useful Commands:"
echo "   View logs:        docker-compose logs -f"
echo "   Stop pipeline:    docker-compose down"
echo "   Restart:          docker-compose restart"
echo ""
echo "📖 For detailed instructions, see README.md" 