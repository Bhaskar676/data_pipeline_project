# MovieLens and News Pipeline

This project contains two Airflow DAGs:
1. `news_pipeline_dag` - Extracts news data from Finshots and YourStory
2. `movielens_pipeline_dag` - Analyzes MovieLens data

## Setup

This project requires Docker and Docker Compose to run.

### Prerequisites

- Docker and Docker Compose

### Local Development

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd pipeline_assesment
   ```

2. Create a `.env` file from the example:
   ```bash
   cp env.example .env
   ```

3. Deploy and run the pipeline:

   **For Linux/macOS:**
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```

   **For Windows:**
   ```powershell
   .\deploy.ps1
   ```

4. Access the Airflow UI:
   - URL: http://localhost:8080
   - Username: airflow (or as set in .env)
   - Password: airflow123 (or as set in .env)

### Running the Pipeline

The deployment scripts offer several commands:

**For Linux/macOS:**
```bash
# Deploy containers only
./deploy.sh deploy

# Run the pipelines
./deploy.sh run

# Show pipeline results
./deploy.sh results

# Stop all containers
./deploy.sh stop
```

**For Windows:**
```powershell
# Deploy containers only
.\deploy.ps1 deploy

# Run the pipelines
.\deploy.ps1 run

# Show pipeline results
.\deploy.ps1 results

# Stop all containers
.\deploy.ps1 stop
```

The scripts will:
1. Deploy the Docker containers
2. Trigger the DAGs in sequence
3. Display the results after completion

### Alerts and Notifications

The pipeline includes comprehensive alerting for task failures:

1. **Email Alerts**: Sends detailed error information to specified email addresses
2. **Slack Alerts**: Posts failure notifications to a Slack channel via webhook

To configure alerts:

1. **Email**: Set SMTP configuration in the `.env` file
   ```
   SMTP_HOST=smtp.gmail.com
   SMTP_PORT=587
   SMTP_USER=your-email@gmail.com
   SMTP_PASSWORD=your-app-password
   SMTP_MAIL_FROM=your-email@gmail.com
   ```

2. **Slack**: Add your webhook URL to the `.env` file
   ```
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/your/webhook/url
   ```

The deployment scripts automatically configure these connections in Airflow during deployment.

### Environment Configuration

1. Create a `.env` file with your configuration:
   ```bash
   cp env.example .env
   # Edit the .env file with your settings
   ```

2. Optional configurations:
   - `SLACK_WEBHOOK_URL`: Slack webhook URL for alerts
   - `SMTP_PASSWORD`: SMTP password for email alerts

## Project Structure

- `dags/`: Airflow DAG definitions
- `scripts/`: Python scripts for data processing
- `data/`: Data files including MovieLens dataset
- `logs/`: Airflow logs
- `plugins/`: Airflow plugins
- `deploy.sh`: Bash deployment script for Linux/macOS
- `deploy.ps1`: PowerShell deployment script for Windows
- `docker-compose.yml`: Docker Compose configuration
- `requirements.txt`: Python dependencies 