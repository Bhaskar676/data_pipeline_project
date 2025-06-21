# MovieLens and News Pipeline

This project contains two Airflow DAGs:
1. `news_pipeline_dag` - Extracts news data from Finshots and YourStory
2. `movielens_pipeline_dag` - Analyzes MovieLens data (depends on news_pipeline_dag)

## CI/CD Setup

This project includes a complete CI/CD pipeline using GitHub Actions and Docker.

### Prerequisites

- Docker and Docker Compose
- GitHub account with repository access
- Server for deployment with Docker installed

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

3. Build and start the containers:
   ```bash
   chmod +x deploy.sh
   ./deploy.sh build
   ./deploy.sh deploy
   ```

4. Trigger the pipelines:
   ```bash
   ./deploy.sh trigger
   ```

5. Access the Airflow UI:
   - URL: http://localhost:8080
   - Username: airflow (or as set in .env)
   - Password: airflow123 (or as set in .env)

### CI/CD Pipeline

The CI/CD pipeline is configured in `.github/workflows/airflow-cicd.yml` and performs the following steps:

1. **Test**: Runs linting and unit tests on the DAGs
2. **Build**: Builds and pushes Docker images to the registry
3. **Deploy**: Deploys the stack to the server
4. **Trigger**: Unpauses and triggers the DAGs automatically

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

The `deploy.sh` script automatically configures these connections in Airflow during deployment.

### GitHub Secrets Configuration

Set up the following secrets in your GitHub repository:

- `DOCKERHUB_USERNAME`: Docker Hub username
- `DOCKERHUB_TOKEN`: Docker Hub access token
- `SSH_HOST`: Deployment server hostname/IP
- `SSH_USERNAME`: SSH username for the deployment server
- `SSH_PRIVATE_KEY`: SSH private key for the deployment server
- `SLACK_WEBHOOK_URL`: (Optional) Slack webhook URL for alerts
- `SMTP_PASSWORD`: (Optional) SMTP password for email alerts

### Deployment Server Setup

1. Create a deployment directory:
   ```bash
   mkdir -p /path/to/deployment
   ```

2. Create a `.env` file with your configuration:
   ```bash
   cp env.example .env
   # Edit the .env file with your settings
   ```

3. First-time manual deployment:
   ```bash
   cd /path/to/deployment
   git clone <repository-url> .
   ./deploy.sh deploy
   ./deploy.sh trigger
   ```

After the initial setup, the CI/CD pipeline will handle all future deployments automatically.

## Project Structure

- `dags/`: Airflow DAG definitions
- `scripts/`: Python scripts for data processing
- `data/`: Data files including MovieLens dataset
- `logs/`: Airflow logs
- `plugins/`: Airflow plugins
- `.github/workflows/`: CI/CD workflow definitions
- `docker-compose.yml`: Docker Compose configuration
- `Dockerfile.airflow`: Custom Airflow image definition
- `Dockerfile.postgres-dw`: Custom PostgreSQL data warehouse image definition
- `deploy.sh`: Deployment script
- `requirements.txt`: Python dependencies 