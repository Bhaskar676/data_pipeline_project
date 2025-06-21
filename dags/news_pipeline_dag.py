from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import sys
import os

# Add scripts directory to Python path
sys.path.append('/opt/airflow/scripts')

# Import our custom modules
from finshots_extractor import main as extract_finshots
from yourstory_mock_generator import main as generate_yourstory_mock
from sentiment_analyzer import analyze_batch_sentiment
from database_manager import DatabaseManager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['admin@newsanalysis.com']
}

# Create the DAG
dag = DAG(
    'news_pipeline_dag',
    default_args=default_args,
    description='News scraping and sentiment analysis pipeline for HDFC and Tata Motors',
    schedule_interval='0 19 * * 1-5',  # Run at 7pm (19:00) Monday-Friday
    catchup=False,
    max_active_runs=1,
    tags=['news', 'sentiment', 'data-engineering']
)

# Alert functions
def task_failure_slack_alert(context):
    """Send Slack alert on task failure"""
    slack_webhook_token = SlackWebhookOperator(
        task_id='slack_failure_alert',
        http_conn_id='slack_webhook',
        webhook_token=None,  # This will use the connection defined in Airflow
        message=f"""
        :red_circle: Task Failed.
        *DAG*: {context.get('task_instance').dag_id}
        *Task*: {context.get('task_instance').task_id}
        *Execution Time*: {context.get('execution_date')}
        *Exception*: {context.get('exception')}
        *Log URL*: {context.get('task_instance').log_url}
        """,
        username='airflow',
        dag=dag
    )
    return slack_webhook_token.execute(context=context)

def task_failure_email_alert(context):
    """Send email alert on task failure"""
    email_op = EmailOperator(
        task_id='email_failure_alert',
        to=default_args['email'],
        subject=f"Airflow Alert: {context.get('task_instance').task_id} Failed",
        html_content=f"""
        <h3>Task Failed</h3>
        <p><strong>DAG</strong>: {context.get('task_instance').dag_id}</p>
        <p><strong>Task</strong>: {context.get('task_instance').task_id}</p>
        <p><strong>Execution Time</strong>: {context.get('execution_date')}</p>
        <p><strong>Exception</strong>: {context.get('exception')}</p>
        <p><strong>Log URL</strong>: {context.get('task_instance').log_url}</p>
        """,
        dag=dag
    )
    return email_op.execute(context=context)

# Add on_failure_callback to default_args
default_args['on_failure_callback'] = [task_failure_slack_alert, task_failure_email_alert]

def extract_finshots_articles(**context):
    """
    Extract articles from Finshots
    """
    logger.info("Starting Finshots article extraction...")
    
    try:
        articles = extract_finshots()
        logger.info(f"Extracted {len(articles)} articles from Finshots")
        
        # Store in XCom for next task
        context['task_instance'].xcom_push(key='finshots_articles', value=articles)
        
        return f"Successfully extracted {len(articles)} Finshots articles"
        
    except Exception as e:
        logger.error(f"Failed to extract Finshots articles: {e}")
        raise

def generate_yourstory_articles(**context):
    """
    Generate mock YourStory articles
    """
    logger.info("Starting YourStory mock article generation...")
    
    try:
        articles = generate_yourstory_mock()
        logger.info(f"Generated {len(articles)} mock YourStory articles")
        
        # Store in XCom for next task
        context['task_instance'].xcom_push(key='yourstory_articles', value=articles)
        
        return f"Successfully generated {len(articles)} YourStory mock articles"
        
    except Exception as e:
        logger.error(f"Failed to generate YourStory articles: {e}")
        raise

def combine_and_analyze_sentiment(**context):
    """
    Combine articles from both sources and perform sentiment analysis
    """
    logger.info("Starting article combination and sentiment analysis...")
    
    try:
        # Get articles from previous tasks
        finshots_articles = context['task_instance'].xcom_pull(key='finshots_articles', task_ids='extract_finshots')
        yourstory_articles = context['task_instance'].xcom_pull(key='yourstory_articles', task_ids='generate_yourstory_mock')
        
        # Combine all articles
        all_articles = []
        if finshots_articles:
            all_articles.extend(finshots_articles)
        if yourstory_articles:
            all_articles.extend(yourstory_articles)
        
        logger.info(f"Combined {len(all_articles)} articles for sentiment analysis")
        
        if not all_articles:
            logger.warning("No articles found for sentiment analysis")
            return "No articles to process"
        
        # Perform sentiment analysis
        analyzed_articles = analyze_batch_sentiment(all_articles)
        logger.info(f"Completed sentiment analysis for {len(analyzed_articles)} articles")
        
        # Store in XCom for database insertion
        context['task_instance'].xcom_push(key='analyzed_articles', value=analyzed_articles)
        
        return f"Successfully analyzed sentiment for {len(analyzed_articles)} articles"
        
    except Exception as e:
        logger.error(f"Failed to analyze sentiment: {e}")
        raise

def load_to_database(**context):
    """
    Load analyzed articles to PostgreSQL database
    """
    logger.info("Starting database loading...")
    
    try:
        # Get analyzed articles from previous task
        analyzed_articles = context['task_instance'].xcom_pull(key='analyzed_articles', task_ids='sentiment_analysis')
        
        if not analyzed_articles:
            logger.warning("No analyzed articles found for database loading")
            return "No articles to load"
        
        # Initialize database manager
        db_manager = DatabaseManager()
        
        if not db_manager.connect():
            raise Exception("Failed to connect to database")
        
        # Create tables if they don't exist
        if not db_manager.create_tables():
            raise Exception("Failed to create database tables")
        
        # Insert articles
        inserted_count = db_manager.insert_articles(analyzed_articles)
        
        # Log pipeline run
        dag_id = context['dag'].dag_id
        run_id = context['dag_run'].run_id
        
        # Log for each source
        sources = set(article.get('source', 'unknown') for article in analyzed_articles)
        for source in sources:
            source_articles = [a for a in analyzed_articles if a.get('source') == source]
            db_manager.log_pipeline_run(
                dag_id=dag_id,
                run_id=run_id,
                source=source,
                articles_processed=len(source_articles),
                articles_inserted=len([a for a in source_articles if a.get('url') in [inserted.get('url') for inserted in analyzed_articles[:inserted_count]]]),
                status='completed'
            )
        
        # Get statistics
        stats = db_manager.get_article_count_by_source()
        logger.info(f"Database statistics after loading: {stats}")
        
        db_manager.disconnect()
        
        # Store stats in XCom for alerting
        context['task_instance'].xcom_push(key='database_stats', value=stats)
        context['task_instance'].xcom_push(key='inserted_count', value=inserted_count)
        
        return f"Successfully loaded {inserted_count} articles to database"
        
    except Exception as e:
        logger.error(f"Failed to load articles to database: {e}")
        raise

def send_success_alert(**context):
    """
    Send success notification with pipeline statistics
    """
    logger.info("Preparing success notification...")
    
    try:
        # Get statistics from previous task
        stats = context['task_instance'].xcom_pull(key='database_stats', task_ids='load_to_database')
        inserted_count = context['task_instance'].xcom_pull(key='inserted_count', task_ids='load_to_database')
        
        # Format statistics message
        stats_message = "Pipeline Statistics:\n"
        stats_message += f"Total articles processed: {inserted_count}\n\n"
        
        if stats:
            for source, source_data in stats.items():
                stats_message += f"{source.upper()}:\n"
                for keyword, keyword_data in source_data.items():
                    stats_message += f"  {keyword}:\n"
                    for sentiment, sentiment_data in keyword_data.items():
                        count = sentiment_data.get('count', 0)
                        avg_score = sentiment_data.get('avg_sentiment', 0.0)
                        stats_message += f"    {sentiment}: {count} articles (avg: {avg_score:.3f})\n"
                stats_message += "\n"
        
        logger.info(f"Pipeline completed successfully. {stats_message}")
        
        # In a real implementation, you would send this via email/Slack/etc.
        # For now, we'll just log it
        return f"Success alert sent. {stats_message}"
        
    except Exception as e:
        logger.error(f"Failed to send success alert: {e}")
        return "Alert sending failed but pipeline completed successfully"

def send_failure_alert(**context):
    """
    Send failure notification
    """
    logger.error("Pipeline failed - sending failure alert...")
    
    try:
        # Get failure information
        dag_id = context['dag'].dag_id
        run_id = context['dag_run'].run_id
        failed_task = context.get('task_instance')
        
        failure_message = f"""
        Pipeline Failure Alert:
        
        DAG: {dag_id}
        Run ID: {run_id}
        Failed Task: {failed_task.task_id if failed_task else 'Unknown'}
        Failure Time: {datetime.now()}
        
        Please check the Airflow logs for detailed error information.
        """
        
        logger.error(failure_message)
        
        # In a real implementation, you would send this via email/Slack/etc.
        return "Failure alert sent"
        
    except Exception as e:
        logger.error(f"Failed to send failure alert: {e}")
        return "Alert sending failed"

# Define tasks
extract_finshots_task = PythonOperator(
    task_id='extract_finshots',
    python_callable=extract_finshots_articles,
    dag=dag
)

generate_yourstory_task = PythonOperator(
    task_id='generate_yourstory_mock',
    python_callable=generate_yourstory_articles,
    dag=dag
)

sentiment_analysis_task = PythonOperator(
    task_id='sentiment_analysis',
    python_callable=combine_and_analyze_sentiment,
    dag=dag
)

database_load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag
)

success_alert_task = PythonOperator(
    task_id='send_success_alert',
    python_callable=send_success_alert,
    dag=dag,
    trigger_rule='all_success'
)

failure_alert_task = PythonOperator(
    task_id='send_failure_alert',
    python_callable=send_failure_alert,
    dag=dag,
    trigger_rule='one_failed'
)

# Define task dependencies
# Both extraction tasks run in parallel
[extract_finshots_task, generate_yourstory_task] >> sentiment_analysis_task

# After sentiment analysis, load to database
sentiment_analysis_task >> database_load_task

# After database loading, send success alert
database_load_task >> success_alert_task

# If any task fails, send failure alert
[extract_finshots_task, generate_yourstory_task, sentiment_analysis_task, database_load_task] >> failure_alert_task 