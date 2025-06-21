"""
MovieLens Data Analysis Pipeline (Pipeline 2)
Scheduled: 8pm every working day
Dependency: Only runs if Pipeline 1 succeeded on the same day
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import logging

# Import our custom modules
import sys
sys.path.append('/opt/airflow/scripts')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 21),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'movielens_pipeline_dag',
    default_args=default_args,
    description='MovieLens data analysis pipeline with dependency on news pipeline',
    schedule_interval='0 20 * * 1-5',  # Run at 8pm (20:00) Monday-Friday
    catchup=False,
    max_active_runs=1,
    tags=['movielens', 'analytics', 'data-engineering']
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
        to=default_args['owner'].replace('-', '.') + '@example.com',
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

def check_pipeline1_success(**context):
    """Check if Pipeline 1 completed successfully on the same day"""
    logger.info("Checking Pipeline 1 success status...")
    
    try:
        from airflow.models import DagRun
        from airflow.utils import timezone
        from datetime import datetime
        
        # Get today's date from the logical_date (new way, not deprecated execution_date)
        logical_date = context.get('logical_date') or context.get('data_interval_start') or context.get('execution_date')
        
        if not logical_date:
            raise ValueError("Unable to determine execution date from context")
        
        today = logical_date.date()
        
        # Use the simpler approach with just state and dag_id
        # Let the function use its default session handling
        dag_runs = DagRun.find(
            dag_id='news_pipeline_dag',
            state='success'
        )
        
        # Filter the results manually for today's date
        today_runs = []
        for run in dag_runs:
            run_date = run.execution_date.date() if hasattr(run, 'execution_date') else run.logical_date.date()
            if run_date == today:
                today_runs.append(run)
        
        if today_runs:
            logger.info(f"Found {len(today_runs)} successful Pipeline 1 runs today")
            return True
        else:
            logger.warning("No successful Pipeline 1 runs found for today")
            raise Exception("Pipeline 1 has not completed successfully today. Skipping Pipeline 2.")
            
    except Exception as e:
        logger.error(f"Error checking Pipeline 1 status: {e}")
        raise

def load_movielens_data(**context):
    """Download and load MovieLens 100k dataset"""
    logger.info("Starting MovieLens data loading...")
    
    try:
        from movielens_data_loader import download_and_load_data
        data_info = download_and_load_data()
        logger.info(f"Successfully loaded MovieLens data: {data_info}")
        
        context['task_instance'].xcom_push(key='data_info', value=data_info)
        return f"Successfully loaded MovieLens dataset with {data_info['total_ratings']} ratings"
        
    except Exception as e:
        logger.error(f"Failed to load MovieLens data: {e}")
        raise

def task_mean_age_by_occupation(**context):
    """Task 1: Find the mean age of users in each occupation"""
    logger.info("Starting mean age by occupation analysis...")
    
    try:
        from movielens_analyzer import analyze_mean_age_by_occupation
        results = analyze_mean_age_by_occupation()
        logger.info(f"Analyzed {len(results)} occupations")
        
        context['task_instance'].xcom_push(key='mean_age_results', value=results)
        return f"Successfully analyzed mean age for {len(results)} occupations"
        
    except Exception as e:
        logger.error(f"Failed to analyze mean age by occupation: {e}")
        raise

def task_top_rated_movies(**context):
    """Task 2: Find top 20 highest rated movies (at least 35 times rated)"""
    logger.info("Starting top rated movies analysis...")
    
    try:
        from movielens_analyzer import analyze_top_rated_movies
        results = analyze_top_rated_movies(min_ratings=35, top_n=20)
        logger.info(f"Found {len(results)} top rated movies")
        
        context['task_instance'].xcom_push(key='top_movies_results', value=results)
        return f"Successfully found {len(results)} top rated movies"
        
    except Exception as e:
        logger.error(f"Failed to analyze top rated movies: {e}")
        raise

def task_top_genres_by_occupation_age(**context):
    """Task 3: Find top genres by occupation in every age-group"""
    logger.info("Starting top genres by occupation and age analysis...")
    
    try:
        from movielens_analyzer import analyze_top_genres_by_occupation_age
        results = analyze_top_genres_by_occupation_age()
        logger.info(f"Analyzed genres for {len(results)} occupation-age combinations")
        
        context['task_instance'].xcom_push(key='top_genres_results', value=results)
        return f"Successfully analyzed genres for {len(results)} occupation-age combinations"
        
    except Exception as e:
        logger.error(f"Failed to analyze top genres: {e}")
        raise

def task_similar_movies(**context):
    """Task 4: Find top 10 similar movies using collaborative filtering"""
    logger.info("Starting similar movies analysis...")
    
    try:
        from movielens_analyzer import find_similar_movies
        target_movie = "Usual Suspects, The (1995)"
        results = find_similar_movies(
            target_movie=target_movie,
            similarity_threshold=0.95,
            cooccurrence_threshold=50,
            top_n=10
        )
        
        logger.info(f"Found {len(results)} similar movies for '{target_movie}'")
        context['task_instance'].xcom_push(key='similar_movies_results', value=results)
        return f"Successfully found {len(results)} similar movies for '{target_movie}'"
        
    except Exception as e:
        logger.error(f"Failed to analyze similar movies: {e}")
        raise

def send_success_alert(**context):
    """Send success notification with analysis statistics"""
    logger.info("Preparing success notification...")
    
    try:
        # Get results from previous tasks
        mean_age_results = context['task_instance'].xcom_pull(key='mean_age_results', task_ids='mean_age_by_occupation')
        top_movies_results = context['task_instance'].xcom_pull(key='top_movies_results', task_ids='top_rated_movies')
        top_genres_results = context['task_instance'].xcom_pull(key='top_genres_results', task_ids='top_genres_by_occupation_age')
        similar_movies_results = context['task_instance'].xcom_pull(key='similar_movies_results', task_ids='similar_movies_analysis')
        
        stats_message = "MovieLens Analysis Results:\n"
        stats_message += f"1. Mean Age Analysis: {len(mean_age_results) if mean_age_results else 0} occupations\n"
        stats_message += f"2. Top Rated Movies: {len(top_movies_results) if top_movies_results else 0} movies\n"
        stats_message += f"3. Genre Analysis: {len(top_genres_results) if top_genres_results else 0} combinations\n"
        stats_message += f"4. Similar Movies: {len(similar_movies_results) if similar_movies_results else 0} recommendations\n"
        
        logger.info(f"MovieLens pipeline completed successfully. {stats_message}")
        return f"Success alert sent. {stats_message}"
        
    except Exception as e:
        logger.error(f"Failed to send success alert: {e}")
        return "Alert sending failed but pipeline completed successfully"

# Define tasks
check_pipeline1_task = PythonOperator(
    task_id='check_pipeline1_success',
    python_callable=check_pipeline1_success,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_movielens_data',
    python_callable=load_movielens_data,
    dag=dag
)

mean_age_task = PythonOperator(
    task_id='mean_age_by_occupation',
    python_callable=task_mean_age_by_occupation,
    dag=dag
)

top_movies_task = PythonOperator(
    task_id='top_rated_movies',
    python_callable=task_top_rated_movies,
    dag=dag
)

top_genres_task = PythonOperator(
    task_id='top_genres_by_occupation_age',
    python_callable=task_top_genres_by_occupation_age,
    dag=dag
)

similar_movies_task = PythonOperator(
    task_id='similar_movies_analysis',
    python_callable=task_similar_movies,
    dag=dag
)

success_alert_task = PythonOperator(
    task_id='send_success_alert',
    python_callable=send_success_alert,
    dag=dag,
    trigger_rule='all_success'
)

# Define task dependencies
check_pipeline1_task >> load_data_task
load_data_task >> [mean_age_task, top_movies_task, top_genres_task, similar_movies_task]
[mean_age_task, top_movies_task, top_genres_task, similar_movies_task] >> success_alert_task 