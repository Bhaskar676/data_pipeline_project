import psycopg2
import psycopg2.extras
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'postgres_dw',  # Docker service name
    'port': 5432,
    'database': 'news_warehouse',
    'user': 'dataeng',
    'password': 'dataeng123'
}

class DatabaseManager:
    """
    Manages database connections and operations for the news warehouse
    """
    
    def __init__(self, db_config: Dict = None):
        self.db_config = db_config or DB_CONFIG
        self.connection = None
        
    def connect(self) -> bool:
        """
        Establish database connection
        """
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = False
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """
        Close database connection
        """
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def create_tables(self) -> bool:
        """
        Create database tables if they don't exist
        """
        try:
            cursor = self.connection.cursor()
            
            # Create articles table
            create_articles_table = """
            CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                source VARCHAR(50) NOT NULL,
                keyword VARCHAR(100) NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                published_date DATE,
                excerpt TEXT,
                full_text TEXT,
                sentiment_score FLOAT,
                sentiment_label VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            # Create indexes
            create_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source);",
                "CREATE INDEX IF NOT EXISTS idx_articles_keyword ON articles(keyword);",
                "CREATE INDEX IF NOT EXISTS idx_articles_published_date ON articles(published_date);",
                "CREATE INDEX IF NOT EXISTS idx_articles_sentiment_label ON articles(sentiment_label);",
                "CREATE INDEX IF NOT EXISTS idx_articles_created_at ON articles(created_at);"
            ]
            
            # Execute table creation
            cursor.execute(create_articles_table)
            
            # Execute index creation
            for index_query in create_indexes:
                cursor.execute(index_query)
            
            # Create pipeline runs table
            create_pipeline_table = """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id SERIAL PRIMARY KEY,
                dag_id VARCHAR(100) NOT NULL,
                run_id VARCHAR(100) NOT NULL,
                source VARCHAR(50) NOT NULL,
                articles_processed INTEGER DEFAULT 0,
                articles_inserted INTEGER DEFAULT 0,
                status VARCHAR(20) DEFAULT 'running',
                error_message TEXT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            );
            """
            
            cursor.execute(create_pipeline_table)
            self.connection.commit()
            
            logger.info("Database tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def insert_articles(self, articles: List[Dict]) -> int:
        """
        Insert articles into the database
        Returns number of articles successfully inserted
        """
        if not articles:
            logger.warning("No articles to insert")
            return 0
        
        try:
            cursor = self.connection.cursor()
            
            insert_query = """
            INSERT INTO articles (
                source, keyword, title, url, published_date, 
                excerpt, full_text, sentiment_score, sentiment_label
            ) VALUES (
                %(source)s, %(keyword)s, %(title)s, %(url)s, %(published_date)s,
                %(excerpt)s, %(full_text)s, %(sentiment_score)s, %(sentiment_label)s
            ) ON CONFLICT (url) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label,
                updated_at = CURRENT_TIMESTAMP;
            """
            
            inserted_count = 0
            for article in articles:
                try:
                    # Prepare article data
                    article_data = {
                        'source': article.get('source', ''),
                        'keyword': article.get('keyword', ''),
                        'title': article.get('title', ''),
                        'url': article.get('url', ''),
                        'published_date': article.get('published_date'),
                        'excerpt': article.get('excerpt', ''),
                        'full_text': article.get('full_text', ''),
                        'sentiment_score': article.get('sentiment_score', 0.0),
                        'sentiment_label': article.get('sentiment_label', 'neutral')
                    }
                    
                    cursor.execute(insert_query, article_data)
                    inserted_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to insert article '{article.get('title', '')}': {e}")
                    continue
            
            self.connection.commit()
            logger.info(f"Successfully inserted {inserted_count} articles into database")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Failed to insert articles: {e}")
            self.connection.rollback()
            return 0
        finally:
            cursor.close()
    
    def log_pipeline_run(self, dag_id: str, run_id: str, source: str, 
                        articles_processed: int = 0, articles_inserted: int = 0,
                        status: str = 'running', error_message: str = None) -> bool:
        """
        Log pipeline run information
        """
        try:
            cursor = self.connection.cursor()
            
            insert_query = """
            INSERT INTO pipeline_runs (
                dag_id, run_id, source, articles_processed, articles_inserted,
                status, error_message, completed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            );
            """
            
            completed_at = datetime.now() if status in ['completed', 'failed'] else None
            
            cursor.execute(insert_query, (
                dag_id, run_id, source, articles_processed, articles_inserted,
                status, error_message, completed_at
            ))
            
            self.connection.commit()
            logger.info(f"Logged pipeline run: {dag_id} - {run_id} - {source} - {status}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to log pipeline run: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def get_article_count_by_source(self) -> Dict:
        """
        Get article count by source and keyword
        """
        try:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            query = """
            SELECT 
                source,
                keyword,
                sentiment_label,
                COUNT(*) as count,
                AVG(sentiment_score) as avg_sentiment
            FROM articles 
            GROUP BY source, keyword, sentiment_label
            ORDER BY source, keyword, sentiment_label;
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Format results
            summary = {}
            for row in results:
                source_key = f"{row['source']}"
                if source_key not in summary:
                    summary[source_key] = {}
                
                keyword_key = row['keyword']
                if keyword_key not in summary[source_key]:
                    summary[source_key][keyword_key] = {}
                
                summary[source_key][keyword_key][row['sentiment_label']] = {
                    'count': row['count'],
                    'avg_sentiment': float(row['avg_sentiment']) if row['avg_sentiment'] else 0.0
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get article statistics: {e}")
            return {}
        finally:
            cursor.close()
    
    def cleanup_old_articles(self, days_old: int = 30) -> int:
        """
        Clean up articles older than specified days
        """
        try:
            cursor = self.connection.cursor()
            
            delete_query = """
            DELETE FROM articles 
            WHERE created_at < NOW() - INTERVAL '%s days';
            """
            
            cursor.execute(delete_query, (days_old,))
            deleted_count = cursor.rowcount
            
            self.connection.commit()
            logger.info(f"Cleaned up {deleted_count} old articles")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup old articles: {e}")
            self.connection.rollback()
            return 0
        finally:
            cursor.close()

    def store_movielens_results(self, results: Dict[str, Any]) -> int:
        """
        Store MovieLens analysis results to database
        
        Args:
            results: Dictionary containing all analysis results
            
        Returns:
            Number of records stored
        """
        try:
            if not self.connection:
                raise Exception("Database connection not established")
            
            cursor = self.connection.cursor()
            stored_count = 0
            
            # Create tables if they don't exist
            self._create_movielens_tables(cursor)
            
            # Store mean age by occupation results
            if 'mean_age_by_occupation' in results:
                for result in results['mean_age_by_occupation']:
                    cursor.execute("""
                        INSERT INTO movielens_mean_age_by_occupation 
                        (occupation, mean_age, user_count, std_dev, min_age, max_age, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (occupation) DO UPDATE SET
                        mean_age = EXCLUDED.mean_age,
                        user_count = EXCLUDED.user_count,
                        std_dev = EXCLUDED.std_dev,
                        min_age = EXCLUDED.min_age,
                        max_age = EXCLUDED.max_age,
                        updated_at = NOW()
                    """, (
                        result['occupation'],
                        result['mean_age'],
                        result['user_count'],
                        result['std_dev'],
                        result['min_age'],
                        result['max_age']
                    ))
                    stored_count += 1
            
            # Store top rated movies results
            if 'top_rated_movies' in results:
                for result in results['top_rated_movies']:
                    cursor.execute("""
                        INSERT INTO movielens_top_rated_movies 
                        (movie_id, title, avg_rating, rating_count, rating_std, release_date, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (movie_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        avg_rating = EXCLUDED.avg_rating,
                        rating_count = EXCLUDED.rating_count,
                        rating_std = EXCLUDED.rating_std,
                        release_date = EXCLUDED.release_date,
                        updated_at = NOW()
                    """, (
                        result['movie_id'],
                        result['title'],
                        result['avg_rating'],
                        result['rating_count'],
                        result['rating_std'],
                        result['release_date']
                    ))
                    stored_count += 1
            
            # Store top genres by occupation and age results
            if 'top_genres_by_occupation_age' in results:
                for result in results['top_genres_by_occupation_age']:
                    for i, genre_data in enumerate(result['top_genres'][:3]):  # Top 3 genres
                        cursor.execute("""
                            INSERT INTO movielens_top_genres_by_occupation_age 
                            (occupation, age_group, genre, avg_rating, rating_count, genre_rank, total_users, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (occupation, age_group, genre) DO UPDATE SET
                            avg_rating = EXCLUDED.avg_rating,
                            rating_count = EXCLUDED.rating_count,
                            genre_rank = EXCLUDED.genre_rank,
                            total_users = EXCLUDED.total_users,
                            updated_at = NOW()
                        """, (
                            result['occupation'],
                            result['age_group'],
                            genre_data['genre'],
                            genre_data['avg_rating'],
                            genre_data['rating_count'],
                            i + 1,  # Rank (1, 2, 3)
                            result['total_users']
                        ))
                        stored_count += 1
            
            # Store similar movies results
            if 'similar_movies' in results:
                for i, result in enumerate(results['similar_movies']):
                    cursor.execute("""
                        INSERT INTO movielens_similar_movies 
                        (target_movie, similar_movie_id, similar_movie_title, similarity_score, 
                         cooccurrence_count, correlation, similarity_rank, release_date, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (target_movie, similar_movie_id) DO UPDATE SET
                        similar_movie_title = EXCLUDED.similar_movie_title,
                        similarity_score = EXCLUDED.similarity_score,
                        cooccurrence_count = EXCLUDED.cooccurrence_count,
                        correlation = EXCLUDED.correlation,
                        similarity_rank = EXCLUDED.similarity_rank,
                        release_date = EXCLUDED.release_date,
                        updated_at = NOW()
                    """, (
                        "Usual Suspects, The (1995)",  # Target movie
                        result['movie_id'],
                        result['title'],
                        result['similarity_score'],
                        result['cooccurrence_count'],
                        result['correlation'],
                        i + 1,  # Rank
                        result['release_date']
                    ))
                    stored_count += 1
            
            # Store pipeline run metadata
            cursor.execute("""
                INSERT INTO movielens_pipeline_runs 
                (run_date, total_results, mean_age_count, top_movies_count, 
                 genre_combinations_count, similar_movies_count, status, created_at)
                VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, 'success', NOW())
            """, (
                stored_count,
                len(results.get('mean_age_by_occupation', [])),
                len(results.get('top_rated_movies', [])),
                len(results.get('top_genres_by_occupation_age', [])),
                len(results.get('similar_movies', []))
            ))
            
            self.connection.commit()
            logger.info(f"Successfully stored {stored_count} MovieLens analysis results")
            
            return stored_count
            
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            logger.error(f"Failed to store MovieLens results: {e}")
            raise

    def _create_movielens_tables(self, cursor):
        """Create MovieLens analysis tables if they don't exist"""
        try:
            # Mean age by occupation table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movielens_mean_age_by_occupation (
                    id SERIAL PRIMARY KEY,
                    occupation VARCHAR(100) UNIQUE NOT NULL,
                    mean_age DECIMAL(5,2) NOT NULL,
                    user_count INTEGER NOT NULL,
                    std_dev DECIMAL(5,2),
                    min_age INTEGER,
                    max_age INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Top rated movies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movielens_top_rated_movies (
                    id SERIAL PRIMARY KEY,
                    movie_id INTEGER UNIQUE NOT NULL,
                    title VARCHAR(500) NOT NULL,
                    avg_rating DECIMAL(4,3) NOT NULL,
                    rating_count INTEGER NOT NULL,
                    rating_std DECIMAL(4,3),
                    release_date VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Top genres by occupation and age table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movielens_top_genres_by_occupation_age (
                    id SERIAL PRIMARY KEY,
                    occupation VARCHAR(100) NOT NULL,
                    age_group VARCHAR(20) NOT NULL,
                    genre VARCHAR(50) NOT NULL,
                    avg_rating DECIMAL(4,3) NOT NULL,
                    rating_count INTEGER NOT NULL,
                    genre_rank INTEGER NOT NULL,
                    total_users INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(occupation, age_group, genre)
                )
            """)
            
            # Similar movies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movielens_similar_movies (
                    id SERIAL PRIMARY KEY,
                    target_movie VARCHAR(500) NOT NULL,
                    similar_movie_id INTEGER NOT NULL,
                    similar_movie_title VARCHAR(500) NOT NULL,
                    similarity_score DECIMAL(6,4) NOT NULL,
                    cooccurrence_count INTEGER NOT NULL,
                    correlation DECIMAL(6,4),
                    similarity_rank INTEGER NOT NULL,
                    release_date VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(target_movie, similar_movie_id)
                )
            """)
            
            # Pipeline runs table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movielens_pipeline_runs (
                    id SERIAL PRIMARY KEY,
                    run_date DATE NOT NULL,
                    total_results INTEGER NOT NULL,
                    mean_age_count INTEGER,
                    top_movies_count INTEGER,
                    genre_combinations_count INTEGER,
                    similar_movies_count INTEGER,
                    status VARCHAR(20) NOT NULL,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for better query performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_movielens_occupation ON movielens_mean_age_by_occupation(occupation)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_movielens_movie_rating ON movielens_top_rated_movies(avg_rating DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_movielens_genre_combo ON movielens_top_genres_by_occupation_age(occupation, age_group)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_movielens_similarity ON movielens_similar_movies(target_movie, similarity_score DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_movielens_run_date ON movielens_pipeline_runs(run_date DESC)")
            
            logger.info("MovieLens database tables created/verified successfully")
            
        except Exception as e:
            logger.error(f"Failed to create MovieLens tables: {e}")
            raise

    def get_movielens_analysis_summary(self) -> Dict[str, Any]:
        """
        Get summary of stored MovieLens analysis results
        
        Returns:
            Dictionary with analysis summary statistics
        """
        try:
            if not self.connection:
                raise Exception("Database connection not established")
            
            cursor = self.connection.cursor()
            summary = {}
            
            # Get occupation analysis summary
            cursor.execute("""
                SELECT COUNT(*) as total_occupations,
                       AVG(mean_age) as overall_avg_age,
                       MAX(mean_age) as highest_avg_age,
                       MIN(mean_age) as lowest_avg_age
                FROM movielens_mean_age_by_occupation
            """)
            result = cursor.fetchone()
            if result:
                summary['occupation_analysis'] = {
                    'total_occupations': result[0],
                    'overall_avg_age': round(float(result[1]) if result[1] else 0, 2),
                    'highest_avg_age': float(result[2]) if result[2] else 0,
                    'lowest_avg_age': float(result[3]) if result[3] else 0
                }
            
            # Get top movies summary
            cursor.execute("""
                SELECT COUNT(*) as total_movies,
                       AVG(avg_rating) as overall_avg_rating,
                       MAX(avg_rating) as highest_rating,
                       MIN(avg_rating) as lowest_rating
                FROM movielens_top_rated_movies
            """)
            result = cursor.fetchone()
            if result:
                summary['movies_analysis'] = {
                    'total_movies': result[0],
                    'overall_avg_rating': round(float(result[1]) if result[1] else 0, 3),
                    'highest_rating': float(result[2]) if result[2] else 0,
                    'lowest_rating': float(result[3]) if result[3] else 0
                }
            
            # Get genre analysis summary
            cursor.execute("""
                SELECT COUNT(DISTINCT CONCAT(occupation, '-', age_group)) as total_combinations,
                       COUNT(DISTINCT occupation) as unique_occupations,
                       COUNT(DISTINCT age_group) as unique_age_groups,
                       COUNT(DISTINCT genre) as unique_genres
                FROM movielens_top_genres_by_occupation_age
            """)
            result = cursor.fetchone()
            if result:
                summary['genre_analysis'] = {
                    'total_combinations': result[0],
                    'unique_occupations': result[1],
                    'unique_age_groups': result[2],
                    'unique_genres': result[3]
                }
            
            # Get similarity analysis summary
            cursor.execute("""
                SELECT COUNT(*) as total_similar_movies,
                       AVG(similarity_score) as avg_similarity,
                       MAX(similarity_score) as max_similarity,
                       MIN(similarity_score) as min_similarity
                FROM movielens_similar_movies
            """)
            result = cursor.fetchone()
            if result:
                summary['similarity_analysis'] = {
                    'total_similar_movies': result[0],
                    'avg_similarity': round(float(result[1]) if result[1] else 0, 4),
                    'max_similarity': float(result[2]) if result[2] else 0,
                    'min_similarity': float(result[3]) if result[3] else 0
                }
            
            # Get latest pipeline run info
            cursor.execute("""
                SELECT run_date, total_results, status, created_at
                FROM movielens_pipeline_runs
                ORDER BY created_at DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            if result:
                summary['latest_run'] = {
                    'run_date': result[0].strftime('%Y-%m-%d') if result[0] else None,
                    'total_results': result[1],
                    'status': result[2],
                    'created_at': result[3].strftime('%Y-%m-%d %H:%M:%S') if result[3] else None
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get MovieLens analysis summary: {e}")
            raise

def test_database_connection():
    """
    Test database connection and basic operations
    """
    db_manager = DatabaseManager()
    
    if not db_manager.connect():
        logger.error("Failed to connect to database")
        return False
    
    # Test table creation
    if not db_manager.create_tables():
        logger.error("Failed to create tables")
        return False
    
    # Test article insertion
    test_articles = [
        {
            'source': 'test',
            'keyword': 'TEST',
            'title': 'Test Article',
            'url': 'https://test.com/test-article',
            'published_date': '2024-01-01',
            'excerpt': 'Test excerpt',
            'full_text': 'Test full text content',
            'sentiment_score': 0.5,
            'sentiment_label': 'positive'
        }
    ]
    
    inserted_count = db_manager.insert_articles(test_articles)
    if inserted_count > 0:
        logger.info("Database test successful")
    else:
        logger.error("Database test failed")
    
    # Get statistics
    stats = db_manager.get_article_count_by_source()
    logger.info(f"Database statistics: {stats}")
    
    db_manager.disconnect()
    return inserted_count > 0

if __name__ == "__main__":
    test_database_connection() 