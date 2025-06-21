"""
MovieLens Data Loader
Loads and processes the local MovieLens 100k dataset
"""

import os
import pandas as pd
import numpy as np
import logging
from typing import Dict, Tuple, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MovieLensDataLoader:
    def __init__(self, data_dir: str = "/opt/airflow/data/ml-100k"):
        self.data_dir = data_dir
        
        # Data file paths (using container data directory)
        self.ratings_file = os.path.join(data_dir, "u.data")
        self.users_file = os.path.join(data_dir, "u.user")
        self.movies_file = os.path.join(data_dir, "u.item")
        self.genres_file = os.path.join(data_dir, "u.genre")
        self.info_file = os.path.join(data_dir, "u.info")
        self.occupation_file = os.path.join(data_dir, "u.occupation")
        
        # Verify data directory exists
        if not os.path.exists(data_dir):
            raise Exception(f"Data directory not found: {data_dir}. Please ensure MovieLens data is extracted to {data_dir}")
        
        # Verify required files exist
        required_files = [self.ratings_file, self.users_file, self.movies_file, self.genres_file]
        for file_path in required_files:
            if not os.path.exists(file_path):
                raise Exception(f"Required data file not found: {file_path}")
    
    def load_ratings(self) -> pd.DataFrame:
        """
        Load ratings data (u.data)
        Format: user id | item id | rating | timestamp (tab separated)
        """
        try:
            logger.info("Loading ratings data...")
            
            # Column names for ratings data
            ratings_columns = ['user_id', 'movie_id', 'rating', 'timestamp']
            
            # Load ratings data (tab separated)
            ratings_df = pd.read_csv(
                self.ratings_file,
                sep='\t',
                names=ratings_columns,
                engine='python'
            )
            
            logger.info(f"Loaded {len(ratings_df)} ratings")
            return ratings_df
            
        except Exception as e:
            logger.error(f"Failed to load ratings data: {e}")
            raise
    
    def load_users(self) -> pd.DataFrame:
        """
        Load user demographics data (u.user)
        Format: user id | age | gender | occupation | zip code (pipe separated)
        """
        try:
            logger.info("Loading users data...")
            
            # Column names for user data
            user_columns = ['user_id', 'age', 'gender', 'occupation', 'zip_code']
            
            # Load user data (pipe separated)
            users_df = pd.read_csv(
                self.users_file,
                sep='|',
                names=user_columns,
                engine='python'
            )
            
            logger.info(f"Loaded {len(users_df)} users")
            return users_df
            
        except Exception as e:
            logger.error(f"Failed to load users data: {e}")
            raise
    
    def load_movies(self) -> pd.DataFrame:
        """
        Load movie information data (u.item)
        Format: movie id | movie title | release date | video release date |
                IMDb URL | unknown | Action | Adventure | Animation |
                Children's | Comedy | Crime | Documentary | Drama | Fantasy |
                Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
                Thriller | War | Western (pipe separated)
        """
        try:
            logger.info("Loading movies data...")
            
            # Column names for movie data (based on README)
            movie_columns = [
                'movie_id', 'title', 'release_date', 'video_release_date', 'imdb_url',
                'unknown', 'action', 'adventure', 'animation', 'childrens', 'comedy',
                'crime', 'documentary', 'drama', 'fantasy', 'film_noir', 'horror',
                'musical', 'mystery', 'romance', 'sci_fi', 'thriller', 'war', 'western'
            ]
            
            # Load movie data with proper encoding (pipe separated)
            movies_df = pd.read_csv(
                self.movies_file,
                sep='|',
                names=movie_columns,
                engine='python',
                encoding='latin-1'  # Handle special characters in movie titles
            )
            
            logger.info(f"Loaded {len(movies_df)} movies")
            return movies_df
            
        except Exception as e:
            logger.error(f"Failed to load movies data: {e}")
            raise
    
    def load_genres(self) -> pd.DataFrame:
        """
        Load genre information data (u.genre)
        Format: genre | genre_id (pipe separated)
        """
        try:
            logger.info("Loading genres data...")
            
            # Column names for genre data
            genre_columns = ['genre', 'genre_id']
            
            # Load genre data (pipe separated)
            genres_df = pd.read_csv(
                self.genres_file,
                sep='|',
                names=genre_columns,
                engine='python'
            )
            
            logger.info(f"Loaded {len(genres_df)} genres")
            return genres_df
            
        except Exception as e:
            logger.error(f"Failed to load genres data: {e}")
            raise
    
    def load_occupations(self) -> pd.DataFrame:
        """Load occupation list (u.occupation)"""
        try:
            logger.info("Loading occupations data...")
            
            # Load occupation data (one occupation per line)
            with open(self.occupation_file, 'r') as f:
                occupations = [line.strip() for line in f.readlines()]
            
            occupations_df = pd.DataFrame({
                'occupation': occupations,
                'occupation_id': range(len(occupations))
            })
            
            logger.info(f"Loaded {len(occupations_df)} occupations")
            return occupations_df
            
        except Exception as e:
            logger.error(f"Failed to load occupations data: {e}")
            raise
    
    def get_movie_genres(self, movies_df: pd.DataFrame, genres_df: pd.DataFrame) -> pd.DataFrame:
        """Extract movie-genre relationships from movies dataframe"""
        try:
            logger.info("Processing movie-genre relationships...")
            
            # Genre columns (binary indicators) - based on README format
            genre_columns = [
                'unknown', 'action', 'adventure', 'animation', 'childrens', 'comedy',
                'crime', 'documentary', 'drama', 'fantasy', 'film_noir', 'horror',
                'musical', 'mystery', 'romance', 'sci_fi', 'thriller', 'war', 'western'
            ]
            
            # Create movie-genre mapping
            movie_genres = []
            
            for _, movie in movies_df.iterrows():
                movie_id = movie['movie_id']
                title = movie['title']
                
                # Find genres for this movie
                for genre_col in genre_columns:
                    if movie[genre_col] == 1:
                        # Map column name to actual genre name
                        genre_name = genre_col.replace('_', '-').title()
                        
                        # Special cases for proper formatting
                        if genre_name == 'Sci-Fi':
                            genre_name = 'Sci-Fi'
                        elif genre_name == 'Film-Noir':
                            genre_name = 'Film-Noir'
                        elif genre_name == 'Childrens':
                            genre_name = "Children's"
                        else:
                            genre_name = genre_name.replace('-', ' ')
                            
                        movie_genres.append({
                            'movie_id': movie_id,
                            'title': title,
                            'genre': genre_name
                        })
            
            movie_genres_df = pd.DataFrame(movie_genres)
            logger.info(f"Created {len(movie_genres_df)} movie-genre relationships")
            
            return movie_genres_df
            
        except Exception as e:
            logger.error(f"Failed to process movie-genre relationships: {e}")
            raise
    
    def get_dataset_info(self) -> Dict[str, Any]:
        """Get basic dataset information"""
        try:
            # Read u.info file if it exists
            info = {}
            if os.path.exists(self.info_file):
                with open(self.info_file, 'r') as f:
                    lines = f.readlines()
                    for line in lines:
                        parts = line.strip().split()
                        if len(parts) >= 2:
                            if 'users' in line.lower():
                                info['users'] = int(parts[0])
                            elif 'items' in line.lower():
                                info['items'] = int(parts[0])
                            elif 'ratings' in line.lower():
                                info['ratings'] = int(parts[0])
            
            return info
            
        except Exception as e:
            logger.warning(f"Could not read dataset info: {e}")
            return {}

def download_and_load_data() -> Dict[str, Any]:
    """
    Main function to load all MovieLens data from local files
    Returns dictionary with data info and statistics
    """
    try:
        # Initialize data loader
        loader = MovieLensDataLoader()
        
        # Load all data
        ratings_df = loader.load_ratings()
        users_df = loader.load_users()
        movies_df = loader.load_movies()
        genres_df = loader.load_genres()
        occupations_df = loader.load_occupations()
        movie_genres_df = loader.get_movie_genres(movies_df, genres_df)
        
        # Store data globally for other modules to access
        global RATINGS_DF, USERS_DF, MOVIES_DF, GENRES_DF, OCCUPATIONS_DF, MOVIE_GENRES_DF
        RATINGS_DF = ratings_df
        USERS_DF = users_df
        MOVIES_DF = movies_df
        GENRES_DF = genres_df
        OCCUPATIONS_DF = occupations_df
        MOVIE_GENRES_DF = movie_genres_df
        
        # Get dataset info
        dataset_info = loader.get_dataset_info()
        
        # Calculate statistics
        data_info = {
            'total_ratings': len(ratings_df),
            'total_users': len(users_df),
            'total_movies': len(movies_df),
            'total_genres': len(genres_df),
            'total_occupations': len(occupations_df),
            'unique_users': ratings_df['user_id'].nunique(),
            'unique_movies': ratings_df['movie_id'].nunique(),
            'rating_range': f"{ratings_df['rating'].min()}-{ratings_df['rating'].max()}",
            'avg_rating': round(ratings_df['rating'].mean(), 2),
            'sparsity': round(1 - len(ratings_df) / (len(users_df) * len(movies_df)), 4),
            'dataset_info': dataset_info
        }
        
        logger.info(f"Successfully loaded MovieLens dataset: {data_info}")
        return data_info
        
    except Exception as e:
        logger.error(f"Failed to load MovieLens data: {e}")
        raise

def get_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Get loaded data (must call download_and_load_data() first)
    Returns: ratings_df, users_df, movies_df, genres_df, movie_genres_df
    """
    try:
        return RATINGS_DF, USERS_DF, MOVIES_DF, GENRES_DF, MOVIE_GENRES_DF
    except NameError:
        raise Exception("Data not loaded. Call download_and_load_data() first.")

# Global variables to store loaded data
RATINGS_DF = None
USERS_DF = None
MOVIES_DF = None
GENRES_DF = None
OCCUPATIONS_DF = None
MOVIE_GENRES_DF = None

if __name__ == "__main__":
    # Test the data loader
    data_info = download_and_load_data()
    print("Data loaded successfully:")
    for key, value in data_info.items():
        print(f"  {key}: {value}")
    
    # Show sample data
    ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
    
    print(f"\nSample ratings data:")
    print(ratings_df.head())
    
    print(f"\nSample users data:")
    print(users_df.head())
    
    print(f"\nSample movies data:")
    print(movies_df.head())
    
    print(f"\nGenres:")
    print(genres_df) 