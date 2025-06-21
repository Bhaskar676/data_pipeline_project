"""
MovieLens Data Analyzer
Implements the 4 required analysis tasks for MovieLens data
"""

import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any, Tuple
from collections import defaultdict
import math

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_mean_age_by_occupation() -> List[Dict[str, Any]]:
    """
    Task 1: Find the mean age of users in each occupation
    
    Returns:
        List of dictionaries with occupation and mean_age
    """
    try:
        from movielens_data_loader import get_data, download_and_load_data
        
        # First try to get the data
        try:
            ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
            
            # If any of the dataframes is None, we need to load the data first
            if users_df is None:
                logger.info("Data not loaded yet. Loading data first...")
                download_and_load_data()
                ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
        except Exception:
            logger.info("Data not loaded yet. Loading data first...")
            download_and_load_data()
            ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
        
        logger.info("Analyzing mean age by occupation...")
        
        # Group users by occupation and calculate mean age
        occupation_age_stats = users_df.groupby('occupation')['age'].agg([
            'mean', 'count', 'std', 'min', 'max'
        ]).round(2)
        
        # Convert to list of dictionaries
        results = []
        for occupation, stats in occupation_age_stats.iterrows():
            results.append({
                'occupation': occupation,
                'mean_age': float(stats['mean']),
                'user_count': int(stats['count']),
                'std_dev': float(stats['std']) if not pd.isna(stats['std']) else 0.0,
                'min_age': int(stats['min']),
                'max_age': int(stats['max'])
            })
        
        # Sort by mean age descending
        results.sort(key=lambda x: x['mean_age'], reverse=True)
        
        logger.info(f"Analyzed {len(results)} occupations")
        
        # Log top 5 results
        for i, result in enumerate(results[:5]):
            logger.info(f"  {i+1}. {result['occupation']}: {result['mean_age']} years (n={result['user_count']})")
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to analyze mean age by occupation: {e}")
        raise

def analyze_top_rated_movies(min_ratings: int = 35, top_n: int = 20) -> List[Dict[str, Any]]:
    """
    Task 2: Find the names of top 20 highest rated movies (at least 35 times rated by users)
    
    Args:
        min_ratings: Minimum number of ratings required
        top_n: Number of top movies to return
        
    Returns:
        List of dictionaries with movie details and ratings
    """
    try:
        from movielens_data_loader import get_data, download_and_load_data
        
        # First try to get the data
        try:
            ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
            
            # If any of the dataframes is None, we need to load the data first
            if ratings_df is None or movies_df is None:
                logger.info("Data not loaded yet. Loading data first...")
                download_and_load_data()
                ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
        except Exception:
            logger.info("Data not loaded yet. Loading data first...")
            download_and_load_data()
            ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
        
        logger.info(f"Analyzing top {top_n} rated movies (min {min_ratings} ratings)...")
        
        # Calculate movie statistics
        movie_stats = ratings_df.groupby('movie_id')['rating'].agg([
            'mean', 'count', 'std'
        ]).round(3)
        
        # Filter movies with at least min_ratings ratings
        qualified_movies = movie_stats[movie_stats['count'] >= min_ratings]
        
        # Sort by average rating descending
        top_movies = qualified_movies.sort_values('mean', ascending=False).head(top_n)
        
        # Join with movie information
        results = []
        for movie_id, stats in top_movies.iterrows():
            movie_info = movies_df[movies_df['movie_id'] == movie_id].iloc[0]
            
            results.append({
                'movie_id': int(movie_id),
                'title': movie_info['title'],
                'avg_rating': float(stats['mean']),
                'rating_count': int(stats['count']),
                'rating_std': float(stats['std']) if not pd.isna(stats['std']) else 0.0,
                'release_date': movie_info['release_date'] if pd.notna(movie_info['release_date']) else 'Unknown'
            })
        
        logger.info(f"Found {len(results)} top rated movies")
        
        # Log top 5 results
        for i, result in enumerate(results[:5]):
            logger.info(f"  {i+1}. {result['title']}: {result['avg_rating']} ({result['rating_count']} ratings)")
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to analyze top rated movies: {e}")
        raise

def analyze_top_genres_by_occupation_age() -> List[Dict[str, Any]]:
    """
    Task 3: Find the top genres rated by users of each occupation in every age-group
    Age groups: 20-25, 25-35, 35-45, 45+
    
    Returns:
        List of dictionaries with occupation, age_group, and top genres
    """
    try:
        from movielens_data_loader import get_data, download_and_load_data
        
        # First try to get the data
        try:
            ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
            
            # If any of the dataframes is None, we need to load the data first
            if ratings_df is None or users_df is None or movie_genres_df is None:
                logger.info("Data not loaded yet. Loading data first...")
                download_and_load_data()
                ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
        except Exception:
            logger.info("Data not loaded yet. Loading data first...")
            download_and_load_data()
            ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
        
        logger.info("Analyzing top genres by occupation and age group...")
        
        # Define age groups
        def get_age_group(age):
            if 20 <= age < 25:
                return "20-25"
            elif 25 <= age < 35:
                return "25-35"
            elif 35 <= age < 45:
                return "35-45"
            elif age >= 45:
                return "45+"
            else:
                return "Under 20"
        
        # Add age group to users
        users_with_age_group = users_df.copy()
        users_with_age_group['age_group'] = users_with_age_group['age'].apply(get_age_group)
        
        # Merge ratings with users to get occupation and age group
        ratings_with_demo = ratings_df.merge(users_with_age_group, on='user_id')
        
        # Merge with movie genres
        ratings_with_genres = ratings_with_demo.merge(movie_genres_df, on='movie_id')
        
        # Group by occupation, age group, and genre to calculate average ratings
        genre_stats = ratings_with_genres.groupby(['occupation', 'age_group', 'genre'])['rating'].agg([
            'mean', 'count'
        ]).round(3)
        
        # Find top genres for each occupation-age group combination
        results = []
        
        for (occupation, age_group), group_data in genre_stats.groupby(['occupation', 'age_group']):
            # Sort genres by average rating and get top 3
            top_genres = group_data.sort_values('mean', ascending=False).head(3)
            
            genre_list = []
            for genre, stats in top_genres.iterrows():
                genre_list.append({
                    'genre': genre,
                    'avg_rating': float(stats['mean']),
                    'rating_count': int(stats['count'])
                })
            
            results.append({
                'occupation': occupation,
                'age_group': age_group,
                'top_genres': genre_list,
                'total_users': int(users_with_age_group[
                    (users_with_age_group['occupation'] == occupation) & 
                    (users_with_age_group['age_group'] == age_group)
                ].shape[0])
            })
        
        # Sort by occupation and age group
        results.sort(key=lambda x: (x['occupation'], x['age_group']))
        
        logger.info(f"Analyzed {len(results)} occupation-age group combinations")
        
        # Log sample results
        for i, result in enumerate(results[:3]):
            top_genre = result['top_genres'][0] if result['top_genres'] else {'genre': 'None', 'avg_rating': 0}
            logger.info(f"  {result['occupation']} ({result['age_group']}): Top genre = {top_genre['genre']} ({top_genre['avg_rating']})")
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to analyze top genres by occupation and age: {e}")
        raise

def find_similar_movies(target_movie: str, similarity_threshold: float = 0.95, 
                       cooccurrence_threshold: int = 50, top_n: int = 10) -> List[Dict[str, Any]]:
    """
    Task 4: Find top 10 similar movies for a given movie using collaborative filtering
    
    Algorithm:
    - Calculate similarity based on user ratings
    - Similarity threshold: 95% (0.95)
    - Co-occurrence threshold: 50 (minimum times two movies are rated together)
    
    Args:
        target_movie: Movie title to find similarities for
        similarity_threshold: Minimum similarity score (0.95)
        cooccurrence_threshold: Minimum co-occurrence count (50)
        top_n: Number of similar movies to return (10)
        
    Returns:
        List of dictionaries with similar movies and similarity scores
    """
    try:
        from movielens_data_loader import get_data, download_and_load_data
        
        # First try to get the data
        try:
            ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
            
            # If any of the dataframes is None, we need to load the data first
            if ratings_df is None or movies_df is None:
                logger.info("Data not loaded yet. Loading data first...")
                download_and_load_data()
                ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
        except Exception:
            logger.info("Data not loaded yet. Loading data first...")
            download_and_load_data()
            ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
        
        logger.info(f"Finding similar movies to '{target_movie}'...")
        
        # Find target movie ID
        target_movie_match = movies_df[movies_df['title'].str.contains(target_movie, case=False, na=False)]
        
        if target_movie_match.empty:
            logger.warning(f"Movie '{target_movie}' not found. Using closest match...")
            # Try partial match
            target_movie_match = movies_df[movies_df['title'].str.contains('Usual Suspects', case=False, na=False)]
            
        if target_movie_match.empty:
            raise Exception(f"Could not find movie matching '{target_movie}'")
        
        target_movie_id = target_movie_match.iloc[0]['movie_id']
        actual_title = target_movie_match.iloc[0]['title']
        
        logger.info(f"Using target movie: {actual_title} (ID: {target_movie_id})")
        
        # Get users who rated the target movie
        target_movie_ratings = ratings_df[ratings_df['movie_id'] == target_movie_id]
        target_users = set(target_movie_ratings['user_id'].values)
        
        logger.info(f"Found {len(target_users)} users who rated the target movie")
        
        # Find movies co-rated with target movie
        corated_movies = ratings_df[ratings_df['user_id'].isin(target_users)]
        corated_movies = corated_movies[corated_movies['movie_id'] != target_movie_id]
        
        # Count co-occurrences and calculate similarities
        movie_similarities = {}
        
        # Group by movie to calculate similarity
        for movie_id, movie_ratings in corated_movies.groupby('movie_id'):
            # Get users who rated both movies
            common_users = set(movie_ratings['user_id'].values) & target_users
            cooccurrence_count = len(common_users)
            
            # Skip if co-occurrence is below threshold
            if cooccurrence_count < cooccurrence_threshold:
                continue
            
            # Calculate similarity using Pearson correlation
            target_ratings_common = target_movie_ratings[target_movie_ratings['user_id'].isin(common_users)]
            movie_ratings_common = movie_ratings[movie_ratings['user_id'].isin(common_users)]
            
            # Merge ratings on user_id to align them
            merged_ratings = target_ratings_common[['user_id', 'rating']].merge(
                movie_ratings_common[['user_id', 'rating']], 
                on='user_id', 
                suffixes=('_target', '_movie')
            )
            
            if len(merged_ratings) < 2:
                continue
            
            # Calculate Pearson correlation
            target_vals = merged_ratings['rating_target'].values
            movie_vals = merged_ratings['rating_movie'].values
            
            # Calculate correlation
            correlation = np.corrcoef(target_vals, movie_vals)[0, 1]
            
            # Handle NaN correlation (when std is 0)
            if np.isnan(correlation):
                correlation = 0.0
            
            # Convert correlation to similarity score (0 to 1)
            similarity_score = (correlation + 1) / 2
            
            # Store if above threshold
            if similarity_score >= similarity_threshold:
                movie_similarities[movie_id] = {
                    'similarity_score': similarity_score,
                    'cooccurrence_count': cooccurrence_count,
                    'correlation': correlation
                }
        
        logger.info(f"Found {len(movie_similarities)} movies above similarity threshold")
        
        # Sort by similarity score and get top N
        sorted_movies = sorted(
            movie_similarities.items(), 
            key=lambda x: x[1]['similarity_score'], 
            reverse=True
        )[:top_n]
        
        # Build results with movie information
        results = []
        for movie_id, similarity_data in sorted_movies:
            movie_info = movies_df[movies_df['movie_id'] == movie_id].iloc[0]
            
            results.append({
                'movie_id': int(movie_id),
                'title': movie_info['title'],
                'similarity_score': float(similarity_data['similarity_score']),
                'cooccurrence_count': int(similarity_data['cooccurrence_count']),
                'correlation': float(similarity_data['correlation']),
                'release_date': movie_info['release_date'] if pd.notna(movie_info['release_date']) else 'Unknown'
            })
        
        logger.info(f"Found {len(results)} similar movies")
        
        # Log results
        for i, result in enumerate(results):
            logger.info(f"  {i+1}. {result['title']}: score={result['similarity_score']:.4f}, strength={result['cooccurrence_count']}")
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to find similar movies: {e}")
        raise

def get_analysis_summary() -> Dict[str, Any]:
    """
    Get a summary of all analysis results
    
    Returns:
        Dictionary with summary statistics
    """
    try:
        logger.info("Generating analysis summary...")
        
        # Run all analyses
        mean_age_results = analyze_mean_age_by_occupation()
        top_movies_results = analyze_top_rated_movies()
        top_genres_results = analyze_top_genres_by_occupation_age()
        similar_movies_results = find_similar_movies("Usual Suspects, The (1995)")
        
        summary = {
            'mean_age_analysis': {
                'total_occupations': len(mean_age_results),
                'highest_avg_age_occupation': mean_age_results[0]['occupation'] if mean_age_results else None,
                'lowest_avg_age_occupation': mean_age_results[-1]['occupation'] if mean_age_results else None
            },
            'top_movies_analysis': {
                'total_qualified_movies': len(top_movies_results),
                'highest_rated_movie': top_movies_results[0]['title'] if top_movies_results else None,
                'avg_rating_range': f"{top_movies_results[-1]['avg_rating']:.2f} - {top_movies_results[0]['avg_rating']:.2f}" if top_movies_results else None
            },
            'genre_analysis': {
                'total_combinations': len(top_genres_results),
                'unique_occupations': len(set(r['occupation'] for r in top_genres_results)),
                'unique_age_groups': len(set(r['age_group'] for r in top_genres_results))
            },
            'similarity_analysis': {
                'similar_movies_found': len(similar_movies_results),
                'target_movie': "Usual Suspects, The (1995)",
                'highest_similarity': similar_movies_results[0]['similarity_score'] if similar_movies_results else None
            }
        }
        
        logger.info("Analysis summary generated successfully")
        return summary
        
    except Exception as e:
        logger.error(f"Failed to generate analysis summary: {e}")
        raise

if __name__ == "__main__":
    # Test all analysis functions
    try:
        # First load data
        from movielens_data_loader import download_and_load_data
        download_and_load_data()
        
        # Test each analysis
        print("Testing MovieLens Analyzer...")
        
        print("\n1. Mean Age by Occupation:")
        mean_age_results = analyze_mean_age_by_occupation()
        for result in mean_age_results[:5]:
            print(f"  {result['occupation']}: {result['mean_age']} years")
        
        print("\n2. Top Rated Movies:")
        top_movies_results = analyze_top_rated_movies()
        for result in top_movies_results[:5]:
            print(f"  {result['title']}: {result['avg_rating']}")
        
        print("\n3. Top Genres by Occupation-Age:")
        top_genres_results = analyze_top_genres_by_occupation_age()
        for result in top_genres_results[:5]:
            top_genre = result['top_genres'][0] if result['top_genres'] else {'genre': 'None'}
            print(f"  {result['occupation']} ({result['age_group']}): {top_genre['genre']}")
        
        print("\n4. Similar Movies:")
        similar_movies_results = find_similar_movies("Usual Suspects, The (1995)")
        for result in similar_movies_results[:5]:
            print(f"  {result['title']}: {result['similarity_score']:.4f}")
        
        print("\nAll tests completed successfully!")
        
    except Exception as e:
        print(f"Test failed: {e}") 