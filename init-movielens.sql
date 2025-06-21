-- MovieLens Database Initialization Script
-- Creates tables for storing MovieLens analysis results

-- Create database if it doesn't exist (handled by docker-compose)

-- Mean age by occupation table
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
);

-- Top rated movies table
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
);

-- Top genres by occupation and age table
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
);

-- Similar movies table
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
);

-- Pipeline runs table
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
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_movielens_occupation ON movielens_mean_age_by_occupation(occupation);
CREATE INDEX IF NOT EXISTS idx_movielens_movie_rating ON movielens_top_rated_movies(avg_rating DESC);
CREATE INDEX IF NOT EXISTS idx_movielens_genre_combo ON movielens_top_genres_by_occupation_age(occupation, age_group);
CREATE INDEX IF NOT EXISTS idx_movielens_similarity ON movielens_similar_movies(target_movie, similarity_score DESC);
CREATE INDEX IF NOT EXISTS idx_movielens_run_date ON movielens_pipeline_runs(run_date DESC);

-- Insert sample data or initial configuration if needed
-- (This would be populated by the pipeline runs)

COMMENT ON TABLE movielens_mean_age_by_occupation IS 'Stores mean age analysis results by occupation';
COMMENT ON TABLE movielens_top_rated_movies IS 'Stores top 20 highest rated movies with minimum rating threshold';
COMMENT ON TABLE movielens_top_genres_by_occupation_age IS 'Stores top genres by occupation and age group combinations';
COMMENT ON TABLE movielens_similar_movies IS 'Stores similar movie recommendations using collaborative filtering';
COMMENT ON TABLE movielens_pipeline_runs IS 'Tracks MovieLens pipeline execution history and statistics'; 