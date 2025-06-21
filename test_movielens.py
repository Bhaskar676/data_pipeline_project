#!/usr/bin/env python3
"""
Test script for MovieLens Pipeline components
"""

import sys
import os
sys.path.append('./scripts')

def test_data_loader():
    """Test MovieLens data loading"""
    print("Testing MovieLens Data Loader...")
    
    try:
        from movielens_data_loader import download_and_load_data, get_data
        
        # Download and load data
        data_info = download_and_load_data()
        print(f"✅ Data loaded successfully: {data_info}")
        
        # Get data
        ratings_df, users_df, movies_df, genres_df, movie_genres_df = get_data()
        print(f"✅ Data access successful:")
        print(f"   - Ratings: {len(ratings_df)} records")
        print(f"   - Users: {len(users_df)} records") 
        print(f"   - Movies: {len(movies_df)} records")
        print(f"   - Genres: {len(genres_df)} records")
        print(f"   - Movie-Genres: {len(movie_genres_df)} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Data loader test failed: {e}")
        return False

def test_analyzer():
    """Test MovieLens analysis functions"""
    print("\nTesting MovieLens Analyzer...")
    
    try:
        from movielens_analyzer import (
            analyze_mean_age_by_occupation,
            analyze_top_rated_movies,
            analyze_top_genres_by_occupation_age,
            find_similar_movies
        )
        
        # Test 1: Mean age by occupation
        print("1. Testing mean age by occupation...")
        results1 = analyze_mean_age_by_occupation()
        print(f"   ✅ Found {len(results1)} occupations")
        if results1:
            print(f"   Sample: {results1[0]['occupation']} - {results1[0]['mean_age']} years")
        
        # Test 2: Top rated movies
        print("2. Testing top rated movies...")
        results2 = analyze_top_rated_movies(min_ratings=35, top_n=20)
        print(f"   ✅ Found {len(results2)} top movies")
        if results2:
            print(f"   Top movie: {results2[0]['title']} - {results2[0]['avg_rating']}")
        
        # Test 3: Top genres by occupation and age
        print("3. Testing top genres by occupation and age...")
        results3 = analyze_top_genres_by_occupation_age()
        print(f"   ✅ Found {len(results3)} occupation-age combinations")
        if results3:
            sample = results3[0]
            top_genre = sample['top_genres'][0] if sample['top_genres'] else {'genre': 'None'}
            print(f"   Sample: {sample['occupation']} ({sample['age_group']}) - {top_genre['genre']}")
        
        # Test 4: Similar movies (with lower thresholds for testing)
        print("4. Testing similar movies...")
        results4 = find_similar_movies(
            target_movie="Usual Suspects, The (1995)",
            similarity_threshold=0.8,  # Lower threshold for testing
            cooccurrence_threshold=10,  # Lower threshold for testing
            top_n=10
        )
        print(f"   ✅ Found {len(results4)} similar movies")
        if results4:
            print(f"   Most similar: {results4[0]['title']} - {results4[0]['similarity_score']:.4f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Analyzer test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_pipeline_integration():
    """Test full pipeline integration"""
    print("\nTesting Pipeline Integration...")
    
    try:
        # Test that all components work together
        from movielens_data_loader import download_and_load_data
        from movielens_analyzer import (
            analyze_mean_age_by_occupation,
            analyze_top_rated_movies,
            analyze_top_genres_by_occupation_age,
            find_similar_movies
        )
        
        # Load data first
        data_info = download_and_load_data()
        
        # Run all analyses
        results = {
            'mean_age_by_occupation': analyze_mean_age_by_occupation(),
            'top_rated_movies': analyze_top_rated_movies(),
            'top_genres_by_occupation_age': analyze_top_genres_by_occupation_age(),
            'similar_movies': find_similar_movies("Usual Suspects, The (1995)", 
                                                similarity_threshold=0.8, 
                                                cooccurrence_threshold=10)
        }
        
        # Summary
        total_results = sum(len(v) if isinstance(v, list) else 1 for v in results.values())
        print(f"✅ Pipeline integration successful!")
        print(f"   Total analysis results: {total_results}")
        print(f"   - Mean age analysis: {len(results['mean_age_by_occupation'])} occupations")
        print(f"   - Top movies: {len(results['top_rated_movies'])} movies")
        print(f"   - Genre analysis: {len(results['top_genres_by_occupation_age'])} combinations")
        print(f"   - Similar movies: {len(results['similar_movies'])} recommendations")
        
        return True
        
    except Exception as e:
        print(f"❌ Pipeline integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("MovieLens Pipeline Component Tests")
    print("=" * 60)
    
    tests = [
        test_data_loader,
        test_analyzer,
        test_pipeline_integration
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 60)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! MovieLens pipeline is ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above.")
        return 1

if __name__ == "__main__":
    exit(main()) 