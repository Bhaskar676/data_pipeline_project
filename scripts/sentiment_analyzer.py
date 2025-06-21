import random
import re
import logging
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Positive and negative word lists for mock sentiment analysis
POSITIVE_WORDS = [
    'strong', 'growth', 'profit', 'success', 'innovative', 'expansion', 'robust', 
    'impressive', 'excellent', 'positive', 'increase', 'boost', 'achievement', 
    'breakthrough', 'optimistic', 'resilient', 'strategic', 'efficient', 'leading',
    'remarkable', 'outstanding', 'promising', 'beneficial', 'advantageous', 'thriving'
]

NEGATIVE_WORDS = [
    'decline', 'loss', 'challenge', 'problem', 'crisis', 'risk', 'concern', 
    'difficulty', 'struggle', 'weakness', 'decrease', 'drop', 'fall', 'issue',
    'uncertainty', 'volatile', 'disappointing', 'concerning', 'challenging', 'weak'
]

NEUTRAL_WORDS = [
    'report', 'announce', 'company', 'business', 'market', 'industry', 'sector',
    'investment', 'strategy', 'development', 'analysis', 'research', 'data',
    'information', 'update', 'news', 'statement', 'launch', 'partnership'
]

def calculate_mock_sentiment_score(text: str) -> Dict[str, Any]:
    """
    Calculate a mock sentiment score based on keyword presence
    Returns a realistic sentiment score between 0 and 1
    """
    if not text:
        return {
            'sentiment_score': 0.5,
            'sentiment_label': 'neutral',
            'confidence': 0.5
        }
    
    # Convert to lowercase for analysis
    text_lower = text.lower()
    
    # Count positive and negative words
    positive_count = sum(1 for word in POSITIVE_WORDS if word in text_lower)
    negative_count = sum(1 for word in NEGATIVE_WORDS if word in text_lower)
    neutral_count = sum(1 for word in NEUTRAL_WORDS if word in text_lower)
    
    # Calculate base sentiment
    total_sentiment_words = positive_count + negative_count
    
    if total_sentiment_words == 0:
        # No sentiment words found, return neutral (0.5)
        base_score = 0.5
    else:
        # Calculate sentiment ratio and convert to 0-1 scale
        sentiment_ratio = (positive_count - negative_count) / total_sentiment_words
        # Convert from -1,1 range to 0,1 range: (x + 1) / 2
        base_score = (sentiment_ratio + 1) / 2
    
    # Add some randomness to make it more realistic
    noise = random.uniform(-0.05, 0.05)
    final_score = max(0.0, min(1.0, base_score + noise))
    
    # Determine sentiment label based on 0-1 scale
    if final_score > 0.6:
        sentiment_label = 'positive'
        confidence = min(0.95, 0.6 + (final_score - 0.5) * 0.8)
    elif final_score < 0.4:
        sentiment_label = 'negative'
        confidence = min(0.95, 0.6 + (0.5 - final_score) * 0.8)
    else:
        sentiment_label = 'neutral'
        confidence = 0.7 + random.uniform(-0.1, 0.1)
    
    return {
        'sentiment_score': round(final_score, 3),
        'sentiment_label': sentiment_label,
        'confidence': round(confidence, 3)
    }

def analyze_article_sentiment(article: Dict) -> Dict:
    """
    Analyze sentiment for a single article
    """
    try:
        # Combine title and content for sentiment analysis
        full_text = f"{article.get('title', '')} {article.get('full_text', '')}"
        
        # Calculate sentiment
        sentiment_result = calculate_mock_sentiment_score(full_text)
        
        # Add sentiment to article
        article_with_sentiment = article.copy()
        article_with_sentiment.update({
            'sentiment_score': sentiment_result['sentiment_score'],
            'sentiment_label': sentiment_result['sentiment_label'],
            'sentiment_confidence': sentiment_result['confidence']
        })
        
        logger.info(f"Analyzed sentiment for '{article.get('title', '')[:50]}...': "
                   f"{sentiment_result['sentiment_label']} ({sentiment_result['sentiment_score']})")
        
        return article_with_sentiment
        
    except Exception as e:
        logger.error(f"Error analyzing sentiment for article: {e}")
        # Return article with neutral sentiment as fallback
        article_with_sentiment = article.copy()
        article_with_sentiment.update({
            'sentiment_score': 0.0,
            'sentiment_label': 'neutral',
            'sentiment_confidence': 0.5
        })
        return article_with_sentiment

def analyze_batch_sentiment(articles: List[Dict]) -> List[Dict]:
    """
    Analyze sentiment for a batch of articles
    """
    logger.info(f"Starting sentiment analysis for {len(articles)} articles...")
    
    analyzed_articles = []
    
    for i, article in enumerate(articles, 1):
        logger.info(f"Processing article {i}/{len(articles)}")
        analyzed_article = analyze_article_sentiment(article)
        analyzed_articles.append(analyzed_article)
    
    # Log summary statistics
    positive_count = len([a for a in analyzed_articles if a['sentiment_label'] == 'positive'])
    negative_count = len([a for a in analyzed_articles if a['sentiment_label'] == 'negative'])
    neutral_count = len([a for a in analyzed_articles if a['sentiment_label'] == 'neutral'])
    
    logger.info(f"Sentiment analysis complete:")
    logger.info(f"  Positive: {positive_count} articles")
    logger.info(f"  Negative: {negative_count} articles")
    logger.info(f"  Neutral:  {neutral_count} articles")
    
    return analyzed_articles

def get_sentiment_summary(articles: List[Dict]) -> Dict:
    """
    Generate a summary of sentiment analysis results
    """
    if not articles:
        return {
            'total_articles': 0,
            'positive_count': 0,
            'negative_count': 0,
            'neutral_count': 0,
            'average_sentiment': 0.0,
            'sentiment_distribution': {}
        }
    
    sentiment_scores = [a.get('sentiment_score', 0.0) for a in articles]
    sentiment_labels = [a.get('sentiment_label', 'neutral') for a in articles]
    
    # Count by label
    positive_count = sentiment_labels.count('positive')
    negative_count = sentiment_labels.count('negative')
    neutral_count = sentiment_labels.count('neutral')
    
    # Calculate average sentiment
    average_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0.0
    
    # Create distribution by source and keyword
    distribution = {}
    for article in articles:
        source = article.get('source', 'unknown')
        keyword = article.get('keyword', 'unknown')
        sentiment = article.get('sentiment_label', 'neutral')
        
        key = f"{source}_{keyword}"
        if key not in distribution:
            distribution[key] = {'positive': 0, 'negative': 0, 'neutral': 0}
        
        distribution[key][sentiment] += 1
    
    return {
        'total_articles': len(articles),
        'positive_count': positive_count,
        'negative_count': negative_count,
        'neutral_count': neutral_count,
        'average_sentiment': round(average_sentiment, 3),
        'sentiment_distribution': distribution
    }

def main():
    """
    Main function for testing sentiment analysis
    """
    # Test with sample articles
    test_articles = [
        {
            'title': 'HDFC Bank Reports Strong Q4 Results',
            'full_text': 'HDFC Bank announced impressive quarterly results with strong growth and robust performance.',
            'source': 'finshots',
            'keyword': 'HDFC'
        },
        {
            'title': 'Tata Motors Faces Production Challenges',
            'full_text': 'Tata Motors is experiencing difficulties due to supply chain issues and declining sales.',
            'source': 'yourstory',
            'keyword': 'Tata Motors'
        },
        {
            'title': 'Market Update on Banking Sector',
            'full_text': 'The banking sector continues to show steady performance with regular updates from major banks.',
            'source': 'finshots',
            'keyword': 'HDFC'
        }
    ]
    
    # Analyze sentiment
    analyzed_articles = analyze_batch_sentiment(test_articles)
    
    # Print results
    for article in analyzed_articles:
        print(f"Title: {article['title']}")
        print(f"Sentiment: {article['sentiment_label']} ({article['sentiment_score']})")
        print(f"Confidence: {article['sentiment_confidence']}")
        print("-" * 50)
    
    # Print summary
    summary = get_sentiment_summary(analyzed_articles)
    print("\nSentiment Summary:")
    print(f"Total articles: {summary['total_articles']}")
    print(f"Positive: {summary['positive_count']}")
    print(f"Negative: {summary['negative_count']}")
    print(f"Neutral: {summary['neutral_count']}")
    print(f"Average sentiment: {summary['average_sentiment']}")

if __name__ == "__main__":
    main() 