import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Target keywords
KEYWORDS = ["HDFC", "Tata Motors"]

def fetch_finshots_articles():
    """
    Fetch articles from Finshots using their Ghost API
    Returns list of articles matching our keywords
    """
    try:
        # Finshots Ghost API endpoint
        api_url = "https://finshots.in/ghost/api/content/posts/"
        params = {
            'key': '8067c49caa4ce48ca16b4c4445',
            'limit': 10000,
            'fields': 'id,slug,title,excerpt,url,updated_at,visibility',
            'order': 'updated_at DESC'
        }
        
        logger.info("Fetching articles from Finshots API...")
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        all_articles = data.get('posts', [])
        logger.info(f"Retrieved {len(all_articles)} total articles from Finshots")
        
        # Filter articles by keywords - limit to 5 per keyword
        matching_articles = []
        keyword_counts = {keyword: 0 for keyword in KEYWORDS}
        
        for article in all_articles:
            title = article.get('title', '').lower()
            excerpt = article.get('excerpt', '').lower()
            
            # Check if any keyword appears in title or excerpt
            for keyword in KEYWORDS:
                if keyword.lower() in title or keyword.lower() in excerpt:
                    # Check if we already have 5 articles for this keyword
                    if keyword_counts[keyword] >= 5:
                        continue
                        
                    logger.info(f"Found match for '{keyword}': {article.get('title')}")
                    
                    # Fetch full article content
                    full_text = fetch_article_content(article.get('url', ''))
                    
                    matching_articles.append({
                        'source': 'finshots',
                        'keyword': keyword,
                        'title': article.get('title', ''),
                        'url': article.get('url', ''),
                        'published_date': parse_date(article.get('updated_at', '')),
                        'excerpt': article.get('excerpt', ''),
                        'full_text': full_text
                    })
                    keyword_counts[keyword] += 1
                    break
        
        logger.info(f"Found {len(matching_articles)} matching articles")
        return matching_articles
        
    except Exception as e:
        logger.error(f"Error fetching Finshots articles: {e}")
        return []

def fetch_article_content(url):
    """
    Fetch full article content from Finshots article URL
    """
    try:
        if not url:
            return ""
            
        logger.info(f"Fetching content from: {url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract article content from Finshots
        content_selectors = [
            '.post-content',
            'article .content',
            '.article-content',
            'main article'
        ]
        
        for selector in content_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                # Get all paragraph text
                paragraphs = content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                content_parts = []
                
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if len(text) > 20:  # Only substantial text
                        content_parts.append(text)
                
                full_content = ' '.join(content_parts)
                if len(full_content) > 200:
                    logger.info(f"Extracted {len(full_content)} characters of content")
                    return full_content
        
        # Fallback: get all paragraphs
        all_paragraphs = soup.find_all('p')
        content_parts = []
        for p in all_paragraphs:
            text = p.get_text(strip=True)
            if len(text) > 20:
                content_parts.append(text)
        
        full_content = ' '.join(content_parts)
        logger.info(f"Extracted {len(full_content)} characters of content (fallback)")
        return full_content
        
    except Exception as e:
        logger.error(f"Error fetching article content from {url}: {e}")
        return ""

def parse_date(date_string):
    """
    Parse date string to YYYY-MM-DD format
    """
    try:
        if not date_string:
            return None
            
        # Handle different date formats
        if 'T' in date_string:
            # ISO format: 2024-01-15T10:30:00.000Z
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        else:
            # Try to parse other formats
            dt = datetime.strptime(date_string, '%Y-%m-%d')
            return dt.strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"Could not parse date '{date_string}': {e}")
        return None

def clean_text(text):
    """
    Clean and normalize text content
    """
    if not text:
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)]', ' ', text)
    
    # Remove extra spaces again
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def main():
    """
    Main function to extract Finshots articles
    """
    logger.info("Starting Finshots article extraction...")
    
    articles = fetch_finshots_articles()
    
    if not articles:
        logger.warning("No articles found matching keywords")
        return []
    
    # Clean the articles
    cleaned_articles = []
    for article in articles:
        cleaned_article = {
            'source': article['source'],
            'keyword': article['keyword'],
            'title': clean_text(article['title']),
            'url': article['url'],
            'published_date': article['published_date'],
            'excerpt': clean_text(article['excerpt']),
            'full_text': clean_text(article['full_text'])
        }
        cleaned_articles.append(cleaned_article)
    
    logger.info(f"Successfully extracted and cleaned {len(cleaned_articles)} articles")
    return cleaned_articles

if __name__ == "__main__":
    articles = main()
    for article in articles:
        print(f"Title: {article['title']}")
        print(f"Keyword: {article['keyword']}")
        print(f"URL: {article['url']}")
        print(f"Date: {article['published_date']}")
        print(f"Content length: {len(article['full_text'])} characters")
        print("-" * 80) 