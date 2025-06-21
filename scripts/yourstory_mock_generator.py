import random
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Target keywords
KEYWORDS = ["HDFC", "Tata Motors"]

# Mock article templates for realistic content
HDFC_ARTICLES = [
    {
        'title': 'HDFC Bank Reports Strong Q4 Results Amid Digital Transformation Push',
        'excerpt': 'HDFC Bank has announced impressive quarterly results, driven by robust digital banking adoption and strategic partnerships in the fintech space.',
        'content_template': '''HDFC Bank, India's largest private sector bank, has reported strong financial performance in the latest quarter, showcasing resilience in a challenging economic environment. The bank's digital transformation initiatives have significantly contributed to customer acquisition and retention.

Key highlights from the quarterly results include a substantial increase in net profit, driven by improved net interest margins and controlled operational expenses. The bank's digital banking platform has witnessed unprecedented growth, with mobile banking transactions increasing by over 40% year-on-year.

HDFC Bank's strategic focus on technology adoption has enabled seamless customer experiences across all touchpoints. The bank has invested heavily in artificial intelligence and machine learning capabilities to enhance risk management and customer service delivery.

The management remains optimistic about future growth prospects, citing strong fundamentals and a robust balance sheet. HDFC Bank continues to strengthen its market position through innovative product offerings and strategic partnerships with fintech companies.

Industry analysts have praised HDFC Bank's consistent performance and strategic vision, positioning it as a leader in India's evolving banking landscape.'''
    },
    {
        'title': 'HDFC Life Insurance Launches New Digital-First Investment Products',
        'excerpt': 'HDFC Life Insurance has unveiled a suite of digital-first investment products targeting millennials and Gen-Z customers seeking flexible financial solutions.',
        'content_template': '''HDFC Life Insurance, one of India's leading life insurance companies, has announced the launch of innovative digital-first investment products designed to meet the evolving needs of younger demographics.

The new product portfolio includes unit-linked insurance plans (ULIPs) with enhanced flexibility and transparency. These products leverage advanced technology to provide real-time portfolio tracking and seamless policy management through mobile applications.

HDFC Life's digital transformation strategy focuses on simplifying the insurance buying process and improving customer engagement. The company has integrated artificial intelligence and data analytics to offer personalized product recommendations and risk assessment.

The insurance sector has witnessed significant changes in customer preferences, with increasing demand for digital-native solutions. HDFC Life's latest offerings address these market trends while maintaining robust risk management practices.

Industry experts believe that HDFC Life's digital innovation will set new benchmarks in the insurance sector, potentially influencing competitive strategies across the industry.'''
    },
    {
        'title': 'HDFC Asset Management Expands ESG Investment Portfolio',
        'excerpt': 'HDFC Asset Management Company has significantly expanded its ESG-focused investment offerings, responding to growing institutional and retail investor demand.',
        'content_template': '''HDFC Asset Management Company (HDFC AMC) has strengthened its commitment to sustainable investing by launching new Environmental, Social, and Governance (ESG) focused mutual fund schemes.

The expanded ESG portfolio includes equity and debt funds that integrate sustainability criteria into investment decision-making processes. HDFC AMC has developed proprietary ESG scoring methodologies to evaluate investment opportunities across various sectors.

Sustainable investing has gained significant traction in India, with institutional investors increasingly incorporating ESG factors into their investment strategies. HDFC AMC's initiative aligns with global trends toward responsible investing and climate-conscious financial products.

The fund house has partnered with leading ESG research providers to enhance its investment analysis capabilities. This collaboration enables comprehensive evaluation of companies based on environmental impact, social responsibility, and governance practices.

Market participants expect HDFC AMC's ESG focus to attract significant investor interest, particularly from institutional clients with sustainability mandates and socially conscious retail investors.'''
    },
    {
        'title': 'HDFC Bank Introduces AI-Powered Customer Service Platform',
        'excerpt': 'HDFC Bank has launched an advanced AI-powered customer service platform to enhance customer experience and streamline banking operations.',
        'content_template': '''HDFC Bank has unveiled a cutting-edge artificial intelligence platform designed to revolutionize customer service delivery and operational efficiency across all banking channels.

The new AI system integrates natural language processing and machine learning algorithms to provide personalized customer interactions and predictive service capabilities. HDFC Bank's investment in AI technology demonstrates its commitment to digital innovation and customer-centric solutions.

The platform enables 24/7 customer support with intelligent chatbots capable of handling complex queries and transactions. HDFC Bank has trained the AI system on millions of customer interactions to ensure accurate and relevant responses.

Early testing results show significant improvements in customer satisfaction scores and reduced response times. HDFC Bank plans to expand the AI platform's capabilities to include predictive analytics for personalized financial advice and risk assessment.

Technology industry experts view HDFC Bank's AI initiative as a benchmark for digital transformation in the Indian banking sector, potentially influencing competitive strategies across the industry.'''
    },
    {
        'title': 'HDFC Bank Partners with Startups for Financial Innovation Hub',
        'excerpt': 'HDFC Bank has established a financial innovation hub in collaboration with leading fintech startups to accelerate digital banking solutions.',
        'content_template': '''HDFC Bank has announced the creation of a dedicated financial innovation hub, partnering with promising fintech startups to develop next-generation banking solutions and services.

The innovation hub will serve as a collaborative platform for experimenting with emerging technologies including blockchain, cryptocurrency, and advanced analytics. HDFC Bank aims to accelerate the development of innovative financial products through these strategic partnerships.

Selected fintech startups will receive access to HDFC Bank's extensive customer base and infrastructure for testing and scaling their solutions. The bank has committed significant resources to support startup development and technology integration.

The initiative reflects HDFC Bank's strategy to maintain its competitive edge in the rapidly evolving financial services landscape. The bank recognizes the importance of startup partnerships in driving innovation and meeting changing customer expectations.

Financial industry observers expect HDFC Bank's innovation hub to generate breakthrough solutions that could reshape digital banking experiences and establish new industry standards.'''
    }
]

TATA_MOTORS_ARTICLES = [
    {
        'title': 'Tata Motors Accelerates Electric Vehicle Strategy with New Manufacturing Hub',
        'excerpt': 'Tata Motors has announced plans for a dedicated electric vehicle manufacturing facility, reinforcing its commitment to sustainable mobility solutions.',
        'content_template': '''Tata Motors, India's leading automotive manufacturer, has unveiled ambitious plans to establish a state-of-the-art electric vehicle manufacturing hub, marking a significant milestone in the company's electrification journey.

The new facility will focus exclusively on producing electric passenger vehicles and commercial vehicles, leveraging advanced manufacturing technologies and sustainable production processes. Tata Motors aims to achieve carbon neutrality in its operations through this strategic initiative.

The electric vehicle market in India has witnessed remarkable growth, driven by government incentives and increasing consumer awareness about environmental sustainability. Tata Motors has emerged as a pioneer in this space with successful models like the Nexon EV and Tigor EV.

The company's electric vehicle strategy encompasses the entire ecosystem, including charging infrastructure development and battery technology advancement. Tata Motors has formed strategic partnerships with leading technology companies to accelerate innovation in electric mobility.

Industry analysts view Tata Motors' electric vehicle expansion as a transformative step that could reshape India's automotive landscape and contribute significantly to the country's clean energy transition goals.'''
    },
    {
        'title': 'Tata Motors Partners with Tech Giants for Autonomous Vehicle Development',
        'excerpt': 'Tata Motors has entered into strategic partnerships with global technology leaders to develop autonomous vehicle capabilities for commercial applications.',
        'content_template': '''Tata Motors has announced groundbreaking partnerships with international technology companies to develop autonomous vehicle solutions, positioning itself at the forefront of automotive innovation.

The collaboration focuses on creating advanced driver assistance systems (ADAS) and autonomous driving technologies specifically tailored for Indian road conditions and commercial vehicle applications. Tata Motors aims to integrate these technologies across its commercial vehicle portfolio.

Autonomous vehicle technology represents the next frontier in automotive innovation, with potential applications in logistics, public transportation, and last-mile delivery services. Tata Motors' strategic approach emphasizes practical implementation and real-world testing.

The company has established dedicated research and development centers to accelerate autonomous vehicle development. These facilities will serve as testing grounds for various autonomous driving scenarios and safety validation processes.

Transportation industry experts believe that Tata Motors' autonomous vehicle initiative could revolutionize commercial transportation in India, potentially improving road safety and operational efficiency across multiple sectors.'''
    },
    {
        'title': 'Tata Motors Reports Record Sales Growth in Commercial Vehicle Segment',
        'excerpt': 'Tata Motors has achieved unprecedented sales growth in the commercial vehicle segment, driven by infrastructure development and economic recovery.',
        'content_template': '''Tata Motors has reported exceptional performance in the commercial vehicle segment, with sales figures reaching record highs amid India's economic recovery and infrastructure development boom.

The company's commercial vehicle division has benefited from increased demand for logistics and transportation services, driven by e-commerce growth and government infrastructure projects. Tata Motors has strategically positioned itself to capitalize on these market opportunities.

Key growth drivers include the introduction of fuel-efficient vehicle models, enhanced after-sales service networks, and competitive financing solutions. Tata Motors has invested significantly in product development to meet evolving customer requirements.

The commercial vehicle market has shown remarkable resilience, with Tata Motors maintaining its leadership position through innovative product offerings and customer-centric strategies. The company's market share has strengthened across various commercial vehicle categories.

Automotive industry analysts predict sustained growth for Tata Motors' commercial vehicle business, citing strong fundamentals and favorable market conditions that support continued expansion and profitability.'''
    },
    {
        'title': 'Tata Motors Expands Global Footprint with New International Partnerships',
        'excerpt': 'Tata Motors has formed strategic alliances with international automotive companies to expand its global presence and technology capabilities.',
        'content_template': '''Tata Motors has announced a series of strategic partnerships with leading international automotive manufacturers, marking a significant expansion of its global operations and technological capabilities.

The partnerships encompass joint ventures in key markets, technology sharing agreements, and collaborative research and development initiatives. Tata Motors aims to leverage these alliances to accelerate its international growth strategy and enhance product competitiveness.

Key focus areas include electric vehicle technology transfer, advanced manufacturing processes, and market entry strategies for emerging economies. Tata Motors has identified several high-growth markets where these partnerships will enable rapid expansion and market penetration.

The company's international expansion strategy emphasizes local partnerships and technology localization to ensure products meet regional requirements and preferences. Tata Motors has committed significant investment to support these international initiatives.

Global automotive industry experts view Tata Motors' partnership strategy as a well-executed approach to international expansion, potentially establishing the company as a major player in global automotive markets.'''
    },
    {
        'title': 'Tata Motors Launches Advanced Safety Features Across Vehicle Portfolio',
        'excerpt': 'Tata Motors has introduced comprehensive safety technology upgrades across its entire vehicle lineup, reinforcing its commitment to customer safety.',
        'content_template': '''Tata Motors has unveiled a comprehensive safety technology upgrade program, introducing advanced safety features across its complete range of passenger and commercial vehicles.

The safety enhancement initiative includes collision avoidance systems, advanced airbag configurations, and intelligent driver assistance technologies. Tata Motors has invested heavily in safety research and development to meet international safety standards and customer expectations.

The company's safety-first approach reflects growing consumer awareness about vehicle safety and regulatory requirements for enhanced safety features. Tata Motors has collaborated with leading safety technology providers to integrate cutting-edge solutions into its vehicles.

Comprehensive testing and validation processes ensure that all safety features meet rigorous performance standards across various driving conditions and scenarios. Tata Motors has established dedicated safety testing facilities to support continuous improvement and innovation.

Automotive safety experts have commended Tata Motors' proactive approach to safety technology integration, positioning the company as a leader in automotive safety innovation within the Indian market.'''
    }
]

def generate_mock_yourstory_articles(num_articles_per_keyword=5):
    """
    Generate mock YourStory articles with realistic content
    """
    logger.info("Generating mock YourStory articles...")
    
    mock_articles = []
    
    # Generate HDFC articles
    for i in range(min(num_articles_per_keyword, len(HDFC_ARTICLES))):
        article_template = HDFC_ARTICLES[i]
        
        # Generate realistic date (last 30 days)
        days_ago = random.randint(1, 30)
        pub_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        # Create mock URL
        title_slug = article_template['title'].lower().replace(' ', '-').replace(',', '').replace(':', '')
        mock_url = f"https://yourstory.com/2024/06/{title_slug}"
        
        mock_article = {
            'source': 'yourstory',
            'keyword': 'HDFC',
            'title': article_template['title'],
            'url': mock_url,
            'published_date': pub_date,
            'excerpt': article_template['excerpt'],
            'full_text': article_template['content_template']
        }
        
        mock_articles.append(mock_article)
        logger.info(f"Generated HDFC article: {article_template['title'][:50]}...")
    
    # Generate Tata Motors articles
    for i in range(min(num_articles_per_keyword, len(TATA_MOTORS_ARTICLES))):
        article_template = TATA_MOTORS_ARTICLES[i]
        
        # Generate realistic date (last 30 days)
        days_ago = random.randint(1, 30)
        pub_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        # Create mock URL
        title_slug = article_template['title'].lower().replace(' ', '-').replace(',', '').replace(':', '')
        mock_url = f"https://yourstory.com/2024/06/{title_slug}"
        
        mock_article = {
            'source': 'yourstory',
            'keyword': 'Tata Motors',
            'title': article_template['title'],
            'url': mock_url,
            'published_date': pub_date,
            'excerpt': article_template['excerpt'],
            'full_text': article_template['content_template']
        }
        
        mock_articles.append(mock_article)
        logger.info(f"Generated Tata Motors article: {article_template['title'][:50]}...")
    
    logger.info(f"Successfully generated {len(mock_articles)} mock YourStory articles")
    return mock_articles

def clean_text(text):
    """
    Clean and normalize text content
    """
    if not text:
        return ""
    
    import re
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)]', ' ', text)
    
    # Remove extra spaces again
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def main():
    """
    Main function to generate mock YourStory articles
    """
    logger.info("Starting YourStory mock article generation...")
    
    articles = generate_mock_yourstory_articles()
    
    if not articles:
        logger.warning("No mock articles generated")
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
    
    logger.info(f"Successfully generated and cleaned {len(cleaned_articles)} mock articles")
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