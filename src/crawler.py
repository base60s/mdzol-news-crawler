import requests
from bs4 import BeautifulSoup
from loguru import logger
from datetime import datetime
import pandas as pd
import time
import os

class MDZCrawler:
    def __init__(self):
        self.base_url = 'https://www.mdzol.com'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.categories = [
            'politica',
            'sociedad',
            'deportes',
            'espectaculos',
            'economia',
            'mundo'
        ]
        self.setup_logging()

    def setup_logging(self):
        log_format = '{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}'
        logger.add(
            f'logs/crawler_{datetime.now().strftime("%Y%m%d")}.log',
            format=log_format,
            level='INFO',
            rotation='1 day'
        )

    def get_article_links(self, category):
        try:
            url = f'{self.base_url}/{category}'
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            articles = soup.find_all('article')
            
            links = []
            for article in articles:
                link = article.find('a')
                if link and link.get('href'):
                    links.append(link['href'])
            
            return links
        except Exception as e:
            logger.error(f'Error getting article links for {category}: {str(e)}')
            return []

    def parse_article(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            article_data = {
                'title': '',
                'date': '',
                'content': '',
                'category': '',
                'url': url
            }
            
            # Extract title
            title = soup.find('h1')
            if title:
                article_data['title'] = title.text.strip()
            
            # Extract date
            date = soup.find('time')
            if date:
                article_data['date'] = date.get('datetime', '')
            
            # Extract content
            content_div = soup.find('div', class_='article-content')
            if content_div:
                paragraphs = content_div.find_all('p')
                article_data['content'] = ' '.join([p.text.strip() for p in paragraphs])
            
            # Extract category
            category = url.split('/')[3]
            article_data['category'] = category
            
            return article_data
        except Exception as e:
            logger.error(f'Error parsing article {url}: {str(e)}')
            return None

    def crawl(self):
        all_articles = []
        
        for category in self.categories:
            logger.info(f'Crawling category: {category}')
            links = self.get_article_links(category)
            
            for link in links:
                full_url = f'{self.base_url}{link}' if link.startswith('/') else link
                article_data = self.parse_article(full_url)
                
                if article_data:
                    all_articles.append(article_data)
                    logger.info(f'Successfully parsed article: {article_data["title"]}')
                
                time.sleep(1)  # Respect the website by waiting between requests
        
        return all_articles

    def save_to_csv(self, articles, filename=None):
        if not filename:
            filename = f'data/mdz_articles_{datetime.now().strftime("%Y%m%d")}.csv'
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df = pd.DataFrame(articles)
        df.to_csv(filename, index=False)
        logger.info(f'Saved {len(articles)} articles to {filename}')

def main():
    crawler = MDZCrawler()
    articles = crawler.crawl()
    crawler.save_to_csv(articles)

if __name__ == '__main__':
    main()