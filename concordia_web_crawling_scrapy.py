import json
import re
import scrapy
from bs4 import BeautifulSoup
from scrapy import signals

class ConcordiaSpider(scrapy.Spider):
    name = "Concordia"

    def __init__(self):
        self.urls_pool = set(['https://www.concordia.ca'])
        self.max_url_num = 20000
        self.url_content_dict = {}

    custom_settings = {
        'ROBOTSTXT_OBEY': True
    }

    def start_requests(self):
        for url in self.urls_pool:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        extract_urls = self.extract_url_content(response)
        for url in extract_urls:
            if url not in self.urls_pool and len(self.urls_pool)<self.max_url_num:
                self.urls_pool.add(url)
                yield scrapy.Request(url=url, callback=self.parse)

    def extract_url_content(self, response):
        url = response.url
        soup = BeautifulSoup(response.body, "lxml")

        tag_texts = []
        tag_texts_title = soup.find_all('title')
        tag_texts_b = soup.find_all('b')
        tag_texts_br = soup.find_all('br')
        tag_texts_p = soup.find_all('p')
        tag_texts_h = soup.find_all('h')
        tag_texts_h1 = soup.find_all('h1')
        tag_texts_h2 = soup.find_all('h2')
        tag_texts_h3 = soup.find_all('h3')
        tag_texts_h4 = soup.find_all('h4')

        tag_texts.extend(tag_texts_title)
        tag_texts.extend(tag_texts_b)
        tag_texts.extend(tag_texts_br)
        tag_texts.extend(tag_texts_h)
        tag_texts.extend(tag_texts_p)
        tag_texts.extend(tag_texts_h1)
        tag_texts.extend(tag_texts_h2)
        tag_texts.extend(tag_texts_h3)
        tag_texts.extend(tag_texts_h4)

        text = ' '.join([str.strip(tag.text) for tag in tag_texts])
        if len(str.strip(text)) > 10:
            self.url_content_dict[url] = text
        self.log(f'url: {url}')
        extract_urls = []
        tag_urls = soup.find_all(href=True)
        for tag in tag_urls:
            sub_url = tag['href']
            pattern1 = re.compile('.*html')
            pattern2 = re.compile('^http.*')
            pattern3 = re.compile('https://www.concordia.ca.*')
            if pattern3.match(sub_url):
                extract_urls.append(sub_url)
                self.log(f'extract : {sub_url}')
            # check if url is completed , if start with "https://"
            elif pattern1.match(sub_url) is not None and pattern2.match(sub_url) is None:
                prefix = 'https://www.concordia.ca'
                sub_url = "".join([prefix,sub_url])
                extract_urls.append(sub_url)
                self.log(f'extract : {sub_url}')
        return extract_urls

    def save_urls_content_to_jason(self):
        json.dump(self.url_content_dict, open('indexer/content_30000_p_h4.json', "w", encoding="utf−8"), indent=3)

    @classmethod
    def from_crawler(cls, crawler):
        self = cls()
        # crawler.signals.connect(self.x1, signal=signals.spider_opened)
        crawler.signals.connect(self.save_urls_content_to_jason, signal=signals.spider_closed)
        return self



spider = ConcordiaSpider()
result = spider.start_requests()
