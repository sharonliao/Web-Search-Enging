from bs4 import BeautifulSoup
import re

html_contentt = None

def extract_html():
    with open("html_test/P_test.html", "r", encoding='utf-8') as f:
        html_contentt = f.read()
    soup = BeautifulSoup(html_contentt, "lxml")
    tag_urls = soup.find_all(href=True)
    tag_texts = []
    tag_texts_title = soup.find_all('title')
    print(f"b : {tag_texts_title}")
    tag_texts_b = soup.find_all('b')
    print(f"b : {tag_texts_b}")
    tag_texts_br = soup.find_all('br')
    print(f"br : {tag_texts_br}")
    tag_texts_p = soup.find_all('p')
    print(f"p : {tag_texts_p}")
    tag_texts_h = soup.find_all('h')
    print(f"h : {tag_texts_h}")
    tag_texts_h1 = soup.find_all('h1')
    print(f"h1 : {tag_texts_h1}")
    tag_texts_h2 = soup.find_all('h2')
    print(f"h2 : {tag_texts_h2}")
    tag_texts_h3 = soup.find_all('h3')
    print(f"h3 : {tag_texts_h3}")
    tag_texts_h4 = soup.find_all('h4')
    print(f"h4 : {tag_texts_h4}")

    tag_texts.extend(tag_texts_title)
    tag_texts.extend(tag_texts_b)
    tag_texts.extend(tag_texts_br)
    tag_texts.extend(tag_texts_p)
    tag_texts.extend(tag_texts_h)
    tag_texts.extend(tag_texts_h1)
    tag_texts.extend(tag_texts_h2)
    tag_texts.extend(tag_texts_h3)
    tag_texts.extend(tag_texts_h4)

    text = ' '.join([str.strip(tag.text) for tag in tag_texts])
    print(text)
    # self.log(f"texts: {' '.join([str.strip(tag.text) for tag in tag_texts])}")
    extract_urls = []
    for tag in tag_urls:
        sub_url = tag['href']
        # pattern1 = re.compile('.*academics.*html')
        # pattern2 = re.compile('^http.*')
        # pattern3 = re.compile('https://www.concordia.ca/academics/.*')
        pattern1 = re.compile('.*html')
        pattern2 = re.compile('^http.*')
        pattern3 = re.compile('https://www.concordia.ca.*')
        if pattern3.match(sub_url):
            extract_urls.append(sub_url)
        # check if url is completed , if start with "https://"
        elif pattern1.match(sub_url) is not None and pattern2.match(sub_url) is None:
            prefix = 'https://www.concordia.ca'
            sub_url = "".join([prefix,sub_url])
            extract_urls.append(sub_url)
    return extract_urls


extract_html()