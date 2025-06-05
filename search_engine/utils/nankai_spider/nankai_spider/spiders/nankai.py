import scrapy
from urllib.parse import urljoin, urlparse
# scrapy crawl nankai
import re
import pymysql
import os
import hashlib
from scrapy.exceptions import CloseSpider
from readability import Document
from bs4 import BeautifulSoup
import urllib



# from requests_toolbelt.downloadutils import guess_filename

# 判断是否已经爬取
def is_url_crawled(url):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db2',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            sql = "SELECT 1 FROM crawled_urls WHERE url=%s LIMIT 1"
            cursor.execute(sql, (url,))
            return cursor.fetchone() is not None
    finally:
        connection.close()

def save_url_to_db(url):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db2',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            sql = "INSERT IGNORE INTO crawled_urls (url) VALUES (%s)"
            cursor.execute(sql, (url,))
        connection.commit()
    finally:
        connection.close()


def is_urlnavigte_crawled(url):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db2',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            sql = "SELECT 1 FROM crawled_navigateurls WHERE url=%s LIMIT 1"
            cursor.execute(sql, (url,))
            return cursor.fetchone() is not None
    finally:
        connection.close()

def save_urlnavigate_to_db(url):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db2',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            sql = "INSERT IGNORE INTO crawled_navigateurls (url) VALUES (%s)"
            cursor.execute(sql, (url,))
        connection.commit()
    finally:
        connection.close()



#爬取数量
def get_crawled_count():
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db2',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM crawled_urls")
            result = cursor.fetchone()
            return result[0] if result else 0
    finally:
        connection.close()


# news_detail_pattern = re.compile(r'/system/\d{4}/\d{2}/\d{2}/\d+\.shtml$')
import re

# news_detail_pattern = re.compile(r'(/system/\d{4}/\d{2}/\d{2}/\d+\.shtml$)|(\?p=\d+$)')
# news_detail_pattern = re.compile(
#     r'(/system/\d{4}/\d{2}/\d{2}/\d+\.shtml$|'
#     r'\?p=\d+$|'
#     r'\d{4}/\d{4}/c[\da-z]+/page\.htm$)'
# )
# news_detail_pattern = re.compile(
#     r'(/system/\d{4}/\d{2}/\d{2}/\d+\.shtml$|'        # 原有格式1
#     r'\?p=\d+$|'                                      # 原有格式2
#     r'\d{4}/\d{4}/c[\da-z]+/page\.htm$|'             # 原有格式3
#     r'info/\d+/\d+\.htm$)'                            # 新增格式：info/数字/数字.htm
# )
# news_detail_pattern = re.compile(
#     r'(/system/\d{4}/\d{2}/\d{2}/\d+\.shtml$|'
#     r'\?p=\d+$|'
#     r'\d{4}/\d{4}/c[\da-z]+/page\.htm$|'
#     r'info/\d+/\d+\.htm$|'
#     r'/n/\d+\.html$)'
# )
import re

# news_detail_pattern = re.compile(
#     r'/doc-[\w]+\.shtml$|'
#     r'/detail-[\w]+\.d\.html$|'
#     r'/\d{4}-\d{2}-\d{2}/doc-[\w]+\.shtml$|'
#     r'/zx/\d{4}-\d{2}-\d{2}/doc-[\w]+\.shtml$'
# )

import re

# news_detail_pattern = re.compile(
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/doc-[\w]+\.shtml$|'           # /channel/YYYY-MM-DD/doc-xxxx.shtml
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/detail-[\w]+\.d\.html$|'       # /channel/YYYY-MM-DD/detail-xxxx.d.html
#     r'/doc-[\w]+\.shtml$|'                                           # /doc-xxxx.shtml
#     r'/detail-[\w]+\.d\.html$|'                                      # /detail-xxxx.d.html
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/\d{8,}\.shtml$'                # /channel/YYYY-MM-DD/123456789.shtml（8位及以上数字）
# )

# http://sports.sina.com.cn/global/200004/2137125.shtml
# news_detail_pattern = re.compile(
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/[a-z]+-[\w]+\.shtml$|'         # /channel/YYYY-MM-DD/任意字母-xxxx.shtml
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/detail-[\w]+\.d\.html$|'       # /channel/YYYY-MM-DD/detail-xxxx.d.html
#     r'/[a-z]+-[\w]+\.shtml$|'                                        # /任意字母-xxxx.shtml
#     r'/detail-[\w]+\.d\.html$|'                                      # /detail-xxxx.d.html
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/\d{8,}\.shtml$'                # /channel/YYYY-MM-DD/123456789.shtml
# )
import re

# news_detail_pattern = re.compile(
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/[a-z]+-[\w]+\.shtml$|'       # /channel/YYYY-MM-DD/任意字母-xxxx.shtml
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/detail-[\w]+\.d\.html$|'     # /channel/YYYY-MM-DD/detail-xxxx.d.html
#     r'/[a-z]+-[\w]+\.shtml$|'                                      # /任意字母-xxxx.shtml
#     r'/detail-[\w]+\.d\.html$|'                                    # /detail-xxxx.d.html
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/\d{8,}\.shtml$|'             # /channel/YYYY-MM-DD/123456789.shtml
#     r'/[a-z]+/\d{6}/\d{6,}\.shtml$|'       # /channel/yyyyMM/dddddddd.shtml
#     r'\d+/\d{8}/\d+\.html$|'
#     r'[\w\-]+/\d{4}-\d{2}-\d{2}/doc-[\w\d]+\.shtml$'                                       
# )

# news_detail_pattern = re.compile(
#     r'(/system/\d{4}/\d{2}/\d{2}/\d+\.shtml$|'
#     r'\?p=\d+$|'
#     r'\d{4}/\d{4}/c[\da-z]+/page\.htm$|'
#     r'info/\d+/\d+\.htm$|'
#     r'/n/\d+\.html$)'
# )
news_detail_pattern = re.compile(
    r'/system/\d{4}/\d{2}/\d{2}/\d+\.shtml$|'  # 原有模式1：/system/年/月/日/数字.shtml
    r'\?p=\d+$|'                                # 原有模式2：?p=数字
    r'\d{4}/\d{4}/c[\da-z]+/page\.htm$|'        # 原有模式3：年/年/c字母数字/page.htm
    r'info/\d+/\d+\.htm$|'                      # 原有模式4：info/数字/数字.htm
    r'/n/\d+\.html$|'                           # 原有模式5：/n/数字.html
    r'/news/content/id/\d+\.html$|'
    r'/news/content/id/[\d-]+\.html$|'             # 新增模式：/news/content/id/数字.html
    r'\d{4}-\d{2}-\d{2}/\d+\.html$'
)

# news_detail_pattern = re.compile(
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/[a-z]+-[\w]+\.shtml$|'
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/detail-[\w]+\.d\.html$|'
#     r'/[a-z]+-[\w]+\.shtml$|'
#     r'/detail-[\w]+\.d\.html$|'
#     r'/(?:[\w\-]+/)?\d{4}-\d{2}-\d{2}/\d{8,}\.shtml$|'
#     r'/[a-z]+/\d{6}/\d{6,}\.shtml$|'
#     r'\d+/\d{8}/\d+\.html$|'
#     r'[\w\-]+/\d{4}-\d{2}-\d{2}/doc-[\w\d]+\.shtml$|'
#     r'[\w\-]+/article/[A-Z0-9]+\.html$'
# )
def is_attachment(url):
    file_extensions = [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", '.rar', '.mp4', '.avi', '.mov','wmv']
    return any(url.lower().endswith(ext) for ext in file_extensions)

attachment_extensions = ('.pdf', '.doc', '.docx', '.xls', '.xlsx')

from urllib.parse import urlparse, parse_qs, unquote

# def is_valid_link(url):
#     invalid_keywords = ['login', 'logout', 'signup', 'register', 'cas', 'auth']
#     url_lower = url.lower()

#     # 先简单判断 URL 本体字符串
#     if any(kw in url_lower for kw in invalid_keywords):
#         return False

#     # 再判断 URL 的 query 参数内容是否包含登录关键字（解码后）
#     parsed = urlparse(url)
#     query_dict = parse_qs(parsed.query)
#     for param_values in query_dict.values():
#         for value in param_values:
#             decoded_value = unquote(value).lower()
#             if any(kw in decoded_value for kw in invalid_keywords):
#                 return False

#     return True

def is_valid_link(url):
    invalid_keywords = [
        'login', 'logout', 'signup', 'register', 'cas', 'auth', 'passport',
        'video', 'photo', 'slide', 'blog', 'mail', 'game', 'help', 
        'ad', 'career', 'job', 'vip', 'db.', 'dealer.', 'auto.', 'eladies', 
        'baby', 'lottery', 'tousu', 'weibo', 'live', 'house', 'zx.', 'jiaju', 'sh.', 'bj.', 'photo.', 'k.', 'i3.sinaimg.cn','livecast','player','realstock'
    ]
    url_lower = url.lower()
    if any(kw in url_lower for kw in invalid_keywords):
        return False
    # 可补充更多路径/参数规则
    return True

import re

def normalize_pub_time(pub_time_raw):
    if not pub_time_raw:
        return None
    pub_time_raw = pub_time_raw.strip()

    # 2025年2月20日 15:30
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日\s*(\d{1,2}):(\d{1,2})", pub_time_raw)
    if m:
        year, month, day, hour, minute = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d} {int(hour):02d}:{int(minute):02d}:00"

    # 2025年2月20日
    m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", pub_time_raw)
    if m:
        year, month, day = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"

    # 2025-02-20 15:30:25 或 2025-02-20 15:30
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})[ T]?(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?", pub_time_raw)
    if m:
        year, month, day, hour, minute, second = m.groups()
        hour = int(hour) if hour else 0
        minute = int(minute) if minute else 0
        second = int(second) if second else 0
        return f"{year}-{int(month):02d}-{int(day):02d} {hour:02d}:{minute:02d}:{second:02d}"

    # 2025.02.20 15:30:25 或 2025.02.20 15:30
    m = re.search(r"(\d{4})\.(\d{1,2})\.(\d{1,2})[ ]?(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?", pub_time_raw)
    if m:
        year, month, day, hour, minute, second = m.groups()
        hour = int(hour) if hour else 0
        minute = int(minute) if minute else 0
        second = int(second) if second else 0
        return f"{year}-{int(month):02d}-{int(day):02d} {hour:02d}:{minute:02d}:{second:02d}"

    # 2025/02/20 15:30:25 或 2025/02/20 15:30
    m = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})[ ]?(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?", pub_time_raw)
    if m:
        year, month, day, hour, minute, second = m.groups()
        hour = int(hour) if hour else 0
        minute = int(minute) if minute else 0
        second = int(second) if second else 0
        return f"{year}-{int(month):02d}-{int(day):02d} {hour:02d}:{minute:02d}:{second:02d}"

    # 2025-02-20
    m = re.search(r"(\d{4})-(\d{1,2})-(\d{1,2})", pub_time_raw)
    if m:
        year, month, day = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"

    # 2025.02.20
    m = re.search(r"(\d{4})\.(\d{1,2})\.(\d{1,2})", pub_time_raw)
    if m:
        year, month, day = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"

    # 2025/02/20
    m = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", pub_time_raw)
    if m:
        year, month, day = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"

    # ISO格式 2025-06-02T15:30:25Z
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})", pub_time_raw)
    if m:
        year, month, day, hour, minute, second = m.groups()
        return f"{year}-{month}-{day} {hour}:{minute}:{second}"

    return None


class NankaiSpider(scrapy.Spider):
    name = "nankai"
    allowed_domains = ["nankai.edu.cn"]
    # allowed_domains = ["sina.com.cn"]
#     allowed_domains = [
#     "news.sina.com.cn",
#     "sports.sina.com.cn",
#     "finance.sina.com.cn",
#     "ent.sina.com.cn",
#     "www.sina.com.cn",
#     "mil.news.sina.com.cn",
#     "mobile.sina.com.cn",
#     "tech.sina.com.cn",
#     "edu.sina.com.cn",
#     "fashion.sina.com.cn",
#     # "blog.sina.com.cn"
#     # ...你关心的其他频道
# ]


    # start_urls = ["https://news.nankai.edu.cn/index.shtml"]
    start_urls=["https://www.nankai.edu.cn/main.htm"]
    # start_urls=["https://news.nankai.edu.cn/"]
    # start_urls=["https://chem.nankai.edu.cn/"]
    # start_urls=["https://www.sina.com.cn/"]
    # start_urls=["https://www.163.com/"]
    
    # start_urls=["https://mil.news.sina.com.cn/"]
    # def __init__(self):
    #     self.count = 0
    #     self.max_count = 2

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_total = 101500
        self.max_total2 = 10
        self.attachment_total2=0
        self.existing_count = get_crawled_count()
        self.crawled_this_run = 0
        self.pagination_started = False
        self.pagination_started2 = False
        self.pagination_started3 = False


    def parse(self, response):           
        # if is_urlnavigte_crawled(response.url):
        #     return
        if self.existing_count+self.crawled_this_run >= self.max_total:
            raise CloseSpider('reach_max_items')
        content_type = response.headers.get('Content-Type', b'').decode('utf-8').lower()
        if 'text/html' in content_type:
            raw_links = response.css('a::attr(href)').getall()
        # 继续你的链接提取逻辑
        else:
        # 这里处理非HTML响应，或者直接跳过
            return
        # raw_links = response.css('a::attr(href)').getall()
        # if not self.pagination_started:
        #     self.pagination_started = True  # 只运行一次
        #     for i in range(999, 700, -1):
        #         page_id = f"{i:09d}"
        #         page_url = f"https://news.nankai.edu.cn/mtnk/system/count/0006000/000000000000/000/000/c0006000000000000000_{page_id}.shtml"
        #         yield scrapy.Request(url=page_url, callback=self.parse_links)
        #         # raw_links = response.css('a::attr(href)').getall()

        # if not self.pagination_started2:
        #     self.pagination_started2 = True  # 只运行一次
        #     for i in range(500, 350, -1):
        #         page_id = f"{i:09d}"
        #         page_url = f"https://news.nankai.edu.cn/ywsd/system/count//0003000/000000000000/000/000/c0003000000000000000_{page_id}.shtml"
        #         yield scrapy.Request(url=page_url, callback=self.parse_links)

        urls=[f"https://chem.nankai.edu.cn/",
              f"https://zfxy.nankai.edu.cn/",
              f"https://history.nankai.edu.cn/",
              f"https://sky.nankai.edu.cn/",
              f"https://cs.nankai.edu.cn/",
              f"https://binhai.nankai.edu.cn/",
              f"https://bs.nankai.edu.cn/",
              f"https://yzb.nankai.edu.cn/",
              f"https://economics.nankai.edu.cn/",
              f"https://law.nankai.edu.cn/",
              f"https://stat.nankai.edu.cn/",
              f"https://mse.nankai.edu.cn/",
              f"https://math.nankai.edu.cn/",
              f"https://physics.nankai.edu.cn/",
              f"https://ceo.nankai.edu.cn/",
              f"https://finance.nankai.edu.cn/",f"https://lib.nankai.edu.cn/",f"https://xgb.nankai.edu.cn/",f"https://zsb.nankai.edu.cn/"]
        if not self.pagination_started3:
            self.pagination_started3 = True  # 只运行一次
            for page_url in urls:
                if is_urlnavigte_crawled(page_url):
                    continue
                save_urlnavigate_to_db(page_url)
                yield scrapy.Request(url=page_url, callback=self.parse)


        # 过滤空链接、javascript、#、mailto、tel等非http(s)链接
        # news_links = [link for link in raw_links if news_detail_pattern.search(link)]
        # print(f"提取到新闻详情页链接数量: {len(news_links)}")
        filtered_links = []
        # print(len(raw_links))
        # self.logger.info(f"提取到新闻详情页链接数量: {len(raw_links)}")
        for link in raw_links:
            if not link:
                continue
            if link.startswith(('javascript:', '#', 'mailto:', 'tel:')):
                continue
            absolute_link = urljoin(response.url, link)
            parsed = urlparse(absolute_link)
            if parsed.scheme not in ('http', 'https'):
                continue
            filtered_links.append(absolute_link)

        # 去重
        unique_links = list(set(filtered_links))
        if self.existing_count+self.crawled_this_run >= self.max_total:
            raise CloseSpider('reach_max_items')

        for link in unique_links:
            
            # === 附件（PDF/Word等） ===
            if link.lower().endswith(attachment_extensions):
                if self.attachment_total2<self.max_total2:
                    continue
                    yield scrapy.Request(link, callback=self.parse_attachment)
            elif not is_valid_link(link):
                continue
            # === 新闻详情页（传统） ===
            elif news_detail_pattern.search(urlparse(link).path):
                yield scrapy.Request(link, callback=self.parse_article)

            # === 其他情况，继续爬链接 ===
            else:
                if is_urlnavigte_crawled(link):
                    continue
                save_urlnavigate_to_db(link)
                yield scrapy.Request(link, callback=self.parse)




    def parse_links(self, response):
        # 提取每页中的新闻详情链接
        for href in response.css("a::attr(href)").getall():
            absolute_url = response.urljoin(href)
            if news_detail_pattern.search(absolute_url):
                yield scrapy.Request(absolute_url, callback=self.parse_article)


    def parse_article(self, response):
        if is_url_crawled(response.url):
            self.logger.info("已存在")
            return
        # 用 readability 提取正文（先获取整个HTML源码）
        html = response.text
        doc = Document(html)
        main_html = doc.summary()   # 这是正文的HTML片段
        main_text = BeautifulSoup(main_html, "lxml").get_text(separator="\n")  # 纯文本
        title=doc.title()
        if not title:
            title = response.css("title::text").get(default="").strip()
        content = main_text.strip()
        if not content:
            xpaths = [
            # 常见的正文、内容区域
            '//*[@id="txt"]/p//text()',
            '//*[@id="txt"]//text()',
            '//*[@id="vsb_content"]//text()',
            '//*[@id="vsb_content"]/div/div/p//text()',
            '//*[@id="vsb_content"]/div/p//text()',
            '//*[@id="content"]//text()',
            '//*[@id="article"]//text()',
            '//*[@id="main"]//text()',
            '//*[@id="container"]//text()',
            '//*[@id="con"]//text()',
            '//*[@class="content"]//text()',
            '//*[@class="article"]//text()',
            '//*[@class="main"]//text()',
            '//*[@class="news_content"]//text()',
            '//*[@class="newsCon"]//text()',
            '//*[@class="text"]//text()',
            '//*[@id="vsb_content"]//p//span//text()',
            '//*[@id="vsb_content"]//p//text()',
            '//*[@id="txt"]/p//span//text()',
            '/html/body/div[1]/div[3]/div/div/div[2]/div/div[2]/div[3]/div/p//text()',
            # 兼容结构层级不固定的情况
            '//*[contains(@id,"content")]//text()',
            '//*[contains(@class,"content")]//text()',
            '//*[contains(@id,"article")]//text()',
            '//*[contains(@class,"article")]//text()',
            '//*[contains(@id,"main")]//text()',
            '//*[contains(@class,"main")]//text()',
            # 通用的所有p标签
            '//p//text()'
            ]
            content_parts = []
            for xp in xpaths:
                parts = response.xpath(xp).getall()
                if parts:
                    content_parts = parts
                    break

            content = "\n".join([part.strip() for part in content_parts if part.strip()])

        pub_time_xpaths = [
            '//span[@class="date"]/text()',
            '//span[@class="time-source"]/text()',
            '//span[@id="pub_date"]/text()',
            '//span[@class="time"]/text()',
            '//div[@class="date-source"]/text()',
            '//div[@class="titer"]/text()',
            '//span[@class="date-source"]/text()',
            '//span[@id="top_bar"]/div/div[2]/span/text()',
            '//span[contains(@class, "source")]/text()',
            '//meta[@property="article:published_time"]/@content',
            '//meta[@name="publishdate"]/@content',
            '//span[contains(text(),"20")]/text()',
            '//div[contains(text(),"20")]/text()',
        ]
        pub_time_raw = ""
        for xp in pub_time_xpaths:
            pub_time_raw = response.xpath(xp).get()
            if pub_time_raw:
                pub_time_raw = pub_time_raw.strip()
                if pub_time_raw:
                    break

        if not pub_time_raw:
            texts = " ".join(response.xpath('//body//text()').getall())
            import re
            m = re.search(r"\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2}[日]?\s*\d{1,2}:?\d{0,2}", texts)
            if m:
                pub_time_raw = m.group(0)

        pub_time = normalize_pub_time(pub_time_raw) if pub_time_raw else None

        a_tags = response.css(".v_news_content a")
        article_links = []

        for a in a_tags:
            href = a.attrib.get("href")
            anchor_text = a.xpath("string(.)").get(default="").strip()

            if href:
                abs_url = response.urljoin(href)
                path = abs_url.split(response.url.split("//")[1].split("/")[0])[-1]

                if news_detail_pattern.search(path):
                    # 保存链接与锚文本一起，用于后续跳转逻辑
                    article_links.append((abs_url, anchor_text))
                elif is_attachment(abs_url):
                    # 是附件
                    continue
                    # yield scrapy.Request(
                    #     abs_url,
                    #     callback=self.parse_attachment,
                    #     meta={"source_url": response.url, "anchor": anchor_text}
                    # )

        # 特殊情况：正文太短且只有一个嵌套文章链接 => 跳转该链接
        if len(content) < 50 and len(article_links) == 1:
            next_url, anchor = article_links[0]
            self.logger.info("正文太短且检测到嵌套文章链接，跳转请求该链接")
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_article,
                meta={"anchor": anchor}
            )
            return

        # 否则继续请求所有嵌套文章
        for link, anchor in article_links:
            yield scrapy.Request(
                url=link,
                callback=self.parse_article,
                meta={"anchor": anchor}
            )
        if self.existing_count+self.crawled_this_run >= self.max_total:
            raise CloseSpider('reach_max_items')
        self.crawled_this_run += 1
        anchor = response.meta.get("anchor", "")
        save_url_to_db(response.url)
        yield {
            "title": title,
            "pub_time": pub_time,
            "content": content,
            "url": response.url,
            "snapshot_path": "",
            "anchor":anchor,
            "type":"html"
        }



    def parse_attachment(self, response):
        if is_url_crawled(response.url):
            return
        if self.attachment_total2 >= self.max_total2:
            return
        # 1. 优先用meta传入的锚文本（页面文件名）
        filename = response.meta.get("anchor", "").strip()

        # 2. fallback到响应头Content-Disposition
        if not filename or '.' not in filename:
            cd = response.headers.get('Content-Disposition', b'').decode(errors="ignore")
            m_star = re.search(r'filename\*=UTF-8\'\'([^\s;]+)', cd)
            if m_star:
                filename = urllib.parse.unquote(m_star.group(1))
            else:
                m = re.search(r'filename="?([^";]+)"?', cd)
                if m:
                    filename = urllib.parse.unquote(m.group(1))
            # 3. fallback到URL
            if not filename or '.' not in filename:
                filename = response.url.split('/')[-1]

        # 4. 清理非法字符
        filename = re.sub(r'[\\/:"*?<>|]+', "_", filename)
        if not filename:
            filename = "unnamed_attachment"
        filename = unquote(filename)
        save_path = os.path.join("H:/SearchSystem/search_engine/static/attachments", filename)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        try:
            with open(save_path, 'wb') as f:
                f.write(response.body)
        except Exception as e:
            self.logger.error(f"保存附件失败: {save_path}，原因：{e}")
            return

        save_url_to_db(response.url)
        self.attachment_total2 += 1  # 注意拼写！
        doc = {
            "title": filename,
            "url": response.url,
            "pub_time": "",
            "content": "",
            "snapshot_path": save_path,
            "anchor": filename,
            "type": "attachment"
        }
        yield doc


    # def parse_attachment(self, response):
    #     attachment_url = response.url
    #     filename = attachment_url.split("/")[-1]
        
    #     save_path = os.path.join("H:/SearchSystem/search_engine/static/attachments", filename)
    #     os.makedirs(os.path.dirname(save_path), exist_ok=True)

    #     # 保存附件
    #     with open(save_path, 'wb') as f:
    #         f.write(response.body)
    #         doc_id = hashlib.md5(attachment_url.encode("utf-8")).hexdigest()
    #     anchor = response.meta.get("anchor", "")
    #     # 构建仅包含文件名的索引文档
    #     doc = {
    #         "title": filename,
    #         "url": attachment_url,
    #         "pub_time": "",              # 没有发布日期可设为空
    #         "content": "",               # 不提取正文
    #         "snapshot_path": save_path,  # 实际是附件路径
    #         "anchor": anchor,
    #         "type": "attachment"         # 类型标记（可选）
    #     }
        
    #     yield doc


    # def parse_attachment(self, response):
    #     if is_url_crawled(response.url):
    #         return
    #     if self.attachment_total >= self.max_total2:
    #         return

    #     # 1. 优先用 meta 传入的锚文本（页面文件名）
    #     filename = response.meta.get("anchor", "").strip()

    #     # 2. 使用 requests-toolbelt 的 guess_filename 优先解析响应头（Content-Disposition），兼容 filename* 和 filename
    #     if not filename or '.' not in filename:
    #         filename = guess_filename(response)

    #     # 3. fallback 到 URL 最后一段
    #     if not filename or '.' not in filename:
    #         filename = response.url.split('/')[-1]

    #     # 4. 清理非法字符，保证文件名安全
    #         #     # 4. 清理非法字符
    #     filename = re.sub(r'[\\/:"*?<>|]+', "_", filename)
    #     if not filename:
    #         filename = "unnamed_attachment"

    #     save_path = os.path.join("H:/SearchSystem/search_engine/static/attachments", filename)
    #     os.makedirs(os.path.dirname(save_path), exist_ok=True)

    #     try:
    #         with open(save_path, 'wb') as f:
    #             f.write(response.body)
    #     except Exception as e:
    #         self.logger.error(f"保存附件失败: {save_path}，原因：{e}")
    #         return

    #     save_url_to_db(response.url)
    #     self.attachment_total += 1
    #     doc = {
    #         "title": filename,
    #         "url": response.url,
    #         "pub_time": "",
    #         "content": "",
    #         "snapshot_path": save_path,
    #         "anchor": filename,
    #         "type": "attachment"
    #     }
    #     yield doc


#scrapy crawl nankai;
# TRUNCATE TABLE crawled_articles;
# TRUNCATE TABLE crawled_urls;
# TRUNCATE TABLE crawled_navigateurls;







#转生了
