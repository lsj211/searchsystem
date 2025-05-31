import scrapy
from urllib.parse import urljoin, urlparse
# scrapy crawl nankai
import re
import pymysql
import os
import hashlib

# 判断是否已经爬取
def is_url_crawled(url):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db',
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
        database='my_spider_db',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            sql = "INSERT IGNORE INTO crawled_urls (url) VALUES (%s)"
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
        database='my_spider_db',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM crawled_urls")
            result = cursor.fetchone()
            return result[0] if result else 0
    finally:
        connection.close()


news_detail_pattern = re.compile(r'/system/\d{4}/\d{2}/\d{2}/\d+\.shtml$')
def is_attachment(url):
    file_extensions = [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"]
    return any(url.lower().endswith(ext) for ext in file_extensions)





class NankaiSpider(scrapy.Spider):
    name = "nankai"
    allowed_domains = ["news.nankai.edu.cn"]
    start_urls = ["https://news.nankai.edu.cn/index.shtml"]

    # def __init__(self):
    #     self.count = 0
    #     self.max_count = 2

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_total = 100
        self.existing_count = get_crawled_count()
        self.crawled_this_run = 0



    def parse(self, response):
        raw_links = response.css('a::attr(href)').getall()

        # 过滤空链接、javascript、#、mailto、tel等非http(s)链接
        filtered_links = []
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

        for link in unique_links:
            if self.existing_count+self.crawled_this_run >= self.max_total:
                break
            self.crawled_this_run += 1
            # 发起请求，递归调用parse
            # yield response.follow(link, callback=self.parse)
            if news_detail_pattern.search(link):
            # 这是新闻详情页链接
                yield scrapy.Request(link, callback=self.parse_article)
            else:
            # 其他链接，可能是翻页或列表页，可以递归继续爬
                yield scrapy.Request(link, callback=self.parse)


    def parse_article(self, response):
        if is_url_crawled(response.url):
            return

        # 标题，一般用页面的h1或者title
        title = response.css("title::text").get(default="").strip()

        # 文章正文：改用 id=txt 下所有p标签的文本，拼接
        content_parts = response.xpath('//*[@id="txt"]/p//text()').getall()
        content = "\n".join([part.strip() for part in content_parts if part.strip()])

        # 发布时间：假设页面中没有 #pubtime 或 .pub-time，尝试用xpath抓取示例
        pub_time_raw = response.xpath('//table[3]//table[2]//span[2]/text()').get()
        pub_time_raw = pub_time_raw.strip() if pub_time_raw else ""
        match = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", pub_time_raw)

        pub_time = match.group(0) if match else ""
        if not pub_time:
            # 也可以尝试用正则或其他办法抓
            pub_time = ""

        # 提取正文中的链接
        links = response.css(".v_news_content a::attr(href)").getall()
        article_links = []

        for href in links:         
            if href:
                abs_url = response.urljoin(href)
                path = abs_url.split(response.url.split("//")[1].split("/")[0])[-1]
                if news_detail_pattern.search(path):
                    article_links.append(abs_url)
                if is_attachment(abs_url):
                    yield scrapy.Request(abs_url, callback=self.parse_attachment, meta={"source_url": response.url})

        # 特殊情况：正文为空/很短，而且有嵌套文章链接 => 跳转，不产出当前页
        if len(content) < 50 and len(article_links) == 1:
            self.logger.info("正文太短且检测到嵌套文章链接，跳转请求该链接")
            yield scrapy.Request(url=article_links[0], callback=self.parse_article)
            return

        # 否则，如果还有多个嵌套链接，就继续请求嵌套文章
        for link in article_links:
            yield scrapy.Request(url=link, callback=self.parse_article)

        save_url_to_db(response.url)
        yield {
            "title": title,
            "pub_time": pub_time,
            "content": content,
            "url": response.url,
            "snapshot_path": "",
            "anchor":"",
            "type":"html"
        }

    def parse_attachment(self, response):
        attachment_url = response.url
        filename = attachment_url.split("/")[-1]
        
        save_path = os.path.join("H:/SearchSystem/search_engine/static/attachments", filename)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        # 保存附件
        with open(save_path, 'wb') as f:
            f.write(response.body)
            doc_id = hashlib.md5(attachment_url.encode("utf-8")).hexdigest()

        # 构建仅包含文件名的索引文档
        doc = {
            "title": filename,
            "url": attachment_url,
            "pub_time": "",              # 没有发布日期可设为空
            "content": "",               # 不提取正文
            "snapshot_path": save_path,  # 实际是附件路径
            "anchor": "",
            "type": "attachment"         # 类型标记（可选）
        }
        
        yield doc





