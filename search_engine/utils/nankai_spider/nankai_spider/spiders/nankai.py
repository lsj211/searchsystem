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


def is_urlnavigte_crawled(url):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db',
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
        database='my_spider_db',
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

news_detail_pattern = re.compile(
    r'('
    r'/system/\d{4}/\d{2}/\d{2}/\d+\.shtml$|'          # /system/2025/06/02/12345.shtml
    r'\?p=\d+$|'                                       # ?p=12345
    r'\d{4}/\d{4}/c[\da-z]+/page\.htm$|'               # 2025/2025/c123abc/page.htm
    r'info/\d+/\d+\.htm$|'                             # info/12345/6789.htm
    r'/n/\d+\.html$|'                                  # /n/12345.html
    r'\d{4}-\d{2}-\d{2}/doc-[a-zA-Z0-9]+\.shtml$'      # 2025-06-02/doc-xxxx.shtml
    r')'
)


def is_attachment(url):
    file_extensions = [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", '.rar', '.mp4', '.avi', '.mov','wmv']
    return any(url.lower().endswith(ext) for ext in file_extensions)

attachment_extensions = ('.pdf', '.doc', '.docx', '.xls', '.xlsx')

from urllib.parse import urlparse, parse_qs, unquote

def is_valid_link(url):
    invalid_keywords = ['login', 'logout', 'signup', 'register', 'cas', 'auth']
    url_lower = url.lower()

    # 先简单判断 URL 本体字符串
    if any(kw in url_lower for kw in invalid_keywords):
        return False

    # 再判断 URL 的 query 参数内容是否包含登录关键字（解码后）
    parsed = urlparse(url)
    query_dict = parse_qs(parsed.query)
    for param_values in query_dict.values():
        for value in param_values:
            decoded_value = unquote(value).lower()
            if any(kw in decoded_value for kw in invalid_keywords):
                return False

    return True


import re

def normalize_pub_time(pub_time_raw):
    # 2025年2月20日 15:30
    m = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日[ ]*(\d{1,2}):(\d{1,2})", pub_time_raw)
    if m:
        year, month, day, hour, minute = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d} {int(hour):02d}:{int(minute):02d}:00"
    # 2025年2月20日
    m = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", pub_time_raw)
    if m:
        year, month, day = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    # 2025-02-20 15:30
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})[ ]*(\d{1,2}):(\d{1,2})", pub_time_raw)
    if m:
        year, month, day, hour, minute = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d} {int(hour):02d}:{int(minute):02d}:00"
    # 2025-02-20
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", pub_time_raw)
    if m:
        year, month, day = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    # 2025.02.20
    m = re.match(r"(\d{4})\.(\d{1,2})\.(\d{1,2})", pub_time_raw)
    if m:
        year, month, day = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    # 2025/02/20
    m = re.match(r"(\d{4})/(\d{1,2})/(\d{1,2})", pub_time_raw)
    if m:
        year, month, day = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    return None


class NankaiSpider(scrapy.Spider):
    name = "nankai"
    # allowed_domains = ["nankai.edu.cn"]
    allowed_domains = ["sina.com.cn"]

    # start_urls = ["https://news.nankai.edu.cn/index.shtml"]
    # start_urls=["https://news.nankai.edu.cn/"]
    start_urls=["https://news.sina.com.cn/"]

    # def __init__(self):
    #     self.count = 0
    #     self.max_count = 2

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_total = 100500
        self.max_total2 = 10
        self.attachment_total2=0
        self.existing_count = get_crawled_count()
        self.crawled_this_run = 0
        self.pagination_started = False
        self.pagination_started2 = False


    def parse(self, response):
        # page_url=[f"https://chem.nankai.edu.cn/",f"https://chem.nankai.edu.cn/",f"https://history.nankai.edu.cn/",f"https://cc.nankai.edu.cn/",f"https://finance.nankai.edu.cn/",f"https://economics.nankai.edu.cn/",f"https://shxy.nankai.edu.cn/",f"https://law.nankai.edu.cn/",f"https://math.nankai.edu.cn/",f"https://graduate.nankai.edu.cn/"]
        # if not self.pagination_started:
        #     self.pagination_started = True  # 只运行一次
        #     for url in page_url:
        #         if not is_urlnavigte_crawled(url):
        #             save_urlnavigate_to_db(url)
        #             yield scrapy.Request(url=url, callback=self.parse)
                    
        if is_urlnavigte_crawled(response.url):
            return
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
        #     for i in range(999, 0, -1):
        #         page_id = f"{i:09d}"
        #         page_url = f"https://news.nankai.edu.cn/mtnk/system/count/0006000/000000000000/000/000/c0006000000000000000_{page_id}.shtml"
        #         if not is_urlnavigte_crawled(page_url):
        #             save_urlnavigate_to_db(page_url)
        #             yield scrapy.Request(url=page_url, callback=self.parse_links)

        # if not self.pagination_started2:
        #     self.pagination_started2 = True  # 只运行一次
        #     for i in range(655, 10, -1):
        #         page_id = f"{i:09d}"
        #         page_url = f"https://news.nankai.edu.cn/ywsd/system/count//0003000/000000000000/000/000/c0003000000000000000_{page_id}.shtml"
        #         if not is_urlnavigte_crawled(page_url):
        #             save_urlnavigate_to_db(page_url)
        #             yield scrapy.Request(url=page_url, callback=self.parse_links)
                # save_urlnavigate_to_db(page_url)
                # yield scrapy.Request(url=page_url, callback=self.parse_links)
        # if not self.pagination_started2:
        #     self.pagination_started2 = True  # 只运行一次
        #     for i in range(113, 0, -1):
        #         page_id = f"{i:09d}"
        #         page_url = f"https://zfxy.nankai.edu.cn/index/important_news/{page_id}.shtml"
        #         if not is_urlnavigte_crawled(page_url):
        #             save_urlnavigate_to_db(page_url)
        #             yield scrapy.Request(url=page_url, callback=self.parse_links)
        # page_url=[f"https://chem.nankai.edu.cn/",f"https://chem.nankai.edu.cn/",f"https://history.nankai.edu.cn/",f"https://cc.nankai.edu.cn/",f"https://finance.nankai.edu.cn/",f"https://economics.nankai.edu.cn/",f"https://shxy.nankai.edu.cn/"]
        # if not self.pagination_started:
        #     self.pagination_started = True  # 只运行一次
        #     for url in page_url:
        #         if not is_urlnavigte_crawled(url):
        #             save_urlnavigate_to_db(url)
        #             yield scrapy.Request(url=url, callback=self.parse_links)

        # 过滤空链接、javascript、#、mailto、tel等非http(s)链接
        # filtered_links = []
        # for link in raw_links:
        #     if not link:
        #         continue
        #     if link.startswith(('javascript:', '#', 'mailto:', 'tel:')):
        #         continue
        #     absolute_link = urljoin(response.url, link)
        #     parsed = urlparse(absolute_link)
        #     if parsed.scheme not in ('http', 'https'):
        #         continue
        #     filtered_links.append(absolute_link)

        # # 去重
        # unique_links = list(set(filtered_links))
        # if self.existing_count+self.crawled_this_run >= self.max_total:
        #     raise CloseSpider('reach_max_items')

        # for link in unique_links:
        #     # === 附件（PDF/Word等） ===
        #     if link.lower().endswith(attachment_extensions):
        #         if self.atrachment_total>self.max_total2:
        #             continue
        #         yield scrapy.Request(link, callback=self.parse_attachment)
        #     elif not is_valid_link(link):
        #         continue
        #     # === 新闻详情页（传统） ===
        #     elif news_detail_pattern.search(link):
        #         yield scrapy.Request(link, callback=self.parse_article)

        #     # === 其他情况，继续爬链接 ===
        #     else:
        #         yield scrapy.Request(link, callback=self.parse)

        seen_links = set()
        for a in response.css('a[href]'):
            href = a.attrib['href']
            if not href:
                continue
            if href.startswith(('javascript:', '#', 'mailto:', 'tel:')):
                continue
            abs_link = urljoin(response.url, href)
            parsed = urlparse(abs_link)
            if parsed.scheme not in ('http', 'https'):
                continue
            if abs_link in seen_links:
                continue
            seen_links.add(abs_link)
            anchor_text = a.xpath('string(.)').get(default='').strip()  # 提取锚文本

            # === 附件（PDF/Word等） ===
            if abs_link.lower().endswith(attachment_extensions):
                if self.attachment_total2 >= self.max_total2:
                    continue
                yield scrapy.Request(
                    abs_link,
                    callback=self.parse_attachment,
                    meta={'anchor': anchor_text}
                )
            elif not is_valid_link(abs_link):
                continue
            # === 新闻详情页（传统） ===
            elif news_detail_pattern.search(abs_link):
                yield scrapy.Request(abs_link, callback=self.parse_article)
            # === 其他情况，继续爬链接 ===
            else:
                if not is_urlnavigte_crawled(abs_link):
                    save_urlnavigate_to_db(abs_link)
                    yield scrapy.Request(abs_link, callback=self.parse)



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
            # 1. 结构型路径
            '//*[@id="root"]//span[contains(text(),"20")]/text()',
            '//*[@id="root"]//td[contains(text(),"20")]/text()',
            '//table//span[contains(text(),"20")]/text()',
            '//table//td[contains(text(),"20")]/text()',
            # 2. 通用结构
            '//*[@class="time"]/text()',
            # ...（前面整理过的常用结构路径）
        ]
        pub_time_raw = ""
        for xp in pub_time_xpaths:
            pub_time_raw = response.xpath(xp).get()
            if pub_time_raw:
                pub_time_raw = pub_time_raw.strip()
                if pub_time_raw:
                    break
        # 3. 全页面兜底
        if not pub_time_raw:
            texts = response.xpath('//body//text()').getall()
            pub_time_raw = "".join(texts)
        # 4. 用正则提取时间
        import re
        pub_time = ""
        if pub_time_raw:
            m = re.search(r"\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2}[日]? ?\d{0,2}:?\d{0,2}", pub_time_raw)
            if m:
                pub_time = m.group(0)
        # 提取正文中的链接
        # 提取正文中所有 a 标签对象
        pub_time=normalize_pub_time(pub_time)
        if not pub_time:
            pub_time = None  # 让MySQL存为NULL
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
