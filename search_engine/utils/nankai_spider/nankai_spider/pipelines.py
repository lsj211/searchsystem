# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


# class NankaiSpiderPipeline:
#     def process_item(self, item, spider):
#         return item
import pymysql
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

class MySQLPipeline:
    def __init__(self):
        # 初始化 MySQL 连接
        self.connection = pymysql.connect(
            host='localhost',
            user='root',
            password='20050721',
            database='my_spider_db2',
            charset='utf8mb4'
        )
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        # 在爬虫结束时关闭 MySQL 连接
        self.connection.close()

    def process_item(self, item, spider):
        pub_time = item.get('pub_time')
        if not pub_time:  # '' 或 None 都成立
            pub_time = None
        """保存数据到 MySQL"""
        sql = """
        INSERT INTO crawled_articles (title, url, pub_time, content, snapshot_path, anchor, type)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(sql, (item['title'], item['url'], pub_time, item['content'], item['snapshot_path'], item['anchor'], item['type']))
        self.connection.commit()
        return item




from es_utils import index_document
from es_utils import create_index_if_not_exists
import hashlib

class ElasticsearchPipeline:
    def __init__(self):
        # 初始化 Elasticsearch 索引
        self.index_name = "nankai_news1"
        self.count = 0
        # 确保索引存在，如果不存在则创建
        create_index_if_not_exists(self.index_name)

    def close_spider(self, spider):
        spider.logger.info(f"ElasticsearchPipeline processed {self.count} items.")
        
    def process_item(self, item, spider):
        """将数据索引到 Elasticsearch"""
        pub_time = item.get('pub_time')
        if not pub_time:
            pub_time = None
        # 构建索引文档
        doc = {
            "title": item['title'],
            "url": item['url'],
            "pub_time": pub_time,
            "content": item['content'],
            "snapshot_path": item.get('snapshot_path', ""),
            "anchor": item.get('anchor', ""),
            "type": item['type']
        }

        # 使用 URL 的哈希值作为 Elasticsearch 文档的 ID
        doc_id = hashlib.md5(item['url'].encode("utf-8")).hexdigest()
        self.count += 1
        # 调用工具类中的方法，将文档写入 Elasticsearch
        index_document(self.index_name, doc_id, doc)
        return item
