# search_engine/utils/snapshot_helper.py
import os, hashlib

import pymysql



#判断快照是否储存
def is_url_snapshot_saved(url):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            sql = "SELECT 1 FROM crawled_snapshots WHERE url=%s LIMIT 1"
            cursor.execute(sql, (url,))
            return cursor.fetchone() is not None
    finally:
        connection.close()

def save_url_snapshot(url):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            sql = "INSERT IGNORE INTO crawled_snapshots (url) VALUES (%s)"
            cursor.execute(sql, (url,))
        connection.commit()
    finally:
        connection.close()

def save_snapshot(url, html_text):
    """保存网页快照"""
    if is_url_snapshot_saved(url):
        return None
    snapshot_dir = "H:/SearchSystem/search_engine/static/snapshots"
    os.makedirs(snapshot_dir, exist_ok=True)

    # 生成唯一的 HTML 文件名
    filename = hashlib.md5(url.encode("utf-8")).hexdigest() + ".html"
    filepath = os.path.join(snapshot_dir, filename)

    # 保存 HTML 快照
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html_text)

    save_url_snapshot(url)  # 保存 URL 防止重复抓取
    return filepath



def update_snapshot_in_db(url, snapshot_path):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='my_spider_db',
        charset='utf8mb4'
    )
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE crawled_articles SET snapshot_path=%s WHERE url=%s"
            cursor.execute(sql, (snapshot_path, url))
            connection.commit()
    finally:
        connection.close()