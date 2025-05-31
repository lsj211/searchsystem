from elasticsearch import Elasticsearch
# es_utils.py
import hashlib
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)
from search_engine.config import ES_HOST, ES_PORT, ES_USER, ES_PASSWORD, ES_CA_CERTS

# 连接 ES
es = Elasticsearch(
    [f"https://{ES_HOST}:{ES_PORT}"],
    basic_auth=(ES_USER, ES_PASSWORD),
    ca_certs=ES_CA_CERTS,
    verify_certs=True
)

def is_connected():
    """测试ES连接"""
    return es.ping()

def print_cluster_health():
    """打印集群健康状态"""
    print(es.cluster.health())

def analyze_text(text, analyzer="ik_max_word"):
    """对文本进行分词，返回分词列表"""
    response = es.indices.analyze(body={
        "analyzer": analyzer,
        "text": text
    })
    tokens = [token["token"] for token in response.get("tokens", [])]
    return tokens


def create_index_if_not_exists(index_name):
    """如果索引不存在，则创建，并定义 mapping"""
    if es.indices.exists(index=index_name):
        print(f"索引 {index_name} 已存在")
        return

    mapping = {
        "mappings": {
            "properties": {
                "url": {"type": "keyword"},               # URL 使用 keyword 类型，适合精确匹配
                "title": {"type": "text", "analyzer": "ik_max_word"},  # 标题使用 text 类型，适合分词查询
                "pub_time": {"type": "date", "format": "yyyy-MM-dd HH:mm"},  # 发布时间使用 date 类型，格式为“yyyy-MM-dd HH:mm”
                "content": {"type": "text", "analyzer": "ik_max_word"},  # 文章内容使用 text 类型，适合分词查询
                "snapshot_path": {"type": "keyword"},     # 快照路径使用 keyword 类型，适合精确匹配
                "anchor": {"type": "text"},               # 锚文本使用 text 类型，支持分词查询
                "type": {"type": "keyword"}               # 类型字段使用 keyword 类型，适合精确匹配
            }
        }
    }

    es.indices.create(index=index_name, body=mapping)
    print(f"索引 {index_name} 创建成功")



def index_document(index, doc_id, document):
    """写入文档到指定索引,doc_id 用于去重"""
    return es.index(index=index, id=doc_id, document=document)

#更新快照路径
def update_snapshot_path_in_es(url, snapshot_path):
    doc_id = hashlib.md5(url.encode("utf-8")).hexdigest()
    es.update(index="nankai_news", id=doc_id, doc={"doc": {"snapshot_path": snapshot_path}})