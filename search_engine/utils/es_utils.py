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
                "pub_time": {"type": "date", "format": "yyyy-MM-dd||yyyy-MM-dd HH:mm||yyyy-MM-dd HH:mm:ss"},  # 发布时间使用 date 类型，格式为“yyyy-MM-dd HH:mm”
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




# resp = es.search(index="nankai_news", body={"query": {"match_all": {}}, "size": 10})

# for hit in resp["hits"]["hits"]:
#     print(hit["_source"]["title"], hit["_source"]["url"])


#index_name = "nankai_news"  # 你自己的索引名

# # 查询关键词，比如查标题或内容里包含“南开”
# query = {
#     "query": {
#         "multi_match": {
#             "query": "南开",
#             "fields": ["title", "content"]
#         }
#     },
#     "size": 2  # 返回5条结果
# }

# resp = es.search(index=index_name, body=query)

# print(f"共找到 {resp['hits']['total']['value']} 条结果，显示前{len(resp['hits']['hits'])}条：")
# for hit in resp['hits']['hits']:
#     source = hit['_source']
#     print(f"标题: {source.get('title')}")
#     print(f"链接: {source.get('url')}")
#     print(f"发布时间: {source.get('pub_time')}")
#     print(f"内容摘要: {source.get('content')[:100]}...")  # 打印内容前100字
#     print("-" * 40)

# count_result = es.count(index="nankai_news")
# print("Document count:", count_result['count'])
es.delete_by_query(index="nankai_news", body={"query": {"match_all": {}}})
# es.indices.delete(index="nankai_news")