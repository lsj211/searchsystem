from elasticsearch import Elasticsearch
# es_utils.py
import hashlib
import sys
import os

import pymysql
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


# def create_index_if_not_exists(index_name):
#     """如果索引不存在，则创建，并定义 mapping"""
#     if es.indices.exists(index=index_name):
#         print(f"索引 {index_name} 已存在")
#         return

#     mapping = {
#         "mappings": {
#             "properties": {
#                 "url": {"type": "keyword"},               # URL 使用 keyword 类型，适合精确匹配
#                 "title": {"type": "text", "analyzer": "ik_max_word"},  # 标题使用 text 类型，适合分词查询
#                 "pub_time": {"type": "date", "format": "yyyy-MM-dd||yyyy-MM-dd HH:mm||yyyy-MM-dd HH:mm:ss"},  # 发布时间使用 date 类型，格式为“yyyy-MM-dd HH:mm”
#                 "content": {"type": "text", "analyzer": "ik_max_word"},  # 文章内容使用 text 类型，适合分词查询
#                 "snapshot_path": {"type": "keyword"},     # 快照路径使用 keyword 类型，适合精确匹配
#                 "anchor": {"type": "text"},               # 锚文本使用 text 类型，支持分词查询
#                 "type": {"type": "keyword"}               # 类型字段使用 keyword 类型，适合精确匹配
#             }
#         }
#     }

#     es.indices.create(index=index_name, body=mapping)
#     print(f"索引 {index_name} 创建成功")

def create_index_if_not_exists(index_name):
    """如果索引不存在，则创建，并定义 mapping"""
    if es.indices.exists(index=index_name):
        print(f"索引 {index_name} 已存在")
        return

    mapping = {
        "mappings": {
            "properties": {
                "url": {"type": "keyword"},
                "title": {"type": "text", "analyzer": "ik_max_word"},
                "pub_time": {
                    "type": "date",
                    "format": "strict_date_optional_time||yyyy-MM-dd HH:mm:ss"
                },
                "content": {"type": "text", "analyzer": "ik_max_word"},
                "snapshot_path": {"type": "keyword"},
                "anchor": {"type": "text"},
                "type": {"type": "keyword"}
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
    es.update(index="nankai_news1", id=doc_id, doc={"doc": {"snapshot_path": snapshot_path}})

def get_snapshot_path_from_es(url):
    # 通过 Elasticsearch 查询该 URL 对应的快照路径
    try:
        response = es.search(index="nankai_news1", body={
            "query": {
                "term": {
                    "url": url
                }
            }
        })
        if response['hits']['total']['value'] > 0:
            # 从结果中获取 snapshot_path
            return response['hits']['hits'][0]['_source'].get('snapshot_path', None)
        else:
            return None
    except Exception as e:
        print(f"Error fetching snapshot from Elasticsearch: {e}")
        return None


# def simple_search(index_name, keyword, fields=None, size=20):
#     """
#     在指定索引下，按关键词全文搜索，默认查 title 和 content 字段
#     :param index_name: ES 索引名
#     :param keyword: 搜索关键词
#     :param fields: 搜索的字段列表（如 ["title", "content"]），默认查 title 和 content
#     :param size: 返回结果数量
#     :return: 命中的文档列表
#     """
#     if fields is None:
#         fields = ["title", "content"]
#     body = {
#         "query": {
#             "multi_match": {
#                 "query": keyword,
#                 "fields": fields
#             }
#         },
#         "size": size
#     }
#     resp = es.search(index=index_name, body=body)
#     return [hit["_source"] for hit in resp["hits"]["hits"]]


def simple_search(index_name, keyword, fields=None, size=20, doc_type=None):
    if fields is None:
        fields = ["title", "content"]
    query = {
        "bool": {
            "must": [
                {
                    "multi_match": {
                        "query": keyword,
                        "fields": fields
                    }
                }
            ]
        }
    }
    if doc_type:
        query["bool"]["filter"] = {
            "term": {
                "type": doc_type
            }
        }
    body = {
        "query": query,
        "size": size
    }
    resp = es.search(index=index_name, body=body)
    return [hit["_source"] for hit in resp["hits"]["hits"]]


def phrase_search(index_name, keyword, fields=None, size=20, doc_type=None):
    if fields is None:
        fields = ["title", "content"]
    
    terms = keyword.strip().split()  # 按空格拆分成多个短语词
    must_clauses = []
    
    # 针对每个field，给每个term都加一个match_phrase查询
    for term in terms:
        for field in fields:
            must_clauses.append({
                "match_phrase": {
                    field: term
                }
            })
    
    query = {
        "bool": {
            "must": must_clauses
        }
    }
    
    if doc_type:
        query["bool"]["filter"] = {
            "term": {
                "type": doc_type
            }
        }
    
    body = {
        "query": query,
        "size": size
    }
    
    resp = es.search(index=index_name, body=body)
    return [hit["_source"] for hit in resp["hits"]["hits"]]


# def wildcard_search(index_name, keyword, doc_type='html'):
#     body = {
#         "query": {
#             "bool": {
#                 "must": [
#                     {
#                         "wildcard": {
#                             "content": {
#                                 "value": keyword.lower(),  # 通配符查询时，通常转为小写
#                                 "boost": 1.0,
#                                 "rewrite": "constant_score"
#                             }
#                         }
#                     }
#                 ],
#                 "filter": [
#                     {
#                         "term": {
#                             "doc_type": doc_type  # 通过 doc_type 过滤
#                         }
#                     }
#                 ]
#             }
#         }
#     }

#     response = es.search(index=index_name, body=body)
#     return [hit['_source'] for hit in response['hits']['hits']]





def wildcard_search(index_name, keyword, fields=None, size=20, doc_type=None):
    print(doc_type)
    print(keyword)
    query={
            "bool": {
                "should": [
                    {
                        "wildcard": {
                            "title": {
                                "value": keyword.lower(),  # 通配符查询时，通常转为小写
                                "boost": 2.0,
                                "rewrite": "constant_score"
                            }
                        }
                    },
                    {
                        "wildcard": {
                            "content": {
                                "value":  keyword.lower(),
                                "boost": 1.0,
                                "rewrite": "constant_score"
                            }
                        }
                    }
                ],
            }
    }
    if doc_type:
        query["bool"]["filter"] = {
            "term": {
                "type": doc_type
            }
        }
    
    body = {
        "query": query,
        "size": size
    }
    response = es.search(index=index_name, body=body)
    return [hit['_source'] for hit in response['hits']['hits']]


# print(wildcard_search("nankai_news1","*南*",'html'))
# resp = es.search(index="nankai_news1", body={   "query": {"match": {"title": "南"}} ,
#   "fields": ["title"], "size": 10})

# for hit in resp["hits"]["hits"]:
#     print(hit["_source"]["title"], hit["_source"]["url"])


index_name = "nankai_news"  # 你自己的索引名

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

# count_result = es.count(index="nankai_news1")
# print("Document count:", count_result['count'])
# es.delete_by_query(index="nankai_news1", body={"query": {"match_all": {}}})
# es.indices.delete(index="nankai_news")

# index_name = "nankai_news"
# es.indices.delete(index=index_name, ignore=[400, 404])
# print(es.indices.exists(index="nankai_news"))  # 返回False表示已删除
# create_index_if_not_exists(index_name)
# conn = pymysql.connect(
#     host='localhost',
#     user='root',
#     password='20050721',
#     database='my_spider_db',
#     charset='utf8mb4'
# )
# cursor = conn.cursor(pymysql.cursors.DictCursor)
# index_name = "nankai_news"
# cursor.execute("SELECT * FROM crawled_articles")
# for row in cursor.fetchall():
#     # 4. 构建ES文档
#     doc_id = hashlib.md5(row["url"].encode("utf-8")).hexdigest()
#     doc = {
#         "title": row["title"],
#         "url": row["url"],
#         "pub_time": row["pub_time"].isoformat() if row["pub_time"] else None,
#         "content": row["content"],
#         "snapshot_path": row["snapshot_path"] or "",
#         "anchor": row["anchor"] or "",
#         "type": row["type"] or "",
#         "crawled_at": row["crawled_at"].isoformat() if row["crawled_at"] else None
#     }
#     es.index(index=index_name, id=doc_id, body=doc)
#     print(f"Indexed: {row['url']}")

# conn.close()
# es.transport.close()