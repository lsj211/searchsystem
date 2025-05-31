# Elasticsearch 连接配置
ES_HOST = "localhost"  # Elasticsearch 主机地址，若为远程服务器需替换为对应 IP
ES_PORT = 9200  # 端口，确保与 elasticsearch.yml 中 http.port 一致
ES_USER = "elastic"  # 用户名，默认 "elastic"
ES_PASSWORD = "ijVnjCtuW0b0U4uk+bic"  # 替换为你的 Elasticsearch 密码
ES_CA_CERTS = r"H:\elasticsearch\elasticsearch-9.0.1\config\certs\http_ca.crt"  # 证书路径，原始字符串避免转义问题