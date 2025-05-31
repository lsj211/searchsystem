# search_engine/routes/snapshot_routes.py

import os
from flask import Blueprint, request, jsonify
import requests
from search_engine.utils.snapshot_helper import (
    save_snapshot,
    update_snapshot_in_db
)

from search_engine.utils.es_utils import update_snapshot_path_in_es 

snapshot_bp = Blueprint("snapshot", __name__)
def get_relative_snapshot_path(snapshot_path):
    parts = snapshot_path.split("static" + os.sep)
    if len(parts) > 1:
        return "static/" + parts[-1].replace("\\", "/")
    else:
        return snapshot_path  # fallback

@snapshot_bp.route("/snapshot", methods=["POST"])

def save_snapshot_by_url():
    url = request.json.get("url")
    if not url:
        return jsonify({"error": "缺少 URL"}), 400

    try:
        response = requests.get(url, timeout=10)
        response.encoding = response.apparent_encoding
    except Exception as e:
        return jsonify({"error": f"下载失败: {str(e)}"}), 500
    
    content_type = response.headers.get("Content-Type", "").lower()
    if "text/html" not in content_type:
        return jsonify({"error": f"URL 内容类型为 {content_type}，不支持构建快照"}), 400

    # 保存快照（内部已判断是否重复）
    snapshot_path = save_snapshot(url, response.text)
    if snapshot_path is None:
        return jsonify({"message": "快照已存在"})

    # 更新数据库和 Elasticsearch
    update_snapshot_in_db(url, get_relative_snapshot_path(snapshot_path))
    update_snapshot_path_in_es(url,  get_relative_snapshot_path(snapshot_path))

    return jsonify({"message": "快照保存成功", "snapshot_path": snapshot_path})

