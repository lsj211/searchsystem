from flask import Flask, jsonify, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import pymysql
import requests
from utils.es_utils import simple_search,phrase_search,wildcard_search,update_snapshot_path_in_es,get_snapshot_path_from_es
from utils.snapshot_helper import save_snapshot,update_snapshot_in_db


app = Flask(__name__)
app.secret_key = 'your_secret_key'

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


def get_db_connection():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='20050721',
        database='searchsystem',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# 假设有一个User类和数据库连接
class User(UserMixin):
    def __init__(self, id, username, password):
        self.id = id
        self.username = username
        self.password = password

    def get_id(self):
        return str(self.id)  # 返回用户 ID（必须是字符串类型）

def get_user_by_username(username):
    # 这里用pymysql直接查数据库，实际生产推荐用ORM
    # connection = pymysql.connect(host='localhost', user='root', password='20050721', database='searchsystem')
    connection=get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id, username, password FROM users WHERE username=%s"
            cursor.execute(sql, (username,))
            result = cursor.fetchone()
            print(result)
            if result:
                return User(**result)
    finally:
        connection.close()
    return None

@login_manager.user_loader
def load_user(user_id):
    # connection = pymysql.connect(host='localhost', user='root', password='20050721', database='searchsystem')
    connection=get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT id, username, password FROM users WHERE id=%s"
            cursor.execute(sql, (user_id,))
            result = cursor.fetchone()
            if result:
                return User(**result)
    finally:
        connection.close()
    
    return None


@app.route('/login', methods=['GET', 'POST'])
def login():
    logout_user()  # 自动清除旧登录状态
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password'].strip()
        user = get_user_by_username(username)
        print(user.password)
        print(user.username)
        print(username)
        if user and user.password == password:  
            login_user(user)
            return redirect(url_for('search'))  
        else:
            flash('用户名或密码错误')
    return render_template('login.html')

@app.route('/')
@login_required
def index():
    return redirect(url_for('login')) 

#注册

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password'].strip()
        if not username or not password:
            flash('用户名和密码不能为空')
            return render_template('register.html')
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 查重
                sql = "SELECT * FROM users WHERE username=%s"
                cursor.execute(sql, (username,))
                user = cursor.fetchone()
                if user:
                    flash('用户名已存在')
                    return render_template('register.html')
                # 插入用户
                sql = "INSERT INTO users (username, password) VALUES (%s, %s)"
                cursor.execute(sql, (username, password))
                conn.commit()
                flash('注册成功，请登录')
                return redirect(url_for('login'))
        finally:
            conn.close()
    return render_template('register.html')



from datetime import datetime
def log_search_to_db(keyword, query_type, is_phrase, user_id):
    conn = get_db_connection()
    try:
        # 关闭自动提交，开启事务
        conn.autocommit(False)
        with conn.cursor() as cursor:
            # 1. 查询是否存在相同 user_id 和 keyword 的旧记录
            exists_sql = "SELECT 1 FROM search_history WHERE user_id = %s AND keyword = %s LIMIT 1"
            cursor.execute(exists_sql, (user_id, keyword))
            
            if cursor.fetchone():
                # 2. 存在旧记录则删除
                delete_sql = "DELETE FROM search_history WHERE user_id = %s AND keyword = %s"
                cursor.execute(delete_sql, (user_id, keyword))
            
            # 3. 插入新记录（无论是否删除过旧记录）
            insert_sql = """
                INSERT INTO search_history 
                (user_id, keyword, query_type, is_phrase, search_time)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (user_id, keyword, query_type, is_phrase, datetime.now()))
        
        # 提交事务
        conn.commit()
    except Exception as e:
        # 异常时回滚事务
        conn.rollback()
        raise e  # 可选：抛出异常通知调用方
    finally:
        # 恢复自动提交并关闭连接
        conn.autocommit(True)
        conn.close()
# def log_search_to_db(keyword, query_type, is_phrase, user_id=None):
#     conn = get_db_connection()
#     try:
#         with conn.cursor() as cursor:
#             sql = "INSERT INTO search_history (user_id, keyword, query_type, is_phrase, search_time) VALUES (%s, %s, %s, %s, %s)"
#             cursor.execute(sql, (user_id, keyword, query_type, is_phrase, datetime.now()))
#             conn.commit()                      
#     finally:
#         conn.close()

#普通搜索
@app.route('/search', methods=['GET', 'POST'])
def search():
    results = []
    keyword = ""
    query_type='web'
    is_phrase=False
    is_wildcard=False
    if request.method == 'POST':
        keyword = request.form.get('keyword', '')
        query_type = request.form.get('query_type', 'web')
        is_phrase = request.form.get('is_phrase', 'false').lower() == 'true'  # 新增：是否短语查询
        is_wildcard = request.form.get('is_wildcard', 'false').lower() == 'true'
        if keyword:
            doc_type = 'html' if query_type == 'web' else 'attachment'
            if is_phrase:
                # 短语查询
                log_search_to_db(keyword,query_type,is_phrase,user_id=int(current_user.get_id()))
                results = phrase_search('nankai_news1', keyword, doc_type=doc_type)
            elif is_wildcard:
                keyword = keyword.strip()
                results= wildcard_search('nankai_news1', keyword, doc_type=doc_type)
            else:    
                log_search_to_db(keyword,query_type,is_phrase,user_id=int(current_user.get_id()))
                keyword = keyword.strip()
                # 普通关键词查询
                results = simple_search('nankai_news1', keyword, doc_type=doc_type)

            # 可选：过滤 pub_time 不为 None 的结果
            # results = [r for r in results if r.get('pub_time') is not None]

    return render_template('search.html', results=results, keyword=keyword, query_type=query_type, is_phrase=is_phrase,is_wildcard=is_wildcard)


from flask_login import current_user

@app.route('/search_history', methods=['GET'])
def search_history():
    if current_user.is_authenticated:
        user_id = int(current_user.get_id())
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT keyword 
                    FROM search_history 
                    WHERE user_id = %s 
                    ORDER BY search_time DESC 
                    LIMIT 10
                """, (user_id,))
                history = [row['keyword'] for row in cursor.fetchall()]
        finally:
            conn.close()
    else:
        history = []
    return jsonify(history)


@app.route('/view_snapshot', methods=['GET'])
def view_snapshot():
    url = request.args.get('url')  # 获取用户点击的 URL
    if not url:
        return "URL is missing", 400
    
    # 先尝试从 Elasticsearch 获取快照路径
    snapshot_path = get_snapshot_path_from_es(url)
    
    # 如果找到了快照路径并且文件存在
    if snapshot_path and os.path.exists(snapshot_path):
        with open(snapshot_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return html_content  # 返回快照内容
    
    # 如果快照不存在或文件没有找到，则请求真实网页
    try:
        response = requests.get(url)
        response.raise_for_status()  # 确保请求成功
        html_text = response.text  # 获取 HTML 内容
        
        # 尝试保存网页快照
        snapshot_path = save_snapshot(url, html_text)
        
        if snapshot_path:
            # 快照保存成功，更新数据库和 Elasticsearch 路径
            update_snapshot_in_db(url, snapshot_path)
            update_snapshot_path_in_es(url, snapshot_path)
        
        # 返回网页的真实内容
        return html_text
    
    except requests.exceptions.RequestException as e:
        return f"Failed to fetch the page: {e}", 500


if __name__ == '__main__':
    app.run(debug=True)