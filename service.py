from weibo import Weibo, handle_config_renaming, get_config as load_config_from_file
import const
import logging
import logging.config
import os
from flask import Flask, jsonify, request
import sqlite3
import json
from concurrent.futures import ThreadPoolExecutor
import threading
import uuid
import time
from datetime import datetime
from html import escape

# 1896820725 天津股侠 2024-12-09T16:47:04

DATABASE_PATH = './weibo/weibodata.db'
print(DATABASE_PATH)

# 如果日志文件夹不存在，则创建
if not os.path.isdir("log/"):
    os.makedirs("log/")
logging_path = os.path.split(os.path.realpath(__file__))[0] + os.sep + "logging.conf"
logging.config.fileConfig(logging_path)
logger = logging.getLogger("api")

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # 确保JSON响应中的中文不会被转义
app.config['JSONIFY_MIMETYPE'] = 'application/json;charset=utf-8'


def wants_html() -> bool:
    """
    判断当前请求是否更适合返回 HTML。
    - 浏览器访问（Accept=text/html）时返回表格 HTML
    - curl / 代码访问默认返回 JSON
    - 也可通过 ?format=html 或 ?format=json 强制指定
    """
    fmt = request.args.get("format")
    if fmt == "json":
        return False
    if fmt == "html":
        return True
    best = request.accept_mimetypes.best
    return best == "text/html"


@app.route('/', methods=['GET'])
def index():
    """根路径：列出所有可用接口及说明"""
    return """
    <html>
      <head>
        <meta charset="utf-8" />
        <title>Weibo Crawler Service</title>
      </head>
      <body>
        <h1>Weibo Crawler Service 接口列表</h1>
        <ul>
          <li>
            <strong>POST /refresh</strong> - 启动一次新的爬虫任务（需要 JSON 参数
            <code>{"user_id_list": ["uid1", "uid2"]}</code>）
          </li>
          <li>
            <a href="/tasks"><strong>GET /tasks</strong></a> - 列出所有任务及其状态
          </li>
          <li>
            <a href="/status"><strong>GET /status</strong></a> - 查看当前爬虫运行状态
          </li>
          <li>
            <a href="/weibos"><strong>GET /weibos</strong></a> - 查看数据库中全部微博列表（按时间倒序）
          </li>
        </ul>
      </body>
    </html>
    """

# 添加线程池和任务状态跟踪
executor = ThreadPoolExecutor(max_workers=1)  # 限制只有1个worker避免并发爬取
tasks = {}  # 存储任务状态

# 在executor定义后添加任务锁相关变量
current_task_id = None
task_lock = threading.Lock()

def get_running_task():
    """
    获取当前运行的任务信息。
    优先使用 current_task_id，如果丢失则从 tasks 中扫描处于运行中的任务。
    """
    # 先尝试用 current_task_id
    with task_lock:
        cid = current_task_id
        if cid and cid in tasks:
            task = tasks[cid]
            if task.get('state') in ['PENDING', 'PROGRESS']:
                return cid, task

        # 回退：从所有任务中找出正在运行的任务（按创建时间倒序，取最新）
        running = []
        for tid, t in tasks.items():
            if t.get('state') in ['PENDING', 'PROGRESS']:
                running.append((tid, t))

    if not running:
        return None, None

    running.sort(
        key=lambda x: x[1].get('created_at') or "",
        reverse=True,
    )
    return running[0]

def get_config(user_id_list=None):
    """
    从 config.json 加载配置，允许动态覆盖 user_id_list。
    其余配置（cookie、since_date 等）完全复用主程序的 config.json。
    """
    current_config = load_config_from_file()
    if user_id_list:
        current_config['user_id_list'] = user_id_list
    handle_config_renaming(current_config, oldName="filter", newName="only_crawl_original")
    handle_config_renaming(current_config, oldName="result_dir_name", newName="user_id_as_folder_name")
    return current_config

def run_refresh_task(task_id, user_id_list=None):
    global current_task_id
    try:
        tasks[task_id]['state'] = 'PROGRESS'
        tasks[task_id]['progress'] = 0
        
        config = get_config(user_id_list)
        wb = Weibo(config)
        tasks[task_id]['progress'] = 50
        
        wb.start()  # 爬取微博信息
        tasks[task_id]['progress'] = 100
        tasks[task_id]['state'] = 'SUCCESS'
        tasks[task_id]['result'] = {"message": "微博列表已刷新"}
        
    except Exception as e:
        tasks[task_id]['state'] = 'FAILED'
        tasks[task_id]['error'] = str(e)
        logger.exception(e)
    finally:
        with task_lock:
            if current_task_id == task_id:
                current_task_id = None

@app.route('/refresh', methods=['POST'])
def refresh():
    global current_task_id
    
    # 获取请求参数
    data = request.get_json()
    user_id_list = data.get('user_id_list') if data else None
    
    # 验证参数：支持 list（显式用户ID列表）或 str（user_id_list.txt 路径），
    # 与 weibo.py 的 config.json 约定保持一致
    if not user_id_list or not isinstance(user_id_list, (list, str)):
        return jsonify({
            'error': 'Invalid user_id_list parameter, must be list or txt path string'
        }), 400
    
    # 检查是否有正在运行的任务
    with task_lock:
        running_task_id, running_task = get_running_task()
        if running_task:
            return jsonify({
                'task_id': running_task_id,
                'status': 'Task already running',
                'state': running_task['state'],
                'progress': running_task['progress']
            }), 409  # 409 Conflict
        
        # 创建新任务
        task_id = str(uuid.uuid4())
        tasks[task_id] = {
            'state': 'PENDING',
            'progress': 0,
            'created_at': datetime.now().isoformat(),
            'user_id_list': user_id_list
        }
        current_task_id = task_id
        
    executor.submit(run_refresh_task, task_id, user_id_list)
    return jsonify({
        'task_id': task_id,
        'status': 'Task started',
        'state': 'PENDING',
        'progress': 0,
        'user_id_list': user_id_list
    }), 202

@app.route('/task/<task_id>', methods=['GET'])
def get_task_status(task_id):
    task = tasks.get(task_id)
    if not task:
        data = {'error': 'Task not found'}
        if wants_html():
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>任务详情</title></head>
              <body>
                <h1>任务 {escape(task_id)} 未找到</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  <tr><th>error</th><td>{escape(data['error'])}</td></tr>
                </table>
                <p><a href="/">返回首页</a></p>
              </body>
            </html>
            """
            return html, 404
        return jsonify(data), 404

    response = {
        'task_id': task_id,
        'state': task.get('state'),
        'progress': task.get('progress'),
        'created_at': task.get('created_at'),
        'user_id_list': task.get('user_id_list'),
    }
    if task.get('state') == 'SUCCESS':
        response['result'] = task.get('result')
    elif task.get('state') == 'FAILED':
        response['error'] = task.get('error')

    if wants_html():
        rows = []
        for k, v in response.items():
            rows.append(
                f"<tr><th>{escape(str(k))}</th><td>{escape(str(v))}</td></tr>"
            )
        html = f"""
        <html>
          <head><meta charset="utf-8"><title>任务详情 {escape(task_id)}</title></head>
          <body>
            <h1>任务详情</h1>
            <table border="1" cellspacing="0" cellpadding="4">
              {''.join(rows)}
            </table>
            <p><a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a></p>
          </body>
        </html>
        """
        return html

    return jsonify(response)


@app.route('/tasks', methods=['GET'])
def list_tasks():
    """
    列出所有任务及其状态（按 created_at 倒序）。
    只做状态查看，不会触发新的任务。
    """
    # 将 tasks 按 created_at 逆序排序
    items = []
    for tid, t in tasks.items():
        items.append({
            'task_id': tid,
            'state': t.get('state'),
            'progress': t.get('progress'),
            'created_at': t.get('created_at'),
            'user_id_list': t.get('user_id_list'),
        })
    items.sort(key=lambda x: x.get('created_at') or "", reverse=True)
    if wants_html():
        header_cells = "".join(
            f"<th>{escape(col)}</th>"
            for col in ["task_id", "state", "progress", "created_at", "user_id_list"]
        )
        body_rows = []
        for it in items:
            body_rows.append(
                "<tr>"
                # task_id 列作为链接，点击跳到 /task/<task_id>
                f"<td><a href=\"/task/{escape(it['task_id'])}\">{escape(it['task_id'])}</a></td>"
                f"<td>{escape(str(it.get('state')))}</td>"
                f"<td>{escape(str(it.get('progress')))}</td>"
                f"<td>{escape(str(it.get('created_at')))}</td>"
                f"<td>{escape(str(it.get('user_id_list')))}</td>"
                "</tr>"
            )
        html = f"""
        <html>
          <head><meta charset="utf-8"><title>任务列表</title></head>
          <body>
            <h1>任务列表</h1>
            <table border="1" cellspacing="0" cellpadding="4">
              <thead><tr>{header_cells}</tr></thead>
              <tbody>
                {''.join(body_rows)}
              </tbody>
            </table>
            <p><a href="/">返回首页</a></p>
          </body>
        </html>
        """
        return html
    return jsonify(items)


@app.route('/status', methods=['GET'])
def get_current_status():
    """
    查询当前爬虫运行状态。
    - 有任务运行时返回当前任务信息
    - 没有任务运行时返回 state=IDLE
    """
    running_task_id, running_task = get_running_task()
    if running_task:
        data = {
            'task_id': running_task_id,
            'state': running_task['state'],
            'progress': running_task['progress'],
            'created_at': running_task.get('created_at'),
            'user_id_list': running_task.get('user_id_list'),
        }
        if wants_html():
            # task_id 挂上跳转到 /task/<task_id> 的链接
            task_link = (
                f"<a href=\"/task/{escape(str(data['task_id']))}\">"
                f"{escape(str(data['task_id']))}</a>"
            )
            rows = [
                f"<tr><th>task_id</th><td>{task_link}</td></tr>",
                f"<tr><th>state</th><td>{escape(str(data['state']))}</td></tr>",
                f"<tr><th>progress</th><td>{escape(str(data['progress']))}</td></tr>",
                f"<tr><th>created_at</th><td>{escape(str(data['created_at']))}</td></tr>",
                f"<tr><th>user_id_list</th><td>{escape(str(data['user_id_list']))}</td></tr>",
            ]
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>当前状态</title></head>
              <body>
                <h1>当前运行任务</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  {''.join(rows)}
                </table>
                <p><a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a></p>
              </body>
            </html>
            """
            return html, 200
        return jsonify(data), 200

    # 没有正在运行的任务，尝试给出最近一个任务的简要信息（如果有）
    last_task_id = None
    last_task = None
    for tid, t in tasks.items():
        if last_task is None:
            last_task_id, last_task = tid, t
            continue
        try:
            ts_new = t.get('created_at') or ''
            ts_old = last_task.get('created_at') or ''
            if ts_new > ts_old:
                last_task_id, last_task = tid, t
        except Exception:
            continue

    if last_task:
        data = {
            'state': 'IDLE',
            'current_task': None,
            'last_task_id': last_task_id,
            'last_state': last_task.get('state'),
            'last_progress': last_task.get('progress'),
            'last_created_at': last_task.get('created_at'),
        }
    else:
        data = {
            'state': 'IDLE',
            'current_task': None,
        }

    if wants_html():
        rows = []
        for k, v in data.items():
            # last_task_id 挂上跳转到 /task/<last_task_id> 的链接
            if k == "last_task_id" and v:
                link = (
                    f"<a href=\"/task/{escape(str(v))}\">"
                    f"{escape(str(v))}</a>"
                )
                rows.append(
                    f"<tr><th>{escape(str(k))}</th><td>{link}</td></tr>"
                )
            else:
                rows.append(
                    f"<tr><th>{escape(str(k))}</th><td>{escape(str(v))}</td></tr>"
                )
        html = f"""
        <html>
          <head><meta charset="utf-8"><title>当前状态</title></head>
          <body>
            <h1>当前状态</h1>
            <table border="1" cellspacing="0" cellpadding="4">
              {''.join(rows)}
            </table>
            <p><a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a></p>
          </body>
        </html>
        """
        return html, 200
    return jsonify(data), 200

@app.route('/weibos', methods=['GET'])
def get_weibos():
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        # 按created_at倒序查询所有微博
        cursor.execute("SELECT * FROM weibo ORDER BY created_at DESC")
        columns = [column[0] for column in cursor.description]
        weibos = []
        for row in cursor.fetchall():
            weibo = dict(zip(columns, row))
            weibos.append(weibo)
        conn.close()
        if wants_html():
            # 简单表格展示所有字段，其中 id 列可点击跳转到 /weibos/<weibo_id>
            header_cells = "".join(f"<th>{escape(str(col))}</th>" for col in columns)
            body_rows = []
            for item in weibos:
                row_cells = []
                for col in columns:
                    val = item.get(col)
                    if col == "id":
                        cell = (
                            f"<td><a href=\"/weibos/{escape(str(val))}\">"
                            f"{escape(str(val))}</a></td>"
                        )
                    else:
                        cell = f"<td>{escape(str(val))}</td>"
                    row_cells.append(cell)
                body_rows.append("<tr>" + "".join(row_cells) + "</tr>")
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>微博列表</title></head>
              <body>
                <h1>微博列表</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  <thead><tr>{header_cells}</tr></thead>
                  <tbody>
                    {''.join(body_rows)}
                  </tbody>
                </table>
                <p><a href=\"/\">返回首页</a></p>
              </body>
            </html>
            """
            return html, 200

        res = jsonify(weibos)
        return res, 200
    except Exception as e:
        logger.exception(e)
        return {"error": str(e)}, 500

@app.route('/weibos/<weibo_id>', methods=['GET'])
def get_weibo_detail(weibo_id):
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM weibo WHERE id=?", (weibo_id,))
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
        conn.close()
        
        if row:
            weibo = dict(zip(columns, row))
            if wants_html():
                rows = []
                for k, v in weibo.items():
                    rows.append(
                        f"<tr><th>{escape(str(k))}</th><td>{escape(str(v))}</td></tr>"
                    )
                html = f"""
                <html>
                  <head><meta charset="utf-8"><title>微博详情 {escape(str(weibo_id))}</title></head>
                  <body>
                    <h1>微博详情</h1>
                    <table border="1" cellspacing="0" cellpadding="4">
                      {''.join(rows)}
                    </table>
                    <p><a href=\"/weibos\">返回微博列表</a> | <a href=\"/\">返回首页</a></p>
                  </body>
                </html>
                """
                return html, 200
            return jsonify(weibo), 200
        else:
            data = {"error": "Weibo not found"}
            if wants_html():
                html = f"""
                <html>
                  <head><meta charset="utf-8"><title>微博未找到</title></head>
                  <body>
                    <h1>微博 {escape(str(weibo_id))} 未找到</h1>
                    <table border="1" cellspacing="0" cellpadding="4">
                      <tr><th>error</th><td>{escape(data['error'])}</td></tr>
                    </table>
                    <p><a href=\"/weibos\">返回微博列表</a> | <a href=\"/\">返回首页</a></p>
                  </body>
                </html>
                """
                return html, 404
            return data, 404
    except Exception as e:
        logger.exception(e)
        return {"error": str(e)}, 500

def schedule_refresh():
    """定时刷新任务"""
    while True:
        try:
            # 每轮调度时从 config.json 读取默认的 user_id_list
            base_cfg = load_config_from_file()
            default_user_ids = base_cfg.get('user_id_list') or []

            # 检查是否有运行中的任务
            running_task_id, running_task = get_running_task()
            if not running_task:
                task_id = str(uuid.uuid4())
                tasks[task_id] = {
                    'state': 'PENDING',
                    'progress': 0,
                    'created_at': datetime.now().isoformat(),
                    'user_id_list': default_user_ids  # 使用 config.json 中的默认配置
                }
                with task_lock:
                    global current_task_id
                    current_task_id = task_id
                executor.submit(run_refresh_task, task_id, default_user_ids)
                logger.info(f"Scheduled task {task_id} started")
            
            time.sleep(600)  # 10分钟间隔
        except Exception as e:
            logger.exception("Schedule task error")
            time.sleep(60)  # 发生错误时等待1分钟后重试

if __name__ == "__main__":
    # 启动定时任务线程
    scheduler_thread = threading.Thread(target=schedule_refresh, daemon=True)
    scheduler_thread.start()
    
    logger.info("服务启动")
    # 启动Flask应用
    app.run(debug=True, use_reloader=False)  # 关闭reloader避免启动两次
