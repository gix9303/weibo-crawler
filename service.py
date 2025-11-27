from weibo import Weibo, handle_config_renaming, get_config as load_config_from_file
import const
import logging
import logging.config
import os
from flask import Flask, jsonify, request, redirect, send_file
import sqlite3
import json
from concurrent.futures import ThreadPoolExecutor
import threading
import uuid
import time
from datetime import datetime
from html import escape
from util.notify import push_deer
import re

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
    # 简化逻辑：只要 Accept 头里包含 text/html，就认为是浏览器，返回 HTML
    accept = request.headers.get("Accept", "")
    if "text/html" in accept:
        return True
    return False


def _truncate_middle(text: str, max_len: int = 80) -> str:
    """
    将过长的字符串中间用 ... 代替，避免表格太宽。
    例如：abcdef...uvwxyz
    """
    if text is None:
        return ""
    s = str(text)
    if len(s) <= max_len:
        return s
    keep = max_len - 3
    head = keep // 2
    tail = keep - head
    return s[:head] + "..." + s[-tail:]


def _is_link_field(field_name: str) -> bool:
    """
    判断字段名是否为链接类字段，在 HTML 展示中可隐藏。
    规则：
    - 显式列出的若干字段：pics, video_url, live_photo_url 等
    - 以 _url 或 _urls 结尾的字段
    """
    name = (field_name or "").lower()
    explicit = {
        "pics",
        "video_url",
        "live_photo_url",
    }
    if name in explicit:
        return True
    if name.endswith("_url") or name.endswith("_urls"):
        return True
    return False


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
            <a href="/refresh"><strong>POST /refresh</strong></a> - 启动一次新的爬虫任务
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

# 使用 SQLite 持久化任务列表，仅用少量内存变量做并发控制
current_task_id = None  # 当前正在后台执行的任务 ID
task_lock = threading.Lock()  # 防止同时创建/修改 current_task_id


def _init_tasks_table():
    """初始化 tasks 表，用于持久化任务列表。"""
    # 确保数据库目录存在（例如 ./weibo），否则 sqlite 无法创建数据库文件
    db_dir = os.path.dirname(os.path.abspath(DATABASE_PATH))
    if db_dir and (not os.path.isdir(db_dir)):
        try:
            os.makedirs(db_dir, exist_ok=True)
        except Exception as e:
            logger.warning("创建数据库目录失败 %s: %s", db_dir, e)

    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id      TEXT PRIMARY KEY,
                state        TEXT,
                progress     INTEGER,
                created_at   TEXT,
                user_id_list TEXT,
                command      TEXT,
                error        TEXT,
                result       TEXT
            )
            """
        )
        conn.commit()
    except Exception as e:
        logger.warning("初始化任务表失败: %s", e)
    finally:
        if conn:
            conn.close()


_init_tasks_table()


class TaskStopped(Exception):
    """用户请求停止当前任务时抛出的异常，用于优雅退出。"""
    pass

def _row_to_task(row):
    """将 tasks 表中的一行转换为字典。"""
    if not row:
        return None
    (
        task_id,
        state,
        progress,
        created_at,
        user_id_json,
        command,
        error,
        result,
    ) = row
    user_id_list = None
    if user_id_json:
        try:
            user_id_list = json.loads(user_id_json)
        except Exception:
            user_id_list = user_id_json
    return {
        "task_id": task_id,
        "state": state,
        "progress": progress if progress is not None else 0,
        "created_at": created_at,
        "user_id_list": user_id_list,
        "command": command,
        "error": error,
        "result": result,
    }


def db_create_task(task_id: str, user_id_list):
    """在数据库中创建一条新任务记录，初始为 PENDING。"""
    user_id_json = None
    if user_id_list is not None:
        try:
            user_id_json = json.dumps(user_id_list, ensure_ascii=False)
        except Exception:
            user_id_json = str(user_id_list)
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tasks (task_id, state, progress, created_at, user_id_list, command)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                "PENDING",
                0,
                datetime.now().isoformat(),
                user_id_json,
                "RUNNING",
            ),
        )
        conn.commit()
    except Exception as e:
        logger.warning("创建任务记录失败: %s", e)
    finally:
        if conn:
            conn.close()


def db_update_task(task_id: str, **fields):
    """更新任务记录中的指定字段。"""
    if not fields:
        return
    allowed = {"state", "progress", "created_at", "user_id_list", "command", "error", "result"}
    sets = []
    params = []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k == "user_id_list" and v is not None:
            try:
                v = json.dumps(v, ensure_ascii=False)
            except Exception:
                v = str(v)
        if k == "result" and isinstance(v, (dict, list)):
            try:
                v = json.dumps(v, ensure_ascii=False)
            except Exception:
                v = str(v)
        sets.append(f"{k} = ?")
        params.append(v)
    if not sets:
        return
    params.append(task_id)
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        sql = f"UPDATE tasks SET {', '.join(sets)} WHERE task_id = ?"
        cur.execute(sql, params)
        conn.commit()
    except Exception as e:
        logger.warning("更新任务 %s 失败: %s", task_id, e)
    finally:
        if conn:
            conn.close()


def db_get_task(task_id: str):
    """从数据库获取单个任务字典。"""
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT task_id, state, progress, created_at, user_id_list, command, error, result
            FROM tasks WHERE task_id = ?
            """,
            (task_id,),
        )
        row = cur.fetchone()
        return _row_to_task(row)
    except Exception as e:
        logger.warning("查询任务 %s 失败: %s", task_id, e)
        return None
    finally:
        if conn:
            conn.close()


def get_running_task():
    """
    获取当前运行的任务信息。
    优先使用 current_task_id，如果丢失则从 tasks 表中扫描处于运行中的任务。
    """
    conn = None
    try:
        # 先尝试用 current_task_id
        with task_lock:
            cid = current_task_id

        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()

        if cid:
            cur.execute(
                """
                SELECT task_id, state, progress, created_at, user_id_list, command, error, result
                FROM tasks
                WHERE task_id = ? AND state IN ('PENDING', 'PROGRESS')
                """,
                (cid,),
            )
            row = cur.fetchone()
            if row:
                task = _row_to_task(row)
                return task["task_id"], task

        # 回退：从所有任务中找出正在运行的任务（按创建时间倒序，取最新）
        cur.execute(
            """
            SELECT task_id, state, progress, created_at, user_id_list, command, error, result
            FROM tasks
            WHERE state IN ('PENDING', 'PROGRESS')
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None, None
        task = _row_to_task(row)
        return task["task_id"], task
    except Exception as e:
        logger.warning("获取当前运行任务失败: %s", e)
        return None, None
    finally:
        if conn:
            conn.close()

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


def _extract_user_ids_from_config(config: dict) -> list[str]:
    """
    从配置中解析用户ID列表：
    - 现在只支持 user_id_list 为 list[dict] 或 list[简单ID]
    """
    raw = config.get("user_id_list")
    ids: list[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                uid = str(item.get("user_id") or item.get("id") or "").strip()
                if uid:
                    ids.append(uid)
            else:
                s = str(item).strip()
                if s:
                    ids.append(s)
    return ids


def _resolve_user_names_for_notification(config: dict) -> str:
    """
    根据 config 中的 user_id_list，从 SQLite user 表解析出用户昵称列表。
    找不到昵称时回退显示 user_id。
    """
    user_ids = _extract_user_ids_from_config(config)
    if not user_ids:
        return ""

    names: list[str] = []
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        for uid in user_ids:
            try:
                cur.execute("SELECT nick_name FROM user WHERE id = ?", (uid,))
                row = cur.fetchone()
                if row and row[0]:
                    names.append(str(row[0]))
                else:
                    names.append(uid)
            except Exception:
                names.append(uid)
    except Exception as e:
        logger.warning("查询用户昵称失败，将使用 user_id 列表: %s", e)
        return ",".join(user_ids)
    finally:
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:
            pass

    return ",".join(names) if names else ",".join(user_ids)

def run_refresh_task(task_id, user_id_list=None):
    global current_task_id
    config = None  # 确保异常路径中也能安全引用
    try:
        # 任务开始时更新数据库状态为 PROGRESS
        db_update_task(task_id, state="PROGRESS", progress=0, command="RUNNING")

        config = get_config(user_id_list)
        # 将当前任务 ID 传给 Weibo，用于隔离输出目录 weibo/<task_id>/
        config["task_id"] = task_id
        wb = Weibo(config)

        # 设置进度回调：由 Weibo 内部按“用户数 + 每个用户的分页”计算整体百分比
        def progress_cb(percent):
            try:
                pct = int(percent)
            except Exception:
                pct = 0
            pct = max(0, min(100, pct))
            db_update_task(task_id, progress=pct)

        wb.set_progress_callback(progress_cb)

        # 设置停止检查回调：当数据库中的 command == 'STOP' 时抛出 TaskStopped
        def stop_checker():
            conn = None
            try:
                conn = sqlite3.connect(DATABASE_PATH)
                cur = conn.cursor()
                cur.execute("SELECT command FROM tasks WHERE task_id = ?", (task_id,))
                row = cur.fetchone()
                if row and row[0] == "STOP":
                    raise TaskStopped("任务已被用户停止")
            finally:
                if conn:
                    conn.close()

        wb.set_stop_checker(stop_checker)

        # 启动爬虫；如果中途被 stop_checker 中断，会抛出 TaskStopped 异常
        try:
            wb.start()  # 爬取微博信息
        except TaskStopped as ts:
            # 用户主动停止视为“失败”状态，并将进度归零，不再执行后续的成功逻辑和 config.json 写回
            db_update_task(
                task_id,
                state="FAILED",
                error=str(ts) or "任务已被用户停止",
                progress=0,
                command="FINISHED",
            )
            logger.info("任务 %s 已被用户停止", task_id)
            # 发送 PushDeer 通知：任务被停止
            try:
                if config and const.NOTIFY.get("NOTIFY"):
                    name_str = _resolve_user_names_for_notification(config) or "未知用户"
                    push_deer(f"微博爬虫任务 {task_id} 已被用户停止，用户：{name_str}")
            except Exception as notify_err:
                logger.warning("发送 PushDeer 停止通知失败: %s", notify_err)
            return

        # 爬取完成后，同步 dict 形式 user_id_list 的 per-user since/end
        try:
            config_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], "config.json")
            with open(config_path, "r", encoding="utf-8") as f:
                latest_cfg = json.load(f)
            # 如果 config.json 中 user_id_list 是 list[dict]，则将 Weibo 内部更新后的
            # 每个用户的 since_date / end_date 写回 config.json
            try:
                ul = latest_cfg.get("user_id_list")
                if isinstance(ul, list) and ul and isinstance(ul[0], dict):
                    # 构建 user_id -> per-user 配置 的映射
                    per_user = {}
                    for u_cfg in wb.user_config_list:
                        uid = str(u_cfg.get("user_id") or u_cfg.get("id") or "").strip()
                        if uid:
                            per_user[uid] = u_cfg

                    for entry in ul:
                        uid = str(entry.get("user_id") or entry.get("id") or "").strip()
                        if not uid:
                            continue
                        u_cfg = per_user.get(uid)
                        if not u_cfg:
                            continue
                        if u_cfg.get("since_date"):
                            entry["since_date"] = u_cfg["since_date"]
                        if u_cfg.get("end_date"):
                            entry["end_date"] = u_cfg["end_date"]
            except Exception as per_err:
                logger.warning("同步 config.json 中 user_id_list per-user 时间失败: %s", per_err)

            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(latest_cfg, f, ensure_ascii=False, indent=4)
        except Exception as cfg_err:
            logger.warning("更新 config.json 中 end_date 失败: %s", cfg_err)

        db_update_task(
            task_id,
            progress=100,
            state="SUCCESS",
            result="微博列表已刷新",
            command="FINISHED",
        )

        # 任务成功完成后发送 PushDeer 通知
        try:
            if config and const.NOTIFY.get("NOTIFY"):
                name_str = _resolve_user_names_for_notification(config) or "未知用户"
                push_deer(f"微博爬虫任务 {task_id} 已完成，用户：{name_str}")
        except Exception as notify_err:
            logger.warning("发送 PushDeer 成功通知失败: %s", notify_err)

    except Exception as e:
        db_update_task(
            task_id,
            state="FAILED",
            error=str(e),
            command="FINISHED",
        )
        logger.exception(e)
        # 任务失败时也发送 PushDeer 通知
        try:
            if config and const.NOTIFY.get("NOTIFY"):
                name_str = _resolve_user_names_for_notification(config) or "未知用户"
                push_deer(f"微博爬虫任务 {task_id} 失败，用户：{name_str}，错误：{e}")
        except Exception as notify_err:
            logger.warning("发送 PushDeer 失败通知失败: %s", notify_err)
    finally:
        with task_lock:
            if current_task_id == task_id:
                current_task_id = None

@app.route('/refresh', methods=['GET', 'POST'])
def refresh():
    """
    刷新任务接口：
    - GET：展示一个可以在线编辑关键配置项的界面（user_id_list、since_date、end_date、cookie、notify.enable、notify.push_key），并展示完整的 config.json
    - POST（表单提交）：保存页面输入到 config.json，并启动一次爬虫任务
    - POST（JSON）：兼容旧用法，仍然可以通过 JSON 传 user_id_list 启动任务
    """
    global current_task_id

    # 计算 config.json 路径
    config_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], "config.json")

    # --- GET：渲染配置编辑页面 ------------------------------------------
    if request.method == 'GET':
        try:
            cfg = load_config_from_file()
        except SystemExit:
            cfg = {}
        except Exception:
            cfg = {}

        raw_user_id_list = cfg.get("user_id_list") or []
        rows_html = ""
        if isinstance(raw_user_id_list, list):
            for item in raw_user_id_list:
                if isinstance(item, dict):
                    uid = str(item.get("user_id") or item.get("id") or "").strip()
                    name = str(item.get("screen_name") or item.get("nick_name") or "").strip()
                    since = str(item.get("since_date") or "").strip()
                    end = str(item.get("end_date") or "").strip()
                else:
                    uid = str(item).strip()
                    name = ""
                    since = ""
                    end = ""
                if not uid and not name and not since and not end:
                    continue

                # 将内部格式（可能包含时间）转换为 date 控件可用的 yyyy-mm-dd
                since_display = ""
                end_display = ""
                if since:
                    since_display = since.split("T")[0]
                if end:
                    end_display = end.split("T")[0]

                rows_html += f"""
                <tr>
                  <td><input type="text" name="user_id" style="width:120px;" 
                             value="{escape(uid)}"
                             placeholder="用户ID必填且为纯数字" /></td>
                  <td><input type="text" name="screen_name" style="width:160px;" value="{escape(name)}" /></td>
                  <td><input type="text" name="since_date" style="width:180px;"
                             value="{escape(since_display)}"
                             placeholder="必填: yyyy-mm-dd" /></td>
                  <td><input type="text" name="end_date" style="width:180px;"
                             value="{escape(end_display)}"
                             placeholder="默认为当前日期" /></td>
                  <td><button type="button" onclick="removeUserRow(this)">删除</button></td>
                </tr>
                """
        cookie_val = cfg.get("cookie", "")
        notify_cfg = cfg.get("notify") or {}
        notify_enable_val = bool(notify_cfg.get("enable", False))
        notify_push_key_val = notify_cfg.get("push_key", "")

        checked_attr = "checked" if notify_enable_val else ""

        html = f"""
        <html>
          <head>
            <meta charset="utf-8" />
            <title>刷新任务（配置编辑）</title>
            <script>
              function togglePushKey() {{
                  var cb = document.getElementById('notify_enable');
                  var div = document.getElementById('notify_push_key_container');
                  if (!cb) return;
                  div.style.display = cb.checked ? 'inline-block' : 'none';
              }}

              function addUserRow() {{
                  var tbody = document.getElementById('user_table_body');
                  var tr = document.createElement('tr');
                  tr.innerHTML = ''
                    + '<td><input type="text" name="user_id" style="width:120px;" '
                    + 'placeholder="用户ID必填且为纯数字" /></td>'
                    + '<td><input type="text" name="screen_name" style="width:160px;" /></td>'
                    + '<td><input type="text" name="since_date" style="width:180px;" '
                    + 'placeholder="必填: yyyy-mm-dd" /></td>'
                    + '<td><input type="text" name="end_date" style="width:180px;" '
                    + 'placeholder="默认为当前日期" /></td>'
                    + '<td><button type="button" onclick="removeUserRow(this)">删除</button></td>';
                  tbody.appendChild(tr);
              }}

              function removeUserRow(btn) {{
                  var tr = btn.parentNode.parentNode;
                  tr.parentNode.removeChild(tr);
              }}
            </script>
          </head>
          <body onload="togglePushKey()">
            <h1>刷新任务（配置编辑）</h1>
            <form method="post" action="/refresh">
              <h2>用户列表</h2>
              <table border="1" cellspacing="0" cellpadding="4">
                <thead>
                  <tr>
                    <th>用户ID</th>
                    <th>昵称</th>
                    <th>since_date</th>
                    <th>end_date</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody id="user_table_body">
                  {rows_html}
                </tbody>
              </table>
              <button type="button" onclick="addUserRow()">新增用户</button>

              <h2>其他配置</h2>
              <table border="1" cellspacing="0" cellpadding="4">
                <tr>
                  <th>cookie</th>
                  <td>
                    <textarea name="cookie" rows="4" cols="80">{escape(str(cookie_val))}</textarea>
                  </td>
                </tr>
                <tr>
                  <th>notify</th>
                  <td>
                    <label style="margin-right: 8px;">
                      <input type="checkbox" id="notify_enable" name="notify_enable" value="1" {checked_attr}
                             onchange="togglePushKey()" />
                      启用通知
                    </label>
                    <span id="notify_push_key_container" style="display:none;">
                      push_key:
                      <input type="text" name="notify_push_key" style="width:260px;"
                             value="{escape(str(notify_push_key_val))}" />
                    </span>
                  </td>
                </tr>
              </table>

              <br/>
              <button type="submit" name="action" value="save">仅保存配置</button>
              <button type="submit" name="action" value="save_and_run">保存配置并启动任务</button>
            </form>
            <p><a href="/">返回首页</a></p>
          </body>
        </html>
        """
        return html

    # --- POST：如果是表单提交，保存页面输入并启动任务 --------------------
    if not request.is_json:
        logger.info("refresh POST: start form handling")
        # 读取原始配置
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}

        form = request.form

        # 更新关键字段
        # 1) 解析用户列表（多行）
        user_ids = form.getlist("user_id")
        screen_names = form.getlist("screen_name")
        since_dates = form.getlist("since_date")
        end_dates = form.getlist("end_date")

        new_user_list = []
        for idx, uid in enumerate(user_ids):
            uid = (uid or "").strip()
            name = (screen_names[idx] if idx < len(screen_names) else "").strip()
            sdate = (since_dates[idx] if idx < len(since_dates) else "").strip()
            edate = (end_dates[idx] if idx < len(end_dates) else "").strip()
            # 跳过完全空的一行
            if not uid and not name and not sdate and not edate:
                continue
            # user_id 必须存在且为数字
            if not uid.isdigit():
                return f"第 {idx+1} 行 user_id 非数字，请检查", 400
            entry = {"user_id": uid}
            if name:
                entry["screen_name"] = name
            if sdate:
                entry["since_date"] = sdate
            if edate:
                entry["end_date"] = edate
            new_user_list.append(entry)

        if not new_user_list:
            return "至少需要配置一个用户", 400

        cfg["user_id_list"] = new_user_list

        cookie_val = form.get("cookie", "").strip()
        notify_enable_val = form.get("notify_enable") is not None
        notify_push_key_val = form.get("notify_push_key", "").strip()

        cfg["cookie"] = cookie_val

        notify_cfg = cfg.get("notify") or {}
        if not isinstance(notify_cfg, dict):
            notify_cfg = {}
        notify_cfg["enable"] = bool(notify_enable_val)
        # 只有在输入非空时才更新 push_key，避免意外清空
        if notify_push_key_val != "":
            notify_cfg["push_key"] = notify_push_key_val
        cfg["notify"] = notify_cfg

        # 写回 config.json
        try:
            logger.info("refresh POST: writing config.json")
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=4)
        except Exception as e:
            return f"保存 config.json 失败: {e}", 500

        # 根据按钮决定是否启动任务
        action = form.get("action") or "save_and_run"
        if action == "save":
            # 仅保存配置，不启动任务，直接回到配置页
            logger.info("refresh POST: save only, redirect to /refresh")
            return redirect("/refresh")

        # 启动任务，使用最新 config.json 中的 user_id_list
        user_id_list = cfg.get("user_id_list")

        # 先在锁外检查是否有正在运行的任务，避免死锁
        running_task_id, running_task = get_running_task()
        if running_task:
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>任务已在运行</title></head>
              <body>
                <h1>已有任务正在运行</h1>
                <p>当前任务 ID：{escape(running_task_id)}</p>
                <p><a href="/status">查看当前状态</a> | <a href="/tasks">查看所有任务</a> | <a href="/">返回首页</a></p>
              </body>
            </html>
            """
            return html, 409

        # 创建新任务时加锁，防止竞态
        with task_lock:
            logger.info("refresh POST: acquiring task_lock to start task")
            task_id = str(uuid.uuid4())
            db_create_task(task_id, user_id_list)
            current_task_id = task_id

        # 提交任务，传入 None，让 run_refresh_task 自行通过 get_config 读取最新配置
        logger.info("refresh POST: submitting background task")
        executor.submit(run_refresh_task, task_id, None)

        # 立即重定向到状态页，避免浏览器长时间等待
        logger.info("refresh POST: redirecting to /status")
        return redirect("/status")

    # 走到这里说明既不是 GET，也不是表单 POST，直接返回 400
    return "Unsupported request type for /refresh", 400


@app.route('/task/<task_id>/stop', methods=['POST'])
def stop_task(task_id):
    """
    停止指定任务（方案1：仅支持“停止”，不支持继续/恢复）。
    通过将数据库中该任务的 command 置为 'STOP'，让后台线程在安全检查点优雅退出。
    """
    task = db_get_task(task_id)
    if not task:
        data = {"error": "Task not found"}
        if wants_html():
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>停止任务</title></head>
              <body>
                <h1>任务 {escape(task_id)} 未找到</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  <tr><th>error</th><td>{escape(data['error'])}</td></tr>
                </table>
                <p><a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a></p>
              </body>
            </html>
            """
            return html, 404
        return jsonify(data), 404

    state = task.get("state")
    if state not in ["PENDING", "PROGRESS"]:
        # 任务已结束，不能再停止
        msg = f"任务当前状态为 {state}，无法停止"
        if wants_html():
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>停止任务</title></head>
              <body>
                <h1>任务 {escape(task_id)} 无法停止</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  <tr><th>state</th><td>{escape(str(state))}</td></tr>
                  <tr><th>message</th><td>{escape(msg)}</td></tr>
                </table>
                <p><a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a> | <a href="/task/{escape(task_id)}">返回任务详情</a></p>
              </body>
            </html>
            """
            return html, 400
        return jsonify({"error": msg, "state": state}), 400

    # 标记为 STOP，后台线程会在合适的检查点抛出 TaskStopped 结束任务
    with task_lock:
        db_update_task(task_id, command="STOP")

    if wants_html():
        # 根据来源页面决定跳转位置：从任务列表来就回到列表，从状态页来就回到状态页，
        # 并通过查询参数带上“已终止”的提示
        ref = request.referrer or ""
        if "/tasks" in ref:
            return redirect(f"/tasks?stopped={task_id}")
        if "/status" in ref:
            return redirect(f"/status?stopped={task_id}")
        # 其他情况默认跳转到任务详情页
        return redirect(f"/task/{task_id}?stopped=1")
    return jsonify({"message": "停止请求已发送", "task_id": task_id}), 200


@app.route('/task/<task_id>/download', methods=['GET'])
def download_task_weibo(task_id):
    """
    为指定任务提供当前 weibo 目录内容的打包下载。
    目前不区分任务产生的文件，直接将 /weibo 目录整体打包为 zip 返回。
    仅在任务 state=SUCCESS 时允许下载。
    """
    task = db_get_task(task_id)
    if not task:
        data = {"error": "Task not found"}
        if wants_html():
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>下载任务结果</title></head>
              <body>
                <h1>任务 {escape(task_id)} 未找到</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  <tr><th>error</th><td>{escape(data['error'])}</td></tr>
                </table>
                <p><a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a></p>
              </body>
            </html>
            """
            return html, 404
        return jsonify(data), 404

    if task.get("state") != "SUCCESS":
        msg = f"任务当前状态为 {task.get('state')}，仅在 SUCCESS 状态下才可下载 weibo 目录内容"
        if wants_html():
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>下载任务结果</title></head>
              <body>
                <h1>无法下载任务 {escape(task_id)} 的结果</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  <tr><th>state</th><td>{escape(str(task.get('state')))}</td></tr>
                  <tr><th>message</th><td>{escape(msg)}</td></tr>
                </table>
                <p><a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a> | <a href="/task/{escape(task_id)}">返回任务详情</a></p>
              </body>
            </html>
            """
            return html, 400
        return jsonify({"error": msg, "state": task.get("state")}), 400

    base_dir = os.path.split(os.path.realpath(__file__))[0]
    weibo_root = os.path.join(base_dir, "weibo")
    # 仅打包该任务专属目录 weibo/<task_id>/；如果不存在则直接报错
    task_weibo_dir = os.path.join(weibo_root, task_id)
    if not os.path.isdir(task_weibo_dir):
        data = {"error": f"未找到该任务对应的结果目录: {task_weibo_dir}"}
        if wants_html():
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>下载任务结果</title></head>
              <body>
                <h1>下载失败</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  <tr><th>error</th><td>{escape(data['error'])}</td></tr>
                </table>
                <p><a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a></p>
              </body>
            </html>
            """
            return html, 500
        return jsonify(data), 500

    # 生成压缩包文件名：用户昵称+开始日期+结束日期+任务id前5位
    user_id_list = task.get("user_id_list") or []
    first_uid = None
    if isinstance(user_id_list, list):
        for item in user_id_list:
            uid = None
            if isinstance(item, dict):
                uid = item.get("user_id") or item.get("id")
            else:
                uid = item
            if uid:
                first_uid = str(uid).strip()
                if first_uid:
                    break

    nick = None
    start_str = "unknown"
    end_str = "unknown"

    if first_uid:
        # 从 SQLite user 表获取昵称
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cur = conn.cursor()
            cur.execute("SELECT nick_name FROM user WHERE id = ?", (first_uid,))
            row = cur.fetchone()
            if row and row[0]:
                nick = str(row[0])
        except Exception as e:
            logger.warning("获取任务 %s 用户昵称失败: %s", task_id, e)
        finally:
            try:
                conn.close()  # type: ignore[name-defined]
            except Exception:
                pass

        # 从 weibo 表计算该用户的最早 / 最晚微博日期
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cur = conn.cursor()
            cur.execute(
                "SELECT MIN(created_at), MAX(created_at) FROM weibo WHERE user_id = ?",
                (first_uid,),
            )
            row = cur.fetchone()
            if row:
                if row[0]:
                    start_str = str(row[0]).split(" ")[0]
                if row[1]:
                    end_str = str(row[1]).split(" ")[0]
        except Exception as e:
            logger.warning("计算任务 %s 的开始/结束日期失败: %s", task_id, e)
        finally:
            try:
                conn.close()  # type: ignore[name-defined]
            except Exception:
                pass

    if not nick:
        nick = first_uid or "task"

    safe_nick = re.sub(r'[\\/:*?"<>|]', "_", str(nick))
    task_prefix = str(task_id)[:5]
    zip_name = f"{safe_nick}_{start_str}_{end_str}_{task_prefix}.zip"

    # 将该任务的 weibo 目录打包为 zip 并返回
    import tempfile
    import shutil

    tmp_dir = tempfile.gettempdir()
    archive_base = os.path.join(tmp_dir, f"weibo_{task_id}")
    try:
        zip_path = shutil.make_archive(archive_base, "zip", task_weibo_dir)
    except Exception as e:
        logger.exception("打包 weibo 目录失败: %s", e)
        data = {"error": f"打包 weibo 目录失败: {e}"}
        if wants_html():
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>下载任务结果</title></head>
              <body>
                <h1>下载失败</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  <tr><th>error</th><td>{escape(data['error'])}</td></tr>
                </table>
                <p><a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a></p>
              </body>
            </html>
            """
            return html, 500
        return jsonify(data), 500

    return send_file(
        zip_path,
        as_attachment=True,
        download_name=zip_name,
        mimetype="application/zip",
    )

@app.route('/task/<task_id>', methods=['GET'])
def get_task_status(task_id):
    task = db_get_task(task_id)
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
        # 计算上一条 / 下一条任务（按 created_at 倒序排列）
        prev_task_id = None
        next_task_id = None
        created_at = task.get("created_at") or ""
        try:
            conn = sqlite3.connect(DATABASE_PATH)
            cur = conn.cursor()
            # 上一条：比当前 created_at 更晚的那条中最早的（相当于列表中的上一条）
            cur.execute(
                """
                SELECT task_id FROM tasks
                WHERE created_at > ?
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (created_at,),
            )
            row = cur.fetchone()
            if row:
                prev_task_id = row[0]
            # 下一条：比当前 created_at 更早的那条中最新的（相当于列表中的下一条）
            cur.execute(
                """
                SELECT task_id FROM tasks
                WHERE created_at < ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (created_at,),
            )
            row = cur.fetchone()
            if row:
                next_task_id = row[0]
        except Exception as e:
            logger.warning("计算任务 %s 的前后任务失败: %s", task_id, e)
        finally:
            try:
                conn.close()  # type: ignore[name-defined]
            except Exception:
                pass
        # 如果任务成功完成，提供下载 weibo 目录内容的链接
        download_link_html = ""
        if task.get('state') == 'SUCCESS':
            download_link_html = (
                f"<p><a href=\"/task/{escape(task_id)}/download\">下载当前 weibo 目录内容</a></p>"
            )
        # 如果任务仍在运行，则提供“停止该任务”的按钮；
        # 当 command 已为 STOP 时，按钮置灰不可用
        stop_button_html = ""
        if task.get('state') in ['PENDING', 'PROGRESS']:
            stop_disabled = task.get('command') == 'STOP'
            stop_button_html = f"""
            <form method="post" action="/task/{escape(task_id)}/stop" style="display:inline;">
              <button type="submit" {'disabled' if stop_disabled else ''}>停止该任务</button>
            </form>
            """
        # 如果通过查询参数标记已停止，则给出提示
        notice_html = ""
        if request.args.get("stopped"):
            notice_html = (
                f"<p style='color:red;'>任务 {escape(task_id)} 已终止</p>"
            )
        # 上一条 / 下一条任务的导航链接（仅在存在时显示）
        nav_links = []
        if prev_task_id:
            nav_links.append(
                f"<a href=\"/task/{escape(str(prev_task_id))}\">上一条任务</a>"
            )
        if next_task_id:
            nav_links.append(
                f"<a href=\"/task/{escape(str(next_task_id))}\">下一条任务</a>"
            )
        nav_html = " | ".join(nav_links) if nav_links else ""
        nav_block = f"<p>{nav_html}</p>" if nav_html else ""
        html = f"""
        <html>
          <head><meta charset="utf-8"><title>任务详情 {escape(task_id)}</title></head>
            <body>
            <h1>任务详情</h1>
            {notice_html}
            <table border="1" cellspacing="0" cellpadding="4">
              {''.join(rows)}
            </table>
            {download_link_html}
            {nav_block}
            <p>{stop_button_html}<a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a></p>
          </body>
        </html>
        """
        return html

    return jsonify(response)


@app.route('/tasks', methods=['GET', 'POST'])
def list_tasks():
    """
    列出所有任务及其状态（按 created_at 倒序）。
    只做状态查看，不会触发新的任务。
    """
    # 如果是 POST 并且带有删除参数，则删除指定任务后重定向
    if request.method == 'POST':
        task_id = request.form.get('task_id')
        if task_id:
            # 仅删除非运行中任务
            conn = None
            try:
                conn = sqlite3.connect(DATABASE_PATH)
                cur = conn.cursor()
                cur.execute("SELECT state FROM tasks WHERE task_id = ?", (task_id,))
                row = cur.fetchone()
                if row and row[0] not in ['PENDING', 'PROGRESS']:
                    cur.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
                    conn.commit()
            except Exception as e:
                logger.warning("删除任务 %s 失败: %s", task_id, e)
            finally:
                if conn:
                    conn.close()
        return redirect('/tasks')

    # GET：从数据库按 created_at 逆序读取所有任务
    items = []
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT task_id, state, progress, created_at, user_id_list, command, error, result
            FROM tasks
            ORDER BY created_at DESC
            """
        )
        rows = cur.fetchall()
        for row in rows:
            t = _row_to_task(row)
            if t:
                items.append(t)
    except Exception as e:
        logger.warning("查询任务列表失败: %s", e)
    finally:
        if conn:
            conn.close()
    if wants_html():
        # 如果通过 stopped 参数传入任务ID，则在页面上给出“任务已终止”的提示
        notice_html = ""
        stopped_id = request.args.get("stopped")
        if stopped_id:
            notice_html = (
                f"<p style='color:red;'>任务 {escape(str(stopped_id))} 已终止</p>"
            )
        header_cells = "".join(
            f"<th>{escape(col)}</th>"
            for col in ["task_id", "state", "progress", "created_at", "user_id_list", "操作"]
        )
        body_rows = []
        for it in items:
            state = str(it.get('state'))
            command = str(it.get('command') or "")
            delete_disabled = state in ['PENDING', 'PROGRESS']
            delete_button = (
                f"<button type=\"submit\" name=\"task_id\" value=\"{escape(it['task_id'])}\" "
                f"{'disabled' if delete_disabled else ''}>删除</button>"
            )
            # 运行中的任务提供“停止”按钮，指向 /task/<task_id>/stop；
            # 如果已发出停止命令，则按钮置灰不可用
            stop_disabled = not (state in ['PENDING', 'PROGRESS'] and command != 'STOP')
            stop_button = (
                f"<button type=\"submit\" "
                f"formaction=\"/task/{escape(it['task_id'])}/stop\" "
                f"formmethod=\"post\" "
                f"{'disabled' if stop_disabled else ''}>停止</button>"
            )
            body_rows.append(
                "<tr>"
                f"<td><a href=\"/task/{escape(it['task_id'])}\">{escape(it['task_id'])}</a></td>"
                f"<td>{escape(state)}</td>"
                f"<td>{escape(str(it.get('progress')))}</td>"
                f"<td>{escape(str(it.get('created_at')))}</td>"
                f"<td>{escape(str(it.get('user_id_list')))}</td>"
                f"<td>{stop_button} {delete_button}</td>"
                "</tr>"
            )
        html = f"""
        <html>
          <head><meta charset="utf-8"><title>任务列表</title></head>
          <body>
            <h1>任务列表</h1>
            {notice_html}
            <form method="post" action="/tasks">
              <table border="1" cellspacing="0" cellpadding="4">
                <thead><tr>{header_cells}</tr></thead>
                <tbody>
                  {''.join(body_rows)}
                </tbody>
              </table>
            </form>
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
            # 对当前运行任务提供“停止该任务”按钮；当 command 已为 STOP 时置灰不可用
            stop_button_html = ""
            if data.get('state') in ['PENDING', 'PROGRESS']:
                stop_disabled = running_task.get('command') == 'STOP'
                stop_button_html = f"""
                <form method="post" action="/task/{escape(str(data['task_id']))}/stop" style="display:inline;">
                  <button type="submit" {'disabled' if stop_disabled else ''}>停止该任务</button>
                </form>
                """
            notice_html = ""
            stopped_id = request.args.get("stopped")
            if stopped_id:
                notice_html = (
                    f"<p style='color:red;'>任务 {escape(str(stopped_id))} 已终止</p>"
                )
            html = f"""
            <html>
              <head><meta charset="utf-8"><title>当前状态</title></head>
              <body>
                <h1>当前运行任务</h1>
                {notice_html}
                <table border="1" cellspacing="0" cellpadding="4">
                  {''.join(rows)}
                </table>
                <p>{stop_button_html}<a href="/">返回首页</a> | <a href="/tasks">查看所有任务</a></p>
              </body>
            </html>
            """
            return html, 200
        return jsonify(data), 200

    # 没有正在运行的任务，尝试给出最近一个任务的简要信息（如果有）
    last_task = None
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT task_id, state, progress, created_at, user_id_list, command, error, result
            FROM tasks
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            last_task = _row_to_task(row)
    except Exception as e:
        logger.warning("查询最近任务失败: %s", e)
    finally:
        if conn:
            conn.close()

    if last_task:
        data = {
            'state': 'IDLE',
            'current_task': None,
            'last_task_id': last_task.get('task_id'),
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
            # 简单表格展示主要字段，其中 id 列可点击跳转到 /weibos/<weibo_id>
            # 为了减少横向滚动，超长字段中间用 "..." 截断，并通过 title 展示完整内容。
            display_columns = [col for col in columns if not _is_link_field(col)]
            header_cells = "".join(f"<th>{escape(str(col))}</th>" for col in display_columns)
            body_rows = []
            for item in weibos:
                row_cells = []
                for col in display_columns:
                    val = item.get(col)
                    text = "" if val is None else str(val)
                    # id 列仍然完整展示，但通常长度较短
                    if col == "id":
                        cell = (
                            f"<td><a href=\"/weibos/{escape(text)}\">"
                            f"{escape(text)}</a></td>"
                        )
                    else:
                        short = _truncate_middle(text, max_len=80)
                        title_attr = f' title="{escape(text)}"' if text else ""
                        cell = (
                            f"<td{title_attr}>{escape(short)}</td>"
                        )
                    row_cells.append(cell)
                body_rows.append("<tr>" + "".join(row_cells) + "</tr>")
            html = f"""
            <html>
              <head>
                <meta charset="utf-8"><title>微博列表</title>
              </head>
              <body>
                <h1>微博列表</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  <thead><tr>{header_cells}</tr></thead>
                  <tbody>
                    {''.join(body_rows)}
                  </tbody>
                </table>
                <p>说明：为避免页面过宽，长文本中间已使用 "..." 截断，鼠标悬停可查看完整内容。</p>
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
        if not row:
            conn.close()
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

        weibo = dict(zip(columns, row))

        # 计算上一条 / 下一条微博（按 created_at DESC 排序）
        prev_weibo_id = None
        next_weibo_id = None
        created_at = weibo.get("created_at") or ""
        try:
            # 上一条：比当前 created_at 更晚的一条（列表中的上一条）
            cursor.execute(
                """
                SELECT id FROM weibo
                WHERE created_at > ?
                ORDER BY created_at ASC, id ASC
                LIMIT 1
                """,
                (created_at,),
            )
            r = cursor.fetchone()
            if r:
                prev_weibo_id = r[0]
            # 下一条：比当前 created_at 更早的一条（列表中的下一条）
            cursor.execute(
                """
                SELECT id FROM weibo
                WHERE created_at < ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (created_at,),
            )
            r = cursor.fetchone()
            if r:
                next_weibo_id = r[0]
        except Exception as e:
            logger.warning("计算微博 %s 的前后微博失败: %s", weibo_id, e)
        finally:
            conn.close()
        
        if wants_html():
            rows = []
            for k, v in weibo.items():
                # pics、video_url 等链接类字段在 HTML 页面上不展示
                if _is_link_field(k):
                    continue
                rows.append(
                    f"<tr><th>{escape(str(k))}</th><td>{escape('' if v is None else str(v))}</td></tr>"
                )

            # 上一条 / 下一条微博导航（仅在存在时显示）
            nav_links = []
            if prev_weibo_id:
                nav_links.append(
                    f"<a href=\"/weibos/{escape(str(prev_weibo_id))}\">上一条微博</a>"
                )
            if next_weibo_id:
                nav_links.append(
                    f"<a href=\"/weibos/{escape(str(next_weibo_id))}\">下一条微博</a>"
                )
            nav_html = " | ".join(nav_links) if nav_links else ""
            nav_block = f"<p>{nav_html}</p>" if nav_html else ""

            html = f"""
            <html>
              <head><meta charset="utf-8"><title>微博详情 {escape(str(weibo_id))}</title></head>
              <body>
                <h1>微博详情</h1>
                <table border="1" cellspacing="0" cellpadding="4">
                  {''.join(rows)}
                </table>
                {nav_block}
                <p><a href=\"/weibos\">返回微博列表</a> | <a href=\"/\">返回首页</a></p>
              </body>
            </html>
            """
            return html, 200
        return jsonify(weibo), 200
    except Exception as e:
        logger.exception(e)
        return {"error": str(e)}, 500

if __name__ == "__main__":
    logger.info("服务启动，仅提供 Web 管理界面，不自动启动爬虫任务")
    # 启动Flask应用
    app.run(debug=True, use_reloader=False)  # 关闭reloader避免启动两次
