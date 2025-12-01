from weibo import Weibo, handle_config_renaming, get_config as load_config_from_file
import const
import logging
import logging.config
import os
from flask import (
    Flask,
    jsonify,
    request,
    redirect,
    send_file,
    send_from_directory,
    render_template,
    url_for,
)
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
    # index 页面总是返回 HTML
    return render_template("index.html")

# 添加线程池和任务状态跟踪
executor = ThreadPoolExecutor(max_workers=1)  # 限制只有1个worker避免并发爬取

# 使用 SQLite 持久化任务列表，仅用少量内存变量做并发控制
current_task_id = None  # 当前正在后台执行的任务 ID
task_lock = threading.Lock()  # 防止同时创建/修改 current_task_id

# 定时调度相关
scheduler_stop_event = threading.Event()
scheduler_thread = None


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
                result       TEXT,
                schedule_id  TEXT
            )
            """
        )
        conn.commit()
        # 尝试为已有表增加 schedule_id 列（若已存在会抛异常，忽略即可）
        try:
            cur.execute("ALTER TABLE tasks ADD COLUMN schedule_id TEXT")
            conn.commit()
        except Exception:
            pass
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
        schedule_id,
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
        "schedule_id": schedule_id,
    }


def db_create_task(task_id: str, user_id_list, schedule_id: str | None = None):
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
            INSERT INTO tasks (task_id, state, progress, created_at, user_id_list, command, schedule_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                "PENDING",
                0,
                datetime.now().isoformat(),
                user_id_json,
                "RUNNING",
                schedule_id,
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
    allowed = {"state", "progress", "created_at", "user_id_list", "command", "error", "result", "schedule_id"}
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
            SELECT task_id, state, progress, created_at, user_id_list, command, error, result, schedule_id
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
                SELECT task_id, state, progress, created_at, user_id_list, command, error, result, schedule_id
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
            SELECT task_id, state, progress, created_at, user_id_list, command, error, result, schedule_id
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


def _schedule_loop():
    """
    简单定时调度器：
    - 每隔 60 秒检查一次 config.json 中的 schedule.enable
    - 若开启定时任务且当前没有运行中的任务，则根据 interval_minutes 判断是否需要新建任务
    - 新任务使用最新的 config.json 中 user_id_list 和其它配置
    """
    global current_task_id
    logger.info("定时调度器启动")
    while not scheduler_stop_event.is_set():
        try:
            time.sleep(60)  # 检查间隔 60 秒

            # 读取配置，判断是否开启定时任务
            try:
                cfg = load_config_from_file()
            except SystemExit:
                cfg = {}
            except Exception:
                cfg = {}

            schedule_cfg = cfg.get("schedule") or {}
            if not isinstance(schedule_cfg, dict):
                continue
            if not schedule_cfg.get("enable"):
                continue
            # 定时任务所属的父任务 ID（schedule_id），由首次“保存并启动”时写入 config.json
            schedule_id = schedule_cfg.get("schedule_id")
            if not schedule_id:
                continue

            # 间隔分钟数，默认 60 分钟
            try:
                interval_minutes = int(schedule_cfg.get("interval_minutes", 60))
            except Exception:
                interval_minutes = 60
            if interval_minutes <= 0:
                interval_minutes = 60
            interval_seconds = interval_minutes * 60

            # 若有任务在运行，则不启动新的任务
            running_task_id, running_task = get_running_task()
            if running_task:
                continue

            # 读取该定时任务（schedule_id）最近一次任务的创建时间
            last_created_at = None
            conn = None
            try:
                conn = sqlite3.connect(DATABASE_PATH)
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT created_at FROM tasks
                    WHERE schedule_id = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                    ,
                    (schedule_id,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    last_created_at = row[0]
            except Exception as e:
                logger.warning("定时调度器查询最近任务失败: %s", e)
            finally:
                if conn:
                    conn.close()

            now = datetime.now()
            if last_created_at:
                try:
                    last_dt = datetime.fromisoformat(last_created_at)
                    if (now - last_dt).total_seconds() < interval_seconds:
                        # 间隔未到，跳过本轮
                        continue
                except Exception:
                    # 解析失败时不阻断调度
                    pass

            # 到达调度时间，且没有运行中的任务 -> 创建新任务
            user_id_list = cfg.get("user_id_list")
            if not isinstance(user_id_list, list) or not user_id_list:
                continue

            with task_lock:
                task_id = str(uuid.uuid4())
                # 定时任务的 schedule_id 统一与父任务 ID 一致
                db_create_task(task_id, user_id_list, schedule_id=str(schedule_id))
                current_task_id = task_id

            logger.info("定时调度器自动启动任务: %s", task_id)
            executor.submit(run_refresh_task, task_id, None)

        except Exception as e:
            logger.warning("定时调度器异常: %s", e)
            continue


def _start_scheduler_if_needed():
    """
    仅在用户通过“保存配置并启动任务”启用了定时任务后，才启动调度线程。
    避免仅运行 python service.py 就立刻自动执行定时任务。
    """
    global scheduler_thread
    if scheduler_thread is not None:
        return
    try:
        scheduler_thread = threading.Thread(target=_schedule_loop, daemon=True)
        scheduler_thread.start()
        logger.info("定时调度器线程已启动")
    except Exception as e:
        logger.warning("启动定时调度器失败: %s", e)


def _auto_start_scheduler_on_boot():
    """
    在服务启动时，根据 config.json 自动恢复之前配置好的定时任务：
    - 仅当 schedule.enable 为 True 且存在有效的 schedule_id 时才启动调度器；
    - 保证只有在用户曾经通过“保存配置并启动任务”创建过定时任务后，重启 service.py 才会恢复。
    """
    try:
        cfg = load_config_from_file()
    except SystemExit:
        # 主配置不存在或格式错误时，直接跳过
        return
    except Exception as e:
        logger.warning("启动时读取 config.json 失败，跳过自动恢复定时任务: %s", e)
        return

    schedule_cfg = cfg.get("schedule") or {}
    if not isinstance(schedule_cfg, dict):
        return
    if not schedule_cfg.get("enable"):
        return
    if not schedule_cfg.get("schedule_id"):
        return

    logger.info("检测到已启用的定时任务配置，尝试恢复定时调度")
    _start_scheduler_if_needed()

def run_refresh_task(task_id, user_id_list=None):
    global current_task_id
    config = None  # 确保异常路径中也能安全引用
    try:
        # 任务开始时更新数据库状态为 PROGRESS
        db_update_task(task_id, state="PROGRESS", progress=0, command="RUNNING")

        # 若当前任务属于某个定时任务（schedule_id 存在且与 task_id 不同），
        # 则视为“子任务”。在子任务开始前，将上一次运行保存在 config.json
        # 中的 end_date 迁移到 since_date，并清空 end_date，
        # 以便本次子任务从上一次的结束时间继续抓取。
        try:
            task_info = db_get_task(task_id)
        except Exception as task_query_err:
            task_info = None
            logger.warning("查询任务 %s 的 schedule_id 失败: %s", task_id, task_query_err)

        try:
            if task_info:
                schedule_id_val = task_info.get("schedule_id")
                # 父任务：schedule_id == task_id
                # 子任务：schedule_id 存在且 != task_id
                if schedule_id_val and str(schedule_id_val) != str(task_id):
                    config_path = os.path.join(os.path.split(os.path.realpath(__file__))[0], "config.json")
                    with open(config_path, "r", encoding="utf-8") as f:
                        cfg_for_child = json.load(f)

                    schedule_cfg = cfg_for_child.get("schedule") or {}
                    if not isinstance(schedule_cfg, dict):
                        schedule_cfg = {}
                    schedule_enabled = bool(schedule_cfg.get("enable"))

                    # 只有在 config.json 中仍然启用了定时任务时，才进行 since/end 的迁移
                    if schedule_enabled:
                        ul = cfg_for_child.get("user_id_list")
                        if isinstance(ul, list):
                            for entry in ul:
                                if not isinstance(entry, dict):
                                    continue
                                prev_end = entry.get("end_date")
                                if prev_end:
                                    # 将上一次的 end_date 作为本次子任务的 since_date，并清空 end_date
                                    entry["since_date"] = prev_end
                                    entry["end_date"] = ""

                        with open(config_path, "w", encoding="utf-8") as f:
                            json.dump(cfg_for_child, f, ensure_ascii=False, indent=4)
                        logger.info("任务 %s 为定时子任务，已将上次 end_date 迁移到 since_date 并清空 end_date", task_id)
        except Exception as shift_err:
            logger.warning("任务 %s 在子任务启动前处理 since/end 失败: %s", task_id, shift_err)

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
            schedule_cfg = (latest_cfg.get("schedule") or {}) if isinstance(latest_cfg, dict) else {}
            schedule_enabled = bool(schedule_cfg.get("enable"))
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
                        since_val = u_cfg.get("since_date")
                        end_val = u_cfg.get("end_date")
                        if end_val:
                            entry["end_date"] = end_val
                        if since_val:
                            entry["since_date"] = since_val
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
                # 如果有实际抓取到微博数据才发送成功通知
                try:
                    got_count = getattr(wb, "got_count", 0)
                except Exception:
                    got_count = 0
                if got_count and got_count > 0:
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
        user_rows: list[dict] = []
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
                user_rows.append(
                    {
                        "user_id": uid,
                        "screen_name": name,
                        "since_date": since,
                        "end_date": end,
                    }
                )

        cookie_val = cfg.get("cookie", "")
        notify_cfg = cfg.get("notify") or {}
        notify_enable_val = bool(notify_cfg.get("enable", False))
        notify_push_key_val = notify_cfg.get("push_key", "")
        schedule_cfg = cfg.get("schedule") or {}
        schedule_enable_val = bool(schedule_cfg.get("enable", False))

        return render_template(
            "refresh.html",
            user_rows=user_rows,
            cookie_val=cookie_val,
            notify_enable=notify_enable_val,
            notify_push_key=notify_push_key_val,
            schedule_enable=schedule_enable_val,
        )

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
        schedule_enable_val = form.get("schedule_enable") is not None

        cfg["cookie"] = cookie_val

        notify_cfg = cfg.get("notify") or {}
        if not isinstance(notify_cfg, dict):
            notify_cfg = {}
        notify_cfg["enable"] = bool(notify_enable_val)
        # 只有在输入非空时才更新 push_key，避免意外清空
        if notify_push_key_val != "":
            notify_cfg["push_key"] = notify_push_key_val
        cfg["notify"] = notify_cfg

        schedule_cfg = cfg.get("schedule") or {}
        if not isinstance(schedule_cfg, dict):
            schedule_cfg = {}
        schedule_cfg["enable"] = bool(schedule_enable_val)
        # 间隔固定为 60 分钟，如需可配置后续再扩展
        if "interval_minutes" not in schedule_cfg:
            schedule_cfg["interval_minutes"] = 60
        # schedule_id 在“保存并启动”时确定，这里先保留原值
        cfg["schedule"] = schedule_cfg

        # 写回 config.json（此时 schedule_id 可能还未更新，稍后在创建任务后再补写一次）
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
            if wants_html():
                # 使用任务详情页引导用户去查看当前任务
                return (
                    render_template(
                        "task_detail.html",
                        task_id=running_task_id,
                        task_title="任务详情(当前已有任务在运行)",
                        response=running_task,
                        state=running_task.get("state"),
                        is_parent=False,
                        is_child=False,
                        parent_task_id=None,
                        child_tasks=[],
                        has_running_child=False,
                        can_download_weibo_dir=False,
                        can_stop=True,
                        stop_disabled=False,
                        notice_html=f"<p style='color:red;'>已有任务正在运行，当前任务 ID：{escape(running_task_id)}</p>",
                        prev_task_id=None,
                        next_task_id=None,
                    ),
                    409,
                )
            return (
                jsonify(
                    {
                        "error": "已有任务正在运行",
                        "task_id": running_task_id,
                    }
                ),
                409,
            )

        # 创建新任务时加锁，防止竞态
        with task_lock:
            logger.info("refresh POST: acquiring task_lock to start task")
            task_id = str(uuid.uuid4())
            # 若启用了定时任务，则将当前任务视为父任务：schedule_id = 本次 task_id
            schedule_cfg = cfg.get("schedule") or {}
            schedule_enable_val = bool(schedule_cfg.get("enable")) if isinstance(schedule_cfg, dict) else False
            schedule_id = task_id if schedule_enable_val else None
            db_create_task(task_id, user_id_list, schedule_id=schedule_id)
            current_task_id = task_id

            # 如果启用定时任务，则将父任务的 task_id 写入 config.json 的 schedule.schedule_id
            if schedule_enable_val:
                try:
                    with open(config_path, "r", encoding="utf-8") as f:
                        latest_cfg = json.load(f)
                except Exception:
                    latest_cfg = {}
                schedule_cfg2 = latest_cfg.get("schedule") or {}
                if not isinstance(schedule_cfg2, dict):
                    schedule_cfg2 = {}
                schedule_cfg2["enable"] = True
                schedule_cfg2["interval_minutes"] = schedule_cfg2.get("interval_minutes", 60)
                schedule_cfg2["schedule_id"] = task_id
                latest_cfg["schedule"] = schedule_cfg2
                try:
                    with open(config_path, "w", encoding="utf-8") as f:
                        json.dump(latest_cfg, f, ensure_ascii=False, indent=4)
                    logger.info("已将定时任务父任务 schedule_id 写入 config.json: %s", task_id)
                except Exception as e:
                    logger.warning("写入 schedule_id 到 config.json 失败: %s", e)

        # 只有在“保存配置并启动任务”并且启用了定时任务时，才允许启动定时调度器
        if schedule_enable_val:
            _start_scheduler_if_needed()

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
            return (
                render_template(
                    "task_not_found.html", task_id=task_id, error=data["error"]
                ),
                404,
            )
        return jsonify(data), 404

    state = task.get("state")
    if state not in ["PENDING", "PROGRESS"]:
        # 任务已结束，不能再停止
        msg = f"任务当前状态为 {state}，无法停止"
        if wants_html():
            # 用任务详情模板展示错误信息
            response = {
                "task_id": task_id,
                "state": state,
                "progress": task.get("progress"),
                "created_at": task.get("created_at"),
                "user_id_list": task.get("user_id_list"),
                "message": msg,
            }
            return (
                render_template(
                    "task_detail.html",
                    task_id=task_id,
                    task_title="任务详情(无法停止)",
                    response=response,
                    state=state,
                    is_parent=False,
                    is_child=False,
                    parent_task_id=None,
                    child_tasks=[],
                    has_running_child=False,
                    can_download_weibo_dir=False,
                    can_stop=False,
                    stop_disabled=True,
                    notice_html="",
                    prev_task_id=None,
                    next_task_id=None,
                ),
                400,
            )
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
            return (
                render_template(
                    "task_not_found.html", task_id=task_id, error=data["error"]
                ),
                404,
            )
        return jsonify(data), 404

    if task.get("state") != "SUCCESS":
        msg = f"任务当前状态为 {task.get('state')}，仅在 SUCCESS 状态下才可下载 weibo 目录内容"
        if wants_html():
            response = {
                "task_id": task_id,
                "state": task.get("state"),
                "progress": task.get("progress"),
                "created_at": task.get("created_at"),
                "user_id_list": task.get("user_id_list"),
                "message": msg,
            }
            return (
                render_template(
                    "task_detail.html",
                    task_id=task_id,
                    task_title="任务详情(无法下载结果)",
                    response=response,
                    state=task.get("state"),
                    is_parent=False,
                    is_child=False,
                    parent_task_id=None,
                    child_tasks=[],
                    has_running_child=False,
                    can_download_weibo_dir=False,
                    can_stop=False,
                    stop_disabled=True,
                    notice_html="",
                    prev_task_id=None,
                    next_task_id=None,
                ),
                400,
            )
        return jsonify({"error": msg, "state": task.get("state")}), 400

    base_dir = os.path.split(os.path.realpath(__file__))[0]
    weibo_root = os.path.join(base_dir, "weibo")
    # 仅打包该任务专属目录 weibo/<task_id>/；如果不存在则直接报错
    task_weibo_dir = os.path.join(weibo_root, task_id)
    if not os.path.isdir(task_weibo_dir):
        data = {"error": f"未找到该任务对应的结果目录: {task_weibo_dir}"}
        if wants_html():
            return (
                render_template("schedule_error.html", error=data["error"]),
                500,
            )
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
            return (
                render_template("schedule_error.html", error=data["error"]),
                500,
            )
        return jsonify(data), 500

    return send_file(
        zip_path,
        as_attachment=True,
        download_name=zip_name,
        mimetype="application/zip",
    )


@app.route('/schedule/download', methods=['GET'])
def download_schedule_results():
    """
    下载某个定时任务（schedule）的聚合结果。
    - 基于 SQLite 中的完整数据，为每个用户导出“所有微博”和“所有评论”的 CSV，以及一份聚合 PDF
    - 默认选择最近一个有 schedule_id 的父任务；也可通过 ?schedule_id=<task_id> 指定
    """
    base_dir = os.path.split(os.path.realpath(__file__))[0]
    schedule_id = request.args.get("schedule_id")

    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()

        # 如果未指定 schedule_id，则选择最近一个存在的 schedule_id
        if not schedule_id:
            cur.execute(
                """
                SELECT schedule_id
                FROM tasks
                WHERE schedule_id IS NOT NULL AND schedule_id != ''
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row and row[0]:
                schedule_id = str(row[0])

        if not schedule_id:
            raise ValueError("未找到任何带有 schedule_id 的任务")

        # 聚合该 schedule 下的所有任务的 user_id_list，形成用户 ID 集合
        cur.execute(
            """
            SELECT user_id_list
            FROM tasks
            WHERE schedule_id = ?
            """,
            (schedule_id,),
        )
        rows = cur.fetchall()
        user_ids: set[str] = set()
        for r in rows:
            raw = r[0]
            if not raw:
                continue
            try:
                ul = json.loads(raw)
            except Exception:
                ul = raw
            if isinstance(ul, list):
                for item in ul:
                    if isinstance(item, dict):
                        uid = item.get("user_id") or item.get("id")
                    else:
                        uid = item
                    if uid:
                        user_ids.add(str(uid).strip())
            else:
                s = str(ul).strip()
                if s:
                    user_ids.add(s)
    except Exception as e:
        logger.warning("查询 schedule 任务列表失败: %s", e)
        if conn:
            conn.close()
        data = {"error": f"查询定时任务失败: {e}"}
        if wants_html():
            return (
                render_template("schedule_error.html", error=data["error"]),
                500,
            )
        return jsonify(data), 500
    finally:
        if conn:
            conn = None  # 真正的关闭操作在后面新的 conn 使用中处理

    if not user_ids:
        data = {"error": f"定时任务 {schedule_id} 下未找到任何用户信息"}
        if wants_html():
            return (
                render_template("schedule_error.html", error=data["error"]),
                404,
            )
        return jsonify(data), 404

    import tempfile
    import shutil
    from pathlib import Path
    from util.pdf_exporter import WeiboPdfExporter

    # 临时目录，用于聚合导出
    tmp_root = tempfile.mkdtemp(prefix=f"schedule_{schedule_id}_")
    agg_root = os.path.join(tmp_root, "aggregated")
    os.makedirs(agg_root, exist_ok=True)

    # 打开 SQLite 连接，后续查询用
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
    except Exception as e:
        shutil.rmtree(tmp_root, ignore_errors=True)
        data = {"error": f"打开 SQLite 数据库失败: {e}"}
        if wants_html():
            return (
                render_template("schedule_error.html", error=data["error"]),
                500,
            )
        return jsonify(data), 500

    # 为每个用户导出聚合微博 / 评论 CSV 和 PDF
    exported_any = False
    try:
        for uid in sorted(user_ids):
            if not uid:
                continue

            # 查询昵称
            nick = uid
            try:
                cur.execute("SELECT nick_name FROM user WHERE id = ?", (uid,))
                row = cur.fetchone()
                if row and row["nick_name"]:
                    nick = row["nick_name"]
            except Exception:
                pass
            safe_nick = re.sub(r'[\\/:*?"<>|]', "_", str(nick))

            user_dir = os.path.join(agg_root, safe_nick)
            os.makedirs(user_dir, exist_ok=True)

            # 导出所有微博
            try:
                cur.execute(
                    """
                    SELECT id, bid, user_id, screen_name, text, article_url,
                           topics, at_users, pics, video_url, live_photo_url,
                           location, created_at, source,
                           attitudes_count, comments_count, reposts_count, retweet_id
                    FROM weibo
                    WHERE user_id = ?
                    ORDER BY datetime(created_at) ASC, id ASC
                    """,
                    (uid,),
                )
                weibo_rows = cur.fetchall()
            except Exception as e:
                logger.warning("导出用户 %s 微博失败: %s", uid, e)
                weibo_rows = []

            if weibo_rows:
                weibo_csv_path = os.path.join(user_dir, f"{safe_nick}_weibos_all.csv")
                headers = list(weibo_rows[0].keys())
                import csv

                with open(weibo_csv_path, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    for r in weibo_rows:
                        writer.writerow([r[h] for h in headers])

            # 导出评论
            try:
                cur.execute(
                    """
                    SELECT
                        c.id,
                        c.weibo_id,
                        c.created_at,
                        c.user_screen_name,
                        c.text,
                        c.pic_url,
                        c.like_count
                    FROM comments c
                    JOIN weibo w ON c.weibo_id = w.id
                    WHERE w.user_id = ?
                    ORDER BY datetime(c.created_at) ASC, c.id ASC
                    """,
                    (uid,),
                )
                comment_rows = cur.fetchall()
            except Exception as e:
                logger.warning("导出用户 %s 评论失败: %s", uid, e)
                comment_rows = []

            if comment_rows:
                comments_csv_path = os.path.join(user_dir, f"{safe_nick}_comments_all.csv")
                headers = list(comment_rows[0].keys())
                import csv

                with open(comments_csv_path, "w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    for r in comment_rows:
                        writer.writerow([r[h] for h in headers])

            # 导出 PDF（聚合所有微博+评论）
            try:
                db_path = Path(DATABASE_PATH)
                pdf_exporter = WeiboPdfExporter(db_path=db_path)
                pdf_path = pdf_exporter.export_user_timeline(
                    user_id=str(uid),
                    output_path=os.path.join(user_dir, f"{safe_nick}_all.pdf"),
                )
                logger.info("为用户 %s 导出聚合 PDF: %s", uid, pdf_path)
            except Exception as e:
                logger.warning("为用户 %s 导出聚合 PDF 失败: %s", uid, e)

            exported_any = exported_any or bool(weibo_rows or comment_rows)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not exported_any:
        shutil.rmtree(tmp_root, ignore_errors=True)
        data = {"error": f"定时任务 {schedule_id} 下没有可导出的微博或评论数据"}
        if wants_html():
            return (
                render_template("schedule_error.html", error=data["error"]),
                404,
            )
        return jsonify(data), 404

    zip_base = os.path.join(tmp_root, f"schedule_{schedule_id}_aggregated")
    try:
        zip_path = shutil.make_archive(zip_base, "zip", agg_root)
    except Exception as e:
        logger.exception("打包定时任务结果失败: %s", e)
        shutil.rmtree(tmp_root, ignore_errors=True)
        data = {"error": f"打包定时任务结果失败: {e}"}
        if wants_html():
            return (
                render_template("schedule_error.html", error=data["error"]),
                500,
            )
        return jsonify(data), 500

    # 不立即删除 zip 文件，让 send_file 可以访问；临时目录稍后由系统回收
    download_name = f"schedule_results_{schedule_id}.zip"
    return send_file(zip_path, as_attachment=True, download_name=download_name, mimetype="application/zip")

@app.route('/task/<task_id>', methods=['GET'])
def get_task_status(task_id):
    task = db_get_task(task_id)
    if not task:
        data = {'error': 'Task not found'}
        if wants_html():
            return (
                render_template(
                    "task_not_found.html", task_id=task_id, error=data["error"]
                ),
                404,
            )
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
        # 父子关系信息
        schedule_id = task.get("schedule_id")
        is_parent = bool(schedule_id and str(schedule_id) == task_id)
        is_child = bool(schedule_id and str(schedule_id) != task_id)
        parent_task_id = str(schedule_id) if is_child else None
        has_running_child = False

        # 如果是父任务（schedule_id == 本任务ID），按时间轴展示所有子任务链接
        child_tasks: list[dict] = []
        if is_parent:
            try:
                conn = sqlite3.connect(DATABASE_PATH)
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT task_id, state, progress, created_at
                    FROM tasks
                    WHERE schedule_id = ? AND task_id != ?
                    ORDER BY created_at ASC
                    """,
                    (task_id, task_id),
                )
                child_rows = cur.fetchall()
            except Exception as e:
                logger.warning("查询父任务 %s 的子任务失败: %s", task_id, e)
                child_rows = []
            finally:
                try:
                    conn.close()  # type: ignore[name-defined]
                except Exception:
                    pass
            if child_rows:
                for cid, cstate, cprog, ccreated in child_rows:
                    if str(cstate) in ["PENDING", "PROGRESS"]:
                        has_running_child = True
                    child_tasks.append(
                        {
                            "task_id": str(cid),
                            "state": str(cstate),
                            "progress": cprog,
                            "created_at": str(ccreated),
                        }
                    )
        # 下载当前任务 weibo 目录内容：
        # - SUCCESS：正常可点
        # - PENDING/PROGRESS：灰色提示，按钮不可点
        state = task.get('state')
        can_download_weibo_dir = state == 'SUCCESS'

        # 如果任务仍在运行，则提供“停止该任务”的按钮；
        # 当 command 已为 STOP 时，按钮置灰不可用
        can_stop = task.get('state') in ['PENDING', 'PROGRESS']
        stop_disabled = bool(task.get('command') == 'STOP')
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

        # 根据是否为定时任务父/子任务或普通任务，设置页面标题
        if schedule_id and str(schedule_id) == task_id:
            task_title = "任务详情(定时任务 -> 父任务)"
        elif schedule_id and str(schedule_id) != task_id:
            task_title = "任务详情(定时任务 -> 子任务)"
        else:
            task_title = "任务详情(普通任务)"

        return render_template(
            "task_detail.html",
            task_id=task_id,
            task_title=task_title,
            response=response,
            state=state,
            is_parent=is_parent,
            is_child=is_child,
            parent_task_id=parent_task_id,
            child_tasks=child_tasks,
            has_running_child=has_running_child,
            can_download_weibo_dir=can_download_weibo_dir,
            can_stop=can_stop,
            stop_disabled=stop_disabled,
            notice_html=notice_html,
            prev_task_id=prev_task_id,
            next_task_id=next_task_id,
        )

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
                    # 同时删除 weibo 目录下对应的任务结果目录 weibo/<task_id>/
                    try:
                        import shutil

                        base_dir = os.path.split(os.path.realpath(__file__))[0]
                        task_weibo_dir = os.path.join(base_dir, "weibo", str(task_id))
                        if os.path.isdir(task_weibo_dir):
                            shutil.rmtree(task_weibo_dir, ignore_errors=True)
                            logger.info("已删除任务 %s 的 weibo 目录: %s", task_id, task_weibo_dir)
                    except Exception as fs_err:
                        logger.warning("删除任务 %s 的 weibo 目录失败: %s", task_id, fs_err)
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
            SELECT task_id, state, progress, created_at, user_id_list, command, error, result, schedule_id
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
        return render_template("tasks.html", items=items, notice_html=notice_html)
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
            # 计算父/子图标类名
            icon_class = ""
            schedule_id = running_task.get("schedule_id")
            if schedule_id:
                if str(schedule_id) == str(data["task_id"]):
                    icon_class = "icon-fu"
                else:
                    icon_class = "icon-zi"

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
            return (
                render_template(
                    "status.html",
                    running=True,
                    data=data,
                    icon_class=icon_class,
                    notice_html=notice_html,
                    stop_button_html=stop_button_html,
                ),
                200,
            )
        return jsonify(data), 200

    # 没有正在运行的任务，尝试给出最近一个任务的简要信息（如果有）
    last_task = None
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT task_id, state, progress, created_at, user_id_list, command, error, result, schedule_id
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
        # 预先计算最近任务的父/子图标类名（如果存在）
        icon_class = ""
        if last_task:
            lt_id = str(last_task.get("task_id") or "")
            schedule_id = last_task.get("schedule_id")
            if schedule_id:
                if str(schedule_id) == lt_id:
                    icon_class = "icon-fu"
                else:
                    icon_class = "icon-zi"

        return (
            render_template(
                "status.html",
                running=False,
                data=data,
                icon_class=icon_class,
                notice_html="",
                stop_button_html="",
            ),
            200,
        )
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
            display_columns = [col for col in columns if not _is_link_field(col)]
            # 将每条微博转换为普通 dict，方便模板访问
            simple_weibos = []
            for item in weibos:
                simple_weibos.append({k: ("" if v is None else v) for k, v in item.items()})
            return (
                render_template(
                    "weibos.html",
                    display_columns=display_columns,
                    weibos=simple_weibos,
                ),
                200,
            )

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
                return (
                    render_template(
                        "weibo_not_found.html",
                        weibo_id=weibo_id,
                        error=data["error"],
                    ),
                    404,
                )
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
            # 将字段值中的 None 转为空字符串，便于模板展示
            weibo_simple = {k: ("" if v is None else v) for k, v in weibo.items()}
            return (
                render_template(
                    "weibo_detail.html",
                    weibo_id=weibo_id,
                    weibo=weibo_simple,
                    prev_weibo_id=prev_weibo_id,
                    next_weibo_id=next_weibo_id,
                    is_link_field=_is_link_field,
                ),
                200,
            )
        return jsonify(weibo), 200
    except Exception as e:
        logger.exception(e)
        return {"error": str(e)}, 500



if __name__ == "__main__":
    # 服务启动时，根据 config.json 自动决定是否恢复定时任务
    _auto_start_scheduler_on_boot()
    logger.info("服务启动，提供 Web 管理界面，如已配置定时任务将自动恢复调度")
    # 启动Flask应用
    app.run(debug=True, use_reloader=False)  # 关闭reloader避免启动两次
