# Weibo API 说明文档

本文档详细介绍了基于 Flask 框架构建的 Weibo API 的各个端点及其使用方法。API 主要用于刷新微博数据、查询任务状态以及获取微博信息。

## 目录
1. [概述](#概述)
2. [配置说明](#配置说明)
3. [API 端点](#api-端点)
    - [刷新微博数据](#刷新微博数据)
    - [查询单个任务状态](#查询单个任务状态)
    - [列出所有任务](#列出所有任务)
    - [当前运行状态](#当前运行状态)
    - [停止任务](#停止任务)
    - [下载任务数据](#下载任务数据)
    - [下载定时任务聚合数据](#下载定时任务聚合数据)
    - [获取所有微博](#获取所有微博)
    - [获取单条微博详情](#获取单条微博详情)
4. [定时任务](#定时任务)
5. [数据过期与清理](#数据过期与清理)
6. [错误处理](#错误处理)
7. [日志记录](#日志记录)

---

## 概述

该 API 旨在通过定时任务和手动触发的任务来抓取和管理微博数据。主要功能包括：
- 刷新指定用户的微博数据
- 查询任务的执行状态
- 获取所有抓取到的微博
- 获取单条微博的详细信息

## 配置说明

在运行 API 之前，需要配置根目录下的 `config.json`。配置项包括用户ID列表、数据库路径、日志配置、下载选项、通知和定时任务等。

主要配置段落：

- 顶层字段基本与原脚本 `weibo.py` 的配置一致（`user_id_list`、`only_crawl_original`、`write_mode`、`cookie` 等）。
- 通知配置：

  ```json
  "notify": {
    "enable": true,
    "push_key": "your_pushdeer_key"
  }
  ```

- 定时任务配置：

  ```json
  "schedule": {
    "enable": true,
    "interval_minutes": 60,
    "schedule_id": "由 Web 界面的“保存配置并启动任务”自动写入"
  }
  ```

`service.py` 通过 `config.json` 驱动爬虫与任务调度，不再依赖内嵌的 Python 字典配置。

- **user_id_list**: 需要抓取微博的用户ID列表。
- **only_crawl_original**: 是否仅抓取原创微博（1为是，0为否）。
- **since_date**: 起始日期。
- **start_page**: 起始页码。
- **write_mode**: 数据存储模式，可选择 CSV、JSON、SQLite 等。
- **original_pic_download**: 是否下载原创微博的图片（1为是，0为否）。
- **retweet_pic_download**: 是否下载转发微博的图片（1为是，0为否）。
- **original_video_download**: 是否下载原创微博的视频（1为是，0为否）。
- **retweet_video_download**: 是否下载转发微博的视频（1为是，0为否）。
- **download_comment**: 是否下载评论（1为是，0为否）。
- **comment_max_download_count**: 评论的最大下载数量。
- **download_repost**: 是否下载转发信息（1为是，0为否）。
- **repost_max_download_count**: 转发的最大下载数量。
- **user_id_as_folder_name**: 是否使用用户ID作为文件夹名称（1为是，0为否）。
- **remove_html_tag**: 是否移除HTML标签（1为是，0为否）。
- **cookie**: 用于认证的微博Cookie。
- **mysql_config**: MySQL数据库的配置信息。
- **mongodb_URI**: MongoDB的连接URI。
- **post_config**: POST请求的配置，包括API URL和Token。

## API 端点

### 刷新微博数据

**URL:** `/refresh`

**方法:** `POST`（主要用于 Web 表单，API 客户端可参考）

**描述:** 触发一次新的刷新任务。推荐通过 Web 管理界面 `/refresh` 进行配置和启动。

**行为概述:**

- 当没有正在运行的任务时，创建一个新的任务记录（写入 `tasks` 表）并在后台异步执行。
- 当已有任务在运行时，返回 409 状态：
  - HTML 请求会渲染提示页面，引导用户跳转到当前任务详情；
  - JSON 请求（`Accept: application/json`）会返回带有 `error` 和当前 `task_id` 的 JSON。

**响应:**

表单模式下通常返回 302 重定向到 `/status`。JSON 客户端应检查状态码并在 409 时读取返回的错误信息。

### 查询单个任务状态

**URL:** `/task/<task_id>`

**方法:** `GET`

**描述:** 查询指定任务的状态和进度。

**URL 参数:**

- `task_id` (必需): 需要查询的任务ID。

**响应:**

- **200 OK**
  ```json
  {
      "task_id": "任务ID",
      "state": "SUCCESS",
      "progress": 100,
      "created_at": "2025-01-01T12:00:00",
      "user_id_list": [...],
      "result": "微博列表已刷新"
  }
  ```
  或者在任务失败时：
  ```json
  {
      "state": "FAILED",
      "progress": 50,
      "error": "错误信息"
  }
  ```
- **404 Not Found** (任务不存在)
  ```json
  {
      "error": "Task not found"
  }
  ```

### 列出所有任务

**URL:** `/tasks`

**方法:** `GET`

**描述:** 按创建时间倒序列出所有任务。

**响应:**

- **200 OK**

  ```json
  [
    {
      "task_id": "任务ID",
      "state": "SUCCESS",
      "progress": 100,
      "created_at": "2025-01-01T12:00:00",
      "user_id_list": [...],
      "command": "FINISHED",
      "error": null,
      "result": "微博列表已刷新",
      "schedule_id": "父任务ID或null",
      "download_expired": false
    },
    ...
  ]
  ```

### 当前运行状态

**URL:** `/status`

**方法:** `GET`

**描述:** 查询当前是否有任务运行，以及最近任务的摘要。

**响应示例:**

- 有任务运行：

  ```json
  {
    "task_id": "当前任务ID",
    "state": "PROGRESS",
    "progress": 42,
    "created_at": "2025-01-01T12:00:00",
    "user_id_list": [...]
  }
  ```

- 无任务运行：

  ```json
  {
    "state": "IDLE",
    "current_task": null,
    "last_task_id": "最近一次任务ID",
    "last_state": "SUCCESS",
    "last_progress": 100,
    "last_created_at": "2025-01-01T11:00:00"
  }
  ```

### 停止任务

**URL:** `/task/<task_id>/stop`

**方法:** `POST`

**描述:** 请求优雅停止指定任务。任务会在下一次安全检查点退出。

**响应:**

- **200 OK**

  ```json
  {
    "message": "停止请求已发送",
    "task_id": "任务ID"
  }
  ```

- **400 Bad Request**（任务已结束，无法停止）  
- **404 Not Found**（任务不存在）

### 下载任务数据

**URL:** `/task/<task_id>/download`

**方法:** `GET`

**描述:** 打包并下载指定任务的 `weibo/<task_id>/` 目录内容（ZIP 文件）。仅在任务 `state=SUCCESS` 且数据未过期时可用。

**行为:**

- 成功时返回 `application/zip` 附件。
- 若任务不存在、未成功或对应目录不存在，会返回错误 JSON 或 HTML 提示。

### 下载定时任务聚合数据

**URL:** `/schedule/download?schedule_id=<task_id>`

**方法:** `GET`

**描述:** 对于定时任务的父任务（`schedule_id == task_id`），聚合该父任务及其子任务的数据，并返回 ZIP 打包结果。

**行为:**

- 成功时返回聚合 ZIP。
- 如无用户信息、无可导出数据或打包异常，返回错误 JSON/HTML。

### 获取所有微博

**URL:** `/weibos`

**方法:** `GET`

**描述:** 获取数据库中所有抓取到的微博，按创建时间倒序排列。

**响应:**

- **200 OK**
  ```json
  [
      {
          "id": "微博ID",
          "content": "微博内容",
          "created_at": "创建时间",
          ...
      },
      ...
  ]
  ```
- **500 Internal Server Error** (服务器错误)
  ```json
  {
      "error": "错误信息"
  }
  ```

### 获取单条微博详情

**URL:** `/weibos/<weibo_id>`

**方法:** `GET`

**描述:** 获取指定ID的微博详细信息。

**URL 参数:**

- `weibo_id` (必需): 需要获取的微博ID。

**响应:**

- **200 OK**
  ```json
  {
      "id": "微博ID",
      "content": "微博内容",
      "created_at": "创建时间",
      ...
  }
  ```
- **404 Not Found** (微博不存在)
  ```json
  {
      "error": "Weibo not found"
  }
  ```
- **500 Internal Server Error** (服务器错误)
  ```json
  {
      "error": "错误信息"
  }
  ```

## 定时任务

API 启动后，会在后台启动一个简单调度线程 `_schedule_loop`，用于轮询 `config.json` 中的定时配置：

- 仅当 `schedule.enable` 为 `true` 且存在 `schedule.schedule_id` 时，调度生效。
- 调度间隔由 `schedule.interval_minutes` 控制，默认 60 分钟。
- 每轮调度会检查：
  - 是否存在运行中的任务；
  - 距离最近一次定时任务的创建时间是否已超过 `interval_minutes`。
- 条件满足时，会以最新的 `config.json` 为配置创建一个新的“子任务”（`schedule_id` 指向父任务 ID）。

父任务及所有子任务可在 Web 界面和 `/tasks`、`/task/<task_id>` 等端点中查看。

## 数据过期与清理

为减少磁盘占用，API 会根据任务配置中的 `end_date` 做简单的数据过期处理：

- 对于每个任务，从 `user_id_list` 中解析出最新的 `end_date`。
- 若该日期距离当前时间超过 7 天，则视为“下载数据已过期”：
  - 会尝试删除对应的 `weibo/<task_id>/` 目录；
  - 在任务列表 JSON 中标记 `download_expired: true`；
  - 任务详情页和下载端点会以灰色提示说明“超过7天，已删除”，不再提供可点击下载链接。

注意：任务记录本身不会被删除，仍可通过 `/task/<task_id>` 和 `/tasks` 查询到。

## 错误处理

API 在处理请求时可能会遇到各种错误，主要包括：

- **400 Bad Request:** 请求参数无效。
- **404 Not Found:** 请求的资源不存在（如任务ID或微博ID）。
- **409 Conflict:** 资源冲突，如已有任务在运行。
- **500 Internal Server Error:** 服务器内部错误。

错误响应的格式统一为包含 `error` 字段的 JSON 对象，例如：

```json
{
    "error": "错误信息"
}
```

## 日志记录

API 使用 Python 的 `logging` 模块进行日志记录。日志文件存储在 `log/` 目录下，默认日志配置文件为 `logging.conf`。主要记录以下内容：

- 服务启动和关闭日志
- 任务的启动、完成和失败日志
- 定时任务的执行情况
- 异常和错误信息

确保 `log/` 目录存在，API 会在启动时自动创建该目录（如果不存在）。

---

## 启动和运行

确保所有依赖库已安装，并正确配置了 `logging.conf` 文件和数据库路径。启动 API 的命令如下：

```bash
python service.py
```

API 将在默认的 `5000` 端口启动，并自动开始监听请求和执行定时任务。

---

## 注意事项

- **并发任务控制:** API 限制同一时间只能运行一个刷新任务，以避免数据冲突和资源竞争。
- **数据存储:** 默认使用 SQLite 数据库存储微博数据，配置中可根据需要调整为其他存储方式（如 MySQL、MongoDB）。
- **安全性:** 确保 `cookie` 和数据库的敏感信息安全存储，避免泄露。

如有任何疑问或问题，请参考源代码中的注释或联系开发团队。
