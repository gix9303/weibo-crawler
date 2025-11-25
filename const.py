"""
全局常量配置。

说明：
- 模式、cookie 检查等为代码级默认值
- 通知配置（NOTIFY）会在读取 config.json 时根据其中的变量覆盖
"""

# 运行模式:
# - "append": 仅增量抓取（要求开启 sqlite）
# - "overwrite": 每次抓取全量微博
MODE = "overwrite"

# cookie 检查配置
CHECK_COOKIE = {
    "CHECK": False,           # 是否检查 cookie
    "CHECKED": False,         # 判断已检查 cookie 的标志位（请勿修改）
    "EXIT_AFTER_CHECK": False,  # append 模式中仅等待 cookie 检查时使用（请勿修改）
    "HIDDEN_WEIBO": "微博内容",  # 用于 cookie 检查时的“隐藏微博”内容
    "GUESS_PIN": False,       # 猜测第一条微博是否为置顶（请勿修改）
}

# 通知配置，实际值会在读取 config.json 时根据其中的字段覆盖
NOTIFY = {
    "NOTIFY": False,   # 是否开启通知，默认关闭
    "PUSH_KEY": "",    # push_deer 的 push_key，默认留空
}
