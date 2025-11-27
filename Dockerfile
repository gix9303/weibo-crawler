# 设置基础镜像
FROM python:3.12.0-alpine

# 安装 tzdata 设置时区
RUN apk add --no-cache tzdata \
    && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone

# 设置工作目录
WORKDIR /app

# 复制项目文件到工作目录
COPY requirements.txt .

# 使用 Aliyun 镜像源加速 pip，并安装依赖
RUN pip install -i https://mirrors.aliyun.com/pypi/simple/ -U pip \
    && pip config set global.index-url https://mirrors.aliyun.com/pypi/simple/ \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn

# 复制项目文件到工作目录
COPY . .

# 暴露 gunicorn 监听端口
EXPOSE 8000

# 使用 gunicorn 启动 service.py 中的 Flask 应用
# -w 2: 两个 worker，视服务器性能可调整
# -b 0.0.0.0:8000: 监听容器内 8000 端口
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8000", "service:app"]
