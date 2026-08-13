# ---- build stage ----
FROM python:3.11-slim AS builder

# 国内 PyPI 镜像加速
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple

WORKDIR /build

# apt 换清华源（deb.debian.org 在国内不稳定）
RUN sed -i 's|deb.debian.org|mirrors.tuna.tsinghua.edu.cn|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends \
        gcc python3-dev libpq-dev libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- runtime stage ----
FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /install /usr/local
COPY . /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command runs the processor. Override with `command:` in compose
# (e.g. to launch the dashboard) when needed.
CMD ["python", "main.py"]
