# syntax=docker/dockerfile:1.7

FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project --python /usr/local/bin/python

COPY sevenma_crawler ./sevenma_crawler
COPY main.py ./main.py
COPY 南信大选点.json ./南信大选点.json

FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY sevenma_crawler ./sevenma_crawler
COPY main.py ./main.py
COPY README.md pyproject.toml uv.lock ./
COPY 南信大选点.json ./南信大选点.json

EXPOSE 8000

CMD ["python", "-m", "sevenma_crawler", "--help"]
