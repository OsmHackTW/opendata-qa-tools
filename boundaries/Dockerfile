FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /bin/uv

WORKDIR /app

COPY pyproject.toml ./

RUN uv sync --no-dev
