FROM python:3.13-slim-bookworm

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
COPY pyproject.toml uv.lock ./
RUN uv pip install --system --no-cache -r pyproject.toml
COPY src/ ./src/
COPY scripts/ ./scripts/
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
CMD ["market-aggregator"]
