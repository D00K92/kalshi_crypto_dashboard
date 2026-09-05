FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.12.6 /uv /bin/uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY scripts ./scripts
COPY sql ./sql

RUN addgroup --system batch-etl && adduser --system --ingroup batch-etl batch-etl
USER 100:101

ENTRYPOINT ["python", "scripts/run_hourly_resampling.py"]
