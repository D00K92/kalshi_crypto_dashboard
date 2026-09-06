FROM python:3.12-slim-bookworm

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
COPY pyproject.toml uv.lock ./
RUN uv pip install --system --no-cache -r pyproject.toml \
    && groupadd --system analytics \
    && useradd --system --gid analytics --home-dir /nonexistent analytics
COPY --chown=analytics:analytics src/ ./src/
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
USER analytics
EXPOSE 8080
CMD ["python", "-m", "kalshi_crypto_analytics"]
