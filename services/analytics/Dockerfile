FROM python:3.12-slim-bookworm

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
COPY pyproject.toml uv.lock ./
RUN uv pip install --system --no-cache -r pyproject.toml \
    && groupadd --gid 10001 analytics \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin analytics
COPY --chown=10001:10001 src/ ./src/
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
USER 10001:10001
EXPOSE 8080
CMD ["python", "-m", "kalshi_crypto_analytics"]
