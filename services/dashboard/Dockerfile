FROM python:3.13-slim-bookworm
WORKDIR /app
RUN pip install --no-cache-dir uv
COPY pyproject.toml uv.lock* ./
RUN uv pip install --system -r pyproject.toml
COPY src/ ./src/
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
EXPOSE 8050
CMD ["python", "-m", "dashboard"]
