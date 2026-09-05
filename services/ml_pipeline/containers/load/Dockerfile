# Build this image from the repository root:
# docker build -f services/ml_pipeline/containers/load/Dockerfile .
FROM python:3.12-slim
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.12.6 /uv /bin/uv
RUN uv pip install --system --no-cache "gcsfs>=2025.9,<2027" "pandas>=2.2,<3" "pyarrow>=20,<24" "google-cloud-bigquery>=3.25,<4" "feast[gcp]>=0.66,<0.67"
COPY services/ml_pipeline/src ./src
COPY services/feast_store ./feast_store
COPY services/ml_pipeline/containers/load/main.py ./main.py
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["python", "main.py"]
