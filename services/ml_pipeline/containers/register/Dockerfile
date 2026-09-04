FROM python:3.12-slim
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.12.6 /uv /bin/uv
RUN uv pip install --system --no-cache "google-cloud-aiplatform>=1.60,<2" "google-cloud-storage>=3.4,<4"
COPY containers/register/main.py ./main.py
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["python", "main.py"]
