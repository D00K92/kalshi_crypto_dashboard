FROM python:3.12-slim
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.12.6 /uv /bin/uv
RUN uv pip install --system --no-cache "gcsfs>=2025.9,<2027" "pandas>=2.2,<3" "pyarrow>=20,<24"
COPY src ./src
COPY containers/load/main.py ./main.py
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["python", "main.py"]
