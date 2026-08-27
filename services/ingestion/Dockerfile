FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN addgroup --system ingestion && adduser --system --ingroup ingestion ingestion

COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir .

USER ingestion

CMD ["python", "-m", "ingestion"]
