FROM python:3.13-slim AS builder

ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

RUN addgroup --system exporter && adduser --system --ingroup exporter exporter
COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
USER exporter
EXPOSE 8080

CMD ["python", "-m", "gcs_exporter"]
