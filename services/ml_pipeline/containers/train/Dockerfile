FROM python:3.12-slim
WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.12.6 /uv /bin/uv
RUN uv pip install --system --no-cache "pandas>=2.2,<3" "pyarrow>=20,<24" "numpy>=2,<3" "scikit-learn>=1.5,<2" "xgboost>=2.1,<3" "joblib>=1.4,<2"
COPY src ./src
COPY containers/train/main.py ./main.py
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["python", "main.py"]
