FROM python:3.12-slim

WORKDIR /app/feast_store
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY . ./
RUN chmod +x materialize.sh

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
EXPOSE 6566
ENTRYPOINT ["feast"]
CMD ["serve", "--host", "0.0.0.0", "--port", "6566"]
