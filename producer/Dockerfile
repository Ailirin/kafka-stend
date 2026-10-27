FROM python:3.12-slim

# librdkafka требует системные библиотеки
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev gcc \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/

EXPOSE 8001
CMD ["python", "-m", "app.main"]
