FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 1000 appuser

COPY app/ ./app/
RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8001
CMD ["python", "-m", "app.main"]
