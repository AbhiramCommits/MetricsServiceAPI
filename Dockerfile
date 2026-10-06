FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY metrics_service ./metrics_service
COPY scripts ./scripts
COPY Makefile .
COPY metrics_service/sql ./metrics_service/sql

EXPOSE 8000

CMD ["uvicorn", "metrics_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
