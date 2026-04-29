FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8080 \
    DB_PATH=/app/data/press_unified.db \
    INTERNAL_RULE_DIR=/app/data/internal_rules \
    EMBEDDING_PROVIDER=sentence-transformers \
    EMBEDDING_MODEL=intfloat/multilingual-e5-small \
    VECTOR_STORE=qdrant \
    QDRANT_URL=http://qdrant:6333 \
    QDRANT_COLLECTION=press_chunks

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        nodejs \
        npm \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["sh", "-c", "python local_dashboard.py --db-path \"${DB_PATH}\" --host \"${HOST}\" --port \"${PORT}\""]
