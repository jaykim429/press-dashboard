# Docker usage

## Build and run

```bash
docker compose up --build
```

Open:

```text
http://localhost:8080
```

The compose file mounts local runtime data:

- `./press_unified.db` -> `/app/data/press_unified.db`
- `./3. 내규목록` -> `/app/data/internal_rules`
- `./attachment_store` -> `/app/attachment_store`
- `./logs` -> `/app/logs`
- `./tmp` -> `/app/tmp`
- `qdrant_storage` -> Qdrant vector database storage

## LLM configuration

Defaults use the local OpenAI-compatible chat endpoint already wired into the app:

```bash
LLM_PROVIDER=openai
LLM_MODEL=google/gemma-4-26B-A4B-it
LLM_API_BASE=http://222.110.207.7:8000/v1
```

Override when needed:

```bash
LLM_API_BASE=https://api.openai.com/v1 \
LLM_MODEL=gpt-4o-mini \
LLM_API_KEY=... \
docker compose up --build
```

## Embedding configuration

Docker defaults to local multilingual sentence-transformer embeddings stored in Qdrant:

```bash
EMBEDDING_PROVIDER=sentence-transformers
EMBEDDING_MODEL=intfloat/multilingual-e5-small
VECTOR_STORE=qdrant
QDRANT_URL=http://qdrant:6333
QDRANT_COLLECTION=press_chunks
```

If you change the embedding model and Qdrant reports a vector-size mismatch, rebuild the collection once:

```bash
QDRANT_RECREATE_COLLECTION=1 docker compose up --build
```

For an OpenAI-compatible embedding endpoint instead:

```bash
EMBEDDING_PROVIDER=openai \
EMBEDDING_API_BASE=https://api.openai.com/v1 \
EMBEDDING_MODEL=text-embedding-3-small \
EMBEDDING_API_KEY=... \
docker compose up --build
```

For quick local smoke tests without a model download or Qdrant, override to:

```bash
EMBEDDING_PROVIDER=hash VECTOR_STORE= docker compose up --build
```

The retrieval layer uses BM25 and RRF in every mode. With Qdrant enabled, vector hits are persisted and reused by deterministic chunk IDs.
