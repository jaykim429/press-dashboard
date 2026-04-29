# Unified retrieval architecture

## Goal

All analysis surfaces should use the same candidate-selection pipeline:

1. Parse source documents with the available parser (`kordoc` for HWP/HWPX/PDF where possible).
2. Split documents into reusable chunks.
3. Retrieve candidates with BM25.
4. Retrieve candidates with embeddings.
5. Merge ranks with RRF.
6. Optionally apply a domain reranker or LLM judge to the fused shortlist.

This avoids separate scoring logic for press releases, administrative guidance, attachments, and internal rules.

## Current implementation

`unified_retrieval.py` provides the common classes:

- `CorpusDocument`
- `TextChunk`
- `TextChunker`
- `Bm25Ranker`
- `HashEmbeddingProvider`
- `SentenceTransformerEmbeddingProvider`
- `OpenAICompatibleEmbeddingProvider`
- `EmbeddingRanker`
- `QdrantVectorStore`
- `QdrantEmbeddingRanker`
- `RrfFusion`
- `UnifiedRetriever`

The local fallback embedding provider is `HashEmbeddingProvider`, so the system can still run without an external vector API. Docker defaults to a real multilingual embedding model with Qdrant:

```bash
EMBEDDING_PROVIDER=sentence-transformers
EMBEDDING_MODEL=intfloat/multilingual-e5-small
VECTOR_STORE=qdrant
QDRANT_URL=http://qdrant:6333
QDRANT_COLLECTION=press_chunks
```

For an OpenAI-compatible embedding endpoint instead, configure:

```bash
EMBEDDING_PROVIDER=openai
EMBEDDING_API_BASE=https://api.openai.com/v1
EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_API_KEY=...
```

Any OpenAI-compatible `/embeddings` endpoint can be used.

When `VECTOR_STORE=qdrant` or `QDRANT_URL` is set, semantic chunks are upserted into Qdrant with deterministic point IDs. This makes repeated analysis runs idempotent while keeping BM25 in memory for the active corpus. Search results are fused as:

- `bm25`
- `qdrant` or in-memory `embedding`
- `rrf` final rank

## First integrated workflow

`internal_rule_impact_builder.py` now uses `UnifiedRetriever` for internal-rule candidate retrieval.

The administrative guidance text is the query. Parsed internal rules are indexed as chunked documents. The builder then:

- retrieves BM25 candidates,
- retrieves embedding candidates,
- fuses them with RRF,
- applies light domain adjustment for internal-rule impact analysis,
- sends the selected rules to the LLM report generator.

## Recommended next extensions

1. Use the same retriever for related-news search.
2. Use the same retriever for similar-disclosure search.
3. Add an LLM reranker for the top 20 candidates only.
4. Add a scheduled Qdrant warm-up job for large corpora.

Suggested SQLite metadata table when Qdrant is used as the vector store:

```sql
CREATE TABLE retrieval_chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  corpus_type TEXT NOT NULL,
  source_id TEXT NOT NULL,
  chunk_id TEXT NOT NULL UNIQUE,
  title TEXT,
  text TEXT NOT NULL,
  metadata_json TEXT,
  created_at TEXT NOT NULL
);
```

Qdrant should hold vectors and payloads; SQLite can remain the durable registry for source metadata, parser status, and refresh timestamps.
