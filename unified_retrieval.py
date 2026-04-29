#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

import numpy as np
import requests

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qmodels
except ImportError:  # pragma: no cover - optional production dependency
    QdrantClient = None  # type: ignore[assignment]
    qmodels = None  # type: ignore[assignment]

try:
    from sentence_transformers import SentenceTransformer
except ImportError:  # pragma: no cover - optional production dependency
    SentenceTransformer = None  # type: ignore[assignment]


TOKEN_RE = re.compile(r"[\uac00-\ud7a3A-Za-z0-9][\uac00-\ud7a3A-Za-z0-9_\-/\.]{1,}")


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def tokenize(text: str) -> List[str]:
    tokens: List[str] = []
    for raw in TOKEN_RE.findall(text or ""):
        token = raw.strip("._-/").lower()
        if len(token) < 2 or token.isdigit():
            continue
        tokens.append(token)
    return tokens


@dataclass
class CorpusDocument:
    doc_id: str
    title: str
    text: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TextChunk:
    chunk_id: str
    doc_id: str
    title: str
    text: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RankedChunk:
    chunk: TextChunk
    rank: int
    score: float
    source: str


@dataclass
class FusedCandidate:
    chunk: TextChunk
    rank: int
    score: float
    component_ranks: Dict[str, int] = field(default_factory=dict)
    component_scores: Dict[str, float] = field(default_factory=dict)


class TextChunker:
    def __init__(self, chunk_size: int = 1200, overlap: int = 160):
        self.chunk_size = max(300, chunk_size)
        self.overlap = max(0, min(overlap, self.chunk_size // 2))

    def split(self, doc: CorpusDocument) -> List[TextChunk]:
        text = normalize_ws(doc.text)
        if not text:
            return []
        chunks: List[TextChunk] = []
        start = 0
        index = 0
        while start < len(text):
            end = min(len(text), start + self.chunk_size)
            if end < len(text):
                split_at = max(text.rfind(". ", start, end), text.rfind(" ", start + self.chunk_size // 2, end))
                if split_at > start:
                    end = split_at + 1
            chunk_text = text[start:end].strip()
            if chunk_text:
                chunks.append(
                    TextChunk(
                        chunk_id=f"{doc.doc_id}#{index}",
                        doc_id=doc.doc_id,
                        title=doc.title,
                        text=chunk_text,
                        metadata={**doc.metadata, "chunk_index": index},
                    )
                )
                index += 1
            if end >= len(text):
                break
            start = max(0, end - self.overlap)
        return chunks


class Bm25Ranker:
    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.chunks: List[TextChunk] = []
        self.term_freqs: List[Counter[str]] = []
        self.doc_freq: Counter[str] = Counter()
        self.doc_lens: List[int] = []
        self.avgdl = 0.0

    def index(self, chunks: Sequence[TextChunk]) -> None:
        self.chunks = list(chunks)
        self.term_freqs = []
        self.doc_freq = Counter()
        self.doc_lens = []
        for chunk in self.chunks:
            terms = tokenize(f"{chunk.title} {chunk.text}")
            tf = Counter(terms)
            self.term_freqs.append(tf)
            self.doc_lens.append(sum(tf.values()))
            for term in tf:
                self.doc_freq[term] += 1
        self.avgdl = (sum(self.doc_lens) / len(self.doc_lens)) if self.doc_lens else 0.0

    def search(self, query: str, limit: int = 50) -> List[RankedChunk]:
        if not self.chunks:
            return []
        query_terms = tokenize(query)
        if not query_terms:
            return []
        n_docs = len(self.chunks)
        scores: List[Tuple[int, float]] = []
        for idx, tf in enumerate(self.term_freqs):
            score = 0.0
            dl = self.doc_lens[idx] or 1
            for term in query_terms:
                freq = tf.get(term, 0)
                if not freq:
                    continue
                df = self.doc_freq.get(term, 0)
                idf = math.log(1 + ((n_docs - df + 0.5) / (df + 0.5)))
                denom = freq + self.k1 * (1 - self.b + self.b * dl / (self.avgdl or 1))
                score += idf * (freq * (self.k1 + 1) / denom)
            if score > 0:
                scores.append((idx, score))
        scores.sort(key=lambda item: item[1], reverse=True)
        return [
            RankedChunk(self.chunks[idx], rank=rank, score=score, source="bm25")
            for rank, (idx, score) in enumerate(scores[:limit], 1)
        ]


class EmbeddingProvider(Protocol):
    def embed(self, texts: Sequence[str]) -> List[np.ndarray]:
        ...


class HashEmbeddingProvider:
    def __init__(self, dimensions: int = 384):
        self.dimensions = dimensions

    def embed(self, texts: Sequence[str]) -> List[np.ndarray]:
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> np.ndarray:
        vec = np.zeros(self.dimensions, dtype=np.float32)
        for token in tokenize(text):
            digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
            value = int.from_bytes(digest, "big", signed=False)
            idx = value % self.dimensions
            sign = 1.0 if (value >> 8) & 1 else -1.0
            vec[idx] += sign
        norm = float(np.linalg.norm(vec))
        if norm:
            vec /= norm
        return vec


class SentenceTransformerEmbeddingProvider:
    def __init__(self, model_name: str = "intfloat/multilingual-e5-small", batch_size: int = 32):
        if SentenceTransformer is None:
            raise RuntimeError("sentence-transformers is not installed")
        self.model_name = model_name
        self.batch_size = batch_size
        self.model = SentenceTransformer(model_name)
        self.dimensions = int(self.model.get_sentence_embedding_dimension() or 0)
        self.uses_e5_prefix = "e5" in model_name.lower()

    def embed(self, texts: Sequence[str]) -> List[np.ndarray]:
        prepared = [self._prepare_text(text, is_query=False) for text in texts]
        vectors = self.model.encode(
            prepared,
            batch_size=self.batch_size,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return [np.asarray(vec, dtype=np.float32) for vec in vectors]

    def embed_query(self, text: str) -> np.ndarray:
        prepared = self._prepare_text(text, is_query=True)
        vector = self.model.encode([prepared], normalize_embeddings=True, show_progress_bar=False)[0]
        return np.asarray(vector, dtype=np.float32)

    def _prepare_text(self, text: str, is_query: bool) -> str:
        text = normalize_ws(text)
        if not self.uses_e5_prefix:
            return text
        prefix = "query: " if is_query else "passage: "
        return f"{prefix}{text}"


class OpenAICompatibleEmbeddingProvider:
    def __init__(self, api_base: str, model: str, api_key: str = "", timeout: int = 60):
        self.api_base = api_base.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.timeout = timeout

    def embed(self, texts: Sequence[str]) -> List[np.ndarray]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        resp = requests.post(
            f"{self.api_base}/embeddings",
            headers=headers,
            json={"model": self.model, "input": list(texts)},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        vectors = []
        for item in sorted(data.get("data") or [], key=lambda x: x.get("index", 0)):
            vec = np.asarray(item.get("embedding") or [], dtype=np.float32)
            norm = float(np.linalg.norm(vec))
            if norm:
                vec = vec / norm
            vectors.append(vec)
        if len(vectors) != len(texts):
            raise RuntimeError("embedding response count mismatch")
        return vectors


def embedding_provider_from_env() -> EmbeddingProvider:
    provider = (os.getenv("EMBEDDING_PROVIDER") or "hash").strip().lower()
    if provider in {"sentence-transformers", "sentence_transformers", "sbert", "local"}:
        model = os.getenv("EMBEDDING_MODEL") or "intfloat/multilingual-e5-small"
        batch_size = int(os.getenv("EMBEDDING_BATCH_SIZE") or "32")
        return SentenceTransformerEmbeddingProvider(model_name=model, batch_size=batch_size)
    if provider in {"openai", "openai-compatible", "compatible"}:
        api_base = os.getenv("EMBEDDING_API_BASE") or os.getenv("LLM_API_BASE") or os.getenv("OPENAI_API_BASE") or ""
        model = os.getenv("EMBEDDING_MODEL") or "text-embedding-3-small"
        api_key = os.getenv("EMBEDDING_API_KEY") or os.getenv("LLM_API_KEY") or os.getenv("OPENAI_API_KEY") or ""
        if api_base:
            return OpenAICompatibleEmbeddingProvider(api_base=api_base, model=model, api_key=api_key)
    dims = int(os.getenv("HASH_EMBEDDING_DIMENSIONS") or "384")
    return HashEmbeddingProvider(dimensions=dims)


class EmbeddingRanker:
    def __init__(self, provider: EmbeddingProvider):
        self.provider = provider
        self.chunks: List[TextChunk] = []
        self.matrix: Optional[np.ndarray] = None

    def index(self, chunks: Sequence[TextChunk]) -> None:
        self.chunks = list(chunks)
        if not self.chunks:
            self.matrix = None
            return
        texts = [f"{chunk.title}\n{chunk.text}" for chunk in self.chunks]
        vectors = self.provider.embed(texts)
        self.matrix = np.vstack(vectors).astype(np.float32)

    def search(self, query: str, limit: int = 50) -> List[RankedChunk]:
        if self.matrix is None or not self.chunks:
            return []
        if hasattr(self.provider, "embed_query"):
            qvec = self.provider.embed_query(query).astype(np.float32)  # type: ignore[attr-defined]
        else:
            qvec = self.provider.embed([query])[0].astype(np.float32)
        scores = self.matrix @ qvec
        order = np.argsort(scores)[::-1][:limit]
        out: List[RankedChunk] = []
        for rank, idx in enumerate(order, 1):
            score = float(scores[idx])
            if score <= 0:
                continue
            out.append(RankedChunk(self.chunks[int(idx)], rank=rank, score=score, source="embedding"))
        return out


def stable_qdrant_id(value: str) -> str:
    return str(uuid.UUID(hashlib.md5(value.encode("utf-8")).hexdigest()))


class QdrantVectorStore:
    def __init__(self, url: str, collection_name: str, distance: str = "Cosine", api_key: str = ""):
        if QdrantClient is None or qmodels is None:
            raise RuntimeError("qdrant-client is not installed")
        self.url = url
        self.collection_name = collection_name
        self.distance = distance
        if url in {":memory:", "memory"}:
            self.client = QdrantClient(":memory:")
        else:
            self.client = QdrantClient(url=url, api_key=api_key or None)

    def ensure_collection(self, vector_size: int, recreate: bool = False) -> None:
        distance = getattr(qmodels.Distance, self.distance.upper(), qmodels.Distance.COSINE)
        vectors_config = qmodels.VectorParams(size=vector_size, distance=distance)
        if recreate:
            self.client.recreate_collection(collection_name=self.collection_name, vectors_config=vectors_config)
            return
        try:
            info = self.client.get_collection(self.collection_name)
            current = getattr(getattr(info.config, "params", None), "vectors", None)
            current_size = getattr(current, "size", None)
            if current_size is not None and int(current_size) != int(vector_size):
                raise RuntimeError(
                    f"Qdrant collection '{self.collection_name}' vector size is {current_size}, "
                    f"but embedding model produced {vector_size}. Set QDRANT_RECREATE_COLLECTION=1 to rebuild it."
                )
        except Exception as exc:
            if exc.__class__.__name__ not in {"UnexpectedResponse", "NotFoundException"} and "not found" not in str(exc).lower():
                raise
            self.client.create_collection(collection_name=self.collection_name, vectors_config=vectors_config)

    def upsert_chunks(self, chunks: Sequence[TextChunk], vectors: Sequence[np.ndarray], batch_size: int = 64) -> None:
        points = []
        for chunk, vector in zip(chunks, vectors):
            payload = {
                "chunk_id": chunk.chunk_id,
                "doc_id": chunk.doc_id,
                "title": chunk.title,
                "text": chunk.text,
                "metadata": chunk.metadata,
            }
            points.append(
                qmodels.PointStruct(
                    id=stable_qdrant_id(chunk.chunk_id),
                    vector=np.asarray(vector, dtype=np.float32).tolist(),
                    payload=payload,
                )
            )
        for start in range(0, len(points), batch_size):
            self.client.upsert(collection_name=self.collection_name, points=points[start : start + batch_size])

    def existing_chunk_ids(self, chunk_ids: Sequence[str]) -> set[str]:
        if not chunk_ids:
            return set()
        point_ids = [stable_qdrant_id(chunk_id) for chunk_id in chunk_ids]
        existing: set[str] = set()
        for start in range(0, len(point_ids), 256):
            batch_point_ids = point_ids[start : start + 256]
            points = self.client.retrieve(
                collection_name=self.collection_name,
                ids=batch_point_ids,
                with_payload=True,
                with_vectors=False,
            )
            for point in points:
                payload = getattr(point, "payload", None) or {}
                chunk_id = payload.get("chunk_id")
                if chunk_id:
                    existing.add(str(chunk_id))
        return existing

    def search(
        self,
        query_vector: np.ndarray,
        limit: int = 50,
        allowed_chunk_ids: Optional[Sequence[str]] = None,
    ) -> List[RankedChunk]:
        query = np.asarray(query_vector, dtype=np.float32).tolist()
        query_filter = None
        if allowed_chunk_ids:
            query_filter = qmodels.Filter(
                must=[
                    qmodels.FieldCondition(
                        key="chunk_id",
                        match=qmodels.MatchAny(any=list(allowed_chunk_ids)),
                    )
                ]
            )
        if hasattr(self.client, "query_points"):
            result = self.client.query_points(
                collection_name=self.collection_name,
                query=query,
                query_filter=query_filter,
                limit=limit,
                with_payload=True,
            )
            points = getattr(result, "points", result)
        else:
            points = self.client.search(
                collection_name=self.collection_name,
                query_vector=query,
                query_filter=query_filter,
                limit=limit,
                with_payload=True,
            )
        out: List[RankedChunk] = []
        for rank, point in enumerate(points, 1):
            payload = getattr(point, "payload", None) or {}
            metadata = payload.get("metadata") or {}
            chunk = TextChunk(
                chunk_id=payload.get("chunk_id") or str(getattr(point, "id", "")),
                doc_id=payload.get("doc_id") or "",
                title=payload.get("title") or "",
                text=payload.get("text") or "",
                metadata=metadata,
            )
            out.append(RankedChunk(chunk=chunk, rank=rank, score=float(getattr(point, "score", 0.0)), source="qdrant"))
        return out


class QdrantEmbeddingRanker:
    def __init__(self, provider: EmbeddingProvider, store: QdrantVectorStore, recreate_collection: bool = False):
        self.provider = provider
        self.store = store
        self.recreate_collection = recreate_collection
        self.chunks: List[TextChunk] = []
        self.allowed_chunk_ids: List[str] = []

    def index(self, chunks: Sequence[TextChunk]) -> None:
        self.chunks = list(chunks)
        self.allowed_chunk_ids = [chunk.chunk_id for chunk in self.chunks]
        if not self.chunks:
            return
        known_dimensions = int(getattr(self.provider, "dimensions", 0) or os.getenv("EMBEDDING_DIMENSIONS") or 0)
        target_chunks = self.chunks
        if known_dimensions:
            self.store.ensure_collection(vector_size=known_dimensions, recreate=self.recreate_collection)
            existing = self.store.existing_chunk_ids(self.allowed_chunk_ids)
            target_chunks = [chunk for chunk in self.chunks if chunk.chunk_id not in existing]
            if not target_chunks:
                return

        texts = [f"{chunk.title}\n{chunk.text}" for chunk in target_chunks]
        vectors = self.provider.embed(texts)
        if not vectors:
            return
        vector_size = int(np.asarray(vectors[0]).shape[0])
        if not known_dimensions:
            self.store.ensure_collection(vector_size=vector_size, recreate=self.recreate_collection)
        batch_size = int(os.getenv("QDRANT_UPSERT_BATCH_SIZE") or "64")
        self.store.upsert_chunks(target_chunks, vectors, batch_size=batch_size)

    def search(self, query: str, limit: int = 50) -> List[RankedChunk]:
        if not self.chunks:
            return []
        if hasattr(self.provider, "embed_query"):
            qvec = self.provider.embed_query(query)  # type: ignore[attr-defined]
        else:
            qvec = self.provider.embed([query])[0]
        return self.store.search(qvec, limit=limit, allowed_chunk_ids=self.allowed_chunk_ids)


class RrfFusion:
    def __init__(self, k: int = 60, weights: Optional[Dict[str, float]] = None):
        self.k = k
        self.weights = weights or {"bm25": 1.0, "embedding": 1.0, "qdrant": 1.0}

    def fuse(self, ranked_lists: Sequence[Sequence[RankedChunk]], limit: int = 50) -> List[FusedCandidate]:
        by_id: Dict[str, FusedCandidate] = {}
        for ranked in ranked_lists:
            for item in ranked:
                cid = item.chunk.chunk_id
                if cid not in by_id:
                    by_id[cid] = FusedCandidate(chunk=item.chunk, rank=0, score=0.0)
                weight = self.weights.get(item.source, 1.0)
                by_id[cid].score += weight / (self.k + item.rank)
                by_id[cid].component_ranks[item.source] = item.rank
                by_id[cid].component_scores[item.source] = item.score
        out = sorted(by_id.values(), key=lambda item: item.score, reverse=True)
        for rank, item in enumerate(out[:limit], 1):
            item.rank = rank
        return out[:limit]


class UnifiedRetriever:
    def __init__(
        self,
        chunker: Optional[TextChunker] = None,
        embedding_provider: Optional[EmbeddingProvider] = None,
        rrf_k: int = 60,
        vector_store: Optional[str] = None,
    ):
        self.chunker = chunker or TextChunker()
        self.bm25 = Bm25Ranker()
        provider = embedding_provider or embedding_provider_from_env()
        store_name = (vector_store or os.getenv("VECTOR_STORE") or "").strip().lower()
        qdrant_url = os.getenv("QDRANT_URL") or ""
        if store_name == "qdrant" or qdrant_url:
            store = QdrantVectorStore(
                url=qdrant_url or "http://localhost:6333",
                collection_name=os.getenv("QDRANT_COLLECTION") or "press_chunks",
                distance=os.getenv("QDRANT_DISTANCE") or "Cosine",
                api_key=os.getenv("QDRANT_API_KEY") or "",
            )
            recreate = (os.getenv("QDRANT_RECREATE_COLLECTION") or "").strip().lower() in {"1", "true", "yes", "y"}
            self.embedding = QdrantEmbeddingRanker(provider, store, recreate_collection=recreate)
        else:
            self.embedding = EmbeddingRanker(provider)
        self.fusion = RrfFusion(k=rrf_k)
        self.chunks: List[TextChunk] = []

    def index_documents(self, docs: Sequence[CorpusDocument]) -> None:
        chunks: List[TextChunk] = []
        for doc in docs:
            chunks.extend(self.chunker.split(doc))
        self.index_chunks(chunks)

    def index_chunks(self, chunks: Sequence[TextChunk]) -> None:
        self.chunks = list(chunks)
        self.bm25.index(self.chunks)
        self.embedding.index(self.chunks)

    def search(self, query: str, limit: int = 30, candidate_limit: int = 80) -> List[FusedCandidate]:
        bm25_hits = self.bm25.search(query, limit=candidate_limit)
        emb_hits = self.embedding.search(query, limit=candidate_limit)
        return self.fusion.fuse([bm25_hits, emb_hits], limit=limit)


class JsonlIndexStore:
    def __init__(self, path: str):
        self.path = path

    def save_chunks(self, chunks: Sequence[TextChunk]) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            for chunk in chunks:
                f.write(
                    json.dumps(
                        {
                            "chunk_id": chunk.chunk_id,
                            "doc_id": chunk.doc_id,
                            "title": chunk.title,
                            "text": chunk.text,
                            "metadata": chunk.metadata,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

    def load_chunks(self) -> List[TextChunk]:
        chunks: List[TextChunk] = []
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                data = json.loads(line)
                chunks.append(
                    TextChunk(
                        chunk_id=data["chunk_id"],
                        doc_id=data["doc_id"],
                        title=data["title"],
                        text=data["text"],
                        metadata=data.get("metadata") or {},
                    )
                )
        return chunks
