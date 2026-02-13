from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable

import asyncpg
from openai import AsyncOpenAI

from app.config import Config


@dataclass(slots=True)
class RetrievedChunk:
    text: str
    source_url: str
    title: str


@dataclass(slots=True)
class ChunkRecord:
    chunk_id: str
    text: str
    source_url: str
    title: str


class RAGStore:
    def __init__(self, config: Config, *, pool: asyncpg.Pool | None = None) -> None:
        self._config = config
        self._openai = AsyncOpenAI(api_key=config.openai_api_key) if config.openai_api_key else None
        self._external_pool = pool
        self._owned_pool: asyncpg.Pool | None = None

    @property
    def enabled(self) -> bool:
        return bool(self._openai and self._config.openai_embedding_model)

    async def close(self) -> None:
        if self._owned_pool is not None:
            await self._owned_pool.close()
            self._owned_pool = None

    async def upsert(self, records: Iterable[ChunkRecord]) -> int:
        records_list = list(records)
        if not records_list:
            return 0
        if not self.enabled:
            raise RuntimeError("OPENAI_API_KEY is not configured for embeddings")

        embeddings = await self._embed_texts([r.text for r in records_list])
        rows = [
            (
                r.chunk_id,
                r.source_url,
                r.title,
                r.text,
                _vector_literal(embedding),
            )
            for r, embedding in zip(records_list, embeddings)
        ]

        pool = await self._pool()
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO kb_chunks (id, source_url, title, content, embedding)
                VALUES ($1, $2, $3, $4, $5::vector)
                ON CONFLICT (id) DO UPDATE SET
                    source_url = EXCLUDED.source_url,
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding
                """,
                rows,
            )

        return len(records_list)

    async def search(self, query: str, k: int = 6) -> list[RetrievedChunk]:
        query_text = query.strip()
        if not query_text or not self.enabled:
            return []

        query_embedding = (await self._embed_texts([query_text]))[0]
        vector = _vector_literal(query_embedding)
        limit = max(1, int(k))

        pool = await self._pool()
        async with pool.acquire() as conn:
            try:
                rows = await conn.fetch(
                    """
                    SELECT content, COALESCE(source_url, '') AS source_url, COALESCE(title, '') AS title
                    FROM kb_chunks
                    ORDER BY embedding <=> $1::vector
                    LIMIT $2
                    """,
                    vector,
                    limit,
                )
            except asyncpg.UndefinedTableError:
                return []

        return [
            RetrievedChunk(
                text=row["content"],
                source_url=row["source_url"],
                title=row["title"],
            )
            for row in rows
            if row["content"]
        ]

    async def _pool(self) -> asyncpg.Pool:
        if self._external_pool is not None:
            return self._external_pool
        if self._owned_pool is None:
            self._owned_pool = await asyncpg.create_pool(
                dsn=self._config.database_url,
                min_size=1,
                max_size=5,
                command_timeout=20,
            )
        return self._owned_pool

    async def _embed_texts(self, texts: list[str]) -> list[list[float]]:
        assert self._openai is not None
        response = await self._openai.embeddings.create(
            model=self._config.openai_embedding_model,
            input=texts,
        )
        return [item.embedding for item in response.data]


def _vector_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{value:.8f}" for value in values) + "]"


def make_chunk_id(source_url: str, chunk_index: int) -> str:
    raw = f"{source_url}::{chunk_index}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]
