from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urldefrag, urljoin, urlparse

import httpx
import trafilatura

from app.config import load_config
from app.rag.store import ChunkRecord, RAGStore, make_chunk_id

logger = logging.getLogger(__name__)

MAX_DEPTH = 2
MIN_PAGES_PER_DOMAIN = 20
REQUEST_TIMEOUT = 12.0
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200


@dataclass(slots=True)
class PageDoc:
    url: str
    title: str
    text: str


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.links.append(value)


def _normalize_url(base_url: str, href: str) -> str | None:
    joined = urljoin(base_url, href)
    cleaned, _ = urldefrag(joined)
    parsed = urlparse(cleaned)
    if parsed.scheme not in {"http", "https"}:
        return None
    return cleaned.rstrip("/")


def _same_domain(url_a: str, url_b: str) -> bool:
    return urlparse(url_a).netloc.lower() == urlparse(url_b).netloc.lower()


def _chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    clean = " ".join(text.split())
    if not clean:
        return []
    chunks: list[str] = []
    start = 0
    step = max(1, chunk_size - overlap)
    while start < len(clean):
        end = min(len(clean), start + chunk_size)
        chunks.append(clean[start:end])
        if end >= len(clean):
            break
        start += step
    return chunks


def _extract_text_and_title(html: str, url: str) -> PageDoc | None:
    text = trafilatura.extract(
        html,
        include_links=False,
        include_images=False,
        output_format="txt",
    )
    if not text:
        return None
    metadata = trafilatura.extract_metadata(html)
    title = (metadata.title if metadata else None) or url
    return PageDoc(url=url, title=title.strip(), text=text.strip())


def _extract_links(html: str, base_url: str) -> list[str]:
    parser = LinkExtractor()
    parser.feed(html)
    links: list[str] = []
    for href in parser.links:
        normalized = _normalize_url(base_url, href)
        if normalized:
            links.append(normalized)
    return links


def crawl_site(root_url: str, max_depth: int = MAX_DEPTH, min_pages: int = MIN_PAGES_PER_DOMAIN) -> list[PageDoc]:
    root = root_url.rstrip("/")
    queue: deque[tuple[str, int]] = deque([(root, 0)])
    seen: set[str] = set()
    docs: list[PageDoc] = []

    with httpx.Client(
        timeout=REQUEST_TIMEOUT,
        follow_redirects=True,
        headers={"User-Agent": "AIforAI-RAG-Indexer/1.0"},
    ) as client:
        while queue and len(docs) < min_pages:
            url, depth = queue.popleft()
            if url in seen:
                continue
            seen.add(url)

            try:
                response = client.get(url)
                response.raise_for_status()
            except Exception:
                logger.warning("Failed to fetch page: %s", url)
                continue

            html = response.text
            parsed_doc = _extract_text_and_title(html, url)
            if parsed_doc:
                docs.append(parsed_doc)
                logger.info("Indexed page: %s", url)

            if depth >= max_depth:
                continue

            for link in _extract_links(html, url):
                if link in seen:
                    continue
                if not _same_domain(root, link):
                    continue
                queue.append((link, depth + 1))

    return docs


def build_chunk_records(docs: list[PageDoc]) -> list[ChunkRecord]:
    records: list[ChunkRecord] = []
    for doc in docs:
        chunks = _chunk_text(doc.text)
        for idx, chunk in enumerate(chunks):
            records.append(
                ChunkRecord(
                    chunk_id=make_chunk_id(doc.url, idx),
                    text=chunk,
                    source_url=doc.url,
                    title=doc.title,
                )
            )
    return records


async def _run() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    config = load_config()
    store = RAGStore(config)

    if not store.enabled:
        raise RuntimeError("OPENAI_API_KEY and embedding model are required for indexing")

    total_pages = 0
    total_chunks = 0

    try:
        for site in config.kb_sites:
            logger.info("Crawling site: %s", site)
            pages = await asyncio.to_thread(crawl_site, site)
            total_pages += len(pages)
            records = build_chunk_records(pages)
            inserted = await store.upsert(records)
            total_chunks += inserted
            logger.info("Site done: pages=%s chunks=%s site=%s", len(pages), inserted, site)

        logger.info(
            "Ingestion complete: pages=%s chunks_upserted=%s",
            total_pages,
            total_chunks,
        )
    finally:
        await store.close()


def run() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    run()
