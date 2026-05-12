from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from .ai_client import DeepSeekClient
from .models import DEFAULT_FOCUS_AREAS, EvidenceItem, TaskDetail


OFFICIAL_SOURCE_MAP = {
    "抖音": "https://www.douyin.com",
    "快手": "https://www.kuaishou.com",
    "小红书": "https://www.xiaohongshu.com",
    "淘宝": "https://www.taobao.com",
    "京东": "https://www.jd.com",
}


COLLECTION_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36 RivalFlow/0.1"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
}


@dataclass(slots=True)
class SourceCandidate:
    url: str
    competitor: str
    reason: str


@dataclass(slots=True)
class SourceDocument:
    doc_id: str
    competitor: str
    url: str
    title: str
    text: str
    fetched_at: str


@dataclass(slots=True)
class CollectionOutput:
    evidence: list[EvidenceItem]
    context: dict[str, Any]
    events: list[dict[str, Any]] = field(default_factory=list)


class RealCollector:
    def __init__(self, ai_client: DeepSeekClient, max_pages_per_competitor: int = 4) -> None:
        self.ai_client = ai_client
        self.max_pages_per_competitor = max_pages_per_competitor

    async def collect(self, task: TaskDetail) -> CollectionOutput:
        focus_areas = task.input.focus_areas or DEFAULT_FOCUS_AREAS
        candidates = self._plan_sources(task)
        events: list[dict[str, Any]] = [
            {
                "message": f"已生成 {len(candidates)} 个候选来源",
                "context": {
                    "candidate_count": len(candidates),
                    "candidate_preview": [item.url for item in candidates[:6]],
                },
            }
        ]

        documents, fetch_events = await self._fetch_documents(candidates, focus_areas)
        events.extend(fetch_events)

        evidence = await self._extract_evidence(task, documents, focus_areas)
        if not evidence:
            events.append(
                {
                    "level": "warning",
                    "message": "真实页面证据不足，已从有效正文中生成保守证据",
                    "context": {"document_count": len(documents)},
                }
            )
            evidence = self._fallback_evidence(documents, focus_areas)
        else:
            minimum_expected = min(len(task.input.competitors) * len(focus_areas), 12)
            if len(evidence) < minimum_expected:
                fallback_items = self._fallback_evidence(documents, focus_areas)
                evidence = self._merge_evidence(evidence, fallback_items, minimum_expected)
                events.append(
                    {
                        "message": "真实证据数量偏少，已从有效正文补充证据片段",
                        "context": {
                            "minimum_expected": minimum_expected,
                            "evidence_count": len(evidence),
                        },
                    }
                )

        context = {
            "mode": "real_web_collect",
            "candidate_count": len(candidates),
            "document_count": len(documents),
            "evidence_count": len(evidence),
            "successful_urls": [doc.url for doc in documents[:8]],
        }
        return CollectionOutput(evidence=evidence, context=context, events=events)

    def _plan_sources(self, task: TaskDetail) -> list[SourceCandidate]:
        candidates: list[SourceCandidate] = []
        seen: set[tuple[str, str]] = set()

        def add(url: str, competitor: str, reason: str) -> None:
            normalized = self._normalize_url(url)
            key = (competitor, normalized)
            if not normalized or key in seen:
                return
            seen.add(key)
            candidates.append(SourceCandidate(url=normalized, competitor=competitor, reason=reason))

        for competitor in task.input.competitors:
            for raw_url in task.input.source_urls:
                add(raw_url, competitor, "user_url")

            official_url = self._resolve_official_url(competitor)
            if official_url:
                add(official_url, competitor, "official_mapping")
                add(urljoin(official_url + "/", "robots.txt"), competitor, "robots_hint")
                add(urljoin(official_url + "/", "sitemap.xml"), competitor, "sitemap_hint")

        return candidates

    async def _fetch_documents(
        self,
        candidates: list[SourceCandidate],
        focus_areas: list[str],
    ) -> tuple[list[SourceDocument], list[dict[str, Any]]]:
        events: list[dict[str, Any]] = []
        documents: list[SourceDocument] = []
        seen_text_hashes: set[str] = set()

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(12.0, connect=6.0),
            headers=COLLECTION_HEADERS,
        ) as client:
            for candidate in candidates:
                if len([doc for doc in documents if doc.competitor == candidate.competitor]) >= self.max_pages_per_competitor:
                    continue
                result = await self._fetch_candidate(client, candidate)
                events.append(result["event"])

                for sitemap_url in result.get("discovered_urls", [])[:2]:
                    nested = await self._fetch_candidate(
                        client,
                        SourceCandidate(url=sitemap_url, competitor=candidate.competitor, reason="sitemap_page"),
                    )
                    events.append(nested["event"])
                    for nested_doc in nested["documents"]:
                        nested_hash = hashlib.sha256(nested_doc.text[:4000].encode("utf-8")).hexdigest()
                        if nested_hash in seen_text_hashes:
                            continue
                        seen_text_hashes.add(nested_hash)
                        documents.append(nested_doc)

                for doc in result["documents"]:
                    if len([item for item in documents if item.competitor == doc.competitor]) >= self.max_pages_per_competitor:
                        break
                    text_hash = hashlib.sha256(doc.text[:4000].encode("utf-8")).hexdigest()
                    if text_hash in seen_text_hashes:
                        continue
                    seen_text_hashes.add(text_hash)
                    documents.append(doc)

                    sitemap_urls = self._extract_sitemap_urls(doc.text, doc.url, focus_areas)
                    for sitemap_url in sitemap_urls[:2]:
                        if len([item for item in documents if item.competitor == doc.competitor]) >= self.max_pages_per_competitor:
                            break
                        nested = await self._fetch_candidate(
                            client,
                            SourceCandidate(url=sitemap_url, competitor=doc.competitor, reason="sitemap_page"),
                        )
                        events.append(nested["event"])
                        for nested_doc in nested["documents"]:
                            nested_hash = hashlib.sha256(nested_doc.text[:4000].encode("utf-8")).hexdigest()
                            if nested_hash in seen_text_hashes:
                                continue
                            seen_text_hashes.add(nested_hash)
                            documents.append(nested_doc)

        return documents, events

    async def _fetch_candidate(
        self,
        client: httpx.AsyncClient,
        candidate: SourceCandidate,
    ) -> dict[str, Any]:
        try:
            response = await client.get(candidate.url)
            content_type = response.headers.get("content-type", "")
            text = response.text
            ok = response.status_code < 400 and len(text.strip()) > 40
            if not ok:
                return {
                    "documents": [],
                    "discovered_urls": [],
                    "event": {
                        "level": "warning",
                        "message": f"来源获取失败：{candidate.url}",
                        "context": {
                            "url": candidate.url,
                            "status_code": response.status_code,
                            "reason": candidate.reason,
                        },
                    },
                }

            if "xml" in content_type or candidate.url.endswith(".xml") or candidate.url.endswith("robots.txt"):
                discovered_urls = self._extract_sitemap_urls(text[:200000], candidate.url, [])
                return {
                    "documents": [],
                    "discovered_urls": discovered_urls,
                    "event": {
                        "message": f"来源索引获取成功：{candidate.competitor}",
                        "context": {
                            "url": str(response.url),
                            "reason": candidate.reason,
                            "discovered_url_count": len(discovered_urls),
                        },
                    },
                }
            else:
                title, extracted_text = self._extract_content(text)

            if len(extracted_text) < 80:
                return {
                    "documents": [],
                    "discovered_urls": [],
                    "event": {
                        "level": "warning",
                        "message": f"来源正文过短：{candidate.url}",
                        "context": {"url": candidate.url, "text_length": len(extracted_text)},
                    },
                }

            final_url = str(response.url)
            doc = SourceDocument(
                doc_id=self._build_doc_id(final_url, extracted_text),
                competitor=candidate.competitor,
                url=final_url,
                title=title[:120] or candidate.competitor,
                text=extracted_text[:12000],
                fetched_at=datetime.utcnow().isoformat(),
            )
            return {
                "documents": [doc],
                "discovered_urls": [],
                "event": {
                    "message": f"来源获取成功：{candidate.competitor}",
                    "context": {
                        "url": final_url,
                        "title": doc.title,
                        "text_length": len(doc.text),
                        "reason": candidate.reason,
                    },
                },
            }
        except Exception as exc:
            return {
                "documents": [],
                "discovered_urls": [],
                "event": {
                    "level": "warning",
                    "message": f"来源获取异常：{candidate.url}",
                    "context": {"url": candidate.url, "error": f"{type(exc).__name__}: {exc}"},
                },
            }

    async def _extract_evidence(
        self,
        task: TaskDetail,
        documents: list[SourceDocument],
        focus_areas: list[str],
    ) -> list[EvidenceItem]:
        if not documents:
            return []

        compact_docs = [
            {
                "doc_id": doc.doc_id,
                "competitor": doc.competitor,
                "url": doc.url,
                "title": doc.title,
                "text": doc.text[:3500],
            }
            for doc in documents[:10]
        ]
        system_prompt = (
            "你是严谨的信息抽取助手。只能基于用户提供的真实网页正文抽取证据。"
            "不要补充正文里没有的信息。请严格返回JSON。"
        )
        user_prompt = json.dumps(
            {
                "task": {
                    "industry": task.input.industry,
                    "competitors": task.input.competitors,
                    "focus_areas": focus_areas,
                },
                "documents": compact_docs,
                "output_schema": {
                    "evidence": [
                        {
                            "competitor": "竞品名",
                            "focus_area": "分析维度",
                            "doc_id": "来源doc_id",
                            "snippet": "来自正文的证据片段，80到180字",
                            "confidence": 0.0,
                        }
                    ]
                },
            },
            ensure_ascii=False,
        )

        try:
            payload, _trace = await self.ai_client.complete_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=1600,
                temperature=0.1,
            )
        except Exception:
            return []

        docs_by_id = {doc.doc_id: doc for doc in documents}
        evidence: list[EvidenceItem] = []
        seen_keys: set[tuple[str, str, str]] = set()
        raw_items = payload.get("evidence") or []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            competitor = str(raw.get("competitor", "")).strip()
            focus_area = str(raw.get("focus_area", "")).strip()
            doc_id = str(raw.get("doc_id", "")).strip()
            snippet = self._clean_text(str(raw.get("snippet", "")).strip())
            if competitor not in task.input.competitors or focus_area not in focus_areas:
                continue
            doc = docs_by_id.get(doc_id)
            if not doc or len(snippet) < 20:
                continue
            key = (competitor, focus_area, snippet[:60])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            confidence = self._clamp_float(raw.get("confidence", 0.7), default=0.7)
            evidence.append(
                EvidenceItem(
                    evidence_id=f"ev-{len(evidence) + 1:03d}",
                    competitor=competitor,
                    focus_area=focus_area,
                    source_name=doc.title,
                    source_url=doc.url,
                    snippet=snippet[:220],
                    confidence=confidence,
                )
            )
        return evidence

    @staticmethod
    def _merge_evidence(
        primary: list[EvidenceItem],
        fallback: list[EvidenceItem],
        limit: int,
    ) -> list[EvidenceItem]:
        merged: list[EvidenceItem] = []
        seen: set[tuple[str, str, str]] = set()
        for item in [*primary, *fallback]:
            key = (item.competitor, item.focus_area, item.source_url)
            if key in seen:
                continue
            seen.add(key)
            item.evidence_id = f"ev-{len(merged) + 1:03d}"
            merged.append(item)
            if len(merged) >= limit:
                break
        return merged

    def _fallback_evidence(
        self,
        documents: list[SourceDocument],
        focus_areas: list[str],
    ) -> list[EvidenceItem]:
        evidence: list[EvidenceItem] = []
        for doc in documents:
            for focus_area in focus_areas:
                snippet = self._pick_relevant_sentence(doc.text, focus_area)
                if not snippet:
                    continue
                evidence.append(
                    EvidenceItem(
                        evidence_id=f"ev-{len(evidence) + 1:03d}",
                        competitor=doc.competitor,
                        focus_area=focus_area,
                        source_name=doc.title,
                        source_url=doc.url,
                        snippet=snippet[:220],
                        confidence=0.62,
                    )
                )
                if len(evidence) >= 20:
                    return evidence
        return evidence

    @staticmethod
    def _extract_content(html: str) -> tuple[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg", "canvas"]):
            tag.decompose()
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        candidates = []
        for selector in ["main", "article", "[role=main]", "body"]:
            node = soup.select_one(selector)
            if node:
                candidates.append(node.get_text("\n", strip=True))
        text = max(candidates, key=len, default=soup.get_text("\n", strip=True))
        return title, RealCollector._clean_text(text)

    @staticmethod
    def _extract_sitemap_urls(text: str, base_url: str, focus_areas: list[str]) -> list[str]:
        urls = re.findall(r"<loc>\s*([^<]+)\s*</loc>", text, flags=re.IGNORECASE)
        if not urls and "Sitemap:" in text:
            urls = [line.split(":", 1)[1].strip() for line in text.splitlines() if line.lower().startswith("sitemap:")]
        keywords = ["about", "help", "product", "business", "news", "brand", "ecommerce", "shop", "creator"]
        keywords.extend(focus_areas)
        picked = []
        for raw_url in urls:
            url = urljoin(base_url, raw_url.strip())
            lowered = url.lower()
            if any(str(keyword).lower() in lowered for keyword in keywords):
                picked.append(url)
        return picked[:6]

    @staticmethod
    def _pick_relevant_sentence(text: str, focus_area: str) -> str:
        sentences = re.split(r"(?<=[。！？.!?])\s*", text)
        for sentence in sentences:
            cleaned = RealCollector._clean_text(sentence)
            if len(cleaned) >= 30 and (focus_area in cleaned or any(word in cleaned for word in ["产品", "用户", "商业", "增长", "服务"])):
                return cleaned
        for sentence in sentences:
            cleaned = RealCollector._clean_text(sentence)
            if len(cleaned) >= 40:
                return cleaned
        return ""

    @staticmethod
    def _resolve_official_url(competitor: str) -> str:
        for key, url in OFFICIAL_SOURCE_MAP.items():
            if key in competitor:
                return url
        slug = re.sub(r"[^a-zA-Z0-9-]+", "", competitor).strip("-").lower()
        if slug:
            return f"https://www.{slug}.com"
        return ""

    @staticmethod
    def _normalize_url(url: str) -> str:
        value = url.strip()
        if not value:
            return ""
        if not value.startswith(("http://", "https://")):
            value = f"https://{value}"
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return ""
        return value

    @staticmethod
    def _build_doc_id(url: str, text: str) -> str:
        digest = hashlib.sha1(f"{url}|{text[:500]}".encode("utf-8")).hexdigest()[:10]
        return f"doc-{digest}"

    @staticmethod
    def _clean_text(text: str) -> str:
        cleaned = re.sub(r"\s+", " ", text)
        return cleaned.strip()

    @staticmethod
    def _clamp_float(value: Any, default: float) -> float:
        try:
            result = float(value)
        except (TypeError, ValueError):
            result = default
        return round(max(0.0, min(result, 1.0)), 2)
