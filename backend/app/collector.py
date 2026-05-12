from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections import defaultdict
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
        candidates, planning_events, planning_mode = await self._plan_sources(task, focus_areas)
        events: list[dict[str, Any]] = [
            *planning_events,
            {
                "message": f"已生成 {len(candidates)} 个候选来源",
                "context": {
                    "planning_mode": planning_mode,
                    "candidate_count": len(candidates),
                    "candidate_preview": [
                        {
                            "competitor": item.competitor,
                            "url": item.url,
                            "reason": item.reason,
                        }
                        for item in candidates[:8]
                    ],
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
            "planning_mode": planning_mode,
            "candidate_count": len(candidates),
            "document_count": len(documents),
            "evidence_count": len(evidence),
            "successful_urls": [doc.url for doc in documents[:8]],
        }
        return CollectionOutput(evidence=evidence, context=context, events=events)

    async def _plan_sources(
        self,
        task: TaskDetail,
        focus_areas: list[str],
    ) -> tuple[list[SourceCandidate], list[dict[str, Any]], str]:
        events: list[dict[str, Any]] = [
            {
                "message": "Collector 正在使用 LLM 规划候选来源",
                "stage": "source_planning_request",
                "context": {
                    "model": self.ai_client.model,
                    "competitors": task.input.competitors,
                    "focus_areas": focus_areas,
                    "user_url_count": len(task.input.source_urls),
                },
            }
        ]

        try:
            prompt = self._build_source_planning_prompt(task, focus_areas)
            payload, trace = await self.ai_client.complete_json(
                system_prompt=(
                    "你是竞品研究资料规划助手。你只负责规划可抓取的公开网页来源，不负责编造证据。"
                    "返回的 URL 应该优先包含产品、业务、创作者、商业化、帮助中心、新闻稿等高信号页面。"
                    "不要返回法律声明、隐私政策、用户协议、备案、证照、登录页、robots.txt 或 sitemap.xml。"
                    "必须严格返回 JSON。"
                ),
                user_prompt=prompt,
                max_tokens=1600,
                temperature=0.1,
            )
            candidates = self._parse_source_plan(task, payload)
            if not candidates:
                raise ValueError("LLM source plan returned no valid candidates")
            events.append(
                {
                    "message": "Collector 已完成 LLM 候选来源规划",
                    "stage": "source_planning_response",
                    "context": {
                        "model": trace.model,
                        "latency_ms": trace.latency_ms,
                        "candidate_count": len(candidates),
                    },
                }
            )
            return candidates, events, "llm_source_planning"
        except Exception as exc:
            candidates = self._fallback_plan_sources(task)
            events.append(
                {
                    "level": "warning",
                    "message": "Collector 使用保守兜底来源规划（LLM规划异常）",
                    "stage": "source_planning_fallback",
                    "context": {
                        "error": f"{type(exc).__name__}: {exc}",
                        "candidate_count": len(candidates),
                    },
                }
            )
            return candidates, events, "fallback_source_planning"

    def _build_source_planning_prompt(self, task: TaskDetail, focus_areas: list[str]) -> str:
        payload = {
            "task": {
                "project_name": task.input.project_name,
                "industry": task.input.industry,
                "competitors": task.input.competitors,
                "focus_areas": focus_areas,
                "time_range": task.input.time_range,
            },
            "user_provided_urls": task.input.source_urls,
            "planning_rules": [
                "为每个 competitor 规划 3 到 5 个公开网页 URL。",
                "优先选择能支持分析维度的高信号页面，例如产品介绍、业务介绍、创作者/商家中心、广告/商业化页面、帮助中心、新闻稿。",
                "用户提供的 URL 只能分配给与该 URL 域名或页面主题匹配的 competitor。",
                "不要输出法律声明、隐私政策、用户协议、ICP备案、证照、登录页、纯首页页脚、robots.txt、sitemap.xml。",
                "reason 说明该 URL 预计能支持哪些分析维度。",
                "如果不确定具体路径，可以返回该竞品最可能的官方业务或帮助中心域名，但不要编造证据内容。",
            ],
            "output_schema": {
                "sources": [
                    {
                        "competitor": "竞品名，必须来自 competitors",
                        "url": "https://example.com/path",
                        "reason": "为什么这个来源适合本次分析",
                    }
                ]
            },
        }
        return json.dumps(payload, ensure_ascii=False)

    def _parse_source_plan(self, task: TaskDetail, payload: dict[str, Any]) -> list[SourceCandidate]:
        candidates: list[SourceCandidate] = []
        seen: set[tuple[str, str]] = set()

        def add(url: str, competitor: str, reason: str) -> None:
            normalized = self._normalize_url(url)
            key = (competitor, normalized)
            if (
                not normalized
                or key in seen
                or self._is_low_value_url(normalized)
                or self._url_belongs_to_other_competitor(normalized, competitor, task.input.competitors)
            ):
                return
            seen.add(key)
            candidates.append(SourceCandidate(url=normalized, competitor=competitor, reason=reason[:120]))

        raw_items = payload.get("sources") or payload.get("candidates") or []
        if not isinstance(raw_items, list):
            return []
        per_competitor_count: dict[str, int] = defaultdict(int)
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            competitor = str(raw.get("competitor", "")).strip()
            if competitor not in task.input.competitors or per_competitor_count[competitor] >= 5:
                continue
            reason = str(raw.get("reason", "")).strip() or "LLM planned source"
            before = len(candidates)
            add(str(raw.get("url", "")), competitor, f"llm_planned: {reason}")
            if len(candidates) > before:
                per_competitor_count[competitor] += 1
        return candidates

    def _fallback_plan_sources(self, task: TaskDetail) -> list[SourceCandidate]:
        candidates: list[SourceCandidate] = []
        seen: set[tuple[str, str]] = set()

        def add(url: str, competitor: str, reason: str) -> None:
            normalized = self._normalize_url(url)
            key = (competitor, normalized)
            if not normalized or key in seen or self._is_low_value_url(normalized):
                return
            seen.add(key)
            candidates.append(SourceCandidate(url=normalized, competitor=competitor, reason=reason))

        for competitor in task.input.competitors:
            for raw_url in task.input.source_urls:
                if len(task.input.competitors) == 1 or self._url_matches_competitor(raw_url, competitor):
                    add(raw_url, competitor, "fallback_user_url")

            official_url = self._resolve_official_url(competitor)
            if official_url:
                add(official_url, competitor, "fallback_official_home")

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
            if self._is_low_value_document(final_url := str(response.url), title, extracted_text):
                return {
                    "documents": [],
                    "discovered_urls": [],
                    "event": {
                        "level": "warning",
                        "message": f"来源正文低价值，已跳过：{candidate.url}",
                        "context": {
                            "url": final_url,
                            "title": title[:120],
                            "reason": candidate.reason,
                        },
                    },
                }

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
            if self._is_low_value_text(snippet):
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
            if self._is_low_value_document(doc.url, doc.title, doc.text):
                continue
            for focus_area in focus_areas:
                snippet = self._pick_relevant_sentence(doc.text, focus_area)
                if not snippet:
                    continue
                if self._is_low_value_text(snippet):
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
    def _is_low_value_url(url: str) -> bool:
        lowered = url.lower()
        low_value_markers = [
            "robots.txt",
            "sitemap.xml",
            "privacy",
            "terms",
            "agreement",
            "legal",
            "license",
            "compliance",
            "login",
            "register",
            "passport",
            "auth",
            "icp",
            "beian",
            "备案",
            "隐私",
            "协议",
            "法律",
            "证照",
        ]
        return any(marker in lowered for marker in low_value_markers)

    @classmethod
    def _is_low_value_document(cls, url: str, title: str, text: str) -> bool:
        lowered_url = url.lower()
        combined = cls._clean_text(f"{title} {text[:1200]}").lower()
        if any(marker in lowered_url for marker in ["/404", "not-found", "notfound", "error"]):
            return True
        if any(marker in combined for marker in ["404", "页面无法访问", "页面不存在", "not found", "返回首页"]):
            return True
        return cls._is_low_value_text(combined)

    @staticmethod
    def _is_low_value_text(text: str) -> bool:
        lowered = text.lower()
        legal_markers = [
            "营业执照",
            "增值电信业务经营许可证",
            "网络文化经营许可证",
            "许可证",
            "备案",
            "icp",
            "隐私政策",
            "用户协议",
            "法律声明",
            "copyright",
        ]
        marker_count = sum(1 for marker in legal_markers if marker in lowered)
        return marker_count >= 2

    @staticmethod
    def _url_matches_competitor(url: str, competitor: str) -> bool:
        normalized = RealCollector._normalize_url(url)
        if not normalized:
            return False
        host = urlparse(normalized).netloc.lower()
        competitor_key = re.sub(r"[^a-zA-Z0-9\u4e00-\u9fff]+", "", competitor).lower()
        known_hosts = {
            "抖音": ["douyin.com", "iesdouyin.com", "douyinec.com"],
            "douyin": ["douyin.com", "iesdouyin.com", "douyinec.com"],
            "快手": ["kuaishou.com", "kwaixiaodian.com"],
            "kuaishou": ["kuaishou.com", "kwaixiaodian.com"],
            "小红书": ["xiaohongshu.com", "xhscdn.com"],
            "xiaohongshu": ["xiaohongshu.com", "xhscdn.com"],
            "淘宝": ["taobao.com", "tmall.com", "alibaba.com"],
            "taobao": ["taobao.com", "tmall.com", "alibaba.com"],
            "京东": ["jd.com", "jdcloud.com"],
            "jd": ["jd.com", "jdcloud.com"],
        }
        for key, domains in known_hosts.items():
            if key in competitor_key:
                return any(host == domain or host.endswith(f".{domain}") for domain in domains)
        ascii_key = re.sub(r"[^a-z0-9]+", "", competitor_key)
        return bool(ascii_key and ascii_key in host.replace("-", "").replace(".", ""))

    @staticmethod
    def _url_belongs_to_other_competitor(url: str, competitor: str, competitors: list[str]) -> bool:
        return any(
            other != competitor and RealCollector._url_matches_competitor(url, other)
            for other in competitors
        )

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
