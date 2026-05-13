from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

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

SEARCH_PROVIDER_ENV = "RIVALFLOW_SEARCH_PROVIDER"
WEB_DISCOVERY_ENV = "RIVALFLOW_ENABLE_WEB_DISCOVERY"
TAVILY_API_KEY_ENV = "TAVILY_API_KEY"
DEFAULT_SEARCH_PROVIDER = "tavily"
MAX_SEARCH_QUERIES_PER_COMPETITOR = 3
MAX_SEARCH_RESULTS_PER_QUERY = 4
MAX_FETCH_BYTES = 180_000
MIN_TAVILY_RAW_CONTENT_LENGTH = 80

COLLECTION_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36 RivalFlow/0.2"
    ),
    "Accept": "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.6",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
}


SAMPLE_PUBLIC_MATERIALS = {
    "抖音": (
        "抖音电商公开资料样例：抖音电商围绕短视频内容、直播互动、达人带货和店铺经营形成交易链路。"
        "平台面向商家提供商品发布、内容经营、直播转化、营销活动和经营数据等工具，帮助品牌在内容场景中触达用户。"
        "在用户体验上，商品展示、达人讲解、评论互动和下单路径结合在同一内容消费流程内。"
        "在商业化能力上，广告投放、达人合作和电商交易共同支撑品牌增长。"
    ),
    "快手": (
        "快手电商公开资料样例：快手电商强调信任关系、直播间互动和商家长期经营，平台通过达人、店铺、短视频和直播形成转化链路。"
        "商家可以围绕粉丝关系进行内容运营、商品讲解、售后服务和复购管理。"
        "在用户体验上，直播讲解、评论互动和交易服务更强调实时沟通。"
        "在增长策略上，快手生态重视私域沉淀、达人协作和商家经营效率。"
    ),
    "小红书": (
        "小红书公开资料样例：小红书围绕生活方式社区、内容种草、搜索发现和品牌合作建立产品定位。"
        "平台通过笔记内容、用户评论、收藏分享和搜索链路帮助用户完成消费决策。"
        "蒲公英等商业合作能力连接品牌与创作者，支持内容合作、投放管理和效果评估。"
        "在增长策略上，小红书强调社区内容质量、真实体验分享和用户兴趣发现。"
    ),
    "淘宝": (
        "淘宝公开资料样例：淘宝面向消费者和商家提供综合电商交易平台能力，覆盖商品搜索、店铺经营、营销活动、会员运营和售后服务。"
        "平台通过丰富商品供给、搜索推荐、内容化频道和促销活动提升用户决策效率。"
        "商家侧工具覆盖商品管理、流量获取、客户服务和经营分析。"
        "在商业化能力上，广告、交易佣金和生态服务共同支撑平台经营。"
    ),
    "京东": (
        "京东公开资料样例：京东以自营、供应链、物流履约和品质服务作为核心差异化能力。"
        "平台面向用户提供商品搜索、正品保障、配送履约、售后服务和会员权益。"
        "商家侧能力覆盖店铺经营、营销投放、仓配协同和经营数据分析。"
        "在用户体验上，配送时效、售后可靠性和服务标准化是重要优势。"
    ),
}


@dataclass(slots=True)
class SourceSuggestion:
    competitor: str
    material_type: str
    suggested_source: str
    reason: str
    priority: str = "medium"


@dataclass(slots=True)
class SearchResult:
    query: str
    provider_query: str
    competitor: str
    title: str
    url: str
    snippet: str
    provider: str
    raw_content: str = ""


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


class SearchProvider:
    name = "base"

    async def search(self, query: str, competitor: str, limit: int) -> list[SearchResult]:
        raise NotImplementedError


class DuckDuckGoSearchProvider(SearchProvider):
    name = "duckduckgo_html"

    def __init__(self, timeout_seconds: int = 12) -> None:
        self.timeout_seconds = timeout_seconds

    async def search(self, query: str, competitor: str, limit: int) -> list[SearchResult]:
        params = {"q": query, "kl": "wt-wt"}
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_seconds, connect=5.0),
            headers=COLLECTION_HEADERS,
            follow_redirects=True,
        ) as client:
            response = await client.get("https://duckduckgo.com/html/", params=params)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        results: list[SearchResult] = []
        seen_urls: set[str] = set()
        for result in soup.select(".result"):
            link = result.select_one("a.result__a")
            if not link:
                continue
            title = RealCollector._clean_text(link.get_text(" ", strip=True))
            raw_href = str(link.get("href") or "")
            url = RealCollector._unwrap_search_redirect(raw_href)
            normalized = RealCollector._normalize_url(url)
            if not normalized or normalized in seen_urls:
                continue
            snippet_node = result.select_one(".result__snippet")
            snippet = RealCollector._clean_text(snippet_node.get_text(" ", strip=True) if snippet_node else "")
            seen_urls.add(normalized)
            results.append(
                SearchResult(
                    query=query,
                    provider_query=query,
                    competitor=competitor,
                    title=title[:160] or normalized,
                    url=normalized,
                    snippet=snippet[:300],
                    provider=self.name,
                )
            )
            if len(results) >= limit:
                break
        return results


class TavilySearchProvider(SearchProvider):
    name = "tavily"

    def __init__(self, api_key: str | None = None, timeout_seconds: int = 20) -> None:
        self.api_key = (api_key or os.getenv(TAVILY_API_KEY_ENV) or "").strip()
        self.timeout_seconds = timeout_seconds

    async def search(self, query: str, competitor: str, limit: int) -> list[SearchResult]:
        if not self.api_key:
            raise ValueError(f"Missing {TAVILY_API_KEY_ENV} environment variable")

        provider_query = self._prepare_query(query, competitor)
        payload = {
            "query": provider_query,
            "topic": "general",
            "search_depth": "basic",
            "max_results": limit,
            "include_answer": False,
            "include_images": False,
            "include_raw_content": True,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=httpx.Timeout(self.timeout_seconds, connect=8.0)) as client:
            response = await client.post("https://api.tavily.com/search", headers=headers, json=payload)
            response.raise_for_status()

        data = response.json()
        results: list[SearchResult] = []
        seen_urls: set[str] = set()
        for raw in data.get("results") or []:
            if not isinstance(raw, dict):
                continue
            normalized = RealCollector._normalize_url(str(raw.get("url", "")))
            if not normalized or normalized in seen_urls:
                continue
            seen_urls.add(normalized)
            title = RealCollector._clean_text(str(raw.get("title", "")))[:160] or normalized
            snippet = RealCollector._clean_text(str(raw.get("content", "")))[:300]
            raw_content = RealCollector._clean_text(str(raw.get("raw_content", "") or ""))
            results.append(
                SearchResult(
                    query=query,
                    provider_query=provider_query,
                    competitor=competitor,
                    title=title,
                    url=normalized,
                    snippet=snippet,
                    provider=self.name,
                    raw_content=raw_content,
                )
            )
            if len(results) >= limit:
                break
        return results

    @staticmethod
    def _prepare_query(query: str, competitor: str) -> str:
        prepared = f"{query} {competitor}"
        replacements = {
            "抖音": "Douyin",
            "快手": "Kuaishou",
            "小红书": "Xiaohongshu RedNote",
            "淘宝": "Taobao",
            "天猫": "Tmall",
            "京东": "JD.com",
            "拼多多": "Pinduoduo",
            "美团": "Meituan",
            "饿了么": "Ele.me",
            "哔哩哔哩": "Bilibili",
            "B站": "Bilibili",
            "微信": "WeChat",
            "微博": "Weibo",
            "知乎": "Zhihu",
            "内容电商": "content ecommerce",
            "电商": "ecommerce",
            "产品介绍": "product overview",
            "产品定位": "product positioning",
            "用户体验": "user experience",
            "商业化能力": "monetization capability",
            "商业化": "monetization",
            "增长策略": "growth strategy",
            "创作者": "creator",
            "商家": "merchant",
            "官方": "official",
            "公开资料": "public information",
            "资料": "information",
        }
        for source, target in replacements.items():
            prepared = prepared.replace(source, f" {target} ")
        prepared = re.sub(r"[\u4e00-\u9fff]+", " ", prepared)
        prepared = re.sub(r"\s+", " ", prepared).strip()
        if len(prepared) < 8:
            fallback_competitor = competitor
            for source, target in replacements.items():
                fallback_competitor = fallback_competitor.replace(source, f" {target} ")
            fallback_competitor = re.sub(r"[\u4e00-\u9fff]+", " ", fallback_competitor)
            fallback_competitor = re.sub(r"\s+", " ", fallback_competitor).strip()
            prepared = f"{fallback_competitor or 'China public platform'} official product business"
        return prepared[:380]


class DisabledSearchProvider(SearchProvider):
    name = "disabled"

    async def search(self, query: str, competitor: str, limit: int) -> list[SearchResult]:
        return []


class RealCollector:
    def __init__(
        self,
        ai_client: DeepSeekClient,
        max_pages_per_competitor: int = 4,
        search_provider: SearchProvider | None = None,
        enable_web_discovery: bool | None = None,
    ) -> None:
        self.ai_client = ai_client
        self.max_pages_per_competitor = max_pages_per_competitor
        self.enable_web_discovery = (
            enable_web_discovery
            if enable_web_discovery is not None
            else os.getenv(WEB_DISCOVERY_ENV, "1").strip().lower() not in {"0", "false", "no", "off"}
        )
        self.search_provider = search_provider or self._build_search_provider()

    async def collect(self, task: TaskDetail) -> CollectionOutput:
        focus_areas = task.input.focus_areas or DEFAULT_FOCUS_AREAS
        suggestions, planning_events, planning_mode = await self._plan_public_sources(task, focus_areas)
        documents, intake_events, intake_mode = await self._build_public_documents(task, focus_areas, suggestions)
        events: list[dict[str, Any]] = [
            *planning_events,
            *intake_events,
            {
                "message": f"公共信息采集完成，整理出 {len(documents)} 份可分析资料",
                "stage": "public_materials_ready",
                "context": {
                    "planning_mode": planning_mode,
                    "intake_mode": intake_mode,
                    "suggested_source_count": len(suggestions),
                    "document_count": len(documents),
                    "document_preview": [
                        {
                            "doc_id": item.doc_id,
                            "competitor": item.competitor,
                            "title": item.title,
                            "source": item.url,
                            "text_length": len(item.text),
                        }
                        for item in documents[:8]
                    ],
                },
            },
        ]

        evidence = await self._extract_evidence(task, documents, focus_areas)
        if not evidence:
            events.append(
                {
                    "level": "warning",
                    "message": "公开资料证据不足，已从有效资料正文中生成保守证据",
                    "stage": "evidence_fallback",
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
                        "message": "公开资料证据数量偏少，已从有效正文补充证据片段",
                        "stage": "evidence_supplement",
                        "context": {
                            "minimum_expected": minimum_expected,
                            "evidence_count": len(evidence),
                        },
                    }
                )

        context = {
            "mode": "public_info_intake",
            "planning_mode": planning_mode,
            "intake_mode": intake_mode,
            "network_fetch": "controlled_search" if self.enable_web_discovery else "disabled",
            "search_provider": self.search_provider.name,
            "user_material_count": len(task.input.public_materials),
            "source_reference_count": len(task.input.source_urls),
            "suggested_source_count": len(suggestions),
            "document_count": len(documents),
            "evidence_count": len(evidence),
            "source_names": [doc.title for doc in documents[:8]],
        }
        return CollectionOutput(evidence=evidence, context=context, events=events)

    async def _plan_public_sources(
        self,
        task: TaskDetail,
        focus_areas: list[str],
    ) -> tuple[list[SourceSuggestion], list[dict[str, Any]], str]:
        events: list[dict[str, Any]] = [
            {
                "message": "Collector 正在规划公开资料清单",
                "stage": "public_source_planning_request",
                "context": {
                    "model": self.ai_client.model,
                    "competitors": task.input.competitors,
                    "focus_areas": focus_areas,
                    "user_material_count": len(task.input.public_materials),
                    "source_reference_count": len(task.input.source_urls),
                    "network_fetch": "controlled_search" if self.enable_web_discovery else "disabled",
                    "search_provider": self.search_provider.name,
                },
            }
        ]

        try:
            prompt = self._build_public_source_prompt(task, focus_areas)
            payload, trace = await self.ai_client.complete_json(
                system_prompt=(
                    "你是竞品研究的公开资料采集规划助手。"
                    "你只输出建议用户补充或核对的公开资料清单和可搜索方向，不编造证据。"
                    "建议应覆盖产品定位、用户体验、商业化能力、增长策略等分析维度。"
                    "必须严格返回 JSON。"
                ),
                user_prompt=prompt,
                max_tokens=1200,
                temperature=0.1,
            )
            suggestions = self._parse_source_suggestions(task, payload)
            if not suggestions:
                raise ValueError("public source plan returned no valid suggestions")
            events.append(
                {
                    "message": "Collector 已完成公开资料清单规划",
                    "stage": "public_source_planning_response",
                    "context": {
                        "model": trace.model,
                        "latency_ms": trace.latency_ms,
                        "suggestion_count": len(suggestions),
                        "suggestion_preview": [self._suggestion_to_context(item) for item in suggestions[:8]],
                    },
                }
            )
            return suggestions, events, "llm_public_source_planning"
        except Exception as exc:
            suggestions = self._fallback_public_source_suggestions(task, focus_areas)
            events.append(
                {
                    "level": "warning",
                    "message": "Collector 使用保守公开资料清单（LLM规划异常）",
                    "stage": "public_source_planning_fallback",
                    "context": {
                        "error": f"{type(exc).__name__}: {exc}",
                        "suggestion_count": len(suggestions),
                        "suggestion_preview": [self._suggestion_to_context(item) for item in suggestions[:8]],
                    },
                }
            )
            return suggestions, events, "fallback_public_source_planning"

    def _build_public_source_prompt(self, task: TaskDetail, focus_areas: list[str]) -> str:
        payload = {
            "task": {
                "project_name": task.input.project_name,
                "industry": task.input.industry,
                "competitors": task.input.competitors,
                "focus_areas": focus_areas,
                "time_range": task.input.time_range,
            },
            "user_provided_source_references": task.input.source_urls,
            "user_material_count": len(task.input.public_materials),
            "planning_rules": [
                "只规划资料清单和公开搜索方向，不把搜索建议当作已经采集到的证据。",
                "建议资料应来自可公开核验的信息，例如官网产品介绍、商家/创作者帮助中心、新闻稿、公开报告、应用商店描述、截图文字说明。",
                "不要把建议来源当作已经采集到的证据。",
                "suggested_source 可以是来源类型、官方入口或用户应补充的资料名称。",
                "reason 说明该资料预计能支持哪些分析维度。",
            ],
            "output_schema": {
                "suggestions": [
                    {
                        "competitor": "竞品名，必须来自 competitors",
                        "material_type": "资料类型",
                        "suggested_source": "建议补充或核对的公开资料",
                        "reason": "为什么需要这份资料",
                        "priority": "high|medium|low",
                    }
                ]
            },
        }
        return json.dumps(payload, ensure_ascii=False)

    def _parse_source_suggestions(self, task: TaskDetail, payload: dict[str, Any]) -> list[SourceSuggestion]:
        suggestions: list[SourceSuggestion] = []
        per_competitor_count: dict[str, int] = defaultdict(int)
        raw_items = payload.get("suggestions") or payload.get("sources") or []
        if not isinstance(raw_items, list):
            return []

        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            competitor = str(raw.get("competitor", "")).strip()
            if competitor not in task.input.competitors or per_competitor_count[competitor] >= 5:
                continue
            material_type = str(raw.get("material_type", "")).strip() or "公开资料"
            suggested_source = str(raw.get("suggested_source", "")).strip() or self._resolve_official_reference(competitor)
            reason = str(raw.get("reason", "")).strip() or "用于补充竞品分析证据。"
            priority = str(raw.get("priority", "medium")).strip().lower()
            if priority not in {"high", "medium", "low"}:
                priority = "medium"
            suggestions.append(
                SourceSuggestion(
                    competitor=competitor,
                    material_type=material_type[:40],
                    suggested_source=suggested_source[:160],
                    reason=reason[:180],
                    priority=priority,
                )
            )
            per_competitor_count[competitor] += 1
        return suggestions

    def _fallback_public_source_suggestions(
        self,
        task: TaskDetail,
        focus_areas: list[str],
    ) -> list[SourceSuggestion]:
        suggestions: list[SourceSuggestion] = []
        templates = [
            ("官网/产品介绍", "补充产品定位、核心场景和目标用户。", "high"),
            ("商家/创作者/帮助中心", "补充用户体验、运营流程和生态服务。", "high"),
            ("商业化/广告/电商介绍", "补充商业化能力、投放工具和转化链路。", "medium"),
            ("新闻稿/公开报告", "补充增长策略、阶段变化和外部验证。", "medium"),
        ]
        for competitor in task.input.competitors:
            official = self._resolve_official_reference(competitor)
            for material_type, reason, priority in templates:
                suggestions.append(
                    SourceSuggestion(
                        competitor=competitor,
                        material_type=material_type,
                        suggested_source=official or f"请补充 {competitor} 的{material_type}文本",
                        reason=f"{reason} 关注维度：{', '.join(focus_areas)}。",
                        priority=priority,
                    )
                )
        return suggestions

    @staticmethod
    def _suggestion_to_context(item: SourceSuggestion) -> dict[str, str]:
        return {
            "competitor": item.competitor,
            "material_type": item.material_type,
            "suggested_source": item.suggested_source,
            "reason": item.reason,
            "priority": item.priority,
        }

    async def _build_public_documents(
        self,
        task: TaskDetail,
        focus_areas: list[str],
        suggestions: list[SourceSuggestion],
    ) -> tuple[list[SourceDocument], list[dict[str, Any]], str]:
        events: list[dict[str, Any]] = []
        documents, material_events = self._documents_from_user_materials(task)
        events.extend(material_events)

        if task.input.source_urls:
            events.append(
                {
                    "message": "已记录用户提供的来源引用，不进行自动访问",
                    "stage": "source_references_recorded",
                    "context": {
                        "source_reference_count": len(task.input.source_urls),
                        "source_references": task.input.source_urls[:8],
                        "network_fetch": "controlled_search" if self.enable_web_discovery else "disabled",
                    },
                }
            )

        if len(documents) >= self._minimum_document_target(task):
            events.append(
                {
                    "message": "已从用户提供的公开资料中生成分析文档",
                    "stage": "user_public_materials_ingested",
                    "context": {
                        "document_count": len(documents),
                        "focus_areas": focus_areas,
                    },
                }
            )
            return documents, events, "user_public_materials"

        if documents:
            events.append(
                {
                    "level": "warning",
                    "message": "用户提供资料数量不足，Collector 将尝试受控搜索补充公开来源",
                    "stage": "public_materials_insufficient",
                    "context": {
                        "document_count": len(documents),
                        "minimum_target": self._minimum_document_target(task),
                        "network_fetch": "controlled_search" if self.enable_web_discovery else "disabled",
                    },
                }
            )

        had_user_documents = bool(documents)
        discovered_documents, discovery_events = await self._discover_public_documents(task, focus_areas, suggestions, documents)
        events.extend(discovery_events)
        documents = self._merge_documents([*documents, *discovered_documents])
        if documents:
            if discovered_documents and had_user_documents:
                return documents, events, "user_materials_plus_search"
            if discovered_documents:
                return documents, events, "search_public_sources"
            return documents, events, "user_public_materials"

        sample_documents = self._sample_documents(task)
        if sample_documents:
            events.append(
                {
                    "level": "warning",
                    "message": "未检测到有效用户公开资料，已使用内置演示资料跑通流程",
                    "stage": "demo_sample_materials_loaded",
                    "context": {
                        "document_count": len(sample_documents),
                        "note": "正式分析请替换为用户上传或粘贴的真实公开资料。",
                    },
                }
            )
            return sample_documents, events, "demo_sample_materials"

        events.append(
            {
                "level": "warning",
                "message": "未获得可分析的公开资料",
                "stage": "public_materials_empty",
                "context": {
                    "user_material_count": len(task.input.public_materials),
                    "known_sample_available": False,
                },
            }
        )
        return [], events, "empty_public_materials"

    def _documents_from_user_materials(self, task: TaskDetail) -> tuple[list[SourceDocument], list[dict[str, Any]]]:
        documents: list[SourceDocument] = []
        events: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()
        per_competitor_count: dict[str, int] = defaultdict(int)

        for index, raw_material in enumerate(task.input.public_materials[:12], start=1):
            text = self._material_to_text(raw_material)
            if len(text) < 80:
                events.append(
                    {
                        "level": "warning",
                        "message": "公开资料过短，已跳过",
                        "stage": "public_material_skipped",
                        "context": {"index": index, "text_length": len(text)},
                    }
                )
                continue

            competitor = self._infer_competitor(text, task.input.competitors)
            if not competitor:
                events.append(
                    {
                        "level": "warning",
                        "message": "公开资料未识别到对应竞品，已跳过",
                        "stage": "public_material_skipped",
                        "context": {
                            "index": index,
                            "competitors": task.input.competitors,
                            "text_preview": text[:160],
                        },
                    }
                )
                continue
            if per_competitor_count[competitor] >= self.max_pages_per_competitor:
                continue

            text_hash = hashlib.sha256(text[:4000].encode("utf-8")).hexdigest()
            if text_hash in seen_hashes:
                continue
            seen_hashes.add(text_hash)

            source_ref = self._pick_source_reference(task.input.source_urls, competitor, index)
            title = self._infer_material_title(raw_material, competitor, index)
            documents.append(
                SourceDocument(
                    doc_id=self._build_doc_id(source_ref, text),
                    competitor=competitor,
                    url=source_ref,
                    title=title,
                    text=text[:12000],
                    fetched_at=datetime.utcnow().isoformat(),
                )
            )
            per_competitor_count[competitor] += 1
        return documents, events

    async def _discover_public_documents(
        self,
        task: TaskDetail,
        focus_areas: list[str],
        suggestions: list[SourceSuggestion],
        existing_documents: list[SourceDocument],
    ) -> tuple[list[SourceDocument], list[dict[str, Any]]]:
        events: list[dict[str, Any]] = []
        if not self.enable_web_discovery:
            return (
                [],
                [
                    {
                        "message": "搜索式公开来源发现未启用",
                        "stage": "web_discovery_skipped",
                        "context": {
                            "reason": f"{WEB_DISCOVERY_ENV}=0 或搜索能力被关闭",
                            "search_provider": self.search_provider.name,
                        },
                    }
                ],
            )
        if isinstance(self.search_provider, DisabledSearchProvider):
            return (
                [],
                [
                    {
                        "message": "搜索适配器未配置，跳过公开来源发现",
                        "stage": "web_discovery_skipped",
                        "context": {"search_provider": self.search_provider.name},
                    }
                ],
            )

        queries = self._build_search_queries(task, focus_areas, suggestions, existing_documents)
        events.append(
            {
                "message": f"Collector 已生成 {len(queries)} 个受控搜索查询",
                "stage": "web_search_queries_planned",
                "context": {
                    "search_provider": self.search_provider.name,
                    "queries": queries[:12],
                    "limits": {
                        "max_queries_per_competitor": MAX_SEARCH_QUERIES_PER_COMPETITOR,
                        "max_results_per_query": MAX_SEARCH_RESULTS_PER_QUERY,
                        "max_pages_per_competitor": self.max_pages_per_competitor,
                    },
                },
            }
        )

        documents: list[SourceDocument] = []
        seen_urls = {doc.url for doc in existing_documents}
        per_competitor_docs: dict[str, int] = defaultdict(int)
        for doc in existing_documents:
            per_competitor_docs[doc.competitor] += 1

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(12.0, connect=5.0),
            headers=COLLECTION_HEADERS,
            follow_redirects=True,
            max_redirects=3,
        ) as client:
            for query_item in queries:
                competitor = query_item["competitor"]
                query = query_item["query"]
                if per_competitor_docs[competitor] >= self.max_pages_per_competitor:
                    continue
                try:
                    raw_results = await self.search_provider.search(query, competitor, MAX_SEARCH_RESULTS_PER_QUERY)
                except Exception as exc:
                    events.append(
                        {
                            "level": "warning",
                            "message": "公开来源搜索失败",
                            "stage": "web_search_failed",
                            "context": {
                                "query": query,
                                "competitor": competitor,
                                "provider": self.search_provider.name,
                                "error": f"{type(exc).__name__}: {exc}",
                            },
                        }
                    )
                    continue

                accepted_results: list[SearchResult] = []
                rejected_results: list[dict[str, str]] = []
                for result in raw_results:
                    allowed, reason = self._is_acceptable_search_result(result, task.input.competitors)
                    if allowed:
                        accepted_results.append(result)
                    else:
                        rejected_results.append({"url": result.url, "reason": reason})

                events.append(
                    {
                        "message": f"公开来源搜索返回 {len(raw_results)} 条，采纳 {len(accepted_results)} 条候选",
                        "stage": "web_search_results_filtered",
                        "context": {
                            "query": query,
                            "competitor": competitor,
                            "provider": self.search_provider.name,
                            "provider_query": raw_results[0].provider_query if raw_results else "",
                            "accepted": [
                                {
                                    "title": item.title,
                                    "url": item.url,
                                    "snippet": item.snippet,
                                    "provider_query": item.provider_query,
                                }
                                for item in accepted_results[:4]
                            ],
                            "rejected": rejected_results[:4],
                        },
                    }
                )

                for result in accepted_results:
                    if per_competitor_docs[competitor] >= self.max_pages_per_competitor:
                        break
                    if result.url in seen_urls:
                        continue
                    fetch_result = await self._read_public_page(client, result, focus_areas)
                    events.append(fetch_result["event"])
                    document = fetch_result.get("document")
                    if not document:
                        continue
                    seen_urls.add(document.url)
                    documents.append(document)
                    per_competitor_docs[competitor] += 1

        return documents, events

    def _build_search_queries(
        self,
        task: TaskDetail,
        focus_areas: list[str],
        suggestions: list[SourceSuggestion],
        existing_documents: list[SourceDocument],
    ) -> list[dict[str, str]]:
        existing_competitors = {doc.competitor for doc in existing_documents}
        suggestions_by_competitor: dict[str, list[SourceSuggestion]] = defaultdict(list)
        for suggestion in suggestions:
            suggestions_by_competitor[suggestion.competitor].append(suggestion)

        queries: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for competitor in task.input.competitors:
            if existing_competitors and len([doc for doc in existing_documents if doc.competitor == competitor]) >= 2:
                continue
            base_queries = [
                f"{competitor} {task.input.industry} 产品介绍 官方",
                f"{competitor} 商业化 创作者 商家 官方",
                f"{competitor} {' '.join(focus_areas[:3])} 公开资料",
            ]
            for suggestion in suggestions_by_competitor.get(competitor, [])[:2]:
                base_queries.append(f"{competitor} {suggestion.material_type} {suggestion.suggested_source}")

            for raw_query in base_queries:
                query = self._clean_text(raw_query)[:120]
                key = (competitor, query)
                if not query or key in seen:
                    continue
                seen.add(key)
                queries.append({"competitor": competitor, "query": query})
                if len([item for item in queries if item["competitor"] == competitor]) >= MAX_SEARCH_QUERIES_PER_COMPETITOR:
                    break
        return queries

    def _is_acceptable_search_result(self, result: SearchResult, competitors: list[str]) -> tuple[bool, str]:
        normalized = self._normalize_url(result.url)
        if not normalized:
            return False, "invalid_url"
        if not self._is_public_http_url(normalized):
            return False, "non_public_url"
        if self._is_low_value_url(normalized):
            return False, "low_value_url"
        if self._url_belongs_to_other_competitor(normalized, result.competitor, competitors):
            return False, "belongs_to_other_competitor"
        text = self._clean_text(f"{result.title} {result.snippet}")
        if text and len(text) < 8:
            return False, "thin_search_result"
        return True, "accepted"

    async def _read_public_page(
        self,
        client: httpx.AsyncClient,
        result: SearchResult,
        focus_areas: list[str],
    ) -> dict[str, Any]:
        try:
            if not self._is_public_http_url(result.url) or self._is_low_value_url(result.url):
                return {
                    "document": None,
                    "event": {
                        "level": "warning",
                        "message": "公开页面因来源规则被过滤",
                        "stage": "page_reader_filtered",
                        "context": {"url": result.url, "reason": "non_public_or_low_value"},
                    },
                }

            raw_content = self._clean_text(result.raw_content)
            if (
                len(raw_content) >= MIN_TAVILY_RAW_CONTENT_LENGTH
                and not self._is_low_value_document(result.url, result.title, raw_content)
            ):
                doc = SourceDocument(
                    doc_id=self._build_doc_id(result.url, raw_content),
                    competitor=result.competitor,
                    url=result.url,
                    title=(result.title or result.competitor)[:120],
                    text=raw_content[:12000],
                    fetched_at=datetime.utcnow().isoformat(),
                )
                return {
                    "document": doc,
                    "event": {
                        "message": "Tavily 返回正文可用，已生成候选资料",
                        "stage": "page_reader_success",
                        "context": {
                            "url": result.url,
                            "title": doc.title,
                            "competitor": doc.competitor,
                            "query": result.query,
                            "provider_query": result.provider_query,
                            "provider": result.provider,
                            "content_source": "tavily_raw_content",
                            "text_length": len(doc.text),
                        },
                    },
                }

            response = await client.get(result.url)
            final_url = str(response.url)
            content_type = response.headers.get("content-type", "")
            if response.status_code >= 400:
                return {
                    "document": None,
                    "event": {
                        "level": "warning",
                        "message": "公开页面读取失败",
                        "stage": "page_reader_failed",
                        "context": {
                            "url": result.url,
                            "status_code": response.status_code,
                            "query": result.query,
                        },
                    },
                }
            if not self._is_public_http_url(final_url) or self._is_low_value_url(final_url):
                return {
                    "document": None,
                    "event": {
                        "level": "warning",
                        "message": "公开页面因来源规则被过滤",
                        "stage": "page_reader_filtered",
                        "context": {"url": final_url, "reason": "non_public_or_low_value"},
                    },
                }

            raw = response.content[:MAX_FETCH_BYTES]
            encoding = response.encoding or "utf-8"
            text = raw.decode(encoding, errors="ignore")
            if "html" in content_type or "<html" in text[:1000].lower():
                title, extracted_text = self._extract_content(text)
            else:
                title, extracted_text = result.title, self._clean_text(text)

            if len(extracted_text) < 120:
                return {
                    "document": None,
                    "event": {
                        "level": "warning",
                        "message": "公开页面正文过短，已跳过",
                        "stage": "page_reader_filtered",
                        "context": {
                            "url": final_url,
                            "query": result.query,
                            "text_length": len(extracted_text),
                        },
                    },
                }
            if self._is_low_value_document(final_url, title, extracted_text):
                return {
                    "document": None,
                    "event": {
                        "level": "warning",
                        "message": "公开页面正文低价值，已跳过",
                        "stage": "page_reader_filtered",
                        "context": {"url": final_url, "title": title[:120], "query": result.query},
                    },
                }

            doc = SourceDocument(
                doc_id=self._build_doc_id(final_url, extracted_text),
                competitor=result.competitor,
                url=final_url,
                title=(title or result.title or result.competitor)[:120],
                text=extracted_text[:12000],
                fetched_at=datetime.utcnow().isoformat(),
            )
            return {
                "document": doc,
                "event": {
                    "message": "公开页面读取成功，已生成候选资料",
                    "stage": "page_reader_success",
                    "context": {
                        "url": final_url,
                        "title": doc.title,
                        "competitor": doc.competitor,
                        "query": result.query,
                        "provider": result.provider,
                        "text_length": len(doc.text),
                    },
                },
            }
        except Exception as exc:
            return {
                "document": None,
                "event": {
                    "level": "warning",
                    "message": "公开页面读取异常",
                    "stage": "page_reader_error",
                    "context": {
                        "url": result.url,
                        "query": result.query,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                },
            }

    def _sample_documents(self, task: TaskDetail) -> list[SourceDocument]:
        documents: list[SourceDocument] = []
        for competitor in task.input.competitors:
            sample_text = self._resolve_sample_text(competitor)
            if not sample_text:
                continue
            source_ref = f"内置公开资料样例：{competitor}"
            documents.append(
                SourceDocument(
                    doc_id=self._build_doc_id(source_ref, sample_text),
                    competitor=competitor,
                    url=source_ref,
                    title=f"{competitor} 公开资料样例",
                    text=sample_text,
                    fetched_at=datetime.utcnow().isoformat(),
                )
            )
        return documents

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
                "source": doc.url,
                "title": doc.title,
                "text": doc.text[:3500],
            }
            for doc in documents[:10]
        ]
        system_prompt = (
            "你是严谨的信息抽取助手。只能基于用户提供或系统明确标记的公开资料正文抽取证据。"
            "不要补充资料正文里没有的信息，不要把来源建议当作证据。请严格返回JSON。"
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
    def _material_to_text(material: str) -> str:
        raw = str(material or "").strip()
        if not raw:
            return ""
        if "<" in raw and ">" in raw:
            soup = BeautifulSoup(raw, "html.parser")
            for tag in soup(["script", "style", "noscript", "svg", "canvas"]):
                tag.decompose()
            raw = soup.get_text("\n", strip=True)
        return RealCollector._clean_text(raw)

    @staticmethod
    def _extract_content(html: str) -> tuple[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg", "canvas", "iframe"]):
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
    def _infer_material_title(material: str, competitor: str, index: int) -> str:
        raw_lines = [line.strip(" #\t") for line in str(material).splitlines() if line.strip()]
        for line in raw_lines[:3]:
            if 4 <= len(line) <= 60:
                return line[:60]
        return f"{competitor} 公开资料 #{index}"

    @classmethod
    def _infer_competitor(cls, text: str, competitors: list[str]) -> str:
        if len(competitors) == 1:
            return competitors[0]
        normalized_text = text.lower()
        for competitor in competitors:
            if competitor and competitor in text:
                return competitor
            for alias in cls._competitor_aliases(competitor):
                if alias and alias.lower() in normalized_text:
                    return competitor
        return ""

    @staticmethod
    def _competitor_aliases(competitor: str) -> list[str]:
        aliases = {
            "抖音": ["douyin", "抖音电商"],
            "快手": ["kuaishou", "kwai", "快手电商"],
            "小红书": ["xiaohongshu", "rednote", "小红书蒲公英"],
            "淘宝": ["taobao", "tmall", "天猫"],
            "京东": ["jd", "jingdong"],
        }
        matched: list[str] = []
        for key, values in aliases.items():
            if key in competitor:
                matched.extend(values)
        return matched

    def _pick_source_reference(self, source_urls: list[str], competitor: str, index: int) -> str:
        for raw_url in source_urls:
            if self._url_matches_competitor(raw_url, competitor):
                return self._normalize_url(raw_url) or raw_url.strip()
        if 0 <= index - 1 < len(source_urls):
            normalized = self._normalize_url(source_urls[index - 1])
            if normalized:
                return normalized
        return f"用户提供公开资料 #{index}"

    def _resolve_sample_text(self, competitor: str) -> str:
        for key, sample in SAMPLE_PUBLIC_MATERIALS.items():
            if key in competitor:
                return sample
        return ""

    @staticmethod
    def _minimum_document_target(task: TaskDetail) -> int:
        return min(max(len(task.input.competitors), 1), 3)

    @staticmethod
    def _pick_relevant_sentence(text: str, focus_area: str) -> str:
        sentences = re.split(r"(?<=[。！？.!?])\s*", text)
        for sentence in sentences:
            cleaned = RealCollector._clean_text(sentence)
            if len(cleaned) >= 30 and (
                focus_area in cleaned
                or any(word in cleaned for word in ["产品", "用户", "商业", "增长", "服务", "体验"])
            ):
                return cleaned
        for sentence in sentences:
            cleaned = RealCollector._clean_text(sentence)
            if len(cleaned) >= 40:
                return cleaned
        return ""

    @staticmethod
    def _resolve_official_reference(competitor: str) -> str:
        for key, url in OFFICIAL_SOURCE_MAP.items():
            if key in competitor:
                return url
        return f"请补充 {competitor} 的公开资料文本"

    @staticmethod
    def _is_low_value_document(url: str, title: str, text: str) -> bool:
        combined = RealCollector._clean_text(f"{url} {title} {text[:1200]}").lower()
        if any(marker in combined for marker in ["404", "页面无法访问", "页面不存在", "not found", "返回首页"]):
            return True
        return RealCollector._is_low_value_text(combined)

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
    def _is_public_http_url(url: str) -> bool:
        normalized = RealCollector._normalize_url(url)
        if not normalized:
            return False
        parsed = urlparse(normalized)
        host = parsed.hostname or ""
        if parsed.scheme not in {"http", "https"} or not host:
            return False
        if host in {"localhost", "0.0.0.0"} or host.endswith(".local"):
            return False
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            return True
        return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved)

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
    def _unwrap_search_redirect(url: str) -> str:
        value = str(url or "").strip()
        parsed = urlparse(value)
        query = parse_qs(parsed.query)
        for key in ("uddg", "u", "url"):
            raw_values = query.get(key)
            if raw_values:
                return unquote(raw_values[0])
        return value

    @staticmethod
    def _merge_documents(documents: list[SourceDocument]) -> list[SourceDocument]:
        merged: list[SourceDocument] = []
        seen: set[tuple[str, str]] = set()
        for doc in documents:
            key = (doc.competitor, doc.url)
            if key in seen:
                continue
            seen.add(key)
            merged.append(doc)
        return merged

    @staticmethod
    def _build_search_provider() -> SearchProvider:
        provider = os.getenv(SEARCH_PROVIDER_ENV, DEFAULT_SEARCH_PROVIDER).strip().lower()
        if provider in {"", "disabled", "none", "off"}:
            return DisabledSearchProvider()
        if provider in {"tavily", "tavily_search"}:
            if not os.getenv(TAVILY_API_KEY_ENV, "").strip():
                return DisabledSearchProvider()
            return TavilySearchProvider()
        if provider in {"duckduckgo", "duckduckgo_html", "ddg"}:
            return DuckDuckGoSearchProvider()
        return DisabledSearchProvider()

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
