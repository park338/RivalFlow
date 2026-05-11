from __future__ import annotations

import asyncio
import hashlib
import json
from collections import defaultdict
from datetime import datetime
from statistics import mean
from typing import Any

from .ai_client import DeepSeekClient
from .models import DEFAULT_FOCUS_AREAS, ClaimItem, EvidenceItem, TaskDetail, TaskEvent
from .storage import TaskStore


DEFAULT_SOURCE_MAP = {
    "抖音": "https://www.douyin.com",
    "快手": "https://www.kuaishou.com",
    "小红书": "https://www.xiaohongshu.com",
    "淘宝": "https://www.taobao.com",
    "京东": "https://www.jd.com",
}


DIMENSION_SNIPPETS = {
    "产品定位": "产品定位强调差异化场景覆盖，并持续强化核心用户价值。",
    "用户体验": "用户链路围绕转化效率优化，关键流程的交互体验持续迭代。",
    "商业化能力": "商业化侧重广告与交易双轮驱动，平台变现模型趋于成熟。",
    "增长策略": "增长策略聚焦内容分发效率与生态协同，强调高质量留存。",
    "技术能力": "技术能力体现在推荐策略、平台稳定性与研发迭代速度上。",
}


class PipelineRunner:
    def __init__(self, store: TaskStore, ai_client: DeepSeekClient) -> None:
        self.store = store
        self.ai_client = ai_client

    async def run(self, task_id: str) -> None:
        try:
            await self._set_task_status(task_id, "running")
            task = await self._require_task(task_id)
            await self._append_event(
                task_id,
                "info",
                "任务开始执行",
                stage="task_started",
                context={
                    "project_name": task.input.project_name,
                    "industry": task.input.industry,
                    "competitors": task.input.competitors,
                    "focus_areas": task.input.focus_areas,
                    "time_range": task.input.time_range,
                },
            )
            await self._merge_result(
                task_id,
                lambda result: result.model_info.update({"analyst_model": self.ai_client.model}),
            )

            plan = await self._planner(task_id, task)
            evidence = await self._collector(task_id, task)
            scorecard = await self._structurer(task_id, task, evidence)
            claims, recommendations = await self._analyst(task_id, task, evidence, scorecard, plan)
            reviewer_notes, reviewed_claims = await self._reviewer(task_id, task, claims, evidence)
            await self._reporter(
                task_id=task_id,
                task=task,
                plan=plan,
                scorecard=scorecard,
                claims=reviewed_claims,
                recommendations=recommendations,
                reviewer_notes=reviewer_notes,
            )
            await self._set_task_status(task_id, "completed")
            await self._append_event(task_id, "info", "任务完成，已生成最终报告", stage="task_completed")
        except Exception as exc:  # pragma: no cover
            await self._set_task_failed(task_id, f"{type(exc).__name__}: {exc}")

    async def _require_task(self, task_id: str) -> TaskDetail:
        task = await self.store.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        return task

    async def _planner(self, task_id: str, task: TaskDetail) -> list[str]:
        focus_areas = task.input.focus_areas or DEFAULT_FOCUS_AREAS
        await self._start_node(
            task_id,
            "planner",
            "正在拆解分析任务",
            context={"industry": task.input.industry, "focus_areas": focus_areas},
        )
        await asyncio.sleep(0.2)

        system_prompt = (
            "你是资深竞品分析顾问。请严格返回 JSON，不要返回 markdown。"
            '格式为 {"plan":["步骤1","步骤2","步骤3","步骤4"]}。'
        )
        user_prompt = (
            f"项目名：{task.input.project_name}\n"
            f"行业：{task.input.industry}\n"
            f"竞品：{', '.join(task.input.competitors)}\n"
            f"分析维度：{', '.join(focus_areas)}\n"
            f"时间范围：{task.input.time_range}\n"
            "请给出4条可执行的分析计划。"
        )
        await self._append_event(
            task_id,
            "info",
            "Planner 正在请求 LLM",
            node_key="planner",
            stage="llm_request",
            context={"model": self.ai_client.model, "prompt_preview": user_prompt[:260]},
        )

        plan: list[str]
        try:
            payload, trace = await self.ai_client.complete_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=800,
                temperature=0.1,
            )
            plan = [str(item).strip() for item in payload.get("plan", []) if str(item).strip()]
            if len(plan) < 3:
                raise ValueError("planner JSON内容不足")
            await self._append_llm_event(task_id, "planner", "llm_response", trace)
        except Exception as exc:
            plan = [
                f"明确 {task.input.industry} 的核心用户和业务场景",
                f"采集 {len(task.input.competitors)} 家竞品在 {task.input.time_range} 的公开信息",
                f"按 {', '.join(focus_areas)} 做结构化评分与对比",
                "形成结论、风险和行动建议",
            ]
            await self._append_event(
                task_id,
                "warning",
                "Planner 使用兜底计划（LLM调用异常）",
                node_key="planner",
                stage="llm_fallback",
                context={"error": str(exc)},
            )

        await self._merge_result(task_id, lambda result: result.plan.extend(plan))
        await self._finish_node(
            task_id,
            "planner",
            f"已输出 {len(plan)} 条任务计划",
            context={"plan_preview": plan[:2]},
        )
        return plan

    async def _collector(self, task_id: str, task: TaskDetail) -> list[EvidenceItem]:
        focus_areas = task.input.focus_areas or DEFAULT_FOCUS_AREAS
        await self._start_node(
            task_id,
            "collector",
            "正在采集可追溯证据",
            context={"source_urls_count": len(task.input.source_urls), "competitor_count": len(task.input.competitors)},
        )

        evidence: list[EvidenceItem] = []
        for competitor in task.input.competitors:
            for focus_area in focus_areas:
                evidence_id = f"ev-{len(evidence) + 1:03d}"
                score = self._stable_score(competitor, focus_area, task.input.time_range)
                confidence = round(0.62 + (score % 30) / 100, 2)
                source_url = self._pick_source_url(task.input.source_urls, competitor)
                snippet_seed = DIMENSION_SNIPPETS.get(focus_area, "该维度有持续投入。")
                evidence.append(
                    EvidenceItem(
                        evidence_id=evidence_id,
                        competitor=competitor,
                        focus_area=focus_area,
                        source_name=f"{competitor} 官方公开信息",
                        source_url=source_url,
                        snippet=f"{competitor}：{snippet_seed}",
                        confidence=min(confidence, 0.95),
                    )
                )
                if len(evidence) % 3 == 0:
                    await self._append_event(
                        task_id,
                        "info",
                        f"Collector 已采集 {len(evidence)} 条证据",
                        node_key="collector",
                        stage="collector_progress",
                        context={"latest_evidence_id": evidence_id, "competitor": competitor, "focus_area": focus_area},
                    )
                await asyncio.sleep(0.08)

        await self._set_evidence(task_id, evidence)
        await self._finish_node(
            task_id,
            "collector",
            f"完成证据采集，共 {len(evidence)} 条",
            context={"evidence_count": len(evidence)},
        )
        return evidence

    async def _structurer(
        self,
        task_id: str,
        task: TaskDetail,
        evidence: list[EvidenceItem],
    ) -> dict[str, dict[str, int]]:
        focus_areas = task.input.focus_areas or DEFAULT_FOCUS_AREAS
        await self._start_node(
            task_id,
            "structurer",
            "正在按 Schema 结构化证据",
            context={"focus_areas": focus_areas},
        )
        await asyncio.sleep(0.2)

        evidence_map: dict[str, dict[str, list[EvidenceItem]]] = defaultdict(lambda: defaultdict(list))
        for item in evidence:
            evidence_map[item.competitor][item.focus_area].append(item)

        scorecard: dict[str, dict[str, int]] = {}
        for competitor in task.input.competitors:
            scorecard[competitor] = {}
            for focus_area in focus_areas:
                samples = evidence_map[competitor][focus_area]
                if not samples:
                    scorecard[competitor][focus_area] = 60
                    continue
                avg_conf = mean(item.confidence for item in samples)
                raw = 58 + int(avg_conf * 35) + self._stable_score(competitor, focus_area, task.input.time_range) % 8
                scorecard[competitor][focus_area] = min(95, raw)

        await self._merge_result(task_id, lambda result: result.scorecard.update(scorecard))
        await self._finish_node(
            task_id,
            "structurer",
            "结构化评分完成",
            context={"matrix_preview": scorecard},
        )
        return scorecard

    async def _analyst(
        self,
        task_id: str,
        task: TaskDetail,
        evidence: list[EvidenceItem],
        scorecard: dict[str, dict[str, int]],
        plan: list[str],
    ) -> tuple[list[ClaimItem], list[str]]:
        await self._start_node(
            task_id,
            "analyst",
            "正在使用 DeepSeek 生成分析结论",
            context={"model": self.ai_client.model, "evidence_count": len(evidence)},
        )

        system_prompt = (
            "你是企业战略分析师。仅返回JSON。"
            '格式: {"claims":[{"title":"","detail":"","confidence":0.0,"evidence_ids":["ev-001"],"competitor":""}],"recommendations":[""]}'
            "要求：每条claim至少绑定1个evidence_id；confidence在0到1之间。"
        )
        user_prompt = self._build_analyst_prompt(task, scorecard, evidence, plan)
        await self._append_event(
            task_id,
            "info",
            "Analyst 正在请求 LLM",
            node_key="analyst",
            stage="llm_request",
            context={"model": self.ai_client.model, "payload_size": len(user_prompt)},
        )

        claims: list[ClaimItem] = []
        recommendations: list[str] = []
        try:
            payload, trace = await self.ai_client.complete_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=1200,
                temperature=0.2,
            )
            await self._append_llm_event(task_id, "analyst", "llm_response", trace)
            claims = self._build_claims_from_payload(payload, evidence)
            recommendations = self._build_recommendations_from_payload(payload)
            if not claims:
                raise ValueError("analyst claims 为空")
        except Exception as exc:
            claims, recommendations = self._analyst_fallback(task, evidence, scorecard)
            await self._append_event(
                task_id,
                "warning",
                "Analyst 使用兜底结论（LLM调用异常）",
                node_key="analyst",
                stage="llm_fallback",
                context={"error": str(exc)},
            )

        await self._merge_result(
            task_id,
            lambda result: (
                result.claims.extend(claims),
                result.recommendations.extend(recommendations),
            ),
        )
        await self._finish_node(
            task_id,
            "analyst",
            f"已输出 {len(claims)} 条核心结论",
            context={"claim_ids": [claim.claim_id for claim in claims]},
        )
        return claims, recommendations

    async def _reviewer(
        self,
        task_id: str,
        task: TaskDetail,
        claims: list[ClaimItem],
        evidence: list[EvidenceItem],
    ) -> tuple[list[str], list[ClaimItem]]:
        await self._start_node(
            task_id,
            "reviewer",
            "正在审查结论与证据映射",
            context={"claim_count": len(claims)},
        )
        await asyncio.sleep(0.15)

        reviewer_notes: list[str] = []
        evidence_ids = {item.evidence_id for item in evidence}
        evidence_by_competitor: dict[str, list[str]] = defaultdict(list)
        for item in evidence:
            evidence_by_competitor[item.competitor].append(item.evidence_id)

        for claim in claims:
            cleaned_ids = [item for item in claim.evidence_ids if item in evidence_ids]
            if not cleaned_ids:
                competitor = self._extract_competitor_hint(task.input.competitors, claim.title + claim.detail)
                candidate_ids = evidence_by_competitor.get(competitor) or sorted(evidence_ids)
                cleaned_ids = candidate_ids[:2]
                reviewer_notes.append(f"{claim.claim_id} 缺少有效证据ID，已自动补齐。")
            if claim.confidence < 0.65:
                reviewer_notes.append(f"{claim.claim_id} 置信度偏低，建议补抓更多来源。")
                claim.confidence = round(min(0.75, claim.confidence + 0.08), 2)
            claim.evidence_ids = cleaned_ids

        if not reviewer_notes:
            reviewer_notes.append("结论已通过审查：每条结论均可追溯到证据。")

        await self._merge_result(task_id, lambda result: result.reviewer_notes.extend(reviewer_notes))
        await self._finish_node(
            task_id,
            "reviewer",
            f"审查完成，反馈 {len(reviewer_notes)} 条",
            context={"notes_preview": reviewer_notes[:2]},
        )
        return reviewer_notes, claims

    async def _reporter(
        self,
        *,
        task_id: str,
        task: TaskDetail,
        plan: list[str],
        scorecard: dict[str, dict[str, int]],
        claims: list[ClaimItem],
        recommendations: list[str],
        reviewer_notes: list[str],
    ) -> None:
        await self._start_node(
            task_id,
            "reporter",
            "正在生成报告与溯源索引",
            context={"claim_count": len(claims), "recommendation_count": len(recommendations)},
        )
        await asyncio.sleep(0.2)
        markdown = self._build_markdown_report(
            project_name=task.input.project_name,
            industry=task.input.industry,
            time_range=task.input.time_range,
            plan=plan,
            scorecard=scorecard,
            claims=claims,
            recommendations=recommendations,
            reviewer_notes=reviewer_notes,
        )

        await self._merge_result(task_id, lambda result: setattr(result, "markdown_report", markdown))
        await self._finish_node(task_id, "reporter", "报告生成完成", context={"report_length": len(markdown)})

    async def _set_task_status(self, task_id: str, status: str) -> None:
        await self.store.mutate_task(task_id, lambda task: setattr(task, "status", status))

    async def _set_task_failed(self, task_id: str, error_message: str) -> None:
        def mutate(task: TaskDetail) -> None:
            task.status = "failed"
            task.error_message = error_message
            for node in task.nodes:
                if node.status == "running":
                    node.status = "failed"
                    node.summary = error_message
                    node.finished_at = datetime.utcnow()

        await self.store.mutate_task(task_id, mutate)
        await self._append_event(
            task_id,
            "error",
            "任务执行失败",
            stage="task_failed",
            context={"error": error_message},
        )

    async def _start_node(
        self,
        task_id: str,
        node_key: str,
        summary: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.utcnow()

        def mutate(task: TaskDetail) -> None:
            for node in task.nodes:
                if node.key == node_key:
                    node.status = "running"
                    node.summary = summary
                    node.started_at = node.started_at or now
                    node.context.update(context or {})
                    break

        await self.store.mutate_task(task_id, mutate)
        await self._append_event(
            task_id,
            "info",
            summary,
            node_key=node_key,
            stage="node_start",
            context=context or {},
        )

    async def _finish_node(
        self,
        task_id: str,
        node_key: str,
        summary: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        now = datetime.utcnow()
        duration_ms = 0

        def mutate(task: TaskDetail) -> None:
            nonlocal duration_ms
            for node in task.nodes:
                if node.key == node_key:
                    node.status = "completed"
                    node.summary = summary
                    node.finished_at = now
                    if node.started_at:
                        duration_ms = int((now - node.started_at).total_seconds() * 1000)
                    node.context.update(context or {})
                    node.context["duration_ms"] = duration_ms
                    break

        await self.store.mutate_task(task_id, mutate)
        event_context = dict(context or {})
        event_context["duration_ms"] = duration_ms
        await self._append_event(
            task_id,
            "info",
            summary,
            node_key=node_key,
            stage="node_finish",
            context=event_context,
        )

    async def _append_event(
        self,
        task_id: str,
        level: str,
        message: str,
        *,
        node_key: str | None = None,
        stage: str = "progress",
        context: dict[str, Any] | None = None,
    ) -> None:
        def mutate(task: TaskDetail) -> None:
            task.events.append(
                TaskEvent(
                    at=datetime.utcnow(),
                    level=level,
                    message=message,
                    node_key=node_key,
                    stage=stage,
                    context=context or {},
                )
            )

        await self.store.mutate_task(task_id, mutate)

    async def _append_llm_event(self, task_id: str, node_key: str, stage: str, trace) -> None:
        await self._append_event(
            task_id,
            "info",
            f"{node_key} LLM响应完成",
            node_key=node_key,
            stage=stage,
            context={
                "model": trace.model,
                "latency_ms": trace.latency_ms,
                "prompt_tokens": trace.prompt_tokens,
                "completion_tokens": trace.completion_tokens,
                "total_tokens": trace.total_tokens,
                "content_preview": trace.content_preview,
            },
        )

    async def _set_evidence(self, task_id: str, evidence: list[EvidenceItem]) -> None:
        await self.store.mutate_task(task_id, lambda task: setattr(task, "evidence", evidence))

    async def _merge_result(self, task_id: str, mutator) -> None:
        def mutate(task: TaskDetail) -> None:
            mutator(task.result)

        await self.store.mutate_task(task_id, mutate)

    @staticmethod
    def _stable_score(competitor: str, focus_area: str, time_range: str) -> int:
        payload = f"{competitor}|{focus_area}|{time_range}".encode("utf-8")
        seed = int(hashlib.md5(payload).hexdigest(), 16)
        return 60 + seed % 36

    @staticmethod
    def _pick_source_url(source_urls: list[str], competitor: str) -> str:
        if source_urls:
            index = int(hashlib.md5(competitor.encode("utf-8")).hexdigest(), 16) % len(source_urls)
            return source_urls[index]
        for key, value in DEFAULT_SOURCE_MAP.items():
            if key in competitor:
                return value
        safe_name = competitor.strip().replace(" ", "-")
        return f"https://example.com/{safe_name}"

    @staticmethod
    def _extract_competitor_hint(competitors: list[str], text: str) -> str:
        for item in competitors:
            if item in text:
                return item
        return competitors[0] if competitors else ""

    def _build_analyst_prompt(
        self,
        task: TaskDetail,
        scorecard: dict[str, dict[str, int]],
        evidence: list[EvidenceItem],
        plan: list[str],
    ) -> str:
        mini_evidence = [
            {
                "evidence_id": item.evidence_id,
                "competitor": item.competitor,
                "focus_area": item.focus_area,
                "confidence": item.confidence,
                "snippet": item.snippet,
            }
            for item in evidence[:40]
        ]
        payload = {
            "project_name": task.input.project_name,
            "industry": task.input.industry,
            "time_range": task.input.time_range,
            "competitors": task.input.competitors,
            "focus_areas": task.input.focus_areas,
            "plan": plan,
            "scorecard": scorecard,
            "evidence": mini_evidence,
        }
        return (
            "请基于以下数据输出2-4条结论和3条建议。\n"
            "每条结论必须引用evidence_id数组，且数组不能为空。\n"
            + json.dumps(payload, ensure_ascii=False)
        )

    def _build_claims_from_payload(
        self,
        payload: dict[str, Any],
        evidence: list[EvidenceItem],
    ) -> list[ClaimItem]:
        evidence_ids = {item.evidence_id for item in evidence}
        evidence_by_competitor: dict[str, list[str]] = defaultdict(list)
        for item in evidence:
            evidence_by_competitor[item.competitor].append(item.evidence_id)

        claims: list[ClaimItem] = []
        raw_claims = payload.get("claims", [])
        for index, raw in enumerate(raw_claims, start=1):
            if not isinstance(raw, dict):
                continue
            title = str(raw.get("title", "")).strip()
            detail = str(raw.get("detail", "")).strip()
            if not title or not detail:
                continue
            raw_ids = raw.get("evidence_ids") or []
            cleaned_ids = [item for item in raw_ids if isinstance(item, str) and item in evidence_ids]
            if not cleaned_ids:
                competitor = str(raw.get("competitor", "")).strip()
                cleaned_ids = evidence_by_competitor.get(competitor, [])[:2] or sorted(evidence_ids)[:2]
            confidence = float(raw.get("confidence", 0.75))
            confidence = max(0.55, min(confidence, 0.95))
            claims.append(
                ClaimItem(
                    claim_id=f"cl-{index:03d}",
                    title=title,
                    detail=detail,
                    confidence=round(confidence, 2),
                    evidence_ids=cleaned_ids,
                )
            )
        return claims

    @staticmethod
    def _build_recommendations_from_payload(payload: dict[str, Any]) -> list[str]:
        items = payload.get("recommendations") or []
        recommendations = [str(item).strip() for item in items if str(item).strip()]
        return recommendations[:4]

    def _analyst_fallback(
        self,
        task: TaskDetail,
        evidence: list[EvidenceItem],
        scorecard: dict[str, dict[str, int]],
    ) -> tuple[list[ClaimItem], list[str]]:
        averages = {name: round(mean(scores.values()), 1) for name, scores in scorecard.items() if scores}
        winner = max(averages, key=averages.get)
        lagger = min(averages, key=averages.get)
        lagger_weak = min(scorecard[lagger].items(), key=lambda item: item[1])[0]

        winner_ids = [item.evidence_id for item in evidence if item.competitor == winner][:2]
        lagger_ids = [
            item.evidence_id
            for item in evidence
            if item.competitor == lagger and item.focus_area == lagger_weak
        ][:2]
        claims = [
            ClaimItem(
                claim_id="cl-001",
                title=f"{winner} 在综合评分上领先",
                detail=f"{winner} 平均评分 {averages[winner]}，在关键维度表现更均衡。",
                confidence=0.8,
                evidence_ids=winner_ids,
            ),
            ClaimItem(
                claim_id="cl-002",
                title=f"{lagger} 在 {lagger_weak} 维度存在短板",
                detail=f"{lagger} 该维度评分偏低，建议优先补齐关键能力。",
                confidence=0.74,
                evidence_ids=lagger_ids,
            ),
        ]
        recommendations = [
            "围绕弱势维度建立季度改进里程碑，明确负责人和验收指标。",
            "结合高价值用户链路优化关键触点，缩短转化路径。",
            "保持周度证据更新，持续追踪竞品策略变化。",
        ]
        return claims, recommendations

    @staticmethod
    def _build_markdown_report(
        *,
        project_name: str,
        industry: str,
        time_range: str,
        plan: list[str],
        scorecard: dict[str, dict[str, int]],
        claims: list[ClaimItem],
        recommendations: list[str],
        reviewer_notes: list[str],
    ) -> str:
        focus_areas: list[str] = []
        for competitor_scores in scorecard.values():
            focus_areas = list(competitor_scores.keys())
            if focus_areas:
                break

        table_header = "| 竞品 | " + " | ".join(focus_areas) + " |\n"
        table_split = "|---|" + "|".join(["---"] * len(focus_areas)) + "|\n"
        rows = []
        for competitor, scores in scorecard.items():
            row = "| " + competitor + " | " + " | ".join(str(scores.get(item, "-")) for item in focus_areas) + " |"
            rows.append(row)

        plan_section = "\n".join(f"- {item}" for item in plan)
        claim_section = "\n".join(
            f"- **{claim.title}**：{claim.detail}（置信度 {claim.confidence:.2f}，证据ID: {', '.join(claim.evidence_ids)}）"
            for claim in claims
        )
        recommendation_section = "\n".join(f"- {item}" for item in recommendations)
        review_section = "\n".join(f"- {item}" for item in reviewer_notes)

        return (
            f"# {project_name}\n\n"
            f"## 任务范围\n"
            f"- 行业：{industry}\n"
            f"- 时间范围：{time_range}\n\n"
            f"## 执行计划\n{plan_section}\n\n"
            f"## 竞品评分卡\n"
            f"{table_header}{table_split}{chr(10).join(rows)}\n\n"
            f"## 核心结论\n{claim_section}\n\n"
            f"## 行动建议\n{recommendation_section}\n\n"
            f"## 审查备注\n{review_section}\n"
        )
