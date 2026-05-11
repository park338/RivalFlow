from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime
from statistics import mean

from .models import ClaimItem, EvidenceItem, TaskDetail, TaskEvent, TaskResult
from .storage import TaskStore


class PipelineRunner:
    def __init__(self, store: TaskStore) -> None:
        self.store = store

    async def run(self, task_id: str) -> None:
        try:
            await self._set_task_status(task_id, "running")
            task = await self._require_task(task_id)

            plan = await self._planner(task_id, task)
            evidence = await self._collector(task_id, task)
            scorecard = await self._structurer(task_id, task, evidence)
            claims, recommendations = await self._analyst(task_id, task, evidence, scorecard)
            reviewer_notes, reviewed_claims = await self._reviewer(task_id, claims)
            await self._reporter(
                task_id,
                task=task,
                plan=plan,
                scorecard=scorecard,
                claims=reviewed_claims,
                recommendations=recommendations,
                reviewer_notes=reviewer_notes,
            )
            await self._set_task_status(task_id, "completed")
            await self._append_event(task_id, "info", "任务完成，已生成最终报告")
        except Exception as exc:  # pragma: no cover - demo guardrail
            await self._set_task_failed(task_id, f"{type(exc).__name__}: {exc}")

    async def _require_task(self, task_id: str) -> TaskDetail:
        task = await self.store.get_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        return task

    async def _planner(self, task_id: str, task: TaskDetail) -> list[str]:
        await self._start_node(task_id, "planner", "正在拆解分析任务")
        await asyncio.sleep(0.8)
        focus_areas = task.input.focus_areas or ["产品定位", "用户体验", "商业化能力", "增长策略"]
        plan = [
            f"梳理 {task.input.industry} 赛道目标用户与核心场景",
            f"采集 {len(task.input.competitors)} 家竞品在 {task.input.time_range} 的公开资料",
            f"按维度 {', '.join(focus_areas)} 形成结构化评分",
            "输出差异化洞察、风险提示和行动建议",
        ]
        await self._merge_result(task_id, lambda result: result.plan.extend(plan))
        await self._finish_node(task_id, "planner", f"已生成 {len(plan)} 条任务计划")
        await self._append_event(task_id, "info", "Planner 已完成任务拆解")
        return plan

    async def _collector(self, task_id: str, task: TaskDetail) -> list[EvidenceItem]:
        await self._start_node(task_id, "collector", "正在采集公开信息")
        focus_areas = task.input.focus_areas or ["产品定位", "用户体验", "商业化能力", "增长策略"]
        evidence: list[EvidenceItem] = []
        for competitor in task.input.competitors:
            for focus_area in focus_areas:
                evidence_id = f"ev-{len(evidence) + 1:03d}"
                score = self._stable_score(competitor, focus_area)
                confidence = round(0.62 + (score % 30) / 100, 2)
                source_url = self._pick_source_url(task.input.source_urls, competitor)
                evidence.append(
                    EvidenceItem(
                        evidence_id=evidence_id,
                        competitor=competitor,
                        focus_area=focus_area,
                        source_name=f"{competitor} 公开资料",
                        source_url=source_url,
                        snippet=(
                            f"{competitor} 在 {focus_area} 方面持续投入，近期对关键功能和用户触点有明显优化，"
                            "并在公开渠道中强调效率提升与体验一致性。"
                        ),
                        confidence=min(confidence, 0.95),
                    )
                )
                await asyncio.sleep(0.15)
        await self._set_evidence(task_id, evidence)
        await self._finish_node(task_id, "collector", f"已采集 {len(evidence)} 条证据")
        await self._append_event(task_id, "info", f"Collector 完成采集：{len(evidence)} 条")
        return evidence

    async def _structurer(
        self, task_id: str, task: TaskDetail, evidence: list[EvidenceItem]
    ) -> dict[str, dict[str, int]]:
        await self._start_node(task_id, "structurer", "正在做 Schema 结构化映射")
        focus_areas = task.input.focus_areas or ["产品定位", "用户体验", "商业化能力", "增长策略"]
        scorecard: dict[str, dict[str, int]] = {}
        _ = evidence
        for competitor in task.input.competitors:
            scores: dict[str, int] = {}
            for focus_area in focus_areas:
                score = self._stable_score(competitor, focus_area)
                scores[focus_area] = score
            scorecard[competitor] = scores
            await asyncio.sleep(0.1)
        await self._merge_result(task_id, lambda result: result.scorecard.update(scorecard))
        await self._finish_node(task_id, "structurer", "结构化评分完成")
        await self._append_event(task_id, "info", "Structurer 输出标准化评分卡")
        return scorecard

    async def _analyst(
        self,
        task_id: str,
        task: TaskDetail,
        evidence: list[EvidenceItem],
        scorecard: dict[str, dict[str, int]],
    ) -> tuple[list[ClaimItem], list[str]]:
        await self._start_node(task_id, "analyst", "正在生成竞品洞察")
        focus_areas = task.input.focus_areas or ["产品定位", "用户体验", "商业化能力", "增长策略"]
        competitor_avg = {
            name: round(mean(scores.values()), 1) for name, scores in scorecard.items() if scores
        }
        winner = max(competitor_avg, key=competitor_avg.get)
        risk_competitor = min(competitor_avg, key=competitor_avg.get)

        claims: list[ClaimItem] = []
        winner_evidence = [item.evidence_id for item in evidence if item.competitor == winner][:3]
        claims.append(
            ClaimItem(
                claim_id="cl-001",
                title=f"{winner} 在综合表现上领先",
                detail=f"{winner} 的平均评分为 {competitor_avg[winner]}，在关键维度上表现稳定。",
                confidence=0.83,
                evidence_ids=winner_evidence,
            )
        )

        weakest_dimension = self._find_weakest_dimension(scorecard[risk_competitor], focus_areas)
        weak_evidence = [
            item.evidence_id
            for item in evidence
            if item.competitor == risk_competitor and item.focus_area == weakest_dimension
        ]
        claims.append(
            ClaimItem(
                claim_id="cl-002",
                title=f"{risk_competitor} 在 {weakest_dimension} 维度存在短板",
                detail=(
                    f"{risk_competitor} 在 {weakest_dimension} 的评分相对偏低，"
                    "若持续扩大差距，可能影响市场转化效率。"
                ),
                confidence=0.77,
                evidence_ids=weak_evidence,
            )
        )

        recommendations = [
            "优先补齐弱势维度对应的核心功能与体验链路，避免单点短板放大。",
            "围绕高价值用户场景建立差异化能力，并持续跟踪竞品节奏。",
            "对关键结论保持每周更新，确保策略建立在最新证据之上。",
        ]

        await self._merge_result(
            task_id,
            lambda result: (
                result.claims.extend(claims),
                result.recommendations.extend(recommendations),
            ),
        )
        await self._finish_node(task_id, "analyst", f"已输出 {len(claims)} 条核心结论")
        await self._append_event(task_id, "info", "Analyst 已形成初稿洞察")
        return claims, recommendations

    async def _reviewer(
        self, task_id: str, claims: list[ClaimItem]
    ) -> tuple[list[str], list[ClaimItem]]:
        await self._start_node(task_id, "reviewer", "正在执行交叉审查")
        reviewer_notes: list[str] = []
        reviewed_claims: list[ClaimItem] = []
        for claim in claims:
            if claim.confidence < 0.75:
                reviewer_notes.append(f"{claim.claim_id} 置信度偏低，建议补充更多来源。")
                claim.confidence = round(claim.confidence + 0.06, 2)
            reviewed_claims.append(claim)
            await asyncio.sleep(0.2)

        if not reviewer_notes:
            reviewer_notes.append("关键结论证据完整，当前版本可用于业务讨论。")
        await self._merge_result(task_id, lambda result: result.reviewer_notes.extend(reviewer_notes))
        await self._finish_node(task_id, "reviewer", f"审查完成：{len(reviewer_notes)} 条反馈")
        await self._append_event(task_id, "info", "Reviewer 已完成结论审查")
        return reviewer_notes, reviewed_claims

    async def _reporter(
        self,
        task_id: str,
        task: TaskDetail,
        plan: list[str],
        scorecard: dict[str, dict[str, int]],
        claims: list[ClaimItem],
        recommendations: list[str],
        reviewer_notes: list[str],
    ) -> None:
        await self._start_node(task_id, "reporter", "正在生成最终报告")
        await asyncio.sleep(0.8)
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

        def mutate(result: TaskResult) -> None:
            result.markdown_report = markdown

        await self._merge_result(task_id, mutate)
        await self._finish_node(task_id, "reporter", "报告生成完成")
        await self._append_event(task_id, "info", "Reporter 已输出报告")

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

        await self.store.mutate_task(task_id, mutate)
        await self._append_event(task_id, "error", f"流程失败：{error_message}")

    async def _start_node(self, task_id: str, node_key: str, summary: str) -> None:
        def mutate(task: TaskDetail) -> None:
            now = datetime.utcnow()
            for node in task.nodes:
                if node.key == node_key:
                    node.status = "running"
                    node.summary = summary
                    node.started_at = node.started_at or now
                    break

        await self.store.mutate_task(task_id, mutate)

    async def _finish_node(self, task_id: str, node_key: str, summary: str) -> None:
        def mutate(task: TaskDetail) -> None:
            now = datetime.utcnow()
            for node in task.nodes:
                if node.key == node_key:
                    node.status = "completed"
                    node.summary = summary
                    node.finished_at = now
                    break

        await self.store.mutate_task(task_id, mutate)

    async def _append_event(self, task_id: str, level: str, message: str) -> None:
        def mutate(task: TaskDetail) -> None:
            task.events.append(
                TaskEvent(
                    at=datetime.utcnow(),
                    level=level,
                    message=message,
                )
            )

        await self.store.mutate_task(task_id, mutate)

    async def _set_evidence(self, task_id: str, evidence: list[EvidenceItem]) -> None:
        await self.store.mutate_task(task_id, lambda task: setattr(task, "evidence", evidence))

    async def _merge_result(self, task_id: str, mutator) -> None:
        def mutate(task: TaskDetail) -> None:
            mutator(task.result)

        await self.store.mutate_task(task_id, mutate)

    @staticmethod
    def _stable_score(competitor: str, focus_area: str) -> int:
        payload = f"{competitor}::{focus_area}".encode("utf-8")
        seed = int(hashlib.md5(payload).hexdigest(), 16)
        return 60 + seed % 36

    @staticmethod
    def _pick_source_url(source_urls: list[str], competitor: str) -> str:
        if not source_urls:
            return f"https://example.com/{competitor}"
        index = int(hashlib.md5(competitor.encode("utf-8")).hexdigest(), 16) % len(source_urls)
        return source_urls[index]

    @staticmethod
    def _find_weakest_dimension(scores: dict[str, int], focus_areas: list[str]) -> str:
        if not scores:
            return focus_areas[0]
        return min(scores.items(), key=lambda item: item[1])[0]

    @staticmethod
    def _build_markdown_report(
        project_name: str,
        industry: str,
        time_range: str,
        plan: list[str],
        scorecard: dict[str, dict[str, int]],
        claims: list[ClaimItem],
        recommendations: list[str],
        reviewer_notes: list[str],
    ) -> str:
        focus_areas = []
        for competitor_scores in scorecard.values():
            focus_areas = list(competitor_scores.keys())
            if focus_areas:
                break

        table_header = "| 竞品 | " + " | ".join(focus_areas) + " |\n"
        table_split = "|---|" + "|".join(["---"] * len(focus_areas)) + "|\n"
        rows = []
        for competitor, scores in scorecard.items():
            row = "| " + competitor + " | " + " | ".join(str(scores[item]) for item in focus_areas) + " |"
            rows.append(row)

        plan_section = "\n".join(f"- {item}" for item in plan)
        claim_section = "\n".join(
            f"- **{claim.title}**：{claim.detail}（置信度 {claim.confidence:.2f}）" for claim in claims
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
