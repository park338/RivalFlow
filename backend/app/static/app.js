const form = document.getElementById("task-form");
const demoBtn = document.getElementById("demoBtn");
const submitBtn = document.getElementById("submitBtn");
const messageEl = document.getElementById("message");
const taskMetaEl = document.getElementById("taskMeta");
const timelineEl = document.getElementById("timeline");
const nodeDetailEl = document.getElementById("nodeDetail");
const nodeDetailTitleEl = document.getElementById("nodeDetailTitle");
const nodeDetailSummaryEl = document.getElementById("nodeDetailSummary");
const nodeDetailContextEl = document.getElementById("nodeDetailContext");
const nodeEventsEl = document.getElementById("nodeEvents");
const evidenceBodyEl = document.querySelector("#evidenceTable tbody");
const reportEl = document.getElementById("report");
const exportPdfBtn = document.getElementById("exportPdfBtn");
const scorecardEl = document.getElementById("scorecard");
const claimsEl = document.getElementById("claims");
const scorecardWrapEl = document.getElementById("scorecardWrap");
const claimsWrapEl = document.getElementById("claimsWrap");

const FOCUS_AREA_OPTIONS = ["产品定位", "用户体验", "商业化能力", "增长策略", "技术能力"];

let pollingTimer = null;
let selectedNodeKey = null;
let latestTask = null;
let selectedFocusAreas = new Set(["产品定位", "用户体验", "商业化能力", "增长策略"]);
let selectedScoreKey = "";

renderFocusAreaTags();
exportPdfBtn.addEventListener("click", exportReportPdf);

demoBtn.addEventListener("click", () => {
  document.getElementById("projectName").value = "2026 短视频电商商业化策略对比（抖音 vs 快手 vs 小红书）";
  document.getElementById("industry").value = "内容电商";
  document.getElementById("competitors").value = "抖音,快手,小红书";
  document.getElementById("sourceUrls").value = "https://www.douyin.com,https://www.kuaishou.com,https://www.xiaohongshu.com";
  document.getElementById("publicMaterials").value = buildDemoPublicMaterials();
  setFocusAreas(["产品定位", "用户体验", "商业化能力", "增长策略"]);
  document.getElementById("timeRange").value = "近 12 个月";
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const payload = buildPayload();
  if (!payload.competitors.length) {
    setMessage("请至少填写一个竞品名称。", true);
    return;
  }
  if (!payload.focus_areas.length) {
    setMessage("请至少选择一个分析维度。", true);
    return;
  }

  try {
    setLoading(true);
    setMessage("任务提交中...");
    const response = await fetch("/api/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(await response.text());
    }
    const task = await response.json();
    selectedNodeKey = null;
    setMessage(`任务已创建：${task.task_id}`);
    renderTask(task);
    startPolling(task.task_id);
  } catch (error) {
    setMessage(`提交失败：${error.message || "未知错误"}`, true);
    setLoading(false);
  }
});

function buildPayload() {
  const toList = (value) =>
    value
      .split(/[,\n]/)
      .map((item) => item.trim())
      .filter(Boolean);
  const toMaterialList = (value) =>
    value
      .split(/\n\s*\n/)
      .map((item) => item.trim())
      .filter(Boolean);

  const focusAreas = Array.from(selectedFocusAreas);
  return {
    project_name: document.getElementById("projectName").value.trim() || "RivalFlow 竞品分析 Demo",
    industry: document.getElementById("industry").value.trim(),
    competitors: toList(document.getElementById("competitors").value),
    focus_areas: focusAreas,
    source_urls: toList(document.getElementById("sourceUrls").value),
    public_materials: toMaterialList(document.getElementById("publicMaterials").value),
    time_range: document.getElementById("timeRange").value,
  };
}

function buildDemoPublicMaterials() {
  return [
    "抖音电商公开资料样例：抖音电商围绕短视频内容、直播互动、达人带货和店铺经营形成交易链路。平台面向商家提供商品发布、内容经营、直播转化、营销活动和经营数据等工具，帮助品牌在内容场景中触达用户。在用户体验上，商品展示、达人讲解、评论互动和下单路径结合在同一内容消费流程内。在商业化能力上，广告投放、达人合作和电商交易共同支撑品牌增长。",
    "快手电商公开资料样例：快手电商强调信任关系、直播间互动和商家长期经营，平台通过达人、店铺、短视频和直播形成转化链路。商家可以围绕粉丝关系进行内容运营、商品讲解、售后服务和复购管理。在用户体验上，直播讲解、评论互动和交易服务更强调实时沟通。在增长策略上，快手生态重视私域沉淀、达人协作和商家经营效率。",
    "小红书公开资料样例：小红书围绕生活方式社区、内容种草、搜索发现和品牌合作建立产品定位。平台通过笔记内容、用户评论、收藏分享和搜索链路帮助用户完成消费决策。蒲公英等商业合作能力连接品牌与创作者，支持内容合作、投放管理和效果评估。在增长策略上，小红书强调社区内容质量、真实体验分享和用户兴趣发现。",
  ].join("\\n\\n");
}

function startPolling(taskId) {
  stopPolling();
  pollingTimer = window.setInterval(async () => {
    try {
      const response = await fetch(`/api/tasks/${taskId}`);
      if (!response.ok) {
        throw new Error("任务查询失败");
      }
      const task = await response.json();
      renderTask(task);
      if (task.status === "completed") {
        setMessage("分析已完成。");
        setLoading(false);
        stopPolling();
      } else if (task.status === "failed") {
        setMessage(`任务失败：${task.error_message || "未知错误"}`, true);
        setLoading(false);
        stopPolling();
      }
    } catch (error) {
      setMessage(`轮询异常：${error.message || "未知错误"}`, true);
      setLoading(false);
      stopPolling();
    }
  }, 1200);
}

function stopPolling() {
  if (pollingTimer) {
    window.clearInterval(pollingTimer);
    pollingTimer = null;
  }
}

function renderTask(task) {
  latestTask = task;
  const model = task.result?.model_info?.analyst_model || "-";
  taskMetaEl.textContent = `任务ID：${task.task_id} | 状态：${labelTaskStatus(task.status)} | 模型：${model} | 更新时间：${formatTime(task.updated_at)}`;

  const nodes = task.nodes || [];
  const runningNode = nodes.find((node) => node.status === "running");
  const executedNodes = nodes.filter((node) => node.status !== "pending");

  if (!selectedNodeKey) {
    selectedNodeKey = runningNode?.key || executedNodes.at(-1)?.key || null;
  } else {
    const selectedNode = nodes.find((node) => node.key === selectedNodeKey);
    if (!selectedNode || selectedNode.status === "pending") {
      selectedNodeKey = runningNode?.key || executedNodes.at(-1)?.key || null;
    }
  }

  renderTimeline(nodes);
  renderNodeDetail(task, selectedNodeKey);
  renderEvidence(task.evidence || []);
  renderScorecard(task.result?.scorecard || {}, task);
  renderClaims(task.result?.claims || [], task.evidence || []);
  renderReport(task.result?.markdown_report || "");
}

function renderTimeline(nodes) {
  timelineEl.innerHTML = "";
  if (!nodes.length) return;

  const stepsEl = document.createElement("div");
  stepsEl.className = "timeline-steps";

  const visitedEnd = getVisitedEndIndex(nodes);
  nodes.forEach((node, index) => {
    const stepEl = document.createElement("div");
    stepEl.className = "timeline-step";
    if (index <= visitedEnd) {
      stepEl.classList.add("visited");
    }

    const dot = document.createElement("button");
    dot.type = "button";
    dot.className = `timeline-dot status-${node.status}`;
    dot.disabled = node.status === "pending";
    dot.title = node.status === "pending" ? "该节点尚未执行" : "点击查看该节点详细日志";
    if (node.key === selectedNodeKey) {
      dot.classList.add("selected");
    }
    if (node.status !== "pending") {
      dot.addEventListener("click", () => {
        selectedNodeKey = node.key;
        renderTimeline(latestTask?.nodes || []);
        renderNodeDetail(latestTask, selectedNodeKey);
      });
    }

    const name = document.createElement("div");
    name.className = "timeline-name";
    name.textContent = node.label;

    const status = document.createElement("div");
    status.className = `timeline-status status-text-${node.status}`;
    status.textContent = labelTaskStatus(node.status);

    stepEl.appendChild(dot);
    stepEl.appendChild(name);
    stepEl.appendChild(status);
    stepsEl.appendChild(stepEl);
  });

  timelineEl.appendChild(stepsEl);
}

function renderNodeDetail(task, nodeKey) {
  if (!task || !nodeKey) {
    nodeDetailEl.classList.add("hidden");
    return;
  }

  const nodes = task.nodes || [];
  const node = nodes.find((item) => item.key === nodeKey);
  if (!node || node.status === "pending") {
    nodeDetailEl.classList.add("hidden");
    return;
  }

  nodeDetailEl.classList.remove("hidden");
  nodeDetailTitleEl.textContent = `${node.label} · ${labelTaskStatus(node.status)}`;

  const summaryParts = [];
  if (node.summary) summaryParts.push(node.summary);
  if (node.started_at) summaryParts.push(`开始：${formatTime(node.started_at)}`);
  if (node.finished_at) summaryParts.push(`结束：${formatTime(node.finished_at)}`);
  if (node.context?.duration_ms) summaryParts.push(`耗时：${node.context.duration_ms}ms`);
  nodeDetailSummaryEl.textContent = summaryParts.join(" | ");

  const nodeContextSummary = buildNodeContextSummary(node.key, node.context || {});
  const contextText = getContextText(node.context || {});
  if (nodeContextSummary || contextText) {
    nodeDetailContextEl.classList.remove("hidden");
    nodeDetailContextEl.innerHTML = `
      ${nodeContextSummary}
      ${contextText ? renderRawContextDetails(contextText, "查看完整节点上下文") : ""}
    `;
  } else {
    nodeDetailContextEl.classList.add("hidden");
    nodeDetailContextEl.innerHTML = "";
  }

  const nodeEvents = (task.events || [])
    .filter((event) => event.node_key === nodeKey)
    .sort((a, b) => new Date(a.at).getTime() - new Date(b.at).getTime());

  nodeEventsEl.innerHTML = "";
  if (!nodeEvents.length) {
    const li = document.createElement("li");
    li.className = "event";
    li.textContent = "该节点暂时没有可展示的日志。";
    nodeEventsEl.appendChild(li);
    return;
  }

  nodeEvents.forEach((event) => {
    const li = document.createElement("li");
    li.className = "event";
    const eventContext = event.context || {};
    const readableContext = buildReadableEventContext(event.stage, eventContext);
    const rawContext = getContextText(eventContext);
    li.innerHTML = `
      <div class="event-head">
        <span>[${formatTime(event.at)}]</span>
        <span class="event-tags">${escapeHtml(labelEventStage(event.stage))}</span>
      </div>
      <div>${escapeHtml(event.message || "")}</div>
      ${readableContext}
      ${rawContext ? renderRawContextDetails(rawContext, "原始上下文") : ""}
    `;
    nodeEventsEl.appendChild(li);
  });
}

function getVisitedEndIndex(nodes) {
  let end = -1;
  for (let index = 0; index < nodes.length; index += 1) {
    const status = nodes[index].status;
    if (status === "completed" || status === "running" || status === "failed") {
      end = index;
    } else {
      break;
    }
  }
  return end;
}

function renderEvidence(evidence) {
  evidenceBodyEl.innerHTML = "";
  if (!evidence.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="6">暂无可展示证据。</td>`;
    evidenceBodyEl.appendChild(tr);
    return;
  }
  evidence.forEach((item) => {
    const tr = document.createElement("tr");
    const sourceHtml = isHttpUrl(item.source_url)
      ? `<a href="${escapeAttr(item.source_url)}" target="_blank" rel="noreferrer">${escapeHtml(item.source_name)}</a>`
      : `${escapeHtml(item.source_name)}<div class="source-note">${escapeHtml(item.source_url)}</div>`;
    tr.innerHTML = `
      <td>${escapeHtml(item.evidence_id)}</td>
      <td>${escapeHtml(item.competitor)}</td>
      <td>${escapeHtml(item.focus_area)}</td>
      <td>${sourceHtml}</td>
      <td>${Number(item.confidence).toFixed(2)}</td>
      <td>${escapeHtml(item.snippet)}</td>
    `;
    evidenceBodyEl.appendChild(tr);
  });
}

function renderScorecard(scorecard, task) {
  const competitors = Object.keys(scorecard);
  if (!competitors.length) {
    scorecardWrapEl.classList.add("hidden");
    scorecardEl.innerHTML = "";
    selectedScoreKey = "";
    return;
  }
  const dimensions = Object.keys(scorecard[competitors[0]] || {});
  const scoringDetails = getScoringDetails(task);
  const validScoreKeys = new Set();
  let table = "<table><thead><tr><th>竞品</th>";
  dimensions.forEach((dimension) => {
    table += `<th>${escapeHtml(dimension)}</th>`;
  });
  table += "</tr></thead><tbody>";
  competitors.forEach((competitor) => {
    table += `<tr><td>${escapeHtml(competitor)}</td>`;
    dimensions.forEach((dimension) => {
      const detail = getScoreDetail(scoringDetails, competitor, dimension);
      const score = scorecard[competitor][dimension] ?? "-";
      const key = buildScoreKey(competitor, dimension);
      validScoreKeys.add(key);
      const buttonClass = detail ? "score-button has-detail" : "score-button";
      table += `
        <td>
          <button
            type="button"
            class="${buttonClass}"
            data-competitor="${escapeAttr(competitor)}"
            data-dimension="${escapeAttr(dimension)}"
            title="${detail ? "查看评分来源、过程与边界" : "暂无详细评分依据"}"
          >${escapeHtml(score)}</button>
        </td>
      `;
    });
    table += "</tr>";
  });
  table += "</tbody></table>";
  if (selectedScoreKey && !validScoreKeys.has(selectedScoreKey)) {
    selectedScoreKey = "";
  }
  const selectedDetail = selectedScoreKey
    ? scoringDetails.find((item) => buildScoreKey(item.competitor, item.focus_area) === selectedScoreKey)
    : null;
  scorecardEl.innerHTML = `
    ${table}
    <div class="scorecard-help">点击任一分数，可查看该分数的来源、计算过程和使用边界。</div>
    ${selectedDetail ? renderScoreExplanation(selectedDetail, task?.evidence || []) : ""}
  `;
  scorecardEl.querySelectorAll(".score-button").forEach((button) => {
    button.addEventListener("click", () => {
      selectedScoreKey = buildScoreKey(button.dataset.competitor || "", button.dataset.dimension || "");
      renderScorecard(latestTask?.result?.scorecard || {}, latestTask);
    });
  });
  scorecardWrapEl.classList.remove("hidden");
}

function getScoringDetails(task) {
  const structurerNode = (task?.nodes || []).find((node) => node.key === "structurer");
  return Array.isArray(structurerNode?.context?.scoring_details)
    ? structurerNode.context.scoring_details
    : [];
}

function getScoreDetail(details, competitor, focusArea) {
  return details.find((item) => item.competitor === competitor && item.focus_area === focusArea);
}

function buildScoreKey(competitor, focusArea) {
  return `${competitor}|||${focusArea}`;
}

function renderScoreExplanation(detail, evidence) {
  const evidenceMap = new Map((evidence || []).map((item) => [item.evidence_id, item]));
  const evidenceIds = Array.isArray(detail.evidence_ids) ? detail.evidence_ids : [];
  const evidenceItems = evidenceIds.map((id) => evidenceMap.get(id)).filter(Boolean);
  const calibration = detail.score_calibration || {};
  const processRows = [
    ["评分方式", labelScoreMethod(detail.method)],
    ["LLM 原始分", detail.base_score ?? detail.score],
    ["最终分", detail.score],
    ["置信度", detail.confidence !== undefined ? Number(detail.confidence).toFixed(2) : ""],
    ["校准", formatScoreCalibration(calibration)],
  ];
  return `
    <div class="score-explainer">
      <div class="score-explainer-head">
        <div>
          <span>评分解释</span>
          <strong>${escapeHtml(detail.competitor || "-")} / ${escapeHtml(detail.focus_area || "-")}</strong>
        </div>
        <strong>${Number(detail.score ?? 0)} 分</strong>
      </div>
      ${renderKeyValueGrid(processRows)}
      <div class="score-section">
        <div class="readable-title">判断理由</div>
        <p>${escapeHtml(detail.reason || "暂无评分理由。")}</p>
      </div>
      <div class="score-section">
        <div class="readable-title">来源证据</div>
        ${renderScoreEvidenceList(evidenceIds, evidenceItems)}
      </div>
      <div class="score-section boundary">
        <div class="readable-title">使用边界</div>
        <p>${escapeHtml(buildScoreBoundary(detail, evidenceItems))}</p>
      </div>
    </div>
  `;
}

function renderScoreEvidenceList(evidenceIds, evidenceItems) {
  if (!evidenceIds.length) {
    return "<p>当前没有绑定证据，分数仅表示中性基线，不建议作为强判断。</p>";
  }
  const rows = evidenceIds.map((id) => {
    const item = evidenceItems.find((candidate) => candidate.evidence_id === id);
    if (!item) {
      return `<li><strong>${escapeHtml(id)}</strong>：证据详情暂不可见。</li>`;
    }
    const source = isHttpUrl(item.source_url)
      ? `<a href="${escapeAttr(item.source_url)}" target="_blank" rel="noreferrer">${escapeHtml(item.source_name || item.source_url)}</a>`
      : `${escapeHtml(item.source_name || "-")}<span>${escapeHtml(item.source_url || "")}</span>`;
    return `
      <li>
        <strong>${escapeHtml(id)}</strong>
        <span>${source}</span>
        <em>${escapeHtml(trimText(item.snippet || "", 180))}</em>
      </li>
    `;
  }).join("");
  return `<ul class="score-evidence-list">${rows}</ul>`;
}

function formatScoreCalibration(calibration) {
  if (!calibration || !Object.keys(calibration).length) return "无";
  const adjustment = Number(calibration.adjustment || 0);
  const spreadAdjustment = Number(calibration.spread_adjustment || 0);
  const total = adjustment + spreadAdjustment;
  if (!total) return "未调整";
  return `${total > 0 ? "+" : ""}${total} 分`;
}

function buildScoreBoundary(detail, evidenceItems) {
  if (detail.method === "insufficient_evidence_baseline") {
    return detail.missing_info || "该维度缺少可验证证据，当前分数不代表真实能力水平。";
  }
  if (detail.missing_info) {
    return detail.missing_info;
  }
  if (!evidenceItems.length) {
    return "评分详情缺少可展示证据，请优先补充公开资料后复评。";
  }
  if (evidenceItems.length < 2) {
    return "当前主要基于 1 条公开证据，适合作为方向性判断；若用于正式决策，建议补充更多来源交叉验证。";
  }
  if (Number(detail.confidence || 0) < 0.68) {
    return "证据能够支撑方向判断，但置信度偏中等，建议结合更多公开材料复核。";
  }
  return "当前证据可支撑该方向性评分，但仍受公开资料粒度限制，不应等同于完整市场审计结论。";
}

function renderClaims(claims, evidence) {
  claimsEl.innerHTML = "";
  if (!claims.length) {
    claimsWrapEl.classList.add("hidden");
    return;
  }
  const evidenceMap = new Map((evidence || []).map((item) => [item.evidence_id, item]));
  claims.forEach((claim) => {
    const li = document.createElement("li");
    li.className = "claim-item";
    li.innerHTML = `
      <div class="claim-title">[${escapeHtml(claim.claim_id)}] ${escapeHtml(claim.title)}</div>
      <p>${escapeHtml(claim.detail)}</p>
      <div class="claim-meta">
        <span>置信度 ${Number(claim.confidence).toFixed(2)}</span>
        <span>${escapeHtml(labelClaimBoundary(claim.confidence, claim.evidence_ids || []))}</span>
      </div>
      ${renderClaimEvidenceTags(claim.evidence_ids || [], evidenceMap)}
    `;
    claimsEl.appendChild(li);
  });
  claimsWrapEl.classList.remove("hidden");
}

function renderClaimEvidenceTags(evidenceIds, evidenceMap) {
  if (!evidenceIds.length) {
    return `<div class="claim-evidence empty">暂无绑定证据，当前结论不建议作为强判断。</div>`;
  }
  const tags = evidenceIds.map((id) => {
    const item = evidenceMap.get(id);
    const title = item ? `${item.competitor} / ${item.focus_area}：${trimText(item.snippet || "", 120)}` : "证据详情暂不可见";
    return `<span title="${escapeAttr(title)}">${escapeHtml(id)}</span>`;
  }).join("");
  return `<div class="claim-evidence">${tags}</div>`;
}

function labelClaimBoundary(confidence, evidenceIds) {
  if (!evidenceIds.length) return "边界：缺少可追溯证据";
  if (Number(confidence || 0) < 0.68) return "边界：方向可参考，建议补充资料复核";
  if (evidenceIds.length < 2) return "边界：主要由少量证据支撑";
  return "边界：证据可追溯，仍需结合业务判断";
}

function setLoading(isLoading) {
  submitBtn.disabled = isLoading;
  submitBtn.textContent = isLoading ? "分析中..." : "开始分析";
}

function setMessage(text, isError = false) {
  messageEl.textContent = text;
  messageEl.style.color = isError ? "#b33030" : "#405172";
}

function labelTaskStatus(status) {
  if (status === "running") return "执行中";
  if (status === "completed") return "已完成";
  if (status === "failed") return "失败";
  return "待执行";
}

function formatTime(isoTime) {
  if (!isoTime) return "-";
  const date = new Date(isoTime);
  return date.toLocaleString("zh-CN", { hour12: false });
}

function getContextText(context) {
  if (!context || !Object.keys(context).length) return "";
  return formatContextValue(context);
}

function buildNodeContextSummary(nodeKey, context) {
  if (!context || !Object.keys(context).length) return "";
  if (nodeKey === "collector") {
    const rows = [
      ["采集模式", labelContextValue(context.intake_mode || context.mode)],
      ["搜索方式", labelContextValue(context.search_provider)],
      ["资料数量", context.document_count],
      ["证据数量", context.evidence_count],
      ["覆盖状态", context.coverage_complete === true ? "已覆盖全部维度" : context.coverage_complete === false ? "仍有维度缺证据" : ""],
    ].filter(([, value]) => value !== undefined && value !== "");
    const coverage = Array.isArray(context.coverage) ? context.coverage : [];
    return `
      ${renderKeyValueGrid(rows)}
      ${coverage.length ? renderCoverageMatrix(coverage) : ""}
    `;
  }
  if (nodeKey === "structurer") {
    const details = Array.isArray(context.scoring_details) ? context.scoring_details : [];
    const rows = [
      ["证据数量", context.evidence_count],
      ["评分方式", details.length ? summarizeScoringMethods(details) : ""],
      ["评分格数", details.length || ""],
    ].filter(([, value]) => value !== undefined && value !== "");
    return `
      ${renderKeyValueGrid(rows)}
      ${details.length ? renderScoringSummary(details) : ""}
    `;
  }
  if (nodeKey === "planner") {
    const planPreview = Array.isArray(context.plan_preview) ? context.plan_preview : [];
    return renderKeyValueGrid([
      ["分析行业", context.industry],
      ["分析维度", formatList(context.focus_areas)],
      ["计划预览", formatList(planPreview)],
    ]);
  }
  if (nodeKey === "analyst") {
    return renderKeyValueGrid([
      ["模型", context.model],
      ["证据数量", context.evidence_count],
      ["结论 ID", formatList(context.claim_ids)],
    ]);
  }
  if (nodeKey === "reviewer") {
    const semanticReview = Array.isArray(context.semantic_review) ? context.semantic_review : [];
    const notes = Array.isArray(context.notes_preview) ? context.notes_preview : [];
    return `
      ${renderKeyValueGrid([
      ["结论数量", context.claim_count],
      ["LLM 审查", context.semantic_review_count ? `${context.semantic_review_count} 条` : ""],
      ["审查意见", formatList(notes)],
    ])}
      ${semanticReview.length ? renderReviewerSummary(semanticReview) : ""}
    `;
  }
  if (nodeKey === "reporter") {
    return renderKeyValueGrid([
      ["报告模式", labelReportMode(context.report_mode)],
      ["报告长度", context.report_length],
      ["引用证据", formatList(context.evidence_ids)],
    ]);
  }
  return "";
}

function buildReadableEventContext(stage, context) {
  if (!context || !Object.keys(context).length) return "";
  if (stage === "web_search_queries_planned") {
    const queries = Array.isArray(context.queries) ? context.queries : [];
    return renderReadableBlock("本轮搜索计划", [
      `搜索服务：${labelContextValue(context.search_provider) || "-"}`,
      `计划查询：${queries.length} 条${queries.length >= 12 ? "（仅展示前 12 条）" : ""}`,
    ], queries.map((item) => `${item.competitor || "-"} / ${item.focus_area || "-"}：${item.query || "-"}`));
  }
  if (stage === "web_search_results_filtered") {
    const accepted = Array.isArray(context.accepted) ? context.accepted : [];
    const rejected = Array.isArray(context.rejected) ? context.rejected : [];
    return renderReadableBlock("搜索结果筛选", [
      `目标：${context.competitor || "-"} / ${context.focus_area || "-"}`,
      `采纳：${accepted.length} 条，过滤：${rejected.length} 条`,
      `实际查询：${context.provider_query || context.query || "-"}`,
    ], accepted.map((item) => `${item.title || item.url || "-"}${item.url ? `（${item.url}）` : ""}`));
  }
  if (stage === "page_reader_success") {
    return renderReadableBlock("资料读取结果", [
      `目标：${context.competitor || "-"} / ${context.focus_area || "-"}`,
      `来源：${context.title || "-"}`,
      `正文长度：${context.text_length || 0} 字符`,
      `内容来源：${labelContextValue(context.content_source) || "网页正文"}`,
    ], [context.url].filter(Boolean));
  }
  if (stage === "public_materials_ready") {
    const preview = Array.isArray(context.document_preview) ? context.document_preview : [];
    return renderReadableBlock("采集汇总", [
      `资料数量：${context.document_count || 0}`,
      `规划方式：${labelContextValue(context.planning_mode) || "-"}`,
      `采集方式：${labelContextValue(context.intake_mode) || "-"}`,
    ], preview.map((item) => `${item.competitor || "-"} / ${item.focus_area || "-"}：${item.title || item.source || "-"}`));
  }
  if (stage === "evidence_supplement") {
    const before = Array.isArray(context.missing_before) ? context.missing_before : [];
    const after = Array.isArray(context.missing_after) ? context.missing_after : [];
    return renderReadableBlock("证据补充", [
      `补充前缺口：${before.length} 个`,
      `补充后缺口：${after.length} 个`,
      `当前证据：${context.evidence_count || 0} 条`,
    ], after.map((item) => `仍缺：${item.competitor || "-"} / ${item.focus_area || "-"}`));
  }
  if (stage === "llm_request") {
    return renderKeyValueGrid([
      ["模型", context.model],
      ["证据数量", context.evidence_count],
      ["结论数量", context.claim_count],
      ["建议数量", context.recommendation_count],
      ["输入大小", context.payload_size ? `${context.payload_size} 字符` : ""],
      ["执行策略", context.policy],
    ]);
  }
  if (stage === "llm_response") {
    return renderKeyValueGrid([
      ["模型", context.model],
      ["耗时", context.latency_ms ? `${context.latency_ms}ms` : ""],
      ["Token", context.total_tokens],
      ["响应预览", context.content_preview ? trimText(context.content_preview, 260) : ""],
    ]);
  }
  return "";
}

function renderKeyValueGrid(rows) {
  const validRows = rows.filter(([, value]) => value !== undefined && value !== null && value !== "");
  if (!validRows.length) return "";
  return `
    <div class="context-grid">
      ${validRows.map(([label, value]) => `
        <div class="context-card">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(formatDisplayValue(value))}</strong>
        </div>
      `).join("")}
    </div>
  `;
}

function renderCoverageMatrix(coverage) {
  return `
    <div class="coverage-matrix">
      <div class="readable-title">证据覆盖</div>
      ${coverage.map((item) => `
        <div class="coverage-item ${Number(item.evidence_count || 0) > 0 ? "covered" : "missing"}">
          <span>${escapeHtml(item.competitor || "-")}</span>
          <strong>${escapeHtml(item.focus_area || "-")}</strong>
          <em>${Number(item.evidence_count || 0) > 0 ? `${Number(item.evidence_count)} 条证据` : "待补充"}</em>
        </div>
      `).join("")}
    </div>
  `;
}

function renderScoringSummary(details) {
  return `
    <div class="readable-list">
      <div class="readable-title">评分依据</div>
      ${details.slice(0, 12).map((item) => `
        <div class="readable-row">
          <span>${escapeHtml(item.competitor || "-")} / ${escapeHtml(item.focus_area || "-")}</span>
          <strong>${Number(item.score ?? 0)} 分</strong>
          <em>${escapeHtml((item.evidence_ids || []).join(", ") || "无证据")}</em>
        </div>
      `).join("")}
    </div>
  `;
}

function renderReviewerSummary(details) {
  return `
    <div class="readable-list">
      <div class="readable-title">语义审查</div>
      ${details.slice(0, 8).map((item) => `
        <div class="readable-row">
          <span>${escapeHtml(item.claim_id || "-")} · ${escapeHtml(labelReviewVerdict(item.verdict))}</span>
          <strong>${Number(item.confidence ?? 0).toFixed(2)}</strong>
          <em>${escapeHtml(trimText(item.reason || "", 120))}</em>
        </div>
      `).join("")}
    </div>
  `;
}

function renderReadableBlock(title, facts, items = []) {
  const factRows = facts.filter(Boolean).map((fact) => `<span>${escapeHtml(fact)}</span>`).join("");
  const itemRows = items.filter(Boolean).slice(0, 8).map((item) => `<li>${escapeHtml(trimText(item, 180))}</li>`).join("");
  return `
    <div class="readable-block">
      <div class="readable-title">${escapeHtml(title)}</div>
      ${factRows ? `<div class="readable-facts">${factRows}</div>` : ""}
      ${itemRows ? `<ul>${itemRows}</ul>` : ""}
    </div>
  `;
}

function renderRawContextDetails(contextText, title) {
  return `
    <details class="raw-context">
      <summary>${escapeHtml(title)}</summary>
      <pre>${escapeHtml(contextText)}</pre>
    </details>
  `;
}

function labelEventStage(stage) {
  const labels = {
    node_start: "节点开始",
    node_finish: "节点完成",
    llm_request: "请求模型",
    llm_response: "模型响应",
    public_source_planning_request: "规划资料",
    public_source_planning_response: "规划完成",
    public_source_planning_fallback: "规划兜底",
    web_search_queries_planned: "搜索计划",
    web_search_results_filtered: "结果筛选",
    page_reader_success: "资料读取",
    page_reader_filtered: "资料过滤",
    page_reader_failed: "读取失败",
    page_reader_error: "读取异常",
    public_materials_ready: "采集完成",
    evidence_supplement: "证据补充",
    llm_fallback: "模型兜底",
    task_started: "任务开始",
    task_completed: "任务完成",
  };
  return labels[stage] || stage || "进度";
}

function labelReviewVerdict(verdict) {
  const labels = {
    pass: "通过",
    revise: "已保守改写",
    reject: "证据不足",
  };
  return labels[verdict] || verdict || "未标记";
}

function summarizeScoringMethods(details) {
  const methods = new Set(details.map((item) => item.method).filter(Boolean));
  if (methods.has("llm_evidence_based") && methods.size === 1) return "LLM 基于证据评分";
  if (methods.has("transparent_confidence_fallback")) return "部分使用证据兜底评分";
  if (methods.has("insufficient_evidence_baseline")) return "存在证据不足基线分";
  return Array.from(methods).join(", ");
}

function labelScoreMethod(method) {
  const labels = {
    llm_evidence_based: "LLM 基于证据评分",
    transparent_confidence_fallback: "证据兜底评分",
    insufficient_evidence_baseline: "证据不足基线分",
  };
  return labels[method] || method || "未标记";
}

function labelReportMode(mode) {
  const labels = {
    llm_polished: "LLM 润色报告",
    deterministic_template: "模板兜底报告",
  };
  return labels[mode] || mode || "";
}

function labelContextValue(value) {
  const labels = {
    tavily: "Tavily 公开搜索",
    controlled_search: "受控公开搜索",
    public_info_intake: "公共信息采集",
    search_public_sources: "搜索公开来源",
    user_materials_plus_search: "用户资料 + 搜索补充",
    user_public_materials: "用户提供资料",
    llm_public_source_planning: "LLM 规划公开来源",
    fallback_public_source_planning: "保守资料清单",
    tavily_raw_content: "Tavily 正文",
  };
  return labels[value] || value || "";
}

function formatList(value) {
  if (!Array.isArray(value)) return value || "";
  return value.filter(Boolean).join("、");
}

function formatDisplayValue(value) {
  if (Array.isArray(value)) return formatList(value);
  if (typeof value === "object") return JSON.stringify(value);
  return value;
}

function trimText(value, maxLength) {
  const text = String(value || "").replaceAll("\\n", "\n").replace(/\s+/g, " ").trim();
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength)}...`;
}

function renderFocusAreaTags() {
  const wrap = document.getElementById("focusAreas");
  wrap.innerHTML = "";
  FOCUS_AREA_OPTIONS.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "tag-pill";
    button.textContent = item;
    if (selectedFocusAreas.has(item)) {
      button.classList.add("active");
    }
    button.setAttribute("aria-pressed", String(selectedFocusAreas.has(item)));
    button.addEventListener("click", () => {
      if (selectedFocusAreas.has(item)) {
        selectedFocusAreas.delete(item);
      } else {
        selectedFocusAreas.add(item);
      }
      if (!selectedFocusAreas.size) {
        selectedFocusAreas.add(item);
      }
      renderFocusAreaTags();
    });
    wrap.appendChild(button);
  });
}

function setFocusAreas(values) {
  const mapped = values.filter((item) => FOCUS_AREA_OPTIONS.includes(item));
  selectedFocusAreas = new Set(mapped.length ? mapped : [FOCUS_AREA_OPTIONS[0]]);
  renderFocusAreaTags();
}

function formatContextValue(value, depth = 0) {
  const indent = "  ".repeat(depth);
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) {
    if (!value.length) return "[]";
    return value
      .map((item) => {
        const inner = formatContextValue(item, depth + 1);
        return `${indent}- ${inner.replaceAll("\n", `\n${indent}  `)}`;
      })
      .join("\n");
  }
  if (typeof value === "object") {
    const entries = Object.entries(value);
    if (!entries.length) return "{}";
    return entries
      .map(([key, item]) => {
        const inner = formatContextValue(item, depth + 1);
        if (inner.includes("\n")) {
          return `${indent}${key}:\n${inner}`;
        }
        return `${indent}${key}: ${inner}`;
      })
      .join("\n");
  }
  if (typeof value === "string") {
    return value.replaceAll("\\n", "\n").replaceAll("\r\n", "\n");
  }
  return String(value);
}

function renderReport(markdownText) {
  if (!markdownText || !markdownText.trim()) {
    reportEl.textContent = "任务完成后展示。";
    return;
  }
  reportEl.innerHTML = markdownToHtml(markdownText);
}

function markdownToHtml(markdownText) {
  const lines = markdownText.replaceAll("\r\n", "\n").split("\n");
  const html = [];
  let index = 0;

  while (index < lines.length) {
    const rawLine = lines[index];
    const line = rawLine.trim();

    if (!line) {
      index += 1;
      continue;
    }

    const heading = line.match(/^(#{1,6})\s+(.*)$/);
    if (heading) {
      const level = heading[1].length;
      html.push(`<h${level}>${applyInlineMarkdown(heading[2])}</h${level}>`);
      index += 1;
      continue;
    }

    if (line.startsWith("|") && line.endsWith("|")) {
      const nextLine = index + 1 < lines.length ? lines[index + 1].trim() : "";
      const hasSeparator = /^\|?[\s:\-|]+\|?$/.test(nextLine);
      const hasPipeRows = nextLine.startsWith("|");
      if (hasSeparator || hasPipeRows) {
      const headerCells = parseTableRow(line);
      index += hasSeparator ? 2 : 1;
      const rows = [];
      while (index < lines.length) {
        const rowLine = lines[index].trim();
        if (!rowLine.startsWith("|")) {
          break;
        }
        if (/^\|?[\s:\-|]+\|?$/.test(rowLine)) {
          index += 1;
          continue;
        }
        rows.push(parseTableRow(rowLine));
        index += 1;
      }
      html.push(buildTableHtml(headerCells, rows));
      continue;
      }
    }

    if (line.startsWith("- ")) {
      const items = [];
      while (index < lines.length && lines[index].trim().startsWith("- ")) {
        items.push(lines[index].trim().slice(2));
        index += 1;
      }
      html.push(
        `<ul>${items.map((item) => `<li>${applyInlineMarkdown(item)}</li>`).join("")}</ul>`
      );
      continue;
    }

    html.push(`<p>${applyInlineMarkdown(line)}</p>`);
    index += 1;
  }

  return html.join("");
}

function parseTableRow(line) {
  return line
    .trim()
    .replace(/^\|/, "")
    .replace(/\|$/, "")
    .split("|")
    .map((cell) => cell.trim());
}

function buildTableHtml(headers, rows) {
  const thead = `<thead><tr>${headers.map((cell) => `<th>${applyInlineMarkdown(cell)}</th>`).join("")}</tr></thead>`;
  const tbodyRows = rows
    .map(
      (row) =>
        `<tr>${headers.map((_, idx) => `<td>${applyInlineMarkdown(row[idx] || "")}</td>`).join("")}</tr>`
    )
    .join("");
  return `<table class="report-table">${thead}<tbody>${tbodyRows}</tbody></table>`;
}

function applyInlineMarkdown(text) {
  let html = escapeHtml(text);
  html = html.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
  return html;
}

function exportReportPdf() {
  const reportMarkdown = latestTask?.result?.markdown_report || "";
  if (!reportMarkdown.trim()) {
    setMessage("当前没有可导出的报告。", true);
    return;
  }

  const popup = window.open("", "_blank", "width=1080,height=900");
  if (!popup) {
    setMessage("无法打开导出窗口，请检查浏览器弹窗设置。", true);
    return;
  }

  const title = latestTask?.input?.project_name || "RivalFlow Report";
  const html = `
    <!doctype html>
    <html lang="zh-CN">
      <head>
        <meta charset="UTF-8" />
        <title>${escapeHtml(title)}</title>
        <style>
          body { font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", Arial, sans-serif; margin: 28px; color: #1f2937; }
          h1,h2,h3,h4 { margin: 16px 0 10px; }
          p { margin: 8px 0; line-height: 1.7; }
          ul { margin: 8px 0 12px 18px; line-height: 1.7; }
          table { width: 100%; border-collapse: collapse; margin: 12px 0; }
          th,td { border: 1px solid #d1d9e8; padding: 8px; text-align: left; font-size: 13px; }
          th { background: #f5f7fb; }
        </style>
      </head>
      <body>
        ${markdownToHtml(reportMarkdown)}
      </body>
    </html>
  `;

  popup.document.open();
  popup.document.write(html);
  popup.document.close();
  popup.focus();
  setTimeout(() => {
    popup.print();
  }, 300);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttr(value) {
  return escapeHtml(value).replaceAll("`", "");
}

function isHttpUrl(value) {
  return /^https?:\/\//i.test(String(value || ""));
}
