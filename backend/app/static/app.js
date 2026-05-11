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

renderFocusAreaTags();
exportPdfBtn.addEventListener("click", exportReportPdf);

demoBtn.addEventListener("click", () => {
  document.getElementById("projectName").value = "2026 短视频电商商业化策略对比（抖音 vs 快手 vs 小红书）";
  document.getElementById("industry").value = "内容电商";
  document.getElementById("competitors").value = "抖音,快手,小红书";
  document.getElementById("sourceUrls").value = "https://www.douyin.com,https://www.kuaishou.com,https://www.xiaohongshu.com";
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

  const focusAreas = Array.from(selectedFocusAreas);
  return {
    project_name: document.getElementById("projectName").value.trim() || "RivalFlow 竞品分析 Demo",
    industry: document.getElementById("industry").value.trim(),
    competitors: toList(document.getElementById("competitors").value),
    focus_areas: focusAreas,
    source_urls: toList(document.getElementById("sourceUrls").value),
    time_range: document.getElementById("timeRange").value,
  };
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
  renderScorecard(task.result?.scorecard || {});
  renderClaims(task.result?.claims || []);
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

  const contextText = getContextText(node.context || {});
  if (contextText) {
    nodeDetailContextEl.classList.remove("hidden");
    nodeDetailContextEl.textContent = contextText;
  } else {
    nodeDetailContextEl.classList.add("hidden");
    nodeDetailContextEl.textContent = "";
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
    const context = getContextText(event.context || {});
    li.innerHTML = `
      <div class="event-head">
        <span>[${formatTime(event.at)}]</span>
        <span class="event-tags">${escapeHtml(event.stage || "progress")}</span>
      </div>
      <div>${escapeHtml(event.message || "")}</div>
      ${context ? `<pre class="event-context">${escapeHtml(context)}</pre>` : ""}
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
  evidence.forEach((item) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(item.evidence_id)}</td>
      <td>${escapeHtml(item.competitor)}</td>
      <td>${escapeHtml(item.focus_area)}</td>
      <td><a href="${escapeAttr(item.source_url)}" target="_blank" rel="noreferrer">${escapeHtml(item.source_name)}</a></td>
      <td>${Number(item.confidence).toFixed(2)}</td>
      <td>${escapeHtml(item.snippet)}</td>
    `;
    evidenceBodyEl.appendChild(tr);
  });
}

function renderScorecard(scorecard) {
  const competitors = Object.keys(scorecard);
  if (!competitors.length) {
    scorecardWrapEl.classList.add("hidden");
    scorecardEl.innerHTML = "";
    return;
  }
  const dimensions = Object.keys(scorecard[competitors[0]] || {});
  let table = "<table><thead><tr><th>竞品</th>";
  dimensions.forEach((dimension) => {
    table += `<th>${escapeHtml(dimension)}</th>`;
  });
  table += "</tr></thead><tbody>";
  competitors.forEach((competitor) => {
    table += `<tr><td>${escapeHtml(competitor)}</td>`;
    dimensions.forEach((dimension) => {
      table += `<td>${scorecard[competitor][dimension] ?? "-"}</td>`;
    });
    table += "</tr>";
  });
  table += "</tbody></table>";
  scorecardEl.innerHTML = table;
  scorecardWrapEl.classList.remove("hidden");
}

function renderClaims(claims) {
  claimsEl.innerHTML = "";
  if (!claims.length) {
    claimsWrapEl.classList.add("hidden");
    return;
  }
  claims.forEach((claim) => {
    const evidenceText = (claim.evidence_ids || []).join(", ");
    const li = document.createElement("li");
    li.className = "claim-item";
    li.textContent = `[${claim.claim_id}] ${claim.title}：${claim.detail}（置信度 ${Number(claim.confidence).toFixed(2)}，证据ID: ${evidenceText}）`;
    claimsEl.appendChild(li);
  });
  claimsWrapEl.classList.remove("hidden");
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
