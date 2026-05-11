const form = document.getElementById("task-form");
const demoBtn = document.getElementById("demoBtn");
const submitBtn = document.getElementById("submitBtn");
const messageEl = document.getElementById("message");
const taskMetaEl = document.getElementById("taskMeta");
const nodesEl = document.getElementById("nodes");
const eventsEl = document.getElementById("events");
const evidenceBodyEl = document.querySelector("#evidenceTable tbody");
const reportEl = document.getElementById("report");
const scorecardEl = document.getElementById("scorecard");
const claimsEl = document.getElementById("claims");
const scorecardWrapEl = document.getElementById("scorecardWrap");
const claimsWrapEl = document.getElementById("claimsWrap");

let pollingTimer = null;

demoBtn.addEventListener("click", () => {
  document.getElementById("projectName").value = "短视频电商竞品协作分析 Demo";
  document.getElementById("industry").value = "内容电商";
  document.getElementById("competitors").value = "平台A,平台B,平台C";
  document.getElementById("focusAreas").value = "产品定位,用户体验,商业化能力,增长策略";
  document.getElementById("timeRange").value = "近 12 个月";
  document.getElementById("sourceUrls").value = "";
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const payload = buildPayload();
  if (!payload.competitors.length) {
    setMessage("请至少填写一个竞品名称。", true);
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
      const errorText = await response.text();
      throw new Error(errorText || "任务创建失败");
    }
    const task = await response.json();
    setMessage(`任务已创建：${task.task_id}`);
    renderTask(task);
    startPolling(task.task_id);
  } catch (error) {
    setMessage(`提交失败：${error.message}`, true);
    setLoading(false);
  }
});

function buildPayload() {
  const toList = (value) =>
    value
      .split(/[,\n]/)
      .map((item) => item.trim())
      .filter(Boolean);
  return {
    project_name: document.getElementById("projectName").value.trim() || "字节竞品分析 Demo",
    industry: document.getElementById("industry").value.trim(),
    competitors: toList(document.getElementById("competitors").value),
    focus_areas: toList(document.getElementById("focusAreas").value),
    source_urls: toList(document.getElementById("sourceUrls").value),
    time_range: document.getElementById("timeRange").value.trim() || "近 12 个月",
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
      }
      if (task.status === "failed") {
        setMessage(`任务失败：${task.error_message || "未知错误"}`, true);
        setLoading(false);
        stopPolling();
      }
    } catch (error) {
      setMessage(`轮询异常：${error.message}`, true);
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
  taskMetaEl.textContent = `任务ID：${task.task_id} | 状态：${labelTaskStatus(task.status)} | 更新时间：${formatTime(task.updated_at)}`;
  renderNodes(task.nodes || []);
  renderEvents(task.events || []);
  renderEvidence(task.evidence || []);
  renderScorecard(task.result?.scorecard || {});
  renderClaims(task.result?.claims || []);
  reportEl.textContent = task.result?.markdown_report || "任务完成后展示。";
}

function renderNodes(nodes) {
  nodesEl.innerHTML = "";
  nodes.forEach((node) => {
    const li = document.createElement("li");
    li.className = "node";
    li.innerHTML = `
      <div class="node-title">
        <span>${escapeHtml(node.label)}</span>
        <span class="node-status status-${node.status}">${labelTaskStatus(node.status)}</span>
      </div>
      <div class="node-summary">${escapeHtml(node.summary || "")}</div>
    `;
    nodesEl.appendChild(li);
  });
}

function renderEvents(events) {
  const ordered = [...events].reverse().slice(0, 12);
  eventsEl.innerHTML = "";
  ordered.forEach((event) => {
    const li = document.createElement("li");
    li.className = "event";
    li.textContent = `[${formatTime(event.at)}] ${event.message}`;
    eventsEl.appendChild(li);
  });
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
    const li = document.createElement("li");
    li.textContent = `${claim.title}：${claim.detail}（置信度 ${Number(claim.confidence).toFixed(2)}）`;
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
