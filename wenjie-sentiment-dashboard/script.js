const DEFAULT_DATA_URL = "./data/wenjie-sentiment-events.json";
const EMBEDDED_DATA = window.__WENJIE_DATA__ || null;
const query = new URLSearchParams(window.location.search);
const ACTIVE_DATA_URL = query.get("data") || DEFAULT_DATA_URL;
const USING_EMBEDDED_DATA = Boolean(EMBEDDED_DATA && !query.get("data"));

const FILTERS = {
  range: [
    { value: "all", label: "全部" },
    { value: "24h", label: "24h" },
    { value: "7d", label: "7d" },
    { value: "30d", label: "30d" }
  ],
  entity: [
    { value: "all", label: "全部主体" },
    { value: "brand", label: "品牌" },
    { value: "M5", label: "M5" },
    { value: "M6", label: "M6" },
    { value: "M7", label: "M7" },
    { value: "M8", label: "M8" },
    { value: "M9", label: "M9" }
  ],
  sourceType: [
    { value: "all", label: "全部来源" },
    { value: "official", label: "官方" },
    { value: "media", label: "媒体" }
  ],
  risk: [
    { value: "all", label: "全部风险" },
    { value: "high", label: "高" },
    { value: "medium", label: "中" },
    { value: "low", label: "低" }
  ]
};

const QUERY_ALIASES = {
  range: ["time", "range"],
  entity: ["entity"],
  sourceType: ["source", "sourceType"],
  risk: ["risk"]
};

const LABELS = {
  entity: {
    all: "全部主体",
    brand: "品牌",
    M5: "M5",
    M6: "M6",
    M7: "M7",
    M8: "M8",
    M9: "M9"
  },
  risk: { high: "高风险", medium: "中风险", low: "低风险" },
  sentiment: { positive: "正向", negative: "负向", mixed: "混合", neutral: "中性" },
  sourceType: { official: "官方", media: "媒体" },
  status: {
    archived: "已归档",
    watch: "优先复核",
    active: "高优先级跟进",
    tracking: "跟踪中",
    signal: "正向信号"
  }
};

const state = {
  range: normalizeRange(pickQueryValue(QUERY_ALIASES.range)),
  entity: normalizeEntity(pickQueryValue(QUERY_ALIASES.entity)),
  sourceType: normalizeSourceType(pickQueryValue(QUERY_ALIASES.sourceType)),
  risk: normalizeRisk(pickQueryValue(QUERY_ALIASES.risk))
};

let dataset = null;
let embeddedDownloadUrl = null;

const elements = {
  summaryStrip: document.getElementById("summaryStrip"),
  filterGroups: document.getElementById("filterGroups"),
  eventList: document.getElementById("eventList"),
  listMeta: document.getElementById("listMeta"),
  policyNote: document.getElementById("policyNote"),
  entityHeatmap: document.getElementById("entityHeatmap"),
  keywordCloud: document.getElementById("keywordCloud"),
  watchList: document.getElementById("watchList"),
  metaUpdatedAt: document.getElementById("metaUpdatedAt"),
  metaFrequency: document.getElementById("metaFrequency"),
  metaVersion: document.getElementById("metaVersion"),
  metaPolicy: document.getElementById("metaPolicy"),
  dataSourceNote: document.getElementById("dataSourceNote"),
  copyShareButton: document.getElementById("copyShareButton"),
  downloadDataLink: document.getElementById("downloadDataLink"),
  template: document.getElementById("eventCardTemplate")
};

async function loadData() {
  renderLoading();

  try {
    if (USING_EMBEDDED_DATA) {
      dataset = normalizeDataset(EMBEDDED_DATA);
      render();
      return;
    }

    const response = await fetch(ACTIVE_DATA_URL, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const raw = await response.json();
    dataset = normalizeDataset(raw);
    render();
  } catch (error) {
    renderError(error);
  }
}

function pickQueryValue(keys) {
  for (const key of keys) {
    const value = query.get(key);
    if (value) {
      return value;
    }
  }

  return null;
}

function normalizeRange(value) {
  return FILTERS.range.some((item) => item.value === value) ? value : "all";
}

function normalizeSourceType(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return FILTERS.sourceType.some((item) => item.value === normalized) ? normalized : "all";
}

function normalizeRisk(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return FILTERS.risk.some((item) => item.value === normalized) ? normalized : "all";
}

function normalizeEntity(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized || normalized === "all") {
    return "all";
  }

  if (normalized === "brand") {
    return "brand";
  }

  const match = normalized.match(/^m([5-9])$/);
  if (match) {
    return `M${match[1]}`;
  }

  return "all";
}

function serializeEntity(value) {
  return value === "brand" ? "brand" : value.toLowerCase();
}

function normalizeDataset(raw) {
  const normalizedEvents = (raw.events || [])
    .map((event) => ({
      ...event,
      entity: normalizeEntity(event.entity),
      keywords: Array.isArray(event.keywords) ? event.keywords : [],
      sourceType: normalizeSourceType(event.sourceType),
      riskLevel: normalizeRisk(event.riskLevel),
      sentiment: LABELS.sentiment[event.sentiment] ? event.sentiment : "neutral",
      publishedAt: event.publishedAt || null,
      capturedAt: event.capturedAt || null,
      effectiveAt: event.publishedAt || event.capturedAt || null
    }))
    .filter((event) => event.entity !== "all")
    .sort((left, right) => {
      const leftTime = left.effectiveAt ? new Date(left.effectiveAt).getTime() : 0;
      const rightTime = right.effectiveAt ? new Date(right.effectiveAt).getTime() : 0;
      return rightTime - leftTime;
    });

  return {
    meta: raw.meta || {},
    events: normalizedEvents
  };
}

function applyFilters(events) {
  const now = getReferenceTime();

  return events.filter((event) => {
    if (state.entity !== "all" && event.entity !== state.entity) {
      return false;
    }

    if (state.sourceType !== "all" && event.sourceType !== state.sourceType) {
      return false;
    }

    if (state.risk !== "all" && event.riskLevel !== state.risk) {
      return false;
    }

    return isEventInRange(event, state.range, now);
  });
}

function getReferenceTime() {
  const updatedAt = dataset?.meta?.updatedAt ? new Date(dataset.meta.updatedAt) : new Date();
  return Number.isNaN(updatedAt.getTime()) ? new Date() : updatedAt;
}

function isEventInRange(event, range, now) {
  if (range === "all") {
    return true;
  }

  if (!event.effectiveAt) {
    return false;
  }

  const eventTime = new Date(event.effectiveAt).getTime();
  if (Number.isNaN(eventTime)) {
    return false;
  }

  const hours = (now.getTime() - eventTime) / (1000 * 60 * 60);
  if (range === "24h") {
    return hours <= 24;
  }

  if (range === "7d") {
    return hours <= 24 * 7;
  }

  if (range === "30d") {
    return hours <= 24 * 30;
  }

  return true;
}

function render() {
  if (!dataset) {
    return;
  }

  const filteredEvents = applyFilters(dataset.events);

  syncQuery();
  renderMeta(dataset.meta);
  renderFilters();
  renderSummary(filteredEvents);
  renderListMeta(filteredEvents);
  renderEvents(filteredEvents);
  renderSidebar(filteredEvents);
}

function renderMeta(meta) {
  elements.metaUpdatedAt.textContent = formatDate(meta.updatedAt);
  elements.metaFrequency.textContent = meta.updateFrequency || "未设置";
  elements.metaVersion.textContent = meta.version || "未设置";
  elements.metaPolicy.textContent = meta.sourcePolicy || "未设置";

  if (USING_EMBEDDED_DATA) {
    elements.dataSourceNote.textContent = "当前数据源：内嵌单文件数据";

    if (embeddedDownloadUrl) {
      URL.revokeObjectURL(embeddedDownloadUrl);
    }

    embeddedDownloadUrl = URL.createObjectURL(
      new Blob([JSON.stringify(dataset, null, 2)], { type: "application/json" })
    );
    elements.downloadDataLink.href = embeddedDownloadUrl;
    elements.downloadDataLink.download = "wenjie-sentiment-events.json";
  } else {
    const sourceLabel = ACTIVE_DATA_URL === DEFAULT_DATA_URL ? "本地 events.json" : ACTIVE_DATA_URL;
    elements.dataSourceNote.textContent = `当前数据源：${sourceLabel}`;
    elements.downloadDataLink.href = ACTIVE_DATA_URL;
    elements.downloadDataLink.download = "";
  }

  elements.policyNote.innerHTML = `
    <p>${escapeHtml(meta.scope || "未设置监测范围")}</p>
    <ul class="plain-list">
      <li>每条记录必须能直达具体正文页、具体声明页或单篇活动页，不接受首页、搜索页、栏目页充当事件源。</li>
      <li>来源判断以发布主体为准，明确区分 sourceType、sourceTier、sourceName 与 publisher。</li>
      <li>发布时间优先取正文页披露时间；正文页未披露时，保留抓取时间并在追溯说明中标明。</li>
      <li>风险等级用于帮助业务排优先级，不等于最终定性，仍需结合公关、客服、法务进一步复核。</li>
    </ul>
  `;
}

function renderFilters() {
  elements.filterGroups.innerHTML = "";

  const groups = [
    { key: "range", label: "时间范围", options: FILTERS.range },
    { key: "entity", label: "主体", options: FILTERS.entity },
    { key: "sourceType", label: "来源类型", options: FILTERS.sourceType },
    { key: "risk", label: "风险等级", options: FILTERS.risk }
  ];

  groups.forEach((group) => {
    const wrapper = document.createElement("div");
    wrapper.className = "filter-group";

    const label = document.createElement("div");
    label.className = "filter-label";
    label.textContent = group.label;

    const options = document.createElement("div");
    options.className = "filter-options";

    group.options.forEach((option) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `filter-button${state[group.key] === option.value ? " is-active" : ""}`;
      button.dataset.filterKey = group.key;
      button.dataset.filterValue = option.value;
      button.textContent = option.label;
      options.appendChild(button);
    });

    wrapper.append(label, options);
    elements.filterGroups.appendChild(wrapper);
  });
}

function renderSummary(events) {
  const officialCount = events.filter((event) => event.sourceType === "official").length;
  const highRiskCount = events.filter((event) => event.riskLevel === "high").length;
  const negativeCount = events.filter((event) => event.sentiment === "negative").length;
  const recentSevenDayCount = events.filter((event) => isEventInRange(event, "7d", getReferenceTime())).length;
  const officialRatio = events.length ? `${Math.round((officialCount / events.length) * 100)}%` : "0%";

  const cards = [
    {
      label: "总事件数",
      value: String(events.length),
      description: "当前筛选结果中的全部真实事件条目。",
      tone: "neutral"
    },
    {
      label: "高风险待复核",
      value: String(highRiskCount),
      description: "建议优先进入公关、客服、法务联合复核。",
      tone: "danger"
    },
    {
      label: "近 7 天新增",
      value: String(recentSevenDayCount),
      description: "按当前筛选范围统计的最近一周新增事件。",
      tone: "accent"
    },
    {
      label: "官方来源占比",
      value: officialRatio,
      description: `${officialCount} 条来自品牌官方或合作方官方页面。`,
      tone: "brand"
    },
    {
      label: "负向舆情数",
      value: String(negativeCount),
      description: "按情绪字段统计的明确负向事件。",
      tone: "danger"
    }
  ];

  elements.summaryStrip.innerHTML = cards
    .map(
      (card) => `
        <article class="summary-card is-${card.tone}">
          <span>${card.label}</span>
          <strong>${card.value}</strong>
          <p>${card.description}</p>
        </article>
      `
    )
    .join("");
}

function renderListMeta(events) {
  const latestPublished = events.find((event) => event.publishedAt)?.publishedAt || null;
  const latestCaptured = events.find((event) => event.capturedAt)?.capturedAt || null;
  const parts = [
    `当前显示 ${events.length} 条`,
    "按发布时间优先、抓取时间补位倒序",
    latestPublished ? `最新发布日期：${formatDate(latestPublished)}` : "暂无明确发布日期",
    latestCaptured ? `最近抓取：${formatDate(latestCaptured)}` : null
  ];

  elements.listMeta.textContent = parts.filter(Boolean).join(" · ");
}

function renderEvents(events) {
  if (!events.length) {
    elements.eventList.innerHTML = `
      <div class="empty-state">
        当前筛选条件下没有命中事件。你可以切回更长时间范围，或放宽来源 / 风险筛选后再查看。
      </div>
    `;
    return;
  }

  elements.eventList.innerHTML = "";

  events.forEach((event) => {
    const fragment = elements.template.content.cloneNode(true);
    const article = fragment.querySelector(".event-card");
    article.dataset.eventId = event.id;
    article.classList.add(`event-card-risk-${event.riskLevel}`);

    fragment.querySelector(".event-time").innerHTML = buildTimeMarkup(event);
    fragment.querySelector(".event-title").textContent = event.title;
    fragment.querySelector(".event-summary").textContent = buildSummaryPreview(event.summary);
    fragment.querySelector(".event-risk-reason").textContent = `风险依据：${buildRiskPreview(event.riskReason)}`;

    const badges = fragment.querySelector(".event-badges");
    badges.append(
      createBadge(`badge-risk-${event.riskLevel}`, LABELS.risk[event.riskLevel] || event.riskLevel),
      createBadge(`badge-source-${event.sourceType}`, LABELS.sourceType[event.sourceType] || event.sourceType),
      createBadge("badge-tier", event.sourceTier || "未标注"),
      createBadge(`badge-sentiment-${event.sentiment}`, LABELS.sentiment[event.sentiment] || event.sentiment)
    );

    const metaGrid = fragment.querySelector(".event-meta-grid");
    metaGrid.innerHTML = [
      buildMetaCell("主体", LABELS.entity[event.entity] || event.entity),
      buildMetaCell("车型 / 范围", event.model),
      buildMetaCell("类别", event.category),
      buildMetaCell("来源层级", event.sourceTier),
      buildMetaCell("来源名称", event.sourceName),
      buildMetaCell("发布主体", event.publisher)
    ].join("");

    const detail = fragment.querySelector(".event-detail");
    detail.innerHTML = [
      buildDetailItem("完整摘要", event.summary),
      buildDetailItem("发布时间", formatDate(event.publishedAt)),
      buildDetailItem("抓取时间", formatDate(event.capturedAt)),
      buildDetailItem("风险依据", event.riskReason),
      buildDetailItem("建议动作", event.suggestedAction),
      buildDetailItem("影响范围", event.impactScope),
      buildDetailItem("状态", LABELS.status[event.status] || event.status || "未标注"),
      buildDetailItem("追溯说明", event.traceability)
    ].join("");

    const originLink = fragment.querySelector(".origin-link");
    originLink.href = event.url;
    originLink.textContent = `${event.sourceName} · 打开原始页面`;

    const keywords = fragment.querySelector(".event-keywords");
    event.keywords.forEach((keyword) => {
      keywords.appendChild(createKeyword(keyword));
    });

    elements.eventList.appendChild(fragment);
  });
}

function renderSidebar(events) {
  renderEntityHeatmap(events);
  renderKeywordCloud(events);
  renderWatchList(events);
}

function renderEntityHeatmap(events) {
  const counts = FILTERS.entity
    .filter((item) => item.value !== "all")
    .map((item) => ({
      label: item.label,
      count: events.filter((event) => event.entity === item.value).length
    }));

  const max = Math.max(...counts.map((item) => item.count), 1);

  elements.entityHeatmap.innerHTML = counts
    .map(
      (item) => `
        <div class="stat-row">
          <strong>${item.label}</strong>
          <div class="bar-track"><div class="bar-fill" style="width:${(item.count / max) * 100}%"></div></div>
          <span>${item.count}</span>
        </div>
      `
    )
    .join("");
}

function renderKeywordCloud(events) {
  const pool = new Map();

  events.forEach((event) => {
    event.keywords.forEach((keyword) => {
      pool.set(keyword, (pool.get(keyword) || 0) + 1);
    });
  });

  const topKeywords = [...pool.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, 18);

  if (!topKeywords.length) {
    elements.keywordCloud.innerHTML = `<div class="empty-state">当前筛选结果没有可聚合的关键词。</div>`;
    return;
  }

  elements.keywordCloud.innerHTML = topKeywords
    .map(([keyword, count]) => `<span class="keyword">${escapeHtml(keyword)}<span>${count}</span></span>`)
    .join("");
}

function renderWatchList(events) {
  const items = events
    .filter((event) => event.riskLevel === "high" || event.status === "watch" || event.status === "active")
    .slice(0, 5);

  if (!items.length) {
    elements.watchList.innerHTML = `<div class="empty-state">当前视图没有需要优先复核的记录。</div>`;
    return;
  }

  elements.watchList.innerHTML = items
    .map(
      (item) => `
        <article class="watch-item">
          <div>
            <strong>${escapeHtml(item.title)}</strong>
            <p>${escapeHtml(item.riskReason)}</p>
          </div>
          <span class="badge badge-risk-${item.riskLevel}">${LABELS.risk[item.riskLevel] || item.riskLevel}</span>
        </article>
      `
    )
    .join("");
}

function buildSummaryPreview(summary) {
  const normalized = String(summary || "").trim();
  if (!normalized) {
    return "暂无摘要。";
  }

  return normalized.length > 140 ? `${normalized.slice(0, 140)}...` : normalized;
}

function buildRiskPreview(reason) {
  const normalized = String(reason || "").trim();
  if (!normalized) {
    return "未补充风险依据。";
  }

  return normalized.length > 96 ? `${normalized.slice(0, 96)}...` : normalized;
}

function buildTimeMarkup(event) {
  const rows = [];

  if (event.publishedAt) {
    rows.push(`
      <div class="time-row">
        <span class="time-label">发布时间</span>
        <strong class="time-value">${escapeHtml(formatDate(event.publishedAt))}</strong>
      </div>
    `);
  }

  if (event.capturedAt) {
    rows.push(`
      <div class="time-row">
        <span class="time-label">抓取时间</span>
        <strong class="time-value">${escapeHtml(formatDate(event.capturedAt))}</strong>
      </div>
    `);
  }

  if (!rows.length) {
    rows.push(`
      <div class="time-row">
        <span class="time-label">时间</span>
        <strong class="time-value">未披露</strong>
      </div>
    `);
  }

  return `<div class="time-stack">${rows.join("")}</div>`;
}

function buildMetaCell(label, value) {
  return `
    <div class="event-meta-cell">
      <span>${label}</span>
      <strong>${escapeHtml(value || "未标注")}</strong>
    </div>
  `;
}

function buildDetailItem(label, value) {
  return `
    <div class="detail-item">
      <span>${label}</span>
      <strong>${escapeHtml(value || "未标注")}</strong>
    </div>
  `;
}

function createBadge(className, text) {
  const badge = document.createElement("span");
  badge.className = `badge ${className}`;
  badge.textContent = text;
  return badge;
}

function createKeyword(text) {
  const keyword = document.createElement("span");
  keyword.className = "keyword";
  keyword.textContent = text;
  return keyword;
}

function renderLoading() {
  elements.summaryStrip.innerHTML = Array.from({ length: 5 }, (_, index) => `
    <article class="summary-card is-neutral">
      <span>加载中 ${index + 1}</span>
      <strong>--</strong>
      <p>正在读取舆情数据与分享视图状态。</p>
    </article>
  `).join("");

  elements.listMeta.textContent = "正在读取数据...";
  elements.eventList.innerHTML = `<div class="loading-state">正在读取舆情数据...</div>`;
  elements.entityHeatmap.innerHTML = `<div class="loading-state">正在聚合主体热度...</div>`;
  elements.keywordCloud.innerHTML = `<div class="loading-state">正在聚合关键词...</div>`;
  elements.watchList.innerHTML = `<div class="loading-state">正在识别优先复核项...</div>`;
  elements.dataSourceNote.textContent = "正在连接数据源...";
}

function renderError(error) {
  elements.summaryStrip.innerHTML = `
    <article class="summary-card is-danger">
      <span>数据状态</span>
      <strong>加载失败</strong>
      <p>请检查 JSON 路径、网络状态或本地预览方式后重试。</p>
    </article>
  `;

  elements.listMeta.textContent = "当前无法生成事件流视图";
  elements.dataSourceNote.textContent = `数据源异常：${ACTIVE_DATA_URL}`;
  elements.entityHeatmap.innerHTML = `<div class="empty-state">主体热度暂不可用。</div>`;
  elements.keywordCloud.innerHTML = `<div class="empty-state">关键词聚合暂不可用。</div>`;
  elements.watchList.innerHTML = `<div class="empty-state">高风险清单暂不可用。</div>`;

  elements.eventList.innerHTML = `
    <div class="empty-state">
      数据加载失败：${escapeHtml(error.message)}。<br>
      如果你是直接双击本地文件打开，请改用静态服务器预览，例如 <code>python3 -m http.server</code>。
      <div>
        <button id="retryLoadButton" class="action-button retry-button" type="button">重试加载</button>
      </div>
    </div>
  `;
}

function formatDate(value) {
  if (!value) {
    return "未标注";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  }).format(date);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function syncQuery() {
  const nextQuery = new URLSearchParams();

  if (!USING_EMBEDDED_DATA && ACTIVE_DATA_URL !== DEFAULT_DATA_URL) {
    nextQuery.set("data", ACTIVE_DATA_URL);
  }

  if (state.range !== "all") {
    nextQuery.set("time", state.range);
  }

  if (state.entity !== "all") {
    nextQuery.set("entity", serializeEntity(state.entity));
  }

  if (state.sourceType !== "all") {
    nextQuery.set("source", state.sourceType);
  }

  if (state.risk !== "all") {
    nextQuery.set("risk", state.risk);
  }

  const nextUrl = `${window.location.pathname}${nextQuery.toString() ? `?${nextQuery.toString()}` : ""}`;
  window.history.replaceState({}, "", nextUrl);
}

function showToast(message) {
  const oldToast = document.querySelector(".toast");
  if (oldToast) {
    oldToast.remove();
  }

  const toast = document.createElement("div");
  toast.className = "toast";
  toast.textContent = message;
  document.body.appendChild(toast);

  window.setTimeout(() => {
    toast.remove();
  }, 2200);
}

document.addEventListener("click", async (event) => {
  const filterButton = event.target.closest(".filter-button");
  if (filterButton) {
    const key = filterButton.dataset.filterKey;
    const value = filterButton.dataset.filterValue;
    state[key] = value;
    render();
    return;
  }

  const toggle = event.target.closest(".toggle-summary");
  if (toggle) {
    const card = toggle.closest(".event-card");
    const detail = card?.querySelector(".event-detail");
    if (!detail) {
      return;
    }

    const expanded = toggle.getAttribute("aria-expanded") === "true";
    toggle.setAttribute("aria-expanded", expanded ? "false" : "true");
    toggle.textContent = expanded ? "展开摘要" : "收起摘要";
    detail.hidden = expanded;
    return;
  }

  if (event.target.id === "retryLoadButton") {
    loadData();
    return;
  }

  if (event.target.id === "copyShareButton") {
    try {
      await navigator.clipboard.writeText(window.location.href);
      showToast("当前视图链接已复制");
    } catch (error) {
      showToast("复制失败，请手动复制当前地址");
    }
  }
});

loadData();
