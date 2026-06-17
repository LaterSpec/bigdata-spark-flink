function emptyStreamingSnapshot() {
  return {
    generated_at: "",
    data_mode: "disconnected",
    environment: {},
    counts: {
      raw_youtube_chat: 0,
      nlp_stream_results: 0,
      alerts_polarization: 0,
      filtered_messages: 0,
      spark_curated_rows: 0,
      spark_aggregates_rows: 0
    },
    offsets: {},
    stream_events: [],
    alerts: [],
    spark_jobs: [],
    actors: [],
    timeline: []
  };
}

// ── STATE ─────────────────────────────────────────────────────────────────
let state = {
  snapshot: emptyStreamingSnapshot(),
  filter: "all",
  query: "",
  playing: true,
  awsConnected: false,
  pollingHandle: null,
  statusPollingHandle: null,
  sparkPollingHandle: null,
  pointer: 0,
  // Ring buffer: emitted events in chat order. Filters are applied at render time.
  masterQueue: [],
  rawQueue: [],
  filteredQueue: [],
  MAX_QUEUE: 400,
  chartTick: 0,
  liveRefreshInFlight: false,
  lastLiveGeneratedAt: "",
  liveCursor: null,
  emitSeq: 0,
  renderedStreamSeq: 0,
  renderedRawSeq: 0,
  renderedFilteredSeq: 0,
  renderedAlertSeq: 0,
  actorCounts: {},
  actorTimeline: [],
  sessionCounts: {
    raw_youtube_chat: 0,
    nlp_stream_results: 0,
    alerts_polarization: 0,
    filtered_messages: 0,
    spark_curated_rows: 0,
    spark_aggregates_rows: 0
  },
  dataSize: 0,
  sparkBatchSize: 1000,
  awsStatus: null,
  sparkBatch: {
    batches: {},
    latest_batch_id: ""
  },
  selectedSparkBatchId: "",
  sparkComments: [],
  sparkCommentsQuery: "",
  sparkCommentsLoading: false,
  sparkStartInFlight: false,
  sparkStartingTargets: new Set(),
  chartHover: null,
  chartAnimation: null,
  chartHeadProgress: 1
};

const ACTOR_SERIES = [
  { key: "keiko_fujimori_fp", label: "Keiko / Fujimori / FP", color: "#ff8b52" },
  { key: "onpe_jne", label: "ONPE / JNE", color: "#31d18b" },
  { key: "castillo_peru_libre", label: "Castillo / Peru Libre", color: "#69b9ff" },
  { key: "jp_juntos_por_el_peru", label: "JP / Juntos por el Peru", color: "#f0c86b" },
  { key: "lopez_aliaga_porky", label: "RLA / Porky", color: "#ff5a3d" },
  { key: "antauro_humala", label: "Antauro", color: "#c792ea" }
];

// ── SELECTORS ─────────────────────────────────────────────────────────────
const selectors = {
  rawCount: document.querySelector("#raw-count"),
  nlpCount: document.querySelector("#nlp-count"),
  alertCount: document.querySelector("#alert-count"),
  validatedCount: document.querySelector("#validated-count"),
  flinkEvents: document.querySelector("#flink-events"),
  riskAlerts: document.querySelector("#risk-alerts"),
  sparkOutput: document.querySelector("#spark-output"),
  streamList: document.querySelector("#stream-list"),
  rawChatList: document.querySelector("#raw-chat-list"),
  filteredList: document.querySelector("#filtered-list"),
  rawChatCount: document.querySelector("#raw-chat-count"),
  filteredCount: document.querySelector("#filtered-count"),
  actorBars: document.querySelector("#actor-bars"),
  alertStack: document.querySelector("#alert-stack"),
  sparkBatchStatus: document.querySelector("#spark-batch-status"),
  sparkBatchSummary: document.querySelector("#spark-batch-summary"),
  sparkBatchGrid: document.querySelector("#spark-batch-grid"),
  sparkCommentsCount: document.querySelector("#spark-comments-count"),
  sparkCommentsSearch: document.querySelector("#spark-comments-search"),
  sparkCommentsHint: document.querySelector("#spark-comments-hint"),
  sparkCommentsList: document.querySelector("#spark-comments-list"),
  timelineChart: document.querySelector("#timeline-chart"),
  searchInput: document.querySelector("#search-input"),
  filterButtons: document.querySelectorAll(".filter-button"),
  playStream: document.querySelector("#play-stream"),
  refreshDemo: document.querySelector("#refresh-demo"),
  syncAws: document.querySelector("#sync-aws"),
  syncStatus: document.querySelector("#sync-status"),
  dataMode: document.querySelector("#data-mode"),
  kafkaLabel: document.querySelector(".kafka-label")
};

// ── AWS STREAM START ──────────────────────────────────────────────────────
async function requestAwsSync() {
  if (state.awsConnected) return;
  selectors.syncAws.disabled = true;
  selectors.syncAws.textContent = "Conectando...";
  resetStreamingState();
  setSyncStatus("Iniciando Kafka, producer y jobs Flink en EMR...", "ok");

  try {
    const response = await fetch("/api/aws/start", { method: "POST" });
    const result = await response.json();
    if (!response.ok || !result.ok) {
      throw new Error(result.error || "No se pudo iniciar AWS");
    }
    state.awsConnected = true;
    state.dataSize = Number(result.data_size || state.dataSize || 0);
    state.sparkBatchSize = Number(result.spark_batch_size || state.sparkBatchSize || 1000);
    state.snapshot.data_mode = "aws_streaming";
    selectors.syncAws.textContent = "Conectado";
    if (selectors.kafkaLabel) selectors.kafkaLabel.textContent = "Kafka activo";
    setSyncStatus("AWS conectado. Recibiendo deltas desde Kafka cada 3 segundos.", "ok");
    startPolling();
    fetchLiveDelta();
    fetchAwsStatus();
    fetchSparkStatus();
  } catch (error) {
    state.awsConnected = false;
    selectors.syncAws.disabled = false;
    selectors.syncAws.textContent = "Conectar AWS";
    if (selectors.kafkaLabel) selectors.kafkaLabel.textContent = "Kafka en espera";
    state.snapshot.data_mode = "disconnected";
    renderCounts();
    setSyncStatus(`AWS no disponible: ${error.message}. El dashboard queda vacio, sin fallback local.`, "error");
  }
}

async function requestAwsStop() {
  selectors.refreshDemo.disabled = true;
  selectors.refreshDemo.classList.add("is-busy");
  selectors.refreshDemo.textContent = "Deteniendo...";
  selectors.syncAws.disabled = true;
  setSyncStatus("Deteniendo Kafka y procesos streaming en EMR...", "ok");

  try {
    const response = await fetch("/api/aws/stop", { method: "POST" });
    const result = await response.json();
    if (!response.ok || !result.ok) {
      throw new Error(result.error || "No se pudo detener AWS");
    }
    setSyncStatus("Streaming detenido. Puedes volver a conectar AWS.", "ok");
  } catch (error) {
    setSyncStatus(`No se pudo confirmar el stop remoto: ${error.message}. La sesion local fue reiniciada.`, "error");
  } finally {
    resetStreamingState();
    selectors.syncAws.disabled = false;
    selectors.syncAws.textContent = "Conectar AWS";
    selectors.refreshDemo.classList.remove("is-busy");
    selectors.refreshDemo.textContent = "Reiniciar cinta";
    selectors.refreshDemo.disabled = false;
  }
}

function startPolling() {
  if (!state.pollingHandle) state.pollingHandle = window.setInterval(fetchLiveDelta, 3000);
  if (!state.statusPollingHandle) state.statusPollingHandle = window.setInterval(fetchAwsStatus, 5000);
  if (!state.sparkPollingHandle) state.sparkPollingHandle = window.setInterval(fetchSparkStatus, 5000);
}

async function fetchLiveDelta() {
  if (!state.awsConnected) return;
  if (state.liveRefreshInFlight) return;
  state.liveRefreshInFlight = true;

  const params = new URLSearchParams({
    maxRaw: "400",
    maxFlink: "600",
    maxAlerts: "80"
  });
  if (state.liveCursor) {
    params.set("cursor", JSON.stringify(state.liveCursor));
  }

  try {
    const response = await fetch(`/api/live-delta?${params.toString()}`, { cache: "no-store" });
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error || "No se pudo obtener delta live");
    applyLiveDelta(result);
    setSyncStatus(`Streaming activo: ${result.generated_at || "live"}`, "ok");
  } catch (error) {
    setSyncStatus(`Stream remoto no disponible: ${error.message}. Se conserva el ultimo estado.`, "error");
  } finally {
    state.liveRefreshInFlight = false;
  }
}

async function fetchAwsStatus() {
  if (!state.awsConnected) return;
  try {
    const response = await fetch("/api/aws/status", { cache: "no-store" });
    const status = await response.json();
    if (!response.ok || !status.ok) throw new Error(status.error || "No se pudo obtener estado AWS");
    state.awsStatus = status;
    state.dataSize = Number(status.data_size || state.dataSize || 0);
    state.sparkBatchSize = Number(status.spark_batch_size || state.sparkBatchSize || 1000);
    maybeStartNextSparkBatch(status);
    renderSparkBatch();
  } catch (error) {
    if (selectors.sparkBatchStatus) selectors.sparkBatchStatus.textContent = `aws status error`;
  }
}

async function fetchSparkStatus() {
  if (!state.awsConnected) {
    renderSparkBatch();
    return;
  }
  try {
    const response = await fetch("/api/spark/status", { cache: "no-store" });
    const status = await response.json();
    if (!response.ok || !status.ok) throw new Error(status.error || "No se pudo obtener estado Spark");
    state.sparkBatch = {
      batches: status.batches || {},
      latest_batch_id: status.latest_batch_id || ""
    };
    const selected = state.selectedSparkBatchId ? state.sparkBatch.batches[state.selectedSparkBatchId] : null;
    if (selected && selected.status !== "done") {
      state.selectedSparkBatchId = "";
      state.sparkComments = [];
    }
    updateSparkMetricFromBatches();
    renderSparkBatch();
  } catch {
    if (selectors.sparkBatchStatus) selectors.sparkBatchStatus.textContent = "spark status error";
  }
}

async function fetchSparkComments(batchId) {
  const batch = state.sparkBatch.batches?.[batchId];
  if (!state.awsConnected || !batch || batch.status !== "done") return;

  state.selectedSparkBatchId = batchId;
  state.sparkCommentsLoading = true;
  renderSparkComments();

  try {
    const params = new URLSearchParams({ batchId, limit: "300" });
    const response = await fetch(`/api/spark/comments?${params.toString()}`, { cache: "no-store" });
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error || "No se pudieron cargar comentarios Spark");
    state.sparkComments = result.comments || [];
    if (selectors.sparkCommentsHint) {
      selectors.sparkCommentsHint.textContent = `${batchId}: ${state.sparkComments.length} comentarios ofensivos filtrados por Spark ML.`;
    }
  } catch (error) {
    state.sparkComments = [];
    if (selectors.sparkCommentsHint) selectors.sparkCommentsHint.textContent = `${batchId}: ${error.message}`;
  } finally {
    state.sparkCommentsLoading = false;
    renderSparkBatch();
    renderSparkComments();
  }
}

async function restoreAwsSessionIfRunning() {
  try {
    const response = await fetch("/api/aws/status", { cache: "no-store" });
    const status = await response.json();
    if (!response.ok || !status.ok) {
      throw new Error(status.error || "AWS aun no esta conectado.");
    }

    state.awsConnected = true;
    state.awsStatus = status;
    state.dataSize = Number(status.data_size || state.dataSize || 0);
    state.sparkBatchSize = Number(status.spark_batch_size || state.sparkBatchSize || 1000);
    state.snapshot.data_mode = "aws_streaming";
    selectors.syncAws.disabled = true;
    selectors.syncAws.textContent = "Conectado";
    if (selectors.kafkaLabel) selectors.kafkaLabel.textContent = "Kafka activo";
    setSyncStatus("AWS ya estaba conectado. Reanudando streaming desde Kafka.", "ok");
    startPolling();
    fetchLiveDelta();
    fetchAwsStatus();
    fetchSparkStatus();
  } catch {
    state.awsConnected = false;
    selectors.syncAws.disabled = false;
    selectors.syncAws.textContent = "Conectar AWS";
    if (selectors.kafkaLabel) selectors.kafkaLabel.textContent = "Kafka en espera";
    state.snapshot.data_mode = "disconnected";
    setSyncStatus("Frontend listo. AWS aun no se ha iniciado.", "ok");
  }
}

async function maybeStartNextSparkBatch(status = state.awsStatus) {
  if (!state.awsConnected || state.sparkStartInFlight || !status) return;
  const eligible = Number(status.eligible_spark_target || 0);
  const batchSize = Number(status.spark_batch_size || state.sparkBatchSize || 1000);
  if (!eligible || !batchSize) return;

  const batches = state.sparkBatch.batches || {};
  const hasRunning = Object.values(batches).some((batch) => batch.status === "running" || batch.status === "queued");
  if (hasRunning) return;

  let target = 0;
  for (let next = batchSize; next <= eligible; next += batchSize) {
    const id = sparkBatchId(next);
    const existing = batches[id];
    if (!existing && !state.sparkStartingTargets.has(next)) {
      target = next;
      break;
    }
  }
  if (!target) return;

  state.sparkStartInFlight = true;
  state.sparkStartingTargets.add(target);
  renderSparkBatch();
  try {
    const params = new URLSearchParams({ target: String(target), batchId: sparkBatchId(target) });
    const response = await fetch(`/api/spark/start?${params.toString()}`, { method: "POST" });
    const result = await response.json();
    if (!response.ok || !result.ok) throw new Error(result.error || "No se pudo iniciar Spark");
    await fetchSparkStatus();
  } catch (error) {
    const id = sparkBatchId(target);
    state.sparkBatch.batches[id] = {
      batch_id: id,
      target_count: target,
      spark_batch_size: batchSize,
      status: "failed",
      current_job: "start",
      message: error.message,
      jobs: {},
      outputs: {}
    };
    renderSparkBatch();
  } finally {
    state.sparkStartInFlight = false;
  }
}

function applyLiveDelta(delta) {
  state.liveCursor = delta.cursor || state.liveCursor;
  state.lastLiveGeneratedAt = delta.generated_at || state.lastLiveGeneratedAt;

  const rawMessages = (delta.raw_messages || []).map((event) => normalizeIncomingEvent(event, "raw"));
  const flinkEvents = (delta.flink_events || []).map((event) => normalizeIncomingEvent(event, "stream"));
  const filteredMessages = (delta.filtered_messages || []).map((event) => normalizeIncomingEvent(event, "alert"));

  state.sessionCounts.raw_youtube_chat += rawMessages.length;
  state.sessionCounts.nlp_stream_results += flinkEvents.length;
  state.sessionCounts.alerts_polarization += filteredMessages.filter((event) => event.source_topic === "alerts_polarization" || event.source === "alert").length;
  state.sessionCounts.filtered_messages += filteredMessages.length;
  state.snapshot.counts = { ...state.snapshot.counts, ...state.sessionCounts };
  state.snapshot.generated_at = delta.generated_at || state.snapshot.generated_at;
  state.snapshot.data_mode = delta.data_mode || "aws_delta";
  renderCounts();

  if (flinkEvents.length) {
    state.snapshot.stream_events = [...(state.snapshot.stream_events || []), ...flinkEvents].slice(-1000);
  }

  if (filteredMessages.length) {
    state.snapshot.alerts = [...(state.snapshot.alerts || []), ...filteredMessages].slice(-300);
  }

  appendRawMessages(rawMessages);
  appendFilteredMessages(filteredMessages);
  appendStreamEvents(flinkEvents);

  if (flinkEvents.length || filteredMessages.length) {
    renderActors();
    drawTimeline();
    renderAlertsAppend(filteredMessages);
  }
}

function normalizeCounts(counts) {
  return {
    raw_youtube_chat: counts.raw_youtube_chat ?? counts.raw ?? 0,
    nlp_stream_results: counts.nlp_stream_results ?? counts.flink ?? 0,
    alerts_polarization: counts.alerts_polarization ?? counts.alerts ?? 0,
    filtered_messages: counts.filtered_messages ?? state.snapshot.counts?.filtered_messages ?? 0,
    spark_curated_rows: counts.spark_curated_rows ?? 0,
    spark_aggregates_rows: counts.spark_aggregates_rows ?? state.snapshot.counts?.spark_aggregates_rows ?? 0
  };
}

function resetStreamingState() {
  if (state.pollingHandle) {
    window.clearInterval(state.pollingHandle);
    state.pollingHandle = null;
  }
  if (state.statusPollingHandle) {
    window.clearInterval(state.statusPollingHandle);
    state.statusPollingHandle = null;
  }
  if (state.sparkPollingHandle) {
    window.clearInterval(state.sparkPollingHandle);
    state.sparkPollingHandle = null;
  }
  state.snapshot = emptyStreamingSnapshot();
  state.awsConnected = false;
  state.masterQueue = [];
  state.rawQueue = [];
  state.filteredQueue = [];
  state.pointer = 0;
  state.emitSeq = 0;
  state.renderedStreamSeq = 0;
  state.renderedRawSeq = 0;
  state.renderedFilteredSeq = 0;
  state.renderedAlertSeq = 0;
  state.liveCursor = null;
  state.actorCounts = {};
  state.actorTimeline = [];
  state.sessionCounts = { ...state.snapshot.counts };
  state.awsStatus = null;
  state.sparkBatch = { batches: {}, latest_batch_id: "" };
  state.selectedSparkBatchId = "";
  state.sparkComments = [];
  state.sparkCommentsQuery = "";
  state.sparkCommentsLoading = false;
  state.sparkStartInFlight = false;
  state.sparkStartingTargets = new Set();
  if (selectors.streamList) selectors.streamList.innerHTML = "";
  if (selectors.rawChatList) selectors.rawChatList.innerHTML = "";
  if (selectors.filteredList) selectors.filteredList.innerHTML = "";
  if (selectors.sparkCommentsList) selectors.sparkCommentsList.innerHTML = "";
  if (selectors.sparkCommentsSearch) selectors.sparkCommentsSearch.value = "";
  if (selectors.kafkaLabel) selectors.kafkaLabel.textContent = "Kafka en espera";
  renderAll();
}

function normalizeIncomingEvent(event, source) {
  return {
    source,
    ...event,
    source: event.source || source,
    __stream_seq: ++state.emitSeq,
    __chart_ts: Date.now()
  };
}

function setSyncStatus(message, mode = "") {
  selectors.syncStatus.textContent = message;
  selectors.syncStatus.classList.remove("ok", "error");
  if (mode) selectors.syncStatus.classList.add(mode);
}

// ── EVENT HELPERS ─────────────────────────────────────────────────────────
function eventText(event) {
  const payload = event.payload || {};
  return (
    payload.stream_text ||
    payload.message_text ||
    payload.message_clean ||
    payload.message_raw ||
    event.message_clean ||
    event.message_raw ||
    event.raw?.message_clean ||
    event.raw?.message_raw ||
    payload.hybrid_risk_reason ||
    payload.reason ||
    event.event_type ||
    ""
  );
}

function eventTags(event) {
  const payload = event.payload || {};
  const tags = [];
  if (event.event_type) tags.push(event.event_type);
  if (payload.local_rule_tags) tags.push(...String(payload.local_rule_tags).split("|"));
  if (payload.hybrid_risk_level) tags.push(`risk:${payload.hybrid_risk_level}`);
  if (payload.actor) tags.push(payload.actor);
  if (payload.alert_type) tags.push(payload.alert_type);
  if (event.source === "raw") tags.push("raw_youtube_chat");
  return tags.filter(Boolean).slice(0, 4);
}

function rawAuthor(event) {
  return event.author || event.raw?.author || event.payload?.author || "youtube_user";
}

function allEvents() {
  return [
    ...(state.snapshot.stream_events || []),
    ...(state.snapshot.alerts || []),
    ...(state.snapshot.spark_jobs || []).map((job, index) => ({
      source: "spark",
      job_name: job.name,
      event_type: "spark_batch_output",
      source_partition: "-",
      source_offset: index,
      payload: {
        stream_text: `${job.status}: ${job.rows} filas escritas`,
        hybrid_risk_reason: job.output
      }
    }))
  ];
}

// ── STREAM TICK & RENDER ──────────────────────────────────────────────────
function tickStream() {
  if (!state.playing) return;
  const pool = allEvents();
  if (!pool.length) { renderStream(); return; }

  const emitted = [];

  // Emit 3 events per tick for large datasets (7k cycles in ~8 min at 500ms)
  for (let i = 0; i < 3; i++) {
    const next = {
      ...pool[state.pointer % pool.length],
      __stream_seq: ++state.emitSeq,
      __chart_ts: Date.now()
    };
    emitted.push(next);
    state.masterQueue.push(next);
    if (state.masterQueue.length > state.MAX_QUEUE) state.masterQueue.shift();
    state.pointer++;
  }

  updateActorTimeline(emitted);
  renderStream();

  // Recalculate charts every 15 ticks (~7.5 s)
  state.chartTick++;
  if (state.chartTick % 15 === 0) {
    renderActors();
    drawTimeline();
  }
}

function eventMatchesActiveView() {
  return true;
}

function filteredMessageMatchesQuery(event) {
  const query = state.query.trim().toLowerCase();
  if (!query) return true;
  const searchBlob = [
    eventText(event),
    event.job_name || "",
    event.event_type || "",
    event.payload?.actor || "",
    event.payload?.alert_type || "",
    ...eventTags(event)
  ]
    .join(" ")
    .toLowerCase();
  return searchBlob.includes(query);
}

function streamItemHtml(event) {
  const tags = eventTags(event)
    .map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`)
    .join("");
  return `
    <article class="stream-item ${escapeHtml(event.source || "stream")}" data-seq="${event.__stream_seq || ""}">
      <div class="stream-copy">
        <strong>${escapeHtml(event.job_name || "pipeline_event")}</strong>
        <p>${escapeHtml(eventText(event))}</p>
        <div class="stream-tags">${tags}</div>
      </div>
    </article>
  `;
}

function renderStream(options = {}) {
  const reset = Boolean(options.reset);
  const list = selectors.streamList;
  const wasNearBottom = list.scrollTop + list.clientHeight >= list.scrollHeight - 24;

  if (reset) {
    const visible = state.masterQueue.filter(eventMatchesActiveView).slice(-70);
    state.renderedStreamSeq = state.emitSeq;

    if (!visible.length && state.masterQueue.length > 0) {
      list.innerHTML = '<p class="stream-empty">Sin resultados para el filtro actual.</p>';
      return;
    }

    list.innerHTML = visible.map(streamItemHtml).join("");
    list.scrollTop = list.scrollHeight;
    return;
  }

  const nextEvents = state.masterQueue
    .filter((event) => (event.__stream_seq || 0) > state.renderedStreamSeq)
    .filter(eventMatchesActiveView);

  state.renderedStreamSeq = state.emitSeq;
  if (!nextEvents.length) return;

  const empty = list.querySelector(".stream-empty");
  if (empty) empty.remove();

  list.insertAdjacentHTML("beforeend", nextEvents.map(streamItemHtml).join(""));

  while (list.children.length > 80) {
    list.removeChild(list.firstElementChild);
  }

  if (wasNearBottom) {
    list.scrollTo({ top: list.scrollHeight, behavior: "smooth" });
  }
}

function appendStreamEvents(events) {
  if (!events.length) return;
  state.masterQueue.push(...events);
  if (state.masterQueue.length > state.MAX_QUEUE) {
    state.masterQueue = state.masterQueue.slice(-state.MAX_QUEUE);
  }
  updateActorTimeline(events);
  renderStream();
}

function filteredName(event) {
  return event.payload?.author || event.author || event.raw?.author || event.payload?.actor || event.payload?.alert_type || event.event_type || "youtube_user";
}

function categoryText(event) {
  return eventTags(event).join(", ") || "sin_categoria";
}

function chatItemHtml(event, mode = "raw") {
  const tags = eventTags(event)
    .map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`)
    .join("");
  const title = mode === "raw" ? rawAuthor(event) : filteredName(event);
  return `
    <article class="stream-item ${escapeHtml(mode === "raw" ? "raw" : "alert")}" data-seq="${event.__stream_seq || ""}">
      <div class="stream-copy">
        <strong>${escapeHtml(title)}</strong>
        <p>${escapeHtml(eventText(event))}</p>
        ${mode === "filtered" ? `<p class="category-line">Categoria: ${escapeHtml(categoryText(event))}</p>` : ""}
        <div class="stream-tags">${tags}</div>
      </div>
    </article>
  `;
}

function appendChatList(list, events, renderedKey, maxChildren, mode) {
  if (!list || !events.length) return;
  const wasNearBottom = list.scrollTop + list.clientHeight >= list.scrollHeight - 24;
  const nextEvents = events.filter((event) => (event.__stream_seq || 0) > state[renderedKey]);
  if (!nextEvents.length) return;
  state[renderedKey] = Math.max(...nextEvents.map((event) => event.__stream_seq || 0), state[renderedKey]);

  const empty = list.querySelector(".stream-empty");
  if (empty) empty.remove();

  list.insertAdjacentHTML("beforeend", nextEvents.map((event) => chatItemHtml(event, mode)).join(""));
  while (list.children.length > maxChildren) {
    list.removeChild(list.firstElementChild);
  }
  if (wasNearBottom) {
    list.scrollTo({ top: list.scrollHeight, behavior: "smooth" });
  }
}

function appendRawMessages(events) {
  if (!events.length) return;
  state.rawQueue.push(...events);
  state.rawQueue = state.rawQueue.slice(-180);
  appendChatList(selectors.rawChatList, events, "renderedRawSeq", 120, "raw");
  if (selectors.rawChatCount) animateCount(selectors.rawChatCount, state.snapshot.counts?.raw_youtube_chat || state.rawQueue.length);
}

function appendFilteredMessages(events) {
  if (!events.length) return;
  state.filteredQueue.push(...events);
  state.filteredQueue = state.filteredQueue.slice(-300);
  renderFilteredMessages();
}

function renderFilteredMessages() {
  const visible = state.filteredQueue.filter(filteredMessageMatchesQuery).slice(-120);
  selectors.filteredList.innerHTML = visible.map((event) => chatItemHtml(event, "filtered")).join("");
  selectors.filteredList.scrollTop = selectors.filteredList.scrollHeight;
  if (selectors.filteredCount) {
    const value = state.query.trim() ? visible.length : (state.snapshot.counts?.filtered_messages || state.filteredQueue.length);
    animateCount(selectors.filteredCount, value);
  }
}

// ── COUNT-UP ANIMATION ────────────────────────────────────────────────────
function animateCount(element, newValue) {
  const current = Number(element.dataset.value ?? String(element.textContent).replace(/[^0-9]/g, "")) || 0;
  if (current === newValue) return;

  element.dataset.value = String(newValue);
  element.classList.remove("updating");
  element.innerHTML = `
    <span class="count-roll" aria-hidden="true">
      <span class="count-old">${current.toLocaleString()}</span>
      <span class="count-new">${newValue.toLocaleString()}</span>
    </span>
    <span class="sr-only">${newValue.toLocaleString()}</span>
  `;
  requestAnimationFrame(() => element.classList.add("updating"));
  setTimeout(() => {
    element.classList.remove("updating");
    element.textContent = newValue.toLocaleString();
    element.dataset.value = String(newValue);
  }, 520);
}

function renderCounts() {
  const counts = state.snapshot.counts || {};
  const updates = [
    [selectors.rawCount, counts.raw_youtube_chat ?? 0],
    [selectors.nlpCount, counts.nlp_stream_results ?? 0],
    [selectors.alertCount, counts.alerts_polarization ?? 0],
    [selectors.validatedCount, counts.raw_youtube_chat ?? 0],
    [selectors.flinkEvents, counts.nlp_stream_results ?? 0],
    [selectors.riskAlerts, counts.alerts_polarization ?? 0],
    [selectors.sparkOutput, counts.spark_curated_rows ?? 0]
  ];

  for (const [el, val] of updates) {
    if (el) animateCount(el, val);
  }

  if (selectors.rawChatCount) animateCount(selectors.rawChatCount, counts.raw_youtube_chat ?? 0);
  if (selectors.filteredCount) animateCount(selectors.filteredCount, counts.filtered_messages ?? state.filteredQueue.length);

  if (selectors.dataMode) {
    const modeLabels = {
      disconnected: "desconectado",
      aws_streaming: "aws streaming",
      aws_delta: "aws delta",
      starting: "conectando"
    };
    selectors.dataMode.textContent = modeLabels[state.snapshot.data_mode] || "desconectado";
  }
}

// ── ACTORS ────────────────────────────────────────────────────────────────
function normalizeActorKey(rawActor) {
  if (!rawActor) return "";
  const actor = String(rawActor).toLowerCase();
  if (actor.includes("keiko") || actor.includes("fujimori")) return "keiko_fujimori_fp";
  if (actor.includes("onpe") || actor.includes("jne")) return "onpe_jne";
  if (actor.includes("castillo") || actor.includes("peru libre")) return "castillo_peru_libre";
  if (actor.includes("juntos") || actor === "jp" || actor.includes("jp_")) return "jp_juntos_por_el_peru";
  if (actor.includes("porky") || actor.includes("aliaga") || actor.includes("rla")) return "lopez_aliaga_porky";
  if (actor.includes("antauro")) return "antauro_humala";
  return actor.replace(/\s+/g, "_");
}

function seedActorTimeline() {
  if (state.actorTimeline.length) return;

  for (const actor of state.snapshot.actors || []) {
    const key = normalizeActorKey(actor.name);
    state.actorCounts[key] = Math.round(actor.mentions || actor.score || 0);
  }

  const now = Date.now();
  for (let i = 8; i >= 0; i--) {
    const scale = (9 - i) / 9;
    const values = {};
    for (const [key, value] of Object.entries(state.actorCounts)) {
      values[key] = Math.max(0, Math.round(value * scale));
    }
    const d = new Date(now - i * 10000);
    state.actorTimeline.push({
      ts: d.getTime(),
      label: d.toLocaleTimeString("es-PE", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      values
    });
  }
}

function updateActorTimeline(events) {
  let changed = false;
  for (const event of events) {
    const key = normalizeActorKey(event.payload?.actor);
    if (!key) continue;
    state.actorCounts[key] = (state.actorCounts[key] || 0) + 1;
    changed = true;
  }

  if (!state.actorTimeline.length || changed || state.chartTick % 2 === 0) {
    const now = new Date();
    state.actorTimeline.push({
      ts: now.getTime(),
      label: now.toLocaleTimeString("es-PE", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      values: { ...state.actorCounts }
    });
    state.actorTimeline = state.actorTimeline.slice(-90);
  }
}

function computeActors() {
  if (!Object.values(state.actorCounts).some((value) => value > 0) && !state.masterQueue.length) return [];
  const byName = {};
  for (const series of ACTOR_SERIES) {
    const value = state.actorCounts[series.key] || 0;
    byName[series.key] = { name: series.label, score: value, mentions: value };
  }

  for (const event of state.masterQueue) {
    const key = normalizeActorKey(event.payload?.actor);
    if (!key) continue;
    if (!byName[key]) byName[key] = { name: key.replace(/_/g, " "), score: 0, mentions: 0 };
    byName[key].mentions += 0.1;
    byName[key].score += event.payload?.local_risk_score_stream || 0.1;
  }

  return Object.values(byName).sort((a, b) => b.mentions - a.mentions).slice(0, 8);
}

function renderActors() {
  const actors = computeActors();
  const maxScore = Math.max(...actors.map((actor) => actor.score), 1);
  selectors.actorBars.innerHTML = actors
    .map((actor) => {
      const width = Math.max(6, Math.round((actor.score / maxScore) * 100));
      return `
        <div class="actor-row">
          <span>${escapeHtml(actor.name)}</span>
          <div class="bar-track"><div class="bar-fill" style="width:${width}%"></div></div>
          <strong>${Math.round(actor.mentions)}</strong>
        </div>
      `;
    })
    .join("");
}

// ── ALERTS ────────────────────────────────────────────────────────────────
function renderAlerts() {
  const alerts = state.snapshot.alerts || [];
  selectors.alertStack.innerHTML = alerts.map(alertItemHtml).join("");
  state.renderedAlertSeq = Math.max(0, ...alerts.map((alert) => alert.__stream_seq || 0));
}

function renderAlertsAppend(events) {
  if (!events.length) return;
  const nextEvents = events.filter((event) => (event.__stream_seq || 0) > state.renderedAlertSeq);
  if (!nextEvents.length) return;
  state.renderedAlertSeq = Math.max(...nextEvents.map((event) => event.__stream_seq || 0), state.renderedAlertSeq);
  selectors.alertStack.insertAdjacentHTML("beforeend", nextEvents.map(alertItemHtml).join(""));
  while (selectors.alertStack.children.length > 80) {
    selectors.alertStack.removeChild(selectors.alertStack.firstElementChild);
  }
}

function alertItemHtml(alert) {
  return `
    <article class="alert-card">
      <strong>${escapeHtml(alert.payload?.alert_type || alert.event_type || "risk_alert")} · ${escapeHtml(alert.payload?.severity || "stream")}</strong>
      <p>${escapeHtml(alert.payload?.message_text || alert.payload?.stream_text || alert.payload?.reason || "Alerta detectada")}</p>
      <div class="stream-tags">${eventTags(alert)
        .map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`)
        .join("")}</div>
    </article>
  `;
}

// ── SPARK ─────────────────────────────────────────────────────────────────
function renderSpark() {
  if (!selectors.sparkGrid) return;
  const jobs = state.snapshot.spark_jobs || [];
  selectors.sparkGrid.innerHTML = jobs
    .map(
      (job) => `
      <article class="spark-card">
        <strong>${escapeHtml(job.name)}</strong>
        <p>${escapeHtml(String(job.rows))} filas · ${escapeHtml(job.status)}</p>
        ${job.summary ? `<p>${escapeHtml(job.summary)}</p>` : ""}
        <p><code>${escapeHtml(job.output)}</code></p>
      </article>
    `
    )
    .join("");
}

function sparkBatchId(target) {
  return `batch_${String(target).padStart(7, "0")}`;
}

function sortedSparkBatches() {
  return Object.values(state.sparkBatch.batches || {})
    .sort((a, b) => Number(a.target_count || 0) - Number(b.target_count || 0));
}

function expectedSparkBatches() {
  const actual = state.sparkBatch.batches || {};
  const batchSize = Number(state.sparkBatchSize || 1000);
  const eligible = Number(state.awsStatus?.eligible_spark_target || 0);
  const maxTarget = Math.max(eligible, ...Object.values(actual).map((batch) => Number(batch.target_count || 0)), 0);
  const batches = [];
  for (let target = batchSize; target <= maxTarget; target += batchSize) {
    const id = sparkBatchId(target);
    batches.push(actual[id] || {
      batch_id: id,
      target_count: target,
      status: "pending",
      current_job: "pending",
      message: "Esperando turno de ejecucion Spark",
      rows: 0,
      jobs: {},
      outputs: {}
    });
  }
  return batches;
}

function updateSparkMetricFromBatches() {
  const completed = sortedSparkBatches().filter((batch) => batch.status === "done");
  const maxCompleted = completed.reduce((max, batch) => Math.max(max, Number(batch.target_count || batch.rows || 0)), 0);
  state.sessionCounts.spark_curated_rows = maxCompleted;
  state.snapshot.counts = { ...state.snapshot.counts, spark_curated_rows: maxCompleted };
  renderCounts();
}

function renderSparkBatch() {
  if (!selectors.sparkBatchSummary || !selectors.sparkBatchGrid || !selectors.sparkBatchStatus) return;

  const actualBatches = sortedSparkBatches();
  const batches = expectedSparkBatches();
  const latest = batches[batches.length - 1];
  const rawSeen = state.awsStatus?.counts?.raw_youtube_chat?.total
    ?? state.snapshot.counts?.raw_youtube_chat
    ?? 0;
  const dataSize = state.dataSize || "DATA_SIZE";
  const batchSize = state.sparkBatchSize || 1000;
  const eligible = state.awsStatus?.eligible_spark_target || 0;
  const rawLag = state.liveCursor ? "" : "";
  const running = batches.find((batch) => batch.status === "running" || batch.status === "queued");
  const statusText = running
    ? `${running.batch_id}: ${running.current_job || "running"}`
    : actualBatches.length
      ? `${actualBatches[actualBatches.length - 1].batch_id}: ${actualBatches[actualBatches.length - 1].status}`
      : "en espera";

  selectors.sparkBatchStatus.textContent = statusText;
  selectors.sparkBatchSummary.innerHTML = `
    <div class="spark-stat"><span>raw visto</span><strong>${escapeHtml(String(rawSeen))}</strong></div>
    <div class="spark-stat"><span>data size</span><strong>${escapeHtml(String(dataSize))}</strong></div>
    <div class="spark-stat"><span>batch size</span><strong>${escapeHtml(String(batchSize))}</strong></div>
    <div class="spark-stat"><span>elegible</span><strong>${escapeHtml(String(eligible))}</strong></div>
  `;

  if (!batches.length && !state.sparkStartInFlight) {
    selectors.sparkBatchGrid.innerHTML = '<p class="stream-empty">Spark esperara al primer bloque de 1,000 eventos.</p>';
    return;
  }

  const pendingCards = state.sparkStartInFlight
    ? '<article class="spark-card is-running"><strong>Agendando batch Spark</strong><p>Preparando ejecucion remota.</p></article>'
    : "";
  selectors.sparkBatchGrid.innerHTML = pendingCards + batches
    .map((batch) => {
      const jobs = batch.jobs || {};
      const jobRows = ["job1_raw_from_kafka", "job2_rules", "job4_offendes", "job5_hybrid"]
        .map((jobName) => {
          const job = jobs[jobName] || {};
          return `<span class="spark-job ${escapeHtml(job.status || "pending")}">${escapeHtml(jobName.replaceAll("_", " "))}: ${escapeHtml(job.status || "pending")}</span>`;
        })
        .join("");
      const outputs = batch.outputs || {};
      const isDone = batch.status === "done";
      const selectedClass = state.selectedSparkBatchId === batch.batch_id ? "selected" : "";
      const title = isDone ? "Abrir comentarios ofensivos filtrados por Spark" : "Disponible cuando el batch termine";
      return `
        <article class="spark-card ${escapeHtml(batch.status || "queued")} ${selectedClass}" data-batch-id="${escapeHtml(batch.batch_id || "")}" data-clickable="${isDone ? "true" : "false"}" title="${escapeHtml(title)}">
          <strong>${escapeHtml(batch.batch_id || "batch")} · ${escapeHtml(batch.status || "queued")}</strong>
          <p>Target ${escapeHtml(String(batch.target_count || 0))} · rows ${escapeHtml(String(batch.rows || 0))}</p>
          <p>${escapeHtml(batch.message || batch.current_job || "Esperando ejecucion")}</p>
          <div class="spark-jobs">${jobRows}</div>
          ${outputs.hybrid ? `<p><code>${escapeHtml(outputs.hybrid)}</code></p>` : ""}
        </article>
      `;
    })
    .join("");
}

function sparkCommentMatchesQuery(comment) {
  const query = state.sparkCommentsQuery.trim().toLowerCase();
  if (!query) return true;
  return [
    comment.author,
    comment.message,
    comment.binary_label,
    comment.multiclass_label,
    comment.risk_level,
    comment.risk_reason,
    comment.local_rule_tags
  ].join(" ").toLowerCase().includes(query);
}

function sparkCommentTags(comment) {
  return [
    comment.risk_level,
    comment.binary_label,
    comment.multiclass_label,
    ...(String(comment.local_rule_tags || "").split("|"))
  ].filter(Boolean).slice(0, 5);
}

function sparkCommentHtml(comment) {
  const tags = sparkCommentTags(comment).map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join("");
  const confidence = Number(comment.confidence || 0);
  return `
    <article class="stream-item spark">
      <div class="stream-copy">
        <strong>${escapeHtml(comment.author || "youtube_user")}</strong>
        <p>${escapeHtml(comment.message || "")}</p>
        <p class="category-line">Categoria: ${escapeHtml(comment.multiclass_label || comment.binary_label || "ofensivo")} · confianza ${(confidence * 100).toFixed(1)}%</p>
        <div class="stream-tags">${tags}</div>
      </div>
    </article>
  `;
}

function renderSparkComments() {
  if (!selectors.sparkCommentsList) return;
  if (state.sparkCommentsLoading) {
    selectors.sparkCommentsList.innerHTML = '<p class="stream-empty">Cargando comentarios Spark desde S3...</p>';
    return;
  }

  const visible = state.sparkComments.filter(sparkCommentMatchesQuery);
  selectors.sparkCommentsList.innerHTML = visible.length
    ? visible.map(sparkCommentHtml).join("")
    : '<p class="stream-empty">No hay comentarios ofensivos para este filtro.</p>';
  if (selectors.sparkCommentsCount) animateCount(selectors.sparkCommentsCount, visible.length);
}

function drawTimeline() {
  seedActorTimeline();
  if (state.chartAnimation) cancelAnimationFrame(state.chartAnimation);
  const start = performance.now();
  const duration = state.actorTimeline.length > 2 ? 520 : 0;
  state.chartHeadProgress = duration ? 0 : 1;

  function frame(now) {
    state.chartHeadProgress = duration ? Math.min(1, (now - start) / duration) : 1;
    drawActorTimeline();
    if (state.chartHeadProgress < 1) {
      state.chartAnimation = requestAnimationFrame(frame);
    } else {
      state.chartAnimation = null;
    }
  }

  if (duration) {
    state.chartAnimation = requestAnimationFrame(frame);
  } else {
    drawActorTimeline();
  }
}

function drawActorTimeline() {
  const canvas = selectors.timelineChart;
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const data = interpolateTimelineHead(state.actorTimeline, state.chartHeadProgress);
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "rgba(255,255,255,0.025)";
  ctx.fillRect(0, 0, w, h);

  if (!data.length) return;

  const padLeft = 46;
  const padRight = 22;
  const padTop = 18;
  const padBottom = 30;
  const plotW = w - padLeft - padRight;
  const plotH = h - padTop - padBottom;
  const activeSeries = ACTOR_SERIES.filter((series) =>
    data.some((point) => (point.values[series.key] || 0) > 0)
  );
  const maxValue = Math.max(
    5,
    ...data.flatMap((point) => activeSeries.map((series) => point.values[series.key] || 0))
  );

  ctx.strokeStyle = "rgba(240,200,107,0.1)";
  ctx.lineWidth = 1;
  for (let i = 0; i < 4; i++) {
    const y = padTop + (i * plotH) / 3;
    ctx.beginPath();
    ctx.moveTo(padLeft, y);
    ctx.lineTo(w - padRight, y);
    ctx.stroke();
  }

  const points = data.slice(-90);
  const pointStep = plotW / Math.max(points.length - 1, 1);

  for (const series of activeSeries) {
    ctx.beginPath();
    points.forEach((point, index) => {
      const x = padLeft + index * pointStep;
      const value = point.values[series.key] || 0;
      const y = padTop + plotH - (value / maxValue) * plotH;
      if (index === 0) ctx.moveTo(x, y);
      else {
        const previous = points[index - 1];
        const prevX = padLeft + (index - 1) * pointStep;
        const prevY = padTop + plotH - ((previous.values[series.key] || 0) / maxValue) * plotH;
        const midX = (prevX + x) / 2;
        ctx.bezierCurveTo(midX, prevY, midX, y, x, y);
      }
    });
    ctx.strokeStyle = series.color;
    ctx.lineWidth = 2.7;
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.shadowColor = series.color;
    ctx.shadowBlur = 8;
    ctx.stroke();
    ctx.shadowBlur = 0;

    const last = points[points.length - 1];
    const lastValue = last?.values[series.key] || 0;
    const lastY = padTop + plotH - (lastValue / maxValue) * plotH;
    ctx.fillStyle = series.color;
    ctx.beginPath();
    ctx.arc(padLeft + plotW, lastY, 4, 0, Math.PI * 2);
    ctx.fill();
  }

  ctx.fillStyle = "rgba(245,242,223,0.55)";
  ctx.font = "11px Cascadia Code, Consolas, monospace";
  ctx.fillText(points[0]?.label?.slice(0, 5) || "", padLeft, h - 10);
  ctx.fillText(points[points.length - 1]?.label?.slice(0, 5) || "", w - padRight - 34, h - 10);

  if (state.chartHover) {
    const index = Math.max(
      0,
      Math.min(points.length - 1, Math.round((state.chartHover.x - padLeft) / pointStep))
    );
    const point = points[index];
    const x = padLeft + index * pointStep;

    ctx.strokeStyle = "rgba(245,242,223,0.22)";
    ctx.beginPath();
    ctx.moveTo(x, padTop);
    ctx.lineTo(x, padTop + plotH);
    ctx.stroke();

    const rows = activeSeries.map((series) => ({
      ...series,
      value: point.values[series.key] || 0
    }));
    const boxW = 260;
    const boxH = 28 + rows.length * 18;
    const boxX = Math.min(Math.max(12, x - boxW / 2), w - boxW - 12);
    const boxY = 10;
    ctx.fillStyle = "rgba(5,17,14,0.92)";
    ctx.strokeStyle = "rgba(240,200,107,0.22)";
    roundRect(ctx, boxX, boxY, boxW, boxH, 12);
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = "#f0c86b";
    ctx.font = "11px Cascadia Code, Consolas, monospace";
    ctx.fillText(point.label || "timeline", boxX + 12, boxY + 18);
    rows.forEach((row, rowIndex) => {
      const y = boxY + 38 + rowIndex * 18;
      ctx.fillStyle = row.color;
      ctx.beginPath();
      ctx.arc(boxX + 14, y - 4, 4, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = "#f5f2df";
      ctx.fillText(`${row.label}: ${row.value}`, boxX + 26, y);
    });
  }
}

function interpolateTimelineHead(points, progress) {
  if (points.length < 2 || progress >= 1) return points;
  const copy = points.slice();
  const previous = copy[copy.length - 2];
  const current = copy[copy.length - 1];
  const eased = 1 - Math.pow(1 - progress, 3);
  const values = {};

  for (const series of ACTOR_SERIES) {
    const from = previous.values[series.key] || 0;
    const to = current.values[series.key] || 0;
    values[series.key] = from + (to - from) * eased;
  }

  copy[copy.length - 1] = { ...current, values };
  return copy;
}

function roundRect(ctx, x, y, width, height, radius) {
  ctx.beginPath();
  ctx.moveTo(x + radius, y);
  ctx.arcTo(x + width, y, x + width, y + height, radius);
  ctx.arcTo(x + width, y + height, x, y + height, radius);
  ctx.arcTo(x, y + height, x, y, radius);
  ctx.arcTo(x, y, x + width, y, radius);
  ctx.closePath();
}

// ── ESCAPE HTML ───────────────────────────────────────────────────────────
function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

// ── EVENT BINDINGS ────────────────────────────────────────────────────────
function bindEvents() {
  let searchDebounce = null;
  selectors.searchInput.addEventListener("input", (event) => {
    state.query = event.target.value;
    clearTimeout(searchDebounce);
    if (state.query.length <= 2) {
      renderFilteredMessages();
    } else {
      searchDebounce = setTimeout(renderFilteredMessages, 180);
    }
  });

  if (selectors.sparkCommentsSearch) {
    selectors.sparkCommentsSearch.addEventListener("input", (event) => {
      state.sparkCommentsQuery = event.target.value;
      renderSparkComments();
    });
  }

  selectors.playStream.addEventListener("click", () => {
    state.playing = !state.playing;
    selectors.playStream.textContent = state.playing ? "Pausar cinta" : "Reanudar cinta";
    if (!state.playing) renderStream({ reset: true });
  });

  selectors.refreshDemo.addEventListener("click", () => {
    requestAwsStop();
  });

  selectors.syncAws.addEventListener("click", requestAwsSync);

  if (selectors.sparkBatchGrid) {
    selectors.sparkBatchGrid.addEventListener("click", (event) => {
      const card = event.target.closest(".spark-card[data-batch-id]");
      if (!card || card.dataset.clickable !== "true") return;
      fetchSparkComments(card.dataset.batchId);
    });
  }

  if (selectors.timelineChart) {
    selectors.timelineChart.addEventListener("mousemove", (event) => {
      const rect = selectors.timelineChart.getBoundingClientRect();
      const scaleX = selectors.timelineChart.width / rect.width;
      const scaleY = selectors.timelineChart.height / rect.height;
      state.chartHover = {
        x: (event.clientX - rect.left) * scaleX,
        y: (event.clientY - rect.top) * scaleY
      };
      drawActorTimeline();
    });

    selectors.timelineChart.addEventListener("mouseleave", () => {
      state.chartHover = null;
      drawActorTimeline();
    });
  }
}

// ── RENDER ALL ────────────────────────────────────────────────────────────
function renderAll() {
  renderCounts();
  renderActors();
  renderAlerts();
  renderSpark();
  renderSparkBatch();
  renderSparkComments();
  drawTimeline();
  renderStream({ reset: true });
  renderFilteredMessages();
}

// ── INIT ──────────────────────────────────────────────────────────────────
async function init() {
  resetStreamingState();
  renderAll();
  bindEvents();
  await restoreAwsSessionIfRunning();
}

init();
