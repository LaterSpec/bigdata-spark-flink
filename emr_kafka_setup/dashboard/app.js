const fallbackSnapshot = {
  generated_at: "2026-06-17T02:09:35Z",
  environment: {
    emr_master: "ip-172-31-11-3",
    kafka: "3.6.2",
    flink: "1.17.1-amzn-1",
    spark: "3.4.1-amzn-2",
    bootstrap: "ip-172-31-11-3.ec2.internal:9092"
  },
  counts: {
    raw_youtube_chat: 105,
    nlp_stream_results: 213,
    alerts_polarization: 3,
    spark_curated_rows: 105,
    spark_aggregates_rows: 54
  },
  offsets: {
    raw_youtube_chat: [
      { partition: 0, offset: 34 },
      { partition: 1, offset: 31 },
      { partition: 2, offset: 40 }
    ],
    nlp_stream_results: [
      { partition: 0, offset: 94 },
      { partition: 1, offset: 39 },
      { partition: 2, offset: 80 }
    ],
    alerts_polarization: [
      { partition: 0, offset: 0 },
      { partition: 1, offset: 0 },
      { partition: 2, offset: 3 }
    ]
  },
  stream_events: [
    {
      source: "stream",
      job_name: "flink_job1_normalize_stream",
      event_type: "normalized_comment",
      source_partition: 2,
      source_offset: 0,
      payload: {
        event_id: "3fc23294c70ae409e6bd2280993ab85a",
        stream_text: "como keiko esta ganando si en el extranjeto los peruanos halla dicen que no han podido votar en argentina y espana y otros paises",
        message_length: 129,
        is_empty_message: false
      }
    },
    {
      source: "stream",
      job_name: "flink_job1_normalize_stream",
      event_type: "normalized_comment",
      source_partition: 2,
      source_offset: 2,
      payload: {
        event_id: "cb5b643f5d47974a4ccd27e46a2c84fd",
        stream_text: "mejor cuenten todos los votos como acto se transparencia, porque la onpe desde la primera vuelta ya es dudosa",
        message_length: 109,
        is_empty_message: false
      }
    },
    {
      source: "stream",
      job_name: "flink_job3_political_signals",
      event_type: "political_signals",
      source_partition: 1,
      source_offset: 8,
      payload: {
        event_id: "signal-001",
        stream_text: "onpe y jne deben explicar el conteo de votos y las actas observadas",
        local_rule_tags: "electoral_institution|fraude|political_mention",
        local_risk_score_stream: 3
      }
    },
    {
      source: "stream",
      job_name: "flink_job4_actor_polarization",
      event_type: "actor_polarization_window",
      source_partition: 0,
      source_offset: 18,
      payload: {
        actor: "keiko_fujimori_fp",
        mention_count: 7,
        insult_count: 2,
        fraud_count: 1,
        terruqueo_count: 1,
        polarization_score: 4.2
      }
    },
    {
      source: "spark",
      job_name: "spark_hybrid_scoring_from_kafka",
      event_type: "hybrid_batch_result",
      source_partition: 2,
      source_offset: 12,
      payload: {
        stream_text: "comentario evaluado por reglas locales y modelo OffendES",
        hybrid_risk_level: "medium",
        hybrid_risk_reason: "reglas locales fuertes + ofensividad general",
        confidence_binary_bucket: "medium"
      }
    }
  ],
  alerts: [
    {
      source: "alert",
      job_name: "flink_job5_risk_alerts",
      event_type: "risk_alert",
      source_partition: 2,
      source_offset: 12,
      payload: {
        alert_id: "1c353d93-b278-4d1d-97b6-315895cfda2e",
        alert_type: "terruqueo_plus_insult",
        severity: "medium",
        reason: "terruqueo|electoral_institution|political_mention|general_insult",
        actor: "keiko_fujimori_fp",
        message_text: "ROBERTO COMUNISTA SANCHEZ NO A GANADO EN EL PERU, SOLO TIENE VOTOS DE ANTIFUJIMORISMO...",
        local_risk_score_stream: 4
      }
    },
    {
      source: "alert",
      job_name: "flink_job5_risk_alerts",
      event_type: "risk_alert",
      source_partition: 1,
      source_offset: 10,
      payload: {
        alert_id: "2e0d3aad-b98c-4193-b471-a73a0d2c41e9",
        alert_type: "terruqueo_plus_insult",
        severity: "medium",
        reason: "terruqueo|electoral_institution|political_mention|general_insult",
        actor: "keiko_fujimori_fp",
        message_text: "ROBERTO COMUNISTA SANCHEZ NO A GANADO EN EL PERU...",
        local_risk_score_stream: 4
      }
    },
    {
      source: "alert",
      job_name: "flink_job5_risk_alerts",
      event_type: "risk_alert",
      source_partition: 1,
      source_offset: 11,
      payload: {
        alert_id: "90f6ac06-a926-45aa-b115-def695e64081",
        alert_type: "terruqueo_plus_insult",
        severity: "medium",
        reason: "terruqueo|electoral_institution|political_mention|general_insult",
        actor: "keiko_fujimori_fp",
        message_text: "ROBERTO COMUNISTA SANCHEZ NO A GANADO EN EL PERU...",
        local_risk_score_stream: 4
      }
    }
  ],
  spark_jobs: [
    {
      name: "Job 1 Kafka Raw Ingest",
      rows: 105,
      output: "s3://figuretibucket/output/kafka_to_spark/raw_youtube_chat_test/",
      status: "SUCCESS"
    },
    {
      name: "Job 2 Reglas locales",
      rows: 105,
      output: "s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/",
      status: "SUCCESS"
    },
    {
      name: "Job 4 OffendES inference",
      rows: 105,
      output: "s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/",
      status: "SUCCESS"
    },
    {
      name: "Job 5 Hibrido + agregados",
      rows: 105,
      output: "s3://figuretibucket/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/",
      status: "SUCCESS"
    }
  ],
  actors: [
    { name: "Keiko / Fujimori / FP", score: 84, mentions: 12 },
    { name: "ONPE / JNE", score: 64, mentions: 9 },
    { name: "Castillo / Peru Libre", score: 48, mentions: 7 },
    { name: "JP / Juntos por el Peru", score: 28, mentions: 4 },
    { name: "RLA / Porky", score: 18, mentions: 2 }
  ],
  timeline: [
    { label: "00:00", comments: 8, alerts: 0 },
    { label: "00:01", comments: 15, alerts: 1 },
    { label: "00:02", comments: 23, alerts: 1 },
    { label: "00:03", comments: 19, alerts: 0 },
    { label: "00:04", comments: 31, alerts: 1 },
    { label: "00:05", comments: 9, alerts: 0 }
  ]
};

// ── STATE ─────────────────────────────────────────────────────────────────
let state = {
  snapshot: fallbackSnapshot,
  filter: "all",
  query: "",
  playing: true,
  pointer: 0,
  // Ring buffer: all emitted events regardless of active filter.
  // Filters are applied at render time, never on emit.
  masterQueue: [],
  MAX_QUEUE: 400,
  chartTick: 0
};

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
  actorBars: document.querySelector("#actor-bars"),
  alertStack: document.querySelector("#alert-stack"),
  sparkGrid: document.querySelector("#spark-grid"),
  timelineChart: document.querySelector("#timeline-chart"),
  searchInput: document.querySelector("#search-input"),
  filterButtons: document.querySelectorAll(".filter-button"),
  playStream: document.querySelector("#play-stream"),
  refreshDemo: document.querySelector("#refresh-demo"),
  syncAws: document.querySelector("#sync-aws"),
  syncStatus: document.querySelector("#sync-status"),
  dataMode: document.querySelector("#data-mode")
};

// ── DATA LOADING ──────────────────────────────────────────────────────────
async function loadSnapshot() {
  try {
    const response = await fetch("./data/dashboard_snapshot.json", { cache: "no-store" });
    if (response.ok) {
      state.snapshot = await response.json();
    }
  } catch {
    state.snapshot = fallbackSnapshot;
  }
  await mergeSyncedAwsFiles();
}

async function mergeSyncedAwsFiles() {
  const [counts, nlpEvents, alerts] = await Promise.all([
    fetchJson("./data/dashboard_counts.json"),
    fetchJsonLines("./data/flink_nlp_stream_results_sample.jsonl"),
    fetchJsonLines("./data/flink_alerts_sample.jsonl")
  ]);
  const sparkSummaries = await Promise.all([
    fetchText("./data/spark_job2_rules_summary.md"),
    fetchText("./data/spark_job4_ml_summary.md"),
    fetchText("./data/spark_job5_hybrid_summary.md")
  ]);

  if (counts?.counts) {
    state.snapshot.counts = { ...state.snapshot.counts, ...counts.counts };
    state.snapshot.generated_at = counts.generated_at || state.snapshot.generated_at;
    state.snapshot.data_mode = "aws_synced";
  }

  if (nlpEvents.length) {
    state.snapshot.stream_events = nlpEvents.map((event) => ({
      source: "stream",
      ...event
    }));
  }

  if (alerts.length) {
    state.snapshot.alerts = alerts.map((event) => ({
      source: "alert",
      ...event
    }));
  }

  const parsedSparkJobs = parseSparkSummaries(sparkSummaries);
  if (parsedSparkJobs.length) {
    state.snapshot.spark_jobs = parsedSparkJobs;
  }
}

async function requestAwsSync() {
  selectors.syncAws.disabled = true;
  selectors.syncAws.textContent = "Sincronizando...";
  setSyncStatus("Consultando EMR por SSH y actualizando JSONL locales...", "ok");

  try {
    const response = await fetch("/api/sync", { method: "POST" });
    const result = await response.json();
    if (!response.ok || !result.ok) {
      throw new Error(result.error || "No se pudo sincronizar AWS");
    }
    // Reset stream buffer on successful sync so new data flows in fresh
    state.masterQueue = [];
    state.pointer = 0;
    await loadSnapshot();
    renderAll();
    setSyncStatus(`AWS sincronizado: ${state.snapshot.generated_at || "snapshot actualizado"}`, "ok");
  } catch (error) {
    setSyncStatus(`AWS no disponible ahora: ${error.message}. Se mantiene snapshot local validado.`, "error");
  } finally {
    selectors.syncAws.disabled = false;
    selectors.syncAws.textContent = "Sincronizar AWS";
  }
}

function setSyncStatus(message, mode = "") {
  selectors.syncStatus.textContent = message;
  selectors.syncStatus.classList.remove("ok", "error");
  if (mode) selectors.syncStatus.classList.add(mode);
}

// ── FETCH HELPERS ─────────────────────────────────────────────────────────
async function fetchJson(path) {
  try {
    const response = await fetch(path, { cache: "no-store" });
    if (!response.ok) return null;
    return response.json();
  } catch {
    return null;
  }
}

async function fetchJsonLines(path) {
  try {
    const response = await fetch(path, { cache: "no-store" });
    if (!response.ok) return [];
    const text = await response.text();
    return text
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .filter(Boolean);
  } catch {
    return [];
  }
}

async function fetchText(path) {
  try {
    const response = await fetch(path, { cache: "no-store" });
    if (!response.ok) return "";
    return response.text();
  } catch {
    return "";
  }
}

function parseSparkSummaries(summaries) {
  const descriptors = [
    {
      name: "Job 2 Reglas locales",
      output: "s3://figuretibucket/output/batch/from_kafka/job2_rules/run_rules_kafka_test/"
    },
    {
      name: "Job 4 OffendES inference",
      output: "s3://figuretibucket/output/batch/from_kafka/job4_ml_inference/run_sparkml_offendes_kafka_test/"
    },
    {
      name: "Job 5 Hibrido + agregados",
      output: "s3://figuretibucket/output/batch/from_kafka/job5_hybrid/run_hybrid_kafka_test/"
    }
  ];

  return summaries
    .map((summary, index) => {
      if (!summary) return null;
      const rows = Number(
        (summary.match(/(?:OUTPUT_ROWS|Output rows|Rows|VALIDATION_COUNT)\D+(\d+)/i) || [])[1] || 105
      );
      return {
        ...descriptors[index],
        rows,
        status: "SUCCESS",
        summary: summary.replace(/\s+/g, " ").slice(0, 220)
      };
    })
    .filter(Boolean);
}

// ── EVENT HELPERS ─────────────────────────────────────────────────────────
function eventText(event) {
  const payload = event.payload || {};
  return (
    payload.stream_text ||
    payload.message_text ||
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
  return tags.filter(Boolean).slice(0, 4);
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

  // Emit 3 events per tick for large datasets (7k cycles in ~8 min at 500ms)
  for (let i = 0; i < 3; i++) {
    const next = pool[state.pointer % pool.length];
    state.masterQueue = [next, ...state.masterQueue].slice(0, state.MAX_QUEUE);
    state.pointer++;
  }

  renderStream();

  // Recalculate charts every 15 ticks (~7.5 s)
  state.chartTick++;
  if (state.chartTick % 15 === 0) {
    renderActors();
    drawTimeline();
  }
}

function renderStream() {
  const query = state.query.trim().toLowerCase();

  const visible = state.masterQueue.filter((event) => {
    // Filter by source type
    const byFilter = state.filter === "all" || event.source === state.filter;
    if (!byFilter) return false;

    // Search only in human-readable fields (not raw JSON)
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
  });

  if (!visible.length && state.masterQueue.length > 0) {
    selectors.streamList.innerHTML =
      '<p style="color:var(--muted);font-size:0.86rem;padding:12px 4px;">Sin resultados para el filtro actual.</p>';
    return;
  }

  selectors.streamList.innerHTML = visible
    .slice(0, 22)
    .map((event) => {
      const tags = eventTags(event)
        .map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`)
        .join("");
      return `
        <article class="stream-item ${escapeHtml(event.source || "stream")}">
          <div class="stream-copy">
            <strong>${escapeHtml(event.job_name || "pipeline_event")}</strong>
            <p>${escapeHtml(eventText(event))}</p>
            <div class="stream-tags">${tags}</div>
          </div>
          <span class="offset-pill">p${escapeHtml(String(event.source_partition ?? "-"))} / o${escapeHtml(String(event.source_offset ?? "-"))}</span>
        </article>
      `;
    })
    .join("");
}

// ── COUNT-UP ANIMATION ────────────────────────────────────────────────────
function animateCount(element, newValue) {
  const current = parseInt(String(element.textContent).replace(/[^0-9]/g, ""), 10) || 0;
  if (current === newValue) return;

  const diff = newValue - current;
  const steps = Math.min(Math.abs(diff), 40);
  const stepValue = diff / steps;
  let step = 0;

  element.classList.add("updating");
  setTimeout(() => element.classList.remove("updating"), 420);

  const timer = setInterval(() => {
    step++;
    element.textContent = Math.round(current + stepValue * step).toLocaleString();
    if (step >= steps) {
      clearInterval(timer);
      element.textContent = newValue.toLocaleString();
    }
  }, 22);
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

  if (selectors.dataMode) {
    selectors.dataMode.textContent =
      state.snapshot.data_mode === "aws_synced" ? "aws sincronizado" : "snapshot local";
  }
}

// ── ACTORS ────────────────────────────────────────────────────────────────
function computeActors() {
  // Start from snapshot base actors
  const base = (state.snapshot.actors || []).map((a) => ({ ...a }));
  const byName = {};
  for (const a of base) {
    byName[a.name] = { name: a.name, score: a.score, mentions: a.mentions };
  }

  // Augment from live masterQueue events that have actor data
  for (const event of state.masterQueue) {
    const actor = event.payload?.actor;
    if (!actor) continue;
    // Try to match to existing actor by normalized key
    const actorNorm = actor.replace(/_/g, " ").toLowerCase();
    const matched = Object.keys(byName).find((name) => name.toLowerCase().includes(actorNorm.split(" ")[0]));
    if (matched) {
      byName[matched].mentions += 0.5;
      byName[matched].score += event.payload?.local_risk_score_stream || 0.5;
    }
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
  selectors.alertStack.innerHTML = alerts
    .map(
      (alert) => `
      <article class="alert-card">
        <strong>${escapeHtml(alert.payload?.alert_type || "risk_alert")} · ${escapeHtml(alert.payload?.severity || "medium")}</strong>
        <p>${escapeHtml(alert.payload?.message_text || alert.payload?.reason || "Alerta detectada")}</p>
        <div class="stream-tags">${eventTags(alert)
          .map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`)
          .join("")}</div>
      </article>
    `
    )
    .join("");
}

// ── SPARK ─────────────────────────────────────────────────────────────────
function renderSpark() {
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

// ── TIMELINE CHART ────────────────────────────────────────────────────────
function computeTimeline() {
  // Use live masterQueue to build timeline buckets when we have enough data
  const events = state.masterQueue;
  if (events.length < 10) return state.snapshot.timeline || [];

  const buckets = {};
  for (const event of events) {
    const ts = event.processing_ts || event.payload?.created_at;
    let label = "00:00";
    if (ts) {
      const d = new Date(ts);
      if (!Number.isNaN(d.getTime())) {
        const mm = String(d.getUTCMinutes()).padStart(2, "0");
        const ss = String(Math.floor(d.getUTCSeconds() / 10) * 10).padStart(2, "0");
        label = `${mm}:${ss}`;
      }
    }
    if (!buckets[label]) buckets[label] = { label, comments: 0, alerts: 0 };
    buckets[label].comments++;
    if (event.source === "alert") buckets[label].alerts++;
  }

  return Object.values(buckets)
    .sort((a, b) => a.label.localeCompare(b.label))
    .slice(-20);
}

function drawTimeline() {
  const canvas = selectors.timelineChart;
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const data = computeTimeline();
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "rgba(255,255,255,0.03)";
  ctx.fillRect(0, 0, w, h);

  if (!data.length) return;

  const pad = 34;
  const maxComments = Math.max(...data.map((p) => p.comments), 1);
  const step = (w - pad * 2) / Math.max(data.length - 1, 1);

  // Grid lines
  ctx.strokeStyle = "rgba(240,200,107,0.1)";
  ctx.lineWidth = 1;
  for (let i = 0; i < 4; i++) {
    const y = pad + (i * (h - pad * 2)) / 3;
    ctx.beginPath();
    ctx.moveTo(pad, y);
    ctx.lineTo(w - pad, y);
    ctx.stroke();
  }

  // Area fill
  ctx.beginPath();
  data.forEach((point, index) => {
    const x = pad + index * step;
    const y = h - pad - (point.comments / maxComments) * (h - pad * 2);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.lineTo(pad + (data.length - 1) * step, h - pad);
  ctx.lineTo(pad, h - pad);
  ctx.closePath();
  ctx.fillStyle = "rgba(49,209,139,0.07)";
  ctx.fill();

  // Line
  ctx.beginPath();
  data.forEach((point, index) => {
    const x = pad + index * step;
    const y = h - pad - (point.comments / maxComments) * (h - pad * 2);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = "#31d18b";
  ctx.lineWidth = 3;
  ctx.stroke();

  // Dots and labels
  data.forEach((point, index) => {
    const x = pad + index * step;
    const y = h - pad - (point.comments / maxComments) * (h - pad * 2);
    ctx.fillStyle = point.alerts > 0 ? "#ff5a3d" : "#f0c86b";
    ctx.beginPath();
    ctx.arc(x, y, point.alerts > 0 ? 6 : 4, 0, Math.PI * 2);
    ctx.fill();
    if (index % Math.max(1, Math.floor(data.length / 8)) === 0) {
      ctx.fillStyle = "rgba(245,242,223,0.55)";
      ctx.font = "11px Cascadia Code, Consolas, monospace";
      ctx.fillText(point.label, x - 16, h - 8);
    }
  });
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
    // Immediate re-render for short queries, debounce for longer ones
    if (state.query.length <= 2) {
      renderStream();
    } else {
      searchDebounce = setTimeout(renderStream, 180);
    }
  });

  selectors.filterButtons.forEach((button) => {
    button.addEventListener("click", () => {
      selectors.filterButtons.forEach((item) => item.classList.remove("active"));
      button.classList.add("active");
      state.filter = button.dataset.filter;
      // Do NOT reset masterQueue or pointer — just re-render the existing buffer
      // with the new filter applied. Events that already passed stay visible.
      renderStream();
    });
  });

  selectors.playStream.addEventListener("click", () => {
    state.playing = !state.playing;
    selectors.playStream.textContent = state.playing ? "Pausar cinta" : "Reanudar cinta";
    if (!state.playing) renderStream();
  });

  selectors.refreshDemo.addEventListener("click", () => {
    state.masterQueue = [];
    state.pointer = 0;
    renderStream();
  });

  selectors.syncAws.addEventListener("click", requestAwsSync);
}

// ── RENDER ALL ────────────────────────────────────────────────────────────
function renderAll() {
  renderCounts();
  renderActors();
  renderAlerts();
  renderSpark();
  drawTimeline();
  renderStream();
}

// ── INIT ──────────────────────────────────────────────────────────────────
async function init() {
  await loadSnapshot();
  renderAll();
  bindEvents();
  tickStream();
  // 3 events per tick at 500ms = ~6 events/s; 7000-event pool cycles in ~19 min
  window.setInterval(tickStream, 500);
}

init();
