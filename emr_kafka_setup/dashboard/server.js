const http = require("http");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const { spawn } = require("child_process");

const root = __dirname;
const port = Number(process.env.PORT || 8787);
const host = process.env.HOST || "127.0.0.1";
const runtimeDir = path.join(root, "data", "runtime");
const platformStatePath = path.join(runtimeDir, "platform-state.json");
let awsStarted = false;
let awsStarting = false;
let stopping = false;
let startupAttempt = 0;
const activeAwsStartupProcesses = new Set();
const activeSparkLaunchers = new Set();
const sparkLaunchStates = new Map();
let latestSparkBatches = {};
const persistedPlatformState = readPlatformState();
let awsStartupState = {
  status: persistedPlatformState.status === "stopped" ? "stopped" : "idle",
  stage: persistedPlatformState.status === "stopped" ? "stopped" : "idle",
  message: "La plataforma está detenida.",
  started_at: "",
  updated_at: new Date().toISOString(),
  data_size: 0,
  spark_batch_size: 1000,
  spark_max_concurrency: 1,
  stdout_tail: "",
  stderr_tail: ""
};

function readPlatformState() {
  try {
    return JSON.parse(fs.readFileSync(platformStatePath, "utf8"));
  } catch {
    return {
      status: "stopped",
      next_start_mode: "fresh",
      updated_at: new Date().toISOString()
    };
  }
}

function writePlatformState(patch) {
  fs.mkdirSync(runtimeDir, { recursive: true });
  const next = {
    ...readPlatformState(),
    ...patch,
    updated_at: new Date().toISOString()
  };
  const temporaryPath = `${platformStatePath}.tmp`;
  fs.writeFileSync(temporaryPath, JSON.stringify(next, null, 2));
  fs.renameSync(temporaryPath, platformStatePath);
  return next;
}

function shellCommand() {
  if (process.platform !== "win32") return "bash";
  if (process.env.DASHBOARD_BASH && fs.existsSync(process.env.DASHBOARD_BASH)) {
    return process.env.DASHBOARD_BASH;
  }
  const candidates = [
    "C:\\Program Files\\Git\\bin\\bash.exe",
    "C:\\Program Files (x86)\\Git\\bin\\bash.exe",
    process.env.ProgramW6432 && `${process.env.ProgramW6432}\\Git\\bin\\bash.exe`,
    process.env.LOCALAPPDATA && `${process.env.LOCALAPPDATA}\\Programs\\Git\\bin\\bash.exe`
  ].filter(Boolean);
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return "bash";
}

function toBashPath(filePath) {
  const normalized = String(filePath).replace(/\\/g, "/");
  if (!/^[A-Za-z]:\//.test(normalized)) return normalized;
  return `/${normalized[0].toLowerCase()}${normalized.slice(2)}`;
}

function bashQuote(value) {
  return `'${String(value).replace(/'/g, `'\\''`)}'`;
}

function buildShellInvocation(scriptPath, scriptArgs = []) {
  const command = shellCommand();
  if (process.platform === "win32" && command.toLowerCase().endsWith("bash.exe")) {
    const commandLine = [scriptPath, ...scriptArgs]
      .map((item) => {
        const text = String(item);
        const normalized = /^[A-Za-z]:[\\/]/.test(text) ? toBashPath(text) : text;
        return bashQuote(normalized);
      })
      .join(" ");
    return { command, args: ["-lc", commandLine] };
  }
  return { command, args: [scriptPath, ...scriptArgs] };
}

function terminateChildTree(child) {
  if (!child || !child.pid) return;
  if (process.platform === "win32") {
    const killer = spawn("taskkill.exe", ["/PID", String(child.pid), "/T", "/F"], {
      stdio: "ignore",
      windowsHide: true
    });
    killer.on("error", () => {
      child.kill("SIGTERM");
    });
    return;
  }
  child.kill("SIGTERM");
}

const contentTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".jsonl": "application/x-ndjson; charset=utf-8",
  ".md": "text/markdown; charset=utf-8"
};

function readEnvValue(key) {
  try {
    const envPath = path.join(root, "..", "..", ".env");
    const content = fs.readFileSync(envPath, "utf8");
    const match = content.match(new RegExp(`(?:^|\\n)\\s*${key}\\s*=\\s*([^\\n#]+)`));
    return match ? match[1].trim().replace(/^["']|["']$/g, "") : "";
  } catch {
    return "";
  }
}

function safePath(urlPath) {
  const decoded = decodeURIComponent(urlPath.split("?")[0]);
  const clean = decoded === "/" ? "/index.html" : decoded;
  const filePath = path.normalize(path.join(root, clean));
  if (!filePath.startsWith(root)) return null;
  return filePath;
}

const server = http.createServer((request, response) => {
  const requestUrl = new URL(request.url || "/", `http://${request.headers.host || `${host}:${port}`}`);

  if (requestUrl.pathname === "/api/aws/start" && request.method === "POST") {
    runAwsStart(response);
    return;
  }

  if (requestUrl.pathname === "/api/aws/start/status" && request.method === "GET") {
    sendJson(response, 200, startupStatePayload());
    return;
  }

  if (requestUrl.pathname === "/api/aws/stop" && request.method === "POST") {
    runAwsStop(response);
    return;
  }

  if (requestUrl.pathname === "/api/aws/status" && request.method === "GET") {
    runAwsStatus(response);
    return;
  }

  if (requestUrl.pathname === "/api/spark/start" && request.method === "POST") {
    runSparkStart(response, requestUrl);
    return;
  }

  if (requestUrl.pathname === "/api/spark/status" && request.method === "GET") {
    runSparkStatus(response);
    return;
  }

  if (requestUrl.pathname === "/api/spark/comments" && request.method === "GET") {
    runSparkComments(response, requestUrl);
    return;
  }

  if (requestUrl.pathname === "/api/live-delta" && request.method === "GET") {
    runLiveDelta(response, requestUrl);
    return;
  }

  if (requestUrl.pathname === "/api/pipeline/health" && request.method === "GET") {
    runPipelineHealth(response);
    return;
  }

  const filePath = safePath(request.url || "/");
  if (!filePath) {
    response.writeHead(403);
    response.end("Forbidden");
    return;
  }

  fs.readFile(filePath, (error, content) => {
    if (error) {
      response.writeHead(404);
      response.end("Not found");
      return;
    }
    response.writeHead(200, {
      "Content-Type": contentTypes[path.extname(filePath)] || "application/octet-stream",
      "Cache-Control": "no-store"
    });
    response.end(content);
  });
});

function updateAwsStartupState(patch) {
  awsStartupState = {
    ...awsStartupState,
    ...patch,
    updated_at: new Date().toISOString()
  };
}

function startupStatePayload() {
  return {
    ok: awsStartupState.status !== "failed",
    starting: awsStarting,
    next_start_mode: readPlatformState().next_start_mode,
    ...awsStartupState
  };
}

function appendTail(current, chunk, maxLength = 6000) {
  return `${current || ""}${chunk || ""}`.slice(-maxLength);
}

function latestProgressLine(text) {
  const lines = String(text)
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.reverse().find((line) =>
    /^(Kafka inventory|Preparando Kafka|Deteniendo brokers|Esperando quorum|Iniciando producer|Desplegando codigo|Lanzando |Arquitectura distribuida|Kafka brokers)/.test(line)
  ) || "";
}

function finishAwsStartup(attempt, patch) {
  if (attempt !== startupAttempt) return;
  awsStarting = false;
  updateAwsStartupState(patch);
}

function runAwsStart(response) {
  if (stopping) {
    sendJson(response, 409, { ok: false, error: "La plataforma todavía se está deteniendo." });
    return;
  }
  if (awsStarting) {
    sendJson(response, 202, {
      ...startupStatePayload(),
      accepted: true
    });
    return;
  }

  const attempt = ++startupAttempt;
  awsStarting = true;
  const dataSize = process.env.DASHBOARD_STREAM_LIMIT || readEnvValue("DATA_SIZE") || "7000";
  const sparkBatchSize = process.env.SPARK_BATCH_SIZE || readEnvValue("SPARK_BATCH_SIZE") || "1000";
  const sparkMaxConcurrency = process.env.SPARK_MAX_CONCURRENCY || readEnvValue("SPARK_MAX_CONCURRENCY") || "1";
  const persistedState = readPlatformState();
  const startMode = persistedState.next_start_mode === "fresh" ? "fresh" : "recovery";
  updateAwsStartupState({
    status: "starting",
    stage: "checking",
    message: "Verificando Kafka y la sesión anterior en EMR_PRIMARY...",
    started_at: new Date().toISOString(),
    data_size: Number(dataSize),
    spark_batch_size: Number(sparkBatchSize),
    spark_max_concurrency: Number(sparkMaxConcurrency),
    start_mode: startMode,
    stdout_tail: "",
    stderr_tail: ""
  });
  sendJson(response, 202, {
    ...startupStatePayload(),
    accepted: true
  });
  startAwsInBackground(attempt, { dataSize, sparkBatchSize, sparkMaxConcurrency, startMode });
}

async function startAwsInBackground(attempt, config) {
  const { dataSize, sparkBatchSize, sparkMaxConcurrency, startMode } = config;
  const statusScriptPath = path.join(root, "scripts", "aws_status_from_aws.sh");

  if (startMode !== "fresh") {
    try {
      const status = await collectJsonScript(statusScriptPath, [
        "--data-size", dataSize,
        "--spark-batch-size", sparkBatchSize,
        "--spark-max-concurrency", sparkMaxConcurrency
      ], Number(process.env.DASHBOARD_AWS_STATUS_TIMEOUT_MS || 45000), activeAwsStartupProcesses);
      if (attempt !== startupAttempt) return;

      if (status.ok && status.needs_resume) {
        updateAwsStartupState({
          stage: "resuming",
          message: "Kafka está listo. Reanudando el producer desde la siguiente fila pendiente...",
          start_mode: "resume"
        });
        const resumeScriptPath = path.join(root, "scripts", "resume_producer_from_aws.sh");
        const resumed = await collectJsonScript(resumeScriptPath, [
          "--data-size", dataSize,
          "--producer-delay-ms", process.env.DASHBOARD_PRODUCER_DELAY_MS || "10"
        ], Number(process.env.DASHBOARD_RESUME_TIMEOUT_MS || 90000), activeAwsStartupProcesses);
        if (attempt !== startupAttempt) return;
        if (!resumed.ok) throw new Error(resumed.error || "No se pudo reanudar el producer.");
        awsStarted = true;
        writePlatformState({
          status: "running",
          next_start_mode: "recovery",
          session_mode: "resume"
        });
        finishAwsStartup(attempt, {
          status: "ready",
          stage: "ready",
          message: `Producer reanudado desde la fila ${resumed.resumed_from || 1}. Streaming activo.`,
          resumed: true,
          resumed_from: resumed.resumed_from || 1
        });
        return;
      }

      if (status.ok && !status.needs_resume) {
        awsStarted = true;
        writePlatformState({
          status: "running",
          next_start_mode: "recovery",
          session_mode: "existing"
        });
        finishAwsStartup(attempt, {
          status: "ready",
          stage: "ready",
          message: "Kafka y el streaming ya estaban activos.",
          already_running: true,
          started_at: status.generated_at || awsStartupState.started_at
        });
        return;
      }
    } catch (error) {
      if (attempt !== startupAttempt) return;
      finishAwsStartup(attempt, {
        status: "failed",
        stage: "checking",
        message: "No se pudo verificar el estado remoto.",
        error: `No se pudo verificar el estado remoto; se evitó un reinicio potencialmente destructivo: ${error.message}`
      });
      return;
    }
  }

  if (startMode === "fresh") {
    sparkLaunchStates.clear();
    latestSparkBatches = {};
  }
  clearRuntimeCache();
  updateAwsStartupState({
    stage: "bootstrap",
    message: startMode === "fresh"
      ? "Creando una sesión limpia: reiniciando topics y offsets Kafka..."
      : "Recuperando Kafka y los offsets de la sesión anterior...",
    start_mode: startMode
  });
  const scriptPath = path.join(root, "scripts", "bootstrap_emr_streaming.sh");
  const args = [
    scriptPath,
    "--limit", dataSize,
    "--producer-delay-ms", process.env.DASHBOARD_PRODUCER_DELAY_MS || "10",
    "--window-seconds", process.env.DASHBOARD_WINDOW_SECONDS || "5",
    startMode === "fresh" ? "--reset-topics" : "--no-reset-topics"
  ];
  const invocation = buildShellInvocation(scriptPath, args.slice(1));
  const child = spawn(invocation.command, invocation.args, { cwd: root });
  activeAwsStartupProcesses.add(child);

  let stdout = "";
  let stderr = "";
  const killTimer = setTimeout(() => {
    terminateChildTree(child);
  }, Number(process.env.DASHBOARD_AWS_START_TIMEOUT_MS || 600000));

  child.stdout.on("data", (chunk) => {
    const text = chunk.toString();
    stdout = appendTail(stdout, text);
    const message = latestProgressLine(text);
    if (attempt === startupAttempt) {
      updateAwsStartupState({
        message: message || awsStartupState.message,
        stdout_tail: stdout
      });
    }
  });

  child.stderr.on("data", (chunk) => {
    const text = chunk.toString();
    stderr = appendTail(stderr, text);
    if (attempt === startupAttempt) updateAwsStartupState({ stderr_tail: stderr });
  });

  child.on("error", (error) => {
    clearTimeout(killTimer);
    activeAwsStartupProcesses.delete(child);
    finishAwsStartup(attempt, {
      status: "failed",
      stage: "bootstrap",
      message: "El proceso de arranque no pudo ejecutarse.",
      error: error.message,
      stdout_tail: stdout,
      stderr_tail: stderr
    });
  });

  child.on("close", (code) => {
    clearTimeout(killTimer);
    activeAwsStartupProcesses.delete(child);
    if (attempt !== startupAttempt) return;
    if (code === 0) {
      awsStarted = true;
      writePlatformState({
        status: "running",
        next_start_mode: "recovery",
        session_mode: startMode,
        started_at: new Date().toISOString()
      });
      finishAwsStartup(attempt, {
        status: "ready",
        stage: "ready",
        message: "Kafka, producer y compute quedaron iniciados. Streaming activo.",
        stdout_tail: stdout,
        stderr_tail: stderr
      });
    } else {
      finishAwsStartup(attempt, {
        status: "failed",
        stage: "bootstrap",
        message: "El arranque distribuido quedó incompleto.",
        error: stderr.trim() || stdout.trim() || `bootstrap_emr_streaming.sh termino con codigo ${code}`,
        stdout_tail: stdout,
        stderr_tail: stderr
      });
    }
  });
}

async function runAwsStop(response) {
  if (awsStarting || activeAwsStartupProcesses.size) {
    startupAttempt += 1;
    awsStarting = false;
    updateAwsStartupState({
      status: "stopping",
      stage: "stopping",
      message: "Cancelando el arranque antes de detener la plataforma..."
    });
    for (const child of activeAwsStartupProcesses) terminateChildTree(child);
    activeAwsStartupProcesses.clear();
  }
  if (stopping) {
    sendJson(response, 409, { ok: false, error: "Ya hay una detención distribuida en curso." });
    return;
  }
  stopping = true;

  // Un reset no puede adelantarse a un launcher Spark que todavía esté
  // copiando código o creando su driver remoto.
  const launchersSettled = await waitForSparkLaunchers(65000);
  if (!launchersSettled) {
    for (const child of activeSparkLaunchers) terminateChildTree(child);
    await waitForSparkLaunchers(5000);
  }

  const scriptPath = path.join(root, "scripts", "stop_emr_streaming.sh");
  const invocation = buildShellInvocation(scriptPath);
  const child = spawn(invocation.command, invocation.args, { cwd: root });

  let stdout = "";
  let stderr = "";
  let responseSent = false;
  const killTimer = setTimeout(() => {
    terminateChildTree(child);
  }, Number(process.env.DASHBOARD_AWS_STOP_TIMEOUT_MS || 240000));

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  child.on("error", (error) => {
    clearTimeout(killTimer);
    stopping = false;
    responseSent = true;
    sendJson(response, 500, { ok: false, error: error.message, stdout, stderr });
  });

  child.on("close", (code) => {
    clearTimeout(killTimer);
    stopping = false;
    if (responseSent) return;
    responseSent = true;
    if (code === 0) {
      awsStarted = false;
      clearRuntimeCache();
      sparkLaunchStates.clear();
      latestSparkBatches = {};
      writePlatformState({
        status: "stopped",
        next_start_mode: "fresh",
        session_mode: "fresh",
        stopped_at: new Date().toISOString()
      });
      updateAwsStartupState({
        status: "stopped",
        stage: "stopped",
        message: "Kafka, producer, Flink y Spark están detenidos.",
        started_at: "",
        stdout_tail: "",
        stderr_tail: ""
      });
      sendJson(response, 200, {
        ok: true,
        stopped_at: new Date().toISOString(),
        stdout,
        stderr
      });
    } else {
      sendJson(response, 502, {
        ok: false,
        error: stderr.trim() || stdout.trim() || `stop_emr_streaming.sh termino con codigo ${code}`,
        stdout,
        stderr
      });
    }
  });
}

function runAwsStatus(response) {
  const dataSize = process.env.DASHBOARD_STREAM_LIMIT || readEnvValue("DATA_SIZE") || "7000";
  const sparkBatchSize = process.env.SPARK_BATCH_SIZE || readEnvValue("SPARK_BATCH_SIZE") || "1000";
  const sparkMaxConcurrency = process.env.SPARK_MAX_CONCURRENCY || readEnvValue("SPARK_MAX_CONCURRENCY") || "1";
  const scriptPath = path.join(root, "scripts", "aws_status_from_aws.sh");
  runJsonScript(response, scriptPath, [
    "--data-size", dataSize,
    "--spark-batch-size", sparkBatchSize,
    "--spark-max-concurrency", sparkMaxConcurrency
  ], Number(process.env.DASHBOARD_AWS_STATUS_TIMEOUT_MS || 45000));
}

function runSparkStart(response, requestUrl) {
  if (stopping) {
    sendJson(response, 409, { ok: false, error: "La plataforma se está deteniendo; no se aceptan batches nuevos." });
    return;
  }
  const sparkBatchSize = Number(process.env.SPARK_BATCH_SIZE || readEnvValue("SPARK_BATCH_SIZE") || 1000);
  const sparkMaxConcurrency = Number(process.env.SPARK_MAX_CONCURRENCY || readEnvValue("SPARK_MAX_CONCURRENCY") || 1);
  const targetCount = Number(requestUrl.searchParams.get("target") || 0);
  if (!Number.isInteger(sparkMaxConcurrency) || sparkMaxConcurrency <= 0) {
    sendJson(response, 500, { ok: false, error: "SPARK_MAX_CONCURRENCY debe ser un entero positivo." });
    return;
  }
  if (!Number.isFinite(targetCount) || targetCount <= 0) {
    sendJson(response, 400, { ok: false, error: "Falta target valido para Spark batch." });
    return;
  }
  if (!Number.isFinite(sparkBatchSize) || sparkBatchSize <= 0 || targetCount % sparkBatchSize !== 0) {
    sendJson(response, 400, {
      ok: false,
      error: `target debe ser múltiplo de SPARK_BATCH_SIZE=${sparkBatchSize}.`
    });
    return;
  }
  const expectedBatchId = `batch_${String(targetCount).padStart(7, "0")}`;
  const batchId = requestUrl.searchParams.get("batchId") || expectedBatchId;
  const force = requestUrl.searchParams.get("force") === "1";
  if (batchId !== expectedBatchId) {
    sendJson(response, 400, { ok: false, error: `batchId debe ser ${expectedBatchId}.` });
    return;
  }

  const knownBatches = {
    ...latestSparkBatches,
    ...Object.fromEntries(sparkLaunchStates)
  };
  const otherActiveBatch = Object.values(knownBatches).find((batch) =>
    batch.batch_id !== batchId && ["launching", "queued", "running"].includes(batch.status)
  );
  if (otherActiveBatch) {
    sendJson(response, 202, {
      ok: true,
      busy: true,
      retryable: true,
      batch_id: batchId,
      blocked_by: otherActiveBatch.batch_id,
      message: `${otherActiveBatch.batch_id} sigue activo; la cola Spark es secuencial.`
    });
    return;
  }
  for (let previousTarget = sparkBatchSize; previousTarget < targetCount; previousTarget += sparkBatchSize) {
    const previousId = `batch_${String(previousTarget).padStart(7, "0")}`;
    const previousBatch = knownBatches[previousId];
    if (!previousBatch || previousBatch.status !== "done") {
      sendJson(response, 409, {
        ok: false,
        batch_id: batchId,
        blocked_by: previousId,
        error: `${batchId} no puede iniciar hasta que ${previousId} termine correctamente.`
      });
      return;
    }
  }

  const previous = sparkLaunchStates.get(batchId);
  if (!force && previous && ["launching", "queued"].includes(previous.status)) {
    sendJson(response, 202, { ok: true, accepted: true, ...previous });
    return;
  }
  const attempts = force ? 1 : Number(previous?.attempts || 0) + 1;
  if (!force && attempts > 3) {
    sendJson(response, 409, {
      ok: false,
      error: `${batchId} agotó tres intentos de lanzamiento.`,
      batch_id: batchId,
      attempts: Number(previous?.attempts || 3)
    });
    return;
  }

  const launchState = {
    batch_id: batchId,
    range_start: Math.max(1, targetCount - sparkBatchSize + 1),
    range_end: targetCount,
    target_count: targetCount,
    spark_batch_size: sparkBatchSize,
    status: "launching",
    current_job: "launch",
    message: `Preparando lanzamiento remoto (intento ${attempts}/3)`,
    attempts,
    active: true,
    jobs: {},
    outputs: {},
    updated_at: new Date().toISOString()
  };
  sparkLaunchStates.set(batchId, launchState);
  sendJson(response, 202, { ok: true, accepted: true, ...launchState });
  launchSparkInBackground({
    batchId,
    targetCount,
    sparkBatchSize,
    sparkMaxConcurrency,
    force,
    attempts
  });
}

async function launchSparkInBackground(config) {
  const { batchId, targetCount, sparkBatchSize, sparkMaxConcurrency, force, attempts } = config;
  const scriptPath = path.join(root, "scripts", "run_spark_batch_from_kafka.sh");
  const args = [
    "--batch-id", batchId,
    "--target-count", String(targetCount),
    "--spark-batch-size", String(sparkBatchSize),
    "--max-concurrency", String(sparkMaxConcurrency)
  ];
  if (force) args.push("--force");

  try {
    const result = await collectJsonScript(
      scriptPath,
      args,
      Number(process.env.DASHBOARD_SPARK_START_TIMEOUT_MS || 60000),
      activeSparkLaunchers
    );
    if (result.busy) {
      sparkLaunchStates.set(batchId, {
        ...sparkLaunchStates.get(batchId),
        status: "retrying",
        current_job: "launch",
        message: result.error || "El scheduler remoto está ocupado; se reintentará.",
        active: false,
        retryable: true,
        next_retry_at: new Date(Date.now() + 5000).toISOString(),
        updated_at: new Date().toISOString()
      });
      return;
    }
    if (!result.ok) throw new Error(result.error || "El worker rechazó el batch Spark.");
    sparkLaunchStates.set(batchId, {
      ...sparkLaunchStates.get(batchId),
      ...result,
      status: "queued",
      current_job: "queued",
      message: result.message || "Batch Spark lanzado; esperando estado remoto.",
      active: true,
      updated_at: new Date().toISOString()
    });
  } catch (error) {
    const retryable = attempts < 3;
    sparkLaunchStates.set(batchId, {
      ...sparkLaunchStates.get(batchId),
      status: retryable ? "retrying" : "failed",
      current_job: "launch",
      message: error.message,
      active: false,
      retryable,
      next_retry_at: retryable ? new Date(Date.now() + 15000).toISOString() : "",
      updated_at: new Date().toISOString()
    });
  }
}

async function runSparkStatus(response) {
  const sparkMaxConcurrency = process.env.SPARK_MAX_CONCURRENCY || readEnvValue("SPARK_MAX_CONCURRENCY") || "1";
  const scriptPath = path.join(root, "scripts", "spark_status_from_aws.sh");
  try {
    const status = await collectJsonScript(
      scriptPath,
      ["--max-concurrency", sparkMaxConcurrency],
      Number(process.env.DASHBOARD_SPARK_STATUS_TIMEOUT_MS || 45000)
    );
    status.batches = status.batches || {};
    for (const [batchId, localState] of sparkLaunchStates) {
      if (status.batches[batchId]) {
        sparkLaunchStates.delete(batchId);
      } else {
        status.batches[batchId] = localState;
      }
    }
    latestSparkBatches = { ...status.batches };
    status.latest_batch_id = Object.values(status.batches)
      .sort((a, b) => Number(b.range_end || 0) - Number(a.range_end || 0))[0]?.batch_id || "";
    status.active_batches = Object.values(status.batches)
      .filter((batch) => ["launching", "queued", "running"].includes(batch.status)).length;
    sendJson(response, 200, status);
  } catch (error) {
    sendJson(response, 502, { ok: false, error: error.message, batches: {} });
  }
}

function runSparkComments(response, requestUrl) {
  const batchId = requestUrl.searchParams.get("batchId") || "";
  if (!/^batch_[0-9]{7}$/.test(batchId)) {
    sendJson(response, 400, { ok: false, error: "batchId invalido." });
    return;
  }

  const limit = requestUrl.searchParams.get("limit") || "250";
  const scriptPath = path.join(root, "scripts", "spark_batch_comments_from_aws.sh");
  const invocation = buildShellInvocation(scriptPath, ["--batch-id", batchId, "--limit", limit]);
  const child = spawn(invocation.command, invocation.args, { cwd: root });
  let stdout = "";
  let stderr = "";
  const killTimer = setTimeout(() => {
    terminateChildTree(child);
  }, Number(process.env.DASHBOARD_SPARK_COMMENTS_TIMEOUT_MS || 120000));

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  child.on("error", (error) => {
    clearTimeout(killTimer);
    sendJson(response, 500, { ok: false, error: error.message, stderr });
  });

  child.on("close", (code) => {
    clearTimeout(killTimer);
    if (code !== 0) {
      sendJson(response, 502, {
        ok: false,
        error: stderr.trim() || stdout.trim() || "spark_batch_comments_from_aws.sh fallo",
        stderr
      });
      return;
    }

    try {
      const payload = JSON.parse(stdout.trim().split(/\n/).pop() || "{}");
      if (payload.ok) writeRuntimeJson(`spark_batch_comments_${batchId}.json`, payload);
      sendJson(response, 200, payload);
    } catch (error) {
      sendJson(response, 502, {
        ok: false,
        error: `No se pudo parsear spark_batch_comments_from_aws.sh: ${error.message}`,
        stdout,
        stderr
      });
    }
  });
}

function runPipelineHealth(response) {
  const scriptPath = path.join(root, "scripts", "pipeline_health_from_aws.sh");
  runJsonScript(
    response,
    scriptPath,
    [],
    Number(process.env.DASHBOARD_HEALTH_TIMEOUT_MS || 60000)
  );
}

function runLiveDelta(response, requestUrl) {
  const scriptPath = path.join(root, "scripts", "live_delta_from_aws.sh");
  const cursor = requestUrl.searchParams.get("cursor") || "";
  const cursorB64 = Buffer.from(cursor, "utf8").toString("base64");
  const args = [
    scriptPath,
    "--cursor-b64", cursorB64,
    "--max-raw", requestUrl.searchParams.get("maxRaw") || "80",
    "--max-flink", requestUrl.searchParams.get("maxFlink") || "80",
    "--max-alerts", requestUrl.searchParams.get("maxAlerts") || "40"
  ];
  const invocation = buildShellInvocation(scriptPath, args.slice(1));
  const child = spawn(invocation.command, invocation.args, { cwd: root });

  let stdout = "";
  let stderr = "";
  const killTimer = setTimeout(() => {
    terminateChildTree(child);
  }, Number(process.env.DASHBOARD_REMOTE_DELTA_TIMEOUT_MS || 35000));

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  child.on("error", (error) => {
    clearTimeout(killTimer);
    sendJson(response, 500, { ok: false, error: error.message });
  });

  child.on("close", (code) => {
    clearTimeout(killTimer);
    if (code === 0) {
      try {
        sendJson(response, 200, JSON.parse(stdout));
      } catch (error) {
        sendJson(response, 502, {
          ok: false,
          error: `No se pudo parsear live_delta_from_aws.sh: ${error.message}`,
          stderr
        });
      }
    } else {
      sendJson(response, 502, {
        ok: false,
        error: stderr.trim() || `live_delta_from_aws.sh termino con codigo ${code}`,
        stderr
      });
    }
  });
}

function runJsonScript(response, scriptPath, scriptArgs, timeoutMs, trackedChildren = null) {
  const invocation = buildShellInvocation(scriptPath, scriptArgs);
  const child = spawn(invocation.command, invocation.args, { cwd: root });
  if (trackedChildren) trackedChildren.add(child);
  let stdout = "";
  let stderr = "";
  let responseSent = false;
  const killTimer = setTimeout(() => {
    terminateChildTree(child);
  }, timeoutMs);

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  child.on("error", (error) => {
    clearTimeout(killTimer);
    if (trackedChildren) trackedChildren.delete(child);
    responseSent = true;
    sendJson(response, 500, { ok: false, error: error.message, stderr });
  });

  child.on("close", (code) => {
    clearTimeout(killTimer);
    if (trackedChildren) trackedChildren.delete(child);
    if (responseSent) return;
    responseSent = true;
    if (code !== 0) {
      sendJson(response, 502, {
        ok: false,
        error: stderr.trim() || stdout.trim() || `${path.basename(scriptPath)} termino con codigo ${code}`,
        stderr
      });
      return;
    }

    try {
      sendJson(response, 200, JSON.parse(stdout));
    } catch (error) {
      sendJson(response, 502, {
        ok: false,
        error: `No se pudo parsear ${path.basename(scriptPath)}: ${error.message}`,
        stdout,
        stderr
      });
    }
  });
}

function collectJsonScript(scriptPath, scriptArgs, timeoutMs, trackedChildren = null) {
  return new Promise((resolve, reject) => {
    const invocation = buildShellInvocation(scriptPath, scriptArgs);
    const child = spawn(invocation.command, invocation.args, { cwd: root });
    if (trackedChildren) trackedChildren.add(child);
    let stdout = "";
    let stderr = "";
    let settled = false;
    const killTimer = setTimeout(() => {
      terminateChildTree(child);
      if (!settled) {
        settled = true;
        if (trackedChildren) trackedChildren.delete(child);
        reject(new Error(`${path.basename(scriptPath)} excedió ${timeoutMs} ms`));
      }
    }, timeoutMs);

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", (error) => {
      clearTimeout(killTimer);
      if (settled) return;
      settled = true;
      if (trackedChildren) trackedChildren.delete(child);
      reject(error);
    });
    child.on("close", (code) => {
      clearTimeout(killTimer);
      if (settled) return;
      settled = true;
      if (trackedChildren) trackedChildren.delete(child);
      if (code !== 0) {
        reject(new Error(stderr.trim() || stdout.trim() || `${path.basename(scriptPath)} terminó con código ${code}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch (error) {
        reject(new Error(`respuesta inválida de ${path.basename(scriptPath)}: ${error.message}`));
      }
    });
  });
}

function waitForSparkLaunchers(timeoutMs) {
  if (activeSparkLaunchers.size === 0) return Promise.resolve(true);
  return new Promise((resolve) => {
    const startedAt = Date.now();
    const handle = setInterval(() => {
      if (activeSparkLaunchers.size === 0) {
        clearInterval(handle);
        resolve(true);
      } else if (Date.now() - startedAt >= timeoutMs) {
        clearInterval(handle);
        resolve(false);
      }
    }, 100);
  });
}

function sendJson(response, status, payload) {
  response.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  response.end(JSON.stringify(payload));
}

function writeRuntimeJson(fileName, payload) {
  fs.mkdirSync(runtimeDir, { recursive: true });
  fs.writeFileSync(path.join(runtimeDir, fileName), JSON.stringify(payload, null, 2));
}

function clearRuntimeCache() {
  try {
    fs.mkdirSync(runtimeDir, { recursive: true });
    for (const fileName of fs.readdirSync(runtimeDir)) {
      if (fileName === ".gitkeep") continue;
      if (fileName === path.basename(platformStatePath)) continue;
      if (fileName.endsWith(".json")) {
        fs.unlinkSync(path.join(runtimeDir, fileName));
      }
    }
  } catch {
    // Runtime cache is optional; streaming should still start if cleanup fails.
  }
}

server.listen(port, host, () => {
  console.log(`Dashboard local: http://${host}:${port}`);
});
