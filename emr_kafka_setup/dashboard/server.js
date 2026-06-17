const http = require("http");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");
const { spawn } = require("child_process");

const root = __dirname;
const port = Number(process.env.PORT || 8787);
const host = process.env.HOST || "127.0.0.1";
const runtimeDir = path.join(root, "data", "runtime");
let awsStarted = false;

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

function runAwsStart(response) {
  if (process.platform === "win32") {
    sendJson(response, 400, {
      ok: false,
      error: "El arranque streaming bajo demanda esta configurado para macOS/Linux con bootstrap_emr_streaming.sh."
    });
    return;
  }

  const dataSize = process.env.DASHBOARD_STREAM_LIMIT || readEnvValue("DATA_SIZE") || "7000";
  clearRuntimeCache();
  const scriptPath = path.join(root, "scripts", "bootstrap_emr_streaming.sh");
  const args = [
    scriptPath,
    "--limit", dataSize,
    "--producer-delay-ms", process.env.DASHBOARD_PRODUCER_DELAY_MS || "10",
    "--window-seconds", process.env.DASHBOARD_WINDOW_SECONDS || "5"
  ];
  const child = spawn("bash", args, { cwd: root });

  let stdout = "";
  let stderr = "";
  const killTimer = setTimeout(() => {
    child.kill("SIGTERM");
  }, Number(process.env.DASHBOARD_AWS_START_TIMEOUT_MS || 600000));

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  child.on("error", (error) => {
    clearTimeout(killTimer);
    sendJson(response, 500, { ok: false, error: error.message, stdout, stderr });
  });

  child.on("close", (code) => {
    clearTimeout(killTimer);
    if (code === 0) {
      awsStarted = true;
      sendJson(response, 200, {
        ok: true,
        data_mode: "aws_streaming",
        data_size: Number(dataSize),
        spark_batch_size: Number(process.env.SPARK_BATCH_SIZE || readEnvValue("SPARK_BATCH_SIZE") || 1000),
        started_at: new Date().toISOString(),
        stdout,
        stderr
      });
    } else {
      sendJson(response, 502, {
        ok: false,
        error: stderr.trim() || stdout.trim() || `bootstrap_emr_streaming.sh termino con codigo ${code}`,
        stdout,
        stderr
      });
    }
  });
}

function runAwsStop(response) {
  if (process.platform === "win32") {
    sendJson(response, 400, {
      ok: false,
      error: "El stop remoto esta configurado para macOS/Linux con stop_emr_streaming.sh."
    });
    return;
  }

  const scriptPath = path.join(root, "scripts", "stop_emr_streaming.sh");
  const child = spawn("bash", [scriptPath], { cwd: root });

  let stdout = "";
  let stderr = "";
  const killTimer = setTimeout(() => {
    child.kill("SIGTERM");
  }, Number(process.env.DASHBOARD_AWS_STOP_TIMEOUT_MS || 90000));

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  child.on("error", (error) => {
    clearTimeout(killTimer);
    sendJson(response, 500, { ok: false, error: error.message, stdout, stderr });
  });

  child.on("close", (code) => {
    clearTimeout(killTimer);
    awsStarted = false;
    if (code === 0) {
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
  if (!awsStarted) {
    sendJson(response, 409, {
      ok: false,
      error: "AWS aun no esta conectado."
    });
    return;
  }

  const dataSize = process.env.DASHBOARD_STREAM_LIMIT || readEnvValue("DATA_SIZE") || "7000";
  const sparkBatchSize = process.env.SPARK_BATCH_SIZE || readEnvValue("SPARK_BATCH_SIZE") || "1000";
  const scriptPath = path.join(root, "scripts", "aws_status_from_aws.sh");
  runJsonScript(response, "bash", [
    scriptPath,
    "--data-size", dataSize,
    "--spark-batch-size", sparkBatchSize
  ], Number(process.env.DASHBOARD_AWS_STATUS_TIMEOUT_MS || 45000));
}

function runSparkStart(response, requestUrl) {
  if (!awsStarted) {
    sendJson(response, 409, {
      ok: false,
      error: "AWS aun no esta conectado."
    });
    return;
  }

  const sparkBatchSize = Number(process.env.SPARK_BATCH_SIZE || readEnvValue("SPARK_BATCH_SIZE") || 1000);
  const targetCount = Number(requestUrl.searchParams.get("target") || 0);
  if (!Number.isFinite(targetCount) || targetCount <= 0) {
    sendJson(response, 400, { ok: false, error: "Falta target valido para Spark batch." });
    return;
  }
  const batchId = requestUrl.searchParams.get("batchId") || `batch_${String(targetCount).padStart(7, "0")}`;
  const scriptPath = path.join(root, "scripts", "run_spark_batch_from_kafka.sh");
  runJsonScript(response, "bash", [
    scriptPath,
    "--batch-id", batchId,
    "--target-count", String(targetCount),
    "--spark-batch-size", String(sparkBatchSize)
  ], Number(process.env.DASHBOARD_SPARK_START_TIMEOUT_MS || 60000));
}

function runSparkStatus(response) {
  if (!awsStarted) {
    sendJson(response, 200, {
      ok: true,
      batches: {},
      latest_batch_id: "",
      generated_at: new Date().toISOString()
    });
    return;
  }

  const scriptPath = path.join(root, "scripts", "spark_status_from_aws.sh");
  runJsonScript(response, "bash", [scriptPath], Number(process.env.DASHBOARD_SPARK_STATUS_TIMEOUT_MS || 45000));
}

function runSparkComments(response, requestUrl) {
  if (!awsStarted) {
    sendJson(response, 409, {
      ok: false,
      error: "AWS aun no esta conectado."
    });
    return;
  }

  const batchId = requestUrl.searchParams.get("batchId") || "";
  if (!/^batch_[0-9]{7}$/.test(batchId)) {
    sendJson(response, 400, { ok: false, error: "batchId invalido." });
    return;
  }

  const limit = requestUrl.searchParams.get("limit") || "250";
  const scriptPath = path.join(root, "scripts", "spark_batch_comments_from_aws.sh");
  const child = spawn("bash", [scriptPath, "--batch-id", batchId, "--limit", limit], { cwd: root });
  let stdout = "";
  let stderr = "";
  const killTimer = setTimeout(() => {
    child.kill("SIGTERM");
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

function runLiveDelta(response, requestUrl) {
  if (!awsStarted) {
    sendJson(response, 409, {
      ok: false,
      error: "AWS aun no esta conectado. Pulsa Conectar AWS para iniciar el streaming remoto."
    });
    return;
  }

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
  const child = spawn("bash", args, { cwd: root });

  let stdout = "";
  let stderr = "";
  const killTimer = setTimeout(() => {
    child.kill("SIGTERM");
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

function runJsonScript(response, command, args, timeoutMs) {
  const child = spawn(command, args, { cwd: root });
  let stdout = "";
  let stderr = "";
  const killTimer = setTimeout(() => {
    child.kill("SIGTERM");
  }, timeoutMs);

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
        error: stderr.trim() || stdout.trim() || `${path.basename(args[0] || command)} termino con codigo ${code}`,
        stderr
      });
      return;
    }

    try {
      sendJson(response, 200, JSON.parse(stdout));
    } catch (error) {
      sendJson(response, 502, {
        ok: false,
        error: `No se pudo parsear ${path.basename(args[0] || command)}: ${error.message}`,
        stdout,
        stderr
      });
    }
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
