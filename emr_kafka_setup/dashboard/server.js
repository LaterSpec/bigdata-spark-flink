const http = require("http");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const root = __dirname;
const port = Number(process.env.PORT || 8787);

const contentTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".jsonl": "application/x-ndjson; charset=utf-8",
  ".md": "text/markdown; charset=utf-8"
};

function safePath(urlPath) {
  const decoded = decodeURIComponent(urlPath.split("?")[0]);
  const clean = decoded === "/" ? "/index.html" : decoded;
  const filePath = path.normalize(path.join(root, clean));
  if (!filePath.startsWith(root)) return null;
  return filePath;
}

const server = http.createServer((request, response) => {
  if (request.url === "/api/sync" && request.method === "POST") {
    runSync(response);
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

function runSync(response) {
  const scriptPath = path.join(root, "scripts", "sync_from_aws.ps1");
  const powershell = process.platform === "win32" ? "powershell.exe" : "pwsh";
  const child = spawn(
    powershell,
    ["-ExecutionPolicy", "Bypass", "-File", scriptPath, "-NlpMessages", "7000", "-AlertMessages", "500"],
    { cwd: root }
  );

  let stdout = "";
  let stderr = "";

  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });

  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  child.on("error", (error) => {
    sendJson(response, 500, { ok: false, error: error.message, stdout, stderr });
  });

  child.on("close", (code) => {
    if (code === 0) {
      sendJson(response, 200, { ok: true, stdout, stderr });
    } else {
      sendJson(response, 502, {
        ok: false,
        error: stderr.trim() || stdout.trim() || `sync_from_aws.ps1 termino con codigo ${code}`,
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

server.listen(port, () => {
  console.log(`Dashboard local: http://localhost:${port}`);
});
