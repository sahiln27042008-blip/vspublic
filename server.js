"use strict";

const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 10000);
const TOKEN = process.env.RELAY_TOKEN;

if (!TOKEN) {
  console.error("Missing RELAY_TOKEN environment variable.");
  process.exit(1);
}

let bridge = null;
const pending = new Map();

function id() {
  return crypto.randomUUID();
}

function sendJson(res, status, body, extraHeaders = {}) {
  const data = JSON.stringify(body);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, Accept, MCP-Protocol-Version, Mcp-Session-Id, Mcp-Method, Mcp-Name",
    "Access-Control-Expose-Headers": "Mcp-Session-Id, MCP-Protocol-Version",
    "Content-Length": Buffer.byteLength(data),
    ...extraHeaders
  });
  res.end(data);
}

function authorized(req) {
  return req.headers.authorization === `Bearer ${TOKEN}`;
}

function bridgeReady() {
  return !!(bridge && bridge.readyState === WebSocket.OPEN);
}

// FIX: Bulletproof readBody with running byte counter
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let totalBytes = 0;

    req.on("data", chunk => {
      chunks.push(chunk);
      totalBytes += chunk.length;
      if (totalBytes > 10 * 1024 * 1024) {
        reject(new Error("Request body too large"));
        req.destroy();
      }
    });

    req.on("end", () => {
      resolve(Buffer.concat(chunks).toString("utf8"));
    });
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, Accept, MCP-Protocol-Version, Mcp-Session-Id, Mcp-Method, Mcp-Name",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
    });
    return res.end();
  }

  if (req.method === "GET" && req.url === "/health") {
    return sendJson(res, 200, {
      ok: true,
      bridgeConnected: bridgeReady(),
      service: "vscode-mcp-relay"
    });
  }

  if (req.method === "POST" && req.url === "/mcp") {
    if (!authorized(req)) {
      return sendJson(res, 401, { jsonrpc: "2.0", error: { code: -32001, message: "Unauthorized" }, id: null });
    }
    if (!bridgeReady()) {
      return sendJson(res, 503, { jsonrpc: "2.0", error: { code: -32000, message: "Local VS Code bridge is offline" }, id: null });
    }

    let body;
    try {
      body = await readBody(req);
    } catch (err) {
      return sendJson(res, 413, { jsonrpc: "2.0", error: { code: -32600, message: err.message }, id: null });
    }

    if (!body || body.trim() === "") {
      return sendJson(res, 400, { jsonrpc: "2.0", error: { code: -32700, message: "Invalid JSON: Empty body received" }, id: null });
    }

    let payload;
    try {
      payload = JSON.parse(body);
    } catch (e) {
      return sendJson(res, 400, { jsonrpc: "2.0", error: { code: -32700, message: "Invalid JSON: " + e.message }, id: null });
    }

    const requestId = id();
    const headers = {
      "mcp-protocol-version": req.headers["mcp-protocol-version"] || "",
      "mcp-session-id": req.headers["mcp-session-id"] || "",
      "mcp-method": req.headers["mcp-method"] || "",
      "mcp-name": req.headers["mcp-name"] || ""
    };

    console.log(`[RELAY] HTTP request. Method: ${payload.method || "unknown"}. Body length: ${body.length}`);

    const timer = setTimeout(() => {
      const job = pending.get(requestId);
      if (!job) return;
      pending.delete(requestId);
      if (!job.res.headersSent) {
        sendJson(job.res, 504, { jsonrpc: "2.0", error: { code: -32000, message: "Local bridge timeout" }, id: payload.id ?? null });
      }
    }, 120000);

    pending.set(requestId, { res, timer });
    bridge.send(JSON.stringify({ type: "request", id: requestId, payload, headers }));
    return;
  }

  if (req.method === "POST" && req.url === "/push") {
    if (!authorized(req)) return sendJson(res, 401, { ok: false, error: "Unauthorized" });
    if (!bridgeReady()) return sendJson(res, 503, { ok: false, error: "Local VS Code bridge is offline" });

    let body;
    try { body = await readBody(req); } catch (err) { return sendJson(res, 413, { ok: false, error: err.message }); }

    let payload;
    try { payload = JSON.parse(body); } catch { return sendJson(res, 400, { ok: false, error: "Invalid JSON" }); }

    const requestId = id();
    const timer = setTimeout(() => {
      const job = pending.get(requestId);
      if (!job) return;
      pending.delete(requestId);
      if (!job.res.headersSent) sendJson(res, 504, { ok: false, error: "Local bridge timeout", id: requestId });
    }, 120000);

    pending.set(requestId, { res, timer });
    bridge.send(JSON.stringify({ type: "request", id: requestId, payload, headers: {} }));
    return;
  }

  if (req.method === "GET" && req.url === "/mcp") {
    res.writeHead(405, { "Allow": "POST", "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
    return res.end(JSON.stringify({ error: "Use POST /mcp" }));
  }

  sendJson(res, 404, { ok: false, error: "Not found" });
});

const wss = new WebSocket.Server({ server, path: "/bridge" });

wss.on("connection", (socket, request) => {
  const url = new URL(request.url, `http://${request.headers.host}`);
  const suppliedToken = url.searchParams.get("token");

  if (suppliedToken !== TOKEN) {
    socket.close(1008, "Unauthorized");
    return;
  }

  if (bridgeReady()) {
    bridge.close(1000, "Replaced by newer bridge");
  }

  bridge = socket;
  console.log("[RELAY] local bridge connected");

  socket.on("message", raw => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      console.error("[RELAY] invalid bridge message");
      return;
    }

    if (message.type !== "response") return;

    const job = pending.get(message.id);
    if (!job) return;

    pending.delete(message.id);
    clearTimeout(job.timer);

    if (job.res.headersSent) return;

    const responseHeaders = {};

    // FIX: Map lowercase WebSocket keys back to standard HTTP headers
    if (message.headers && message.headers["mcp-session-id"]) {
      responseHeaders["Mcp-Session-Id"] = message.headers["mcp-session-id"];
    }
    if (message.headers && message.headers["mcp-protocol-version"]) {
      responseHeaders["MCP-Protocol-Version"] = message.headers["mcp-protocol-version"];
    }

    sendJson(job.res, message.status || 200, message.payload, responseHeaders);
  });

  const heartbeat = setInterval(() => {
    if (socket.readyState === WebSocket.OPEN) socket.ping();
  }, 30000);

  socket.on("close", () => {
    clearInterval(heartbeat);
    if (bridge === socket) {
      bridge = null;
      console.log("[RELAY] local bridge disconnected");
    }
  });

  socket.on("error", error => {
    console.error("[RELAY] WebSocket error:", error.message);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[RELAY] listening on ${PORT}`);
});
