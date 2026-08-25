"use strict";

const http = require("http");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 10000);
const PUBLIC_ROUTE = process.env.PUBLIC_ROUTE || "/hutgugbrtuy574586776";
const RELAY_TOKEN = process.env.RELAY_TOKEN;
const LOCAL_MCP_URL = process.env.LOCAL_MCP_URL || "http://127.0.0.1:8091/mcp";

if (!RELAY_TOKEN) {
  console.error("Missing RELAY_TOKEN");
  process.exit(1);
}

let bridge = null;
const pending = new Map();

// Cloud load balancers (Render included) silently kill idle WebSocket
// connections after ~55-60s without a clean close frame. Without a
// heartbeat, `bridge.readyState` keeps reporting OPEN on a connection
// that's actually dead, so requests get sent into a black hole and only
// fail once the 120s pending-request timer expires — or once some other
// request happens to notice bridgeReady() is false. Pinging every 25s and
// terminating anything that misses two pongs makes `bridge` become null
// immediately when the link actually dies, which turns a mystery 120s
// hang into an honest, immediate 503.
const HEARTBEAT_INTERVAL_MS = 25000;
const heartbeatTimer = setInterval(() => {
  if (!bridge) return;
  if (bridge.isAlive === false) {
    console.log("[BRIDGE] heartbeat missed twice — terminating stale connection");
    bridge.terminate(); // fires 'close' below, which clears `bridge`
    return;
  }
  bridge.isAlive = false;
  try {
    bridge.ping();
  } catch (_) {
    // socket already going away; 'close' handler will clean up
  }
}, HEARTBEAT_INTERVAL_MS);
heartbeatTimer.unref();

function id() {
  return crypto.randomUUID();
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;

    req.on("data", chunk => {
      chunks.push(chunk);
      total += chunk.length;
      if (total > 10 * 1024 * 1024) {
        reject(new Error("Request body too large"));
        req.destroy();
      }
    });

    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
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

function bridgeReady() {
  return !!(bridge && bridge.readyState === WebSocket.OPEN);
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
      service: "mcp-public-relay"
    });
  }

  if (req.method === "POST" && req.url === PUBLIC_ROUTE) {
    if (!bridgeReady()) {
      return sendJson(res, 503, {
        jsonrpc: "2.0",
        error: { code: -32000, message: "Local bridge is offline" },
        id: null
      });
    }

    let raw;
    try {
      raw = await readBody(req);
    } catch (err) {
      return sendJson(res, 413, {
        jsonrpc: "2.0",
        error: { code: -32600, message: err.message },
        id: null
      });
    }

    let payload;
    try {
      payload = JSON.parse(raw);
    } catch (e) {
      return sendJson(res, 400, {
        jsonrpc: "2.0",
        error: { code: -32700, message: "Invalid JSON: " + e.message },
        id: null
      });
    }

    const requestId = id();
    const headers = {
      "mcp-protocol-version": req.headers["mcp-protocol-version"] || "",
      "mcp-session-id": req.headers["mcp-session-id"] || "",
      "mcp-method": req.headers["mcp-method"] || "",
      "mcp-name": req.headers["mcp-name"] || ""
    };

    const timer = setTimeout(() => {
      const job = pending.get(requestId);
      if (!job) return;
      pending.delete(requestId);
      if (!job.res.headersSent) {
        sendJson(job.res, 504, {
          jsonrpc: "2.0",
          error: { code: -32000, message: "Local bridge timeout" },
          id: payload.id ?? null
        });
      }
    }, 120000);

    pending.set(requestId, { res, timer });

    bridge.send(JSON.stringify({
      type: "request",
      id: requestId,
      payload,
      headers
    }));

    return;
  }

  sendJson(res, 404, { ok: false, error: "Not found" });
});

const wss = new WebSocket.Server({ server, path: "/bridge" });

wss.on("connection", (socket, req) => {
  const url = new URL(req.url, "http://localhost");
  const token = url.searchParams.get("token");

  if (token !== RELAY_TOKEN) {
    socket.close(1008, "Unauthorized");
    return;
  }

  if (bridge && bridge.readyState === WebSocket.OPEN) {
    bridge.close(1000, "Replaced by newer bridge");
  }

  bridge = socket;
  bridge.isAlive = true;
  console.log("[BRIDGE] local client connected");

  socket.on("pong", () => {
    socket.isAlive = true;
  });

  socket.on("message", raw => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      console.error("[BRIDGE] invalid message");
      return;
    }

    if (msg.type !== "response") return;

    const job = pending.get(msg.id);
    if (!job) return;

    pending.delete(msg.id);
    clearTimeout(job.timer);

    if (job.res.headersSent) return;

    const headers = {};
    if (msg.headers && msg.headers["mcp-session-id"]) {
      headers["Mcp-Session-Id"] = msg.headers["mcp-session-id"];
    }
    if (msg.headers && msg.headers["mcp-protocol-version"]) {
      headers["MCP-Protocol-Version"] = msg.headers["mcp-protocol-version"];
    }

    sendJson(job.res, msg.status || 200, msg.payload, headers);
  });

  socket.on("close", () => {
    if (bridge === socket) {
      bridge = null;
      console.log("[BRIDGE] local client disconnected");
    }
  });

  socket.on("error", err => {
    console.error("[BRIDGE] error:", err.message);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[PUBLIC] listening on ${PORT}`);
  console.log(`[PUBLIC] GET /health`);
  console.log(`[PUBLIC] POST ${PUBLIC_ROUTE}`);
  console.log(`[PUBLIC] WS /bridge?token=...`);
});
