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

function sendJson(res, status, body) {
  const data = JSON.stringify(body);

  res.writeHead(status, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers":
      "Content-Type, Authorization",
    "Content-Length": Buffer.byteLength(data)
  });

  res.end(data);
}

function authorized(req) {
  return (
    req.headers.authorization ===
    `Bearer ${TOKEN}`
  );
}

function bridgeReady() {
  return !!(
    bridge &&
    bridge.readyState === WebSocket.OPEN
  );
}

const server = http.createServer((req, res) => {

  /*
   * Health check
   */
  if (
    req.method === "GET" &&
    req.url === "/health"
  ) {
    return sendJson(res, 200, {
      ok: true,
      bridgeConnected: bridgeReady(),
      service: "vscode-mcp-relay"
    });
  }

  /*
   * Public → local message endpoint.
   */
  if (
    req.method === "POST" &&
    req.url === "/push"
  ) {

    if (!authorized(req)) {
      return sendJson(res, 401, {
        ok: false,
        error: "Unauthorized"
      });
    }

    if (!bridgeReady()) {
      return sendJson(res, 503, {
        ok: false,
        error: "Local VS Code bridge is offline"
      });
    }

    let body = "";

    req.on("data", chunk => {
      body += chunk;

      if (body.length > 10 * 1024 * 1024) {
        req.destroy();
      }
    });

    req.on("end", () => {

      let payload;

      try {
        payload = JSON.parse(body);
      } catch {
        return sendJson(res, 400, {
          ok: false,
          error: "Invalid JSON"
        });
      }

      const requestId = id();

      const timer = setTimeout(() => {

        pending.delete(requestId);

        if (!res.headersSent) {
          sendJson(res, 504, {
            ok: false,
            error: "Local bridge timeout",
            id: requestId
          });
        }

      }, 120000);

      pending.set(requestId, {
        res,
        timer
      });

      bridge.send(JSON.stringify({
        type: "request",
        id: requestId,
        payload
      }));
    });

    return;
  }

  sendJson(res, 404, {
    ok: false,
    error: "Not found"
  });
});


/*
 * Local bridge connects OUTBOUND to this WebSocket.
 */
const wss = new WebSocket.Server({
  server,
  path: "/bridge"
});

wss.on("connection", (socket, request) => {

  const url = new URL(
    request.url,
    `http://${request.headers.host}`
  );

  const suppliedToken =
    url.searchParams.get("token");

  if (suppliedToken !== TOKEN) {
    console.log("[RELAY] rejected bridge");
    socket.close(1008, "Unauthorized");
    return;
  }

  if (bridgeReady()) {
    bridge.close(
      1000,
      "Replaced by newer bridge"
    );
  }

  bridge = socket;

  console.log("[RELAY] local bridge connected");


  socket.on("message", raw => {

    let message;

    try {
      message = JSON.parse(
        raw.toString()
      );
    } catch {
      console.error(
        "[RELAY] invalid bridge message"
      );
      return;
    }

    if (message.type === "pong") {
      return;
    }

    if (message.type !== "response") {
      return;
    }

    const job = pending.get(message.id);

    if (!job) {
      return;
    }

    pending.delete(message.id);
    clearTimeout(job.timer);

    if (job.res.headersSent) {
      return;
    }

    sendJson(
      job.res,
      message.status || 200,
      message.payload
    );
  });


  /*
   * Keepalive.
   */
  const heartbeat = setInterval(() => {

    if (
      socket.readyState ===
      WebSocket.OPEN
    ) {
      socket.ping();
    }

  }, 30000);


  socket.on("close", () => {

    clearInterval(heartbeat);

    if (bridge === socket) {
      bridge = null;

      console.log(
        "[RELAY] local bridge disconnected"
      );
    }
  });


  socket.on("error", error => {
    console.error(
      "[RELAY] WebSocket error:",
      error.message
    );
  });
});


server.listen(PORT, "0.0.0.0", () => {
  console.log(
    `[RELAY] listening on ${PORT}`
  );
});
