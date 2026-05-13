/**
 * Workshop query UI.
 *
 * Wraps the Flink SQL Gateway REST API (v1/v2) in a single POST /api/query endpoint:
 *   1. open session     POST   /v1/sessions
 *   2. submit statement POST   /v1/sessions/{sid}/statements
 *   3. fetch result     GET    /v1/sessions/{sid}/operations/{opid}/result/{token}
 *      - paginate via nextResultUri until it disappears or status=FINISHED
 *   4. close session    DELETE /v1/sessions/{sid}    (always, in finally)
 *
 * Returns { columns: [...], rows: [[...], ...], rowCount, jobId? } or { error }.
 */
const express = require("express");
const path = require("path");
const fs = require("fs");

const GATEWAY = process.env.GATEWAY_URL || "http://workshop-sql-gateway:8083";
const PORT = parseInt(process.env.PORT || "3000", 10);
const MAX_ROWS = parseInt(process.env.MAX_ROWS || "5000", 10);
const POLL_MS = 250;
const POLL_TIMEOUT_MS = 60_000;

const app = express();
app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

const presets = JSON.parse(
  fs.readFileSync(path.join(__dirname, "presets.json"), "utf8"));

app.get("/api/presets", (_req, res) => res.json(presets));

app.get("/api/health", async (_req, res) => {
  try {
    const r = await fetch(`${GATEWAY}/v1/info`);
    res.status(r.ok ? 200 : 502).json({ gateway: GATEWAY, ok: r.ok, status: r.status });
  } catch (e) {
    res.status(502).json({ gateway: GATEWAY, ok: false, error: String(e) });
  }
});

app.post("/api/query", async (req, res) => {
  const sql = (req.body && req.body.sql || "").trim();
  if (!sql) return res.status(400).json({ error: "Missing 'sql' in request body" });

  let sessionId;
  try {
    sessionId = await openSession();
    const opId = await submitStatement(sessionId, sql);
    const result = await collectResult(sessionId, opId);
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  } finally {
    if (sessionId) {
      try { await fetch(`${GATEWAY}/v1/sessions/${sessionId}`, { method: "DELETE" }); }
      catch (_) { /* ignore */ }
    }
  }
});

async function openSession() {
  const body = {
    properties: {
      "execution.runtime-mode": "batch",
      "sql-gateway.session.idle-timeout": "5min"
    }
  };
  const r = await fetch(`${GATEWAY}/v1/sessions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  if (!r.ok) throw new Error(`openSession failed: ${r.status} ${await r.text()}`);
  return (await r.json()).sessionHandle;
}

async function submitStatement(sid, sql) {
  const r = await fetch(`${GATEWAY}/v1/sessions/${sid}/statements`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ statement: sql })
  });
  if (!r.ok) throw new Error(`submit failed: ${r.status} ${await r.text()}`);
  return (await r.json()).operationHandle;
}

async function collectResult(sid, opId) {
  const start = Date.now();
  let url = `${GATEWAY}/v1/sessions/${sid}/operations/${opId}/result/0`;
  let columns = null;
  const rows = [];
  let jobId = null;

  while (url) {
    if (Date.now() - start > POLL_TIMEOUT_MS) {
      throw new Error(`Query timed out after ${POLL_TIMEOUT_MS} ms`);
    }
    const r = await fetch(url);
    if (!r.ok) throw new Error(`result fetch failed: ${r.status} ${await r.text()}`);
    const page = await r.json();

    if (page.jobID) jobId = page.jobID;

    // EOS / NOT_READY: wait and re-poll the same URL
    if (page.resultType === "NOT_READY" || page.resultType === "PAYLOAD" && !page.results) {
      await sleep(POLL_MS);
      continue;
    }

    if (page.results) {
      if (!columns && page.results.columns) {
        columns = page.results.columns.map(c => ({
          name: c.name,
          type: c.logicalType && c.logicalType.type || (c.type || "UNKNOWN")
        }));
      }
      for (const d of (page.results.data || [])) {
        // d.kind is INSERT/UPDATE_BEFORE/.. ; we only show fields for INSERT
        if (d.kind && d.kind !== "INSERT") continue;
        rows.push(d.fields);
        if (rows.length >= MAX_ROWS) break;
      }
    }

    if (rows.length >= MAX_ROWS) break;

    if (page.resultType === "EOS") break;
    if (!page.nextResultUri) {
      // No next page yet; wait briefly then re-poll same URL
      await sleep(POLL_MS);
      continue;
    }
    url = page.nextResultUri.startsWith("http")
      ? page.nextResultUri
      : `${GATEWAY}${page.nextResultUri}`;
  }

  return {
    columns: columns || [],
    rows,
    rowCount: rows.length,
    truncated: rows.length >= MAX_ROWS,
    jobId
  };
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

app.listen(PORT, () => {
  console.log(`Query UI listening on :${PORT}, gateway=${GATEWAY}`);
});
