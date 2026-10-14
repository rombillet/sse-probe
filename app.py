"""SSE probe for task 08 — does a hosting platform hold a 600 s event stream open, unbuffered?

⛔ This is NOT CommuniGate. It imports nothing from it, reads no database, holds no secret and
answers no question about management. It exists to answer one question about a *platform*:

    Can a 600-second Server-Sent Events response, silent for its first 45 seconds except for a
    10-second keepalive COMMENT, reach a browser through this vendor's edge, event by event,
    without being buffered and without being cut early?

**Faithfulness to production, stated so it can be checked rather than trusted:**

| what production does | where | what this file does |
|---|---|---|
| `response_class=EventSourceResponse`, generator yields `ServerSentEvent` | `api/streams.py:279` | identical — the same FastAPI 0.141.1 code path |
| ⛔ **sends `Cache-Control: no-cache` AND `X-Accel-Buffering: no`** — not from `streams.py` and not from `EventSourceResponse`, but set by FastAPI's routing layer for every SSE route | `fastapi/routing.py:643-645` | identical, because `/stream/native` travels the same routing layer. **That is the endpoint that tests our actual product.** |
| keepalive is an SSE **comment**, `ServerSentEvent(comment="ping")` | `api/streams.py:309` | identical |
| ends the stream itself at `stream_max_seconds = 600.0` | `api/streams.py:303` | identical, and the default here is 600 |
| polls every `stream_poll_seconds = 0.5` | `config.py` | identical |

⚠ **The one deliberate deviation, declared:** production's comment is the bare word `ping`; this
one appends the server's elapsed seconds (`ping 41.0`). A comment is still a comment — invisible to
`EventSource`, ignored by every SSE parser — but it lets the raw reader below *measure* keepalive
timing instead of merely counting bytes. Nothing about buffering changes with six extra bytes.

⛔⛔ **A CORRECTION, recorded here because it was wrong in the plan Rom approved.** The plan said
production sends no anti-buffering headers at all, inferred from reading `fastapi/sse.py`, where
`EventSourceResponse` sets only `media_type`. **Measured against a running server, that is false:**
FastAPI's *routing* layer adds `Cache-Control: no-cache` and `X-Accel-Buffering: no` to every SSE
response (`routing.py:643-645`), so CommuniGate already sends both today. ★ *A class was read and a
behaviour was concluded — this project's named failure mode, found by running the thing.*

**What that leaves for `/stream/hardened`:** it adds only `no-transform` (which tells a CDN not to
recompress or rewrite the body) and `Connection: keep-alive` (hop-by-hop, and likely ignored). The
A/B is therefore **narrower than advertised** — but it is the difference that matters against a
transforming CDN, so both endpoints stay.
"""

from __future__ import annotations

import sys
import time
from collections.abc import AsyncIterator

import anyio
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.responses import EventSourceResponse
from fastapi.sse import ServerSentEvent

app = FastAPI(title="SSE probe (task 08)", docs_url=None, redoc_url=None)

# Mirrors CommuniGate's own numbers. Overridable per request so a 10-minute test can be
# rehearsed in 20 seconds before anyone waits 10 minutes for it.
STREAM_MAX_SECONDS = 600.0
KEEPALIVE_SECONDS = 10.0
FIRST_EVENT_SECONDS = 45.0
EVENT_EVERY_SECONDS = 30.0
POLL_SECONDS = 0.5


@app.middleware("http")
async def _harden_one_path(request, call_next):
    """Add anti-buffering headers to `/stream/hardened` and to nothing else.

    ⛔ Deliberately a middleware rather than a second response class: both endpoints then travel
    the identical FastAPI SSE code path and the ONLY difference between them is these three
    headers. A comparison where two things changed would prove nothing.
    """
    response = await call_next(request)
    if request.url.path == "/stream/hardened":
        response.headers["Cache-Control"] = "no-cache, no-transform"
        response.headers["X-Accel-Buffering"] = "no"
        response.headers["Connection"] = "keep-alive"
    return response


async def _timeline(
    *, seconds: float, keepalive: float, first_event: float, every: float
) -> AsyncIterator[ServerSentEvent]:
    """The same loop shape as `api/streams.py`: poll, maybe send, maybe keepalive, sleep."""
    t0 = time.monotonic()
    last_sent = t0
    seq = 0
    next_event_at = first_event

    while True:
        now = time.monotonic()
        elapsed = now - t0

        if elapsed >= seconds:
            yield ServerSentEvent(
                event="end",
                id=str(seq + 1),
                data={
                    "seq": seq + 1,
                    "t_server": round(elapsed, 3),
                    "sent_at_ms": int(time.time() * 1000),
                    "reason": "stream_max_seconds",
                },
            )
            return

        if elapsed >= next_event_at:
            seq += 1
            yield ServerSentEvent(
                event="progress",
                id=str(seq),
                data={
                    "seq": seq,
                    "t_server": round(elapsed, 3),
                    "sent_at_ms": int(time.time() * 1000),
                },
            )
            last_sent = time.monotonic()
            next_event_at = first_event + seq * every
        elif now - last_sent >= keepalive:
            # A comment, exactly as production sends. `EventSource` cannot see this line at all —
            # which is the whole reason the probe page also reads the raw bytes.
            yield ServerSentEvent(comment=f"ping {elapsed:.1f}")
            last_sent = time.monotonic()

        await anyio.sleep(POLL_SECONDS)


def _params(
    seconds: float, keepalive: float, first_event: float, every: float
) -> dict[str, float]:
    return {
        "seconds": seconds,
        "keepalive": keepalive,
        "first_event": first_event,
        "every": every,
    }


@app.get("/stream/native", response_class=EventSourceResponse)
async def stream_native(
    seconds: float = Query(STREAM_MAX_SECONDS, gt=0, le=3600),
    keepalive: float = Query(KEEPALIVE_SECONDS, gt=0),
    first_event: float = Query(FIRST_EVENT_SECONDS, ge=0),
    every: float = Query(EVENT_EVERY_SECONDS, gt=0),
) -> AsyncIterator[ServerSentEvent]:
    """⛔ **This is the endpoint that tests the product we actually have.** No added headers."""
    async for event in _timeline(**_params(seconds, keepalive, first_event, every)):
        yield event


@app.get("/stream/hardened", response_class=EventSourceResponse)
async def stream_hardened(
    seconds: float = Query(STREAM_MAX_SECONDS, gt=0, le=3600),
    keepalive: float = Query(KEEPALIVE_SECONDS, gt=0),
    first_event: float = Query(FIRST_EVENT_SECONDS, ge=0),
    every: float = Query(EVENT_EVERY_SECONDS, gt=0),
) -> AsyncIterator[ServerSentEvent]:
    """The same stream, plus `Cache-Control: no-cache, no-transform` and `X-Accel-Buffering: no`."""
    async for event in _timeline(**_params(seconds, keepalive, first_event, every)):
        yield event


@app.get("/healthz")
async def healthz() -> dict[str, object]:
    return {"ok": True, "python": sys.version.split()[0], "server_time_ms": int(time.time() * 1000)}


@app.get("/", response_class=HTMLResponse)
@app.get("/probe", response_class=HTMLResponse)
async def probe_page() -> HTMLResponse:
    return HTMLResponse(PAGE)


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SSE probe — task 08</title>
<style>
  :root { color-scheme: light dark; --fg:#111; --bg:#fff; --mut:#666; --line:#ddd;
          --ok:#0a7c2f; --bad:#c01c28; --warn:#9a6700; }
  @media (prefers-color-scheme: dark) {
    :root { --fg:#e8e8e8; --bg:#16181c; --mut:#9aa0a6; --line:#333;
            --ok:#4ade80; --bad:#ff6b6b; --warn:#fbbf24; }
  }
  body { margin:0; padding:16px; font:14px/1.5 ui-sans-serif,system-ui,sans-serif;
         color:var(--fg); background:var(--bg); }
  h1 { font-size:17px; margin:0 0 4px; } p.sub { margin:0 0 16px; color:var(--mut); }
  fieldset { border:1px solid var(--line); border-radius:8px; padding:12px; margin:0 0 16px; }
  legend { color:var(--mut); padding:0 6px; }
  label { display:inline-block; margin:0 14px 8px 0; }
  input[type=number] { width:76px; } select { padding:2px; }
  button { font:inherit; padding:7px 16px; border-radius:7px; border:1px solid var(--line);
           background:var(--fg); color:var(--bg); cursor:pointer; }
  button.ghost { background:transparent; color:var(--fg); }
  button[disabled] { opacity:.4; cursor:default; }
  #verdict { font-size:16px; font-weight:600; padding:12px; border-radius:8px;
             border:1px solid var(--line); margin:0 0 16px; }
  .ok{color:var(--ok)} .bad{color:var(--bad)} .warn{color:var(--warn)} .mut{color:var(--mut)}
  table { border-collapse:collapse; width:100%; font-variant-numeric:tabular-nums; }
  th,td { text-align:left; padding:3px 10px 3px 0; border-bottom:1px solid var(--line);
          white-space:nowrap; }
  th { color:var(--mut); font-weight:500; }
  .wrap { overflow-x:auto; }
  #log { max-height:420px; overflow-y:auto; }
  code,pre { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }
  pre { white-space:pre-wrap; border:1px solid var(--line); border-radius:8px; padding:12px; }
</style></head><body>

<h1>SSE probe — can this platform hold a 600 s stream open, unbuffered?</h1>
<p class="sub">Two independent readers on two connections. <code>EventSource</code> is what
CommuniGate's frontend actually uses — and it <strong>cannot see keepalive comments</strong>. The raw
<code>fetch</code> reader sees every byte, including <code>: ping</code>. Run both.</p>

<fieldset><legend>settings</legend>
  <label>endpoint
    <select id="ep">
      <option value="/stream/native">/stream/native — production shape, no extra headers</option>
      <option value="/stream/hardened">/stream/hardened — with anti-buffering headers</option>
    </select></label><br>
  <label>total seconds <input type="number" id="seconds" value="600" min="5" max="3600"></label>
  <label>keepalive <input type="number" id="keepalive" value="10" min="1"></label>
  <label>first event at <input type="number" id="first" value="45" min="0"></label>
  <label>event every <input type="number" id="every" value="30" min="1"></label>
  <br>
  <button id="start">Start</button>
  <button id="stop" class="ghost" disabled>Stop</button>
  <button id="copy" class="ghost" disabled>Copy result as Markdown</button>
</fieldset>

<div id="verdict" class="mut">Not started.</div>

<div class="wrap"><table id="summary"><thead><tr>
  <th>reader</th><th>opened</th><th>frames</th><th>pings</th><th>events</th>
  <th>max gap</th><th>worst drift</th><th>ended at</th><th>state</th>
</tr></thead><tbody></tbody></table></div>

<h3 style="font-size:14px;margin:18px 0 6px">Observations</h3>
<div class="wrap" id="log"><table id="obs"><thead><tr>
  <th>client t</th><th>reader</th><th>what</th><th>server t</th>
  <th>client gap</th><th>server gap</th><th>drift</th>
</tr></thead><tbody></tbody></table></div>

<script>
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };
  var run = null;

  function fmt(x, d) { return (x === null || x === undefined || isNaN(x)) ? "—" : x.toFixed(d === undefined ? 1 : d); }

  function newReader(name) {
    return { name: name, opened: null, frames: 0, pings: 0, events: 0, lastClient: null,
             lastServer: null, maxGap: 0, worstDrift: 0, endedAt: null, state: "idle",
             closedCleanly: false, reconnects: 0 };
  }

  function note(r, what, serverT, clientT, cls) {
    var clientGap = r.lastClient === null ? null : clientT - r.lastClient;
    var serverGap = (serverT === null || r.lastServer === null) ? null : serverT - r.lastServer;
    var drift = (clientGap !== null && serverGap !== null) ? clientGap - serverGap : null;
    if (clientGap !== null && clientGap > r.maxGap) r.maxGap = clientGap;
    if (drift !== null && Math.abs(drift) > Math.abs(r.worstDrift)) r.worstDrift = drift;
    r.lastClient = clientT;
    if (serverT !== null) r.lastServer = serverT;

    var tb = $("obs").tBodies[0];
    var tr = tb.insertRow(0);
    tr.className = cls || "";
    [fmt(clientT), r.name, what, serverT === null ? "—" : fmt(serverT),
     fmt(clientGap), fmt(serverGap), drift === null ? "—" : fmt(drift, 2)
    ].forEach(function (v) { tr.insertCell(-1).textContent = v; });
    while (tb.rows.length > 400) tb.deleteRow(tb.rows.length - 1);
    render();
  }

  function render() {
    if (!run) return;
    var tb = $("summary").tBodies[0];
    tb.innerHTML = "";
    [run.es, run.raw].forEach(function (r) {
      var tr = tb.insertRow(-1);
      [r.name, r.opened === null ? "—" : fmt(r.opened) + " s", r.frames, r.pings, r.events,
       fmt(r.maxGap) + " s", fmt(r.worstDrift, 2) + " s",
       r.endedAt === null ? "—" : fmt(r.endedAt) + " s", r.state
      ].forEach(function (v) { tr.insertCell(-1).textContent = v; });
    });
    verdict();
  }

  function verdict() {
    var v = $("verdict"), p = run.params, fails = [], warns = [];
    var elapsed = (performance.now() - run.t0) / 1000;
    var done = run.es.state !== "open" && run.raw.state !== "open";

    // 1. Cut early. The server ends the stream itself at `seconds`; anything sooner is the platform.
    [run.es, run.raw].forEach(function (r) {
      if (r.endedAt !== null && !r.closedCleanly && r.endedAt < p.seconds - 5)
        fails.push(r.name + " was cut at " + fmt(r.endedAt) + " s, before the server's "
                   + p.seconds + " s");
    });
    // 2. Buffering. Frames the server spaced out arriving bunched = a proxy held them.
    var gapCeiling = Math.max(p.keepalive, p.every) * 2 + 5;
    [run.es, run.raw].forEach(function (r) {
      if (r.maxGap > gapCeiling)
        fails.push(r.name + " saw a " + fmt(r.maxGap) + " s silence (ceiling "
                   + fmt(gapCeiling) + " s) — that is buffering");
      if (Math.abs(r.worstDrift) > 2.5)
        fails.push(r.name + " drifted " + fmt(r.worstDrift, 2)
                   + " s against the server's own spacing");
    });
    // 3. Keepalives must actually arrive. Only the raw reader can see them.
    if (done && run.raw.opened !== null) {
      var expected = Math.floor(Math.min(p.seconds, p.first_event) / p.keepalive);
      if (run.raw.pings < expected * 0.8)
        fails.push("raw reader saw " + run.raw.pings + " keepalives, expected about "
                   + expected + " in the first " + p.first_event + " s");
    }
    if (run.es.pings > 0)
      warns.push("EventSource reported a comment — it is not supposed to be able to");
    if (run.es.reconnects > 0)
      warns.push("EventSource reconnected " + run.es.reconnects
                 + "× (expected once, after the clean close at " + p.seconds + " s)");

    var head;
    if (fails.length) { v.className = "bad"; head = "⛔ FAIL"; }
    else if (!done) { v.className = "mut"; head = "running — " + fmt(elapsed) + " s of " + p.seconds; }
    else { v.className = "ok"; head = "✅ PASS"; }

    var body = fails.length ? fails.map(function (f) { return "• " + f; }).join("\n")
             : (done ? "Stream held " + p.seconds + " s. Events arrived with the server's own "
                       + "spacing. " + run.raw.pings + " keepalives seen on the wire."
                     : "");
    if (warns.length) body += (body ? "\n" : "") + warns.map(function (w) { return "⚠ " + w; }).join("\n");
    v.textContent = head + (body ? "\n" + body : "");
    v.style.whiteSpace = "pre-wrap";
    run.verdictText = v.textContent;
    $("copy").disabled = !done && !fails.length;
  }

  function startEventSource(url) {
    var r = run.es, es = new EventSource(url);
    run.esHandle = es;
    r.state = "connecting";
    es.onopen = function () {
      r.state = "open";
      if (r.opened === null) r.opened = (performance.now() - run.t0) / 1000;
      note(r, "open", null, (performance.now() - run.t0) / 1000, "mut");
    };
    function onData(kind) {
      return function (ev) {
        var clientT = (performance.now() - run.t0) / 1000, d = {};
        try { d = JSON.parse(ev.data); } catch (e) { d = {}; }
        r.frames++; r.events++;
        note(r, kind + " #" + (d.seq === undefined ? "?" : d.seq),
             d.t_server === undefined ? null : d.t_server, clientT, "");
        if (kind === "end") {
          r.closedCleanly = true;
          r.endedAt = clientT;
          r.state = "closed by server";
          es.close();
          render();
        }
      };
    }
    es.addEventListener("progress", onData("progress"));
    es.addEventListener("end", onData("end"));
    es.onmessage = onData("message");
    es.onerror = function () {
      var clientT = (performance.now() - run.t0) / 1000;
      if (es.readyState === 2) {
        if (r.endedAt === null) { r.endedAt = clientT; r.state = "closed"; }
      } else {
        r.reconnects++;
        r.state = "reconnecting";
        if (r.endedAt === null) r.endedAt = clientT;
        note(r, "connection dropped → reconnecting", null, clientT, "warn");
        // One reconnect after the server's own clean close is expected and is not a failure;
        // we stop there rather than loop for another ten minutes.
        if (r.closedCleanly || r.reconnects >= 1) { es.close(); r.state = "stopped after reconnect"; }
      }
      render();
    };
  }

  function startRaw(url) {
    var r = run.raw;
    r.state = "connecting";
    var ctl = new AbortController();
    run.rawAbort = ctl;
    fetch(url, { cache: "no-store", signal: ctl.signal }).then(function (resp) {
      r.state = "open";
      r.opened = (performance.now() - run.t0) / 1000;
      note(r, "open · HTTP " + resp.status + " · " + (resp.headers.get("content-type") || "?"),
           null, r.opened, "mut");
      var reader = resp.body.getReader(), dec = new TextDecoder();
      function pump() {
        return reader.read().then(function (res) {
          var clientT = (performance.now() - run.t0) / 1000;
          if (res.done) {
            r.endedAt = clientT;
            r.state = r.closedCleanly ? "closed by server" : "closed";
            render();
            return;
          }
          var text = dec.decode(res.value, { stream: true });
          r.frames++;
          // One network chunk can carry several SSE lines; count and time each line.
          text.split("\n").forEach(function (line) {
            if (line.indexOf(":") === 0 && line.indexOf("ping") > -1) {
              r.pings++;
              var t = parseFloat(line.replace(/[^0-9.]/g, ""));
              note(r, "ping", isNaN(t) ? null : t, clientT, "mut");
            } else if (line.indexOf("data:") === 0) {
              r.events++;
              var d = {};
              try { d = JSON.parse(line.slice(5).trim()); } catch (e) { d = {}; }
              if (d.reason === "stream_max_seconds") r.closedCleanly = true;
              note(r, "data #" + (d.seq === undefined ? "?" : d.seq),
                   d.t_server === undefined ? null : d.t_server, clientT, "");
            }
          });
          return pump();
        });
      }
      return pump();
    }).catch(function (e) {
      if (ctl.signal.aborted) { r.state = "stopped"; return; }
      r.endedAt = (performance.now() - run.t0) / 1000;
      r.state = "error: " + e.message;
      render();
    });
  }

  $("start").onclick = function () {
    var p = {
      seconds: +$("seconds").value, keepalive: +$("keepalive").value,
      first_event: +$("first").value, every: +$("every").value, endpoint: $("ep").value
    };
    var url = p.endpoint + "?seconds=" + p.seconds + "&keepalive=" + p.keepalive
            + "&first_event=" + p.first_event + "&every=" + p.every;
    run = { t0: performance.now(), params: p, url: url, startedAt: new Date().toISOString(),
            es: newReader("EventSource"), raw: newReader("raw fetch") };
    $("obs").tBodies[0].innerHTML = "";
    $("start").disabled = true; $("stop").disabled = false; $("copy").disabled = true;
    startEventSource(url);
    startRaw(url);
    run.ticker = setInterval(render, 1000);
    render();
  };

  $("stop").onclick = function () {
    if (!run) return;
    if (run.esHandle) run.esHandle.close();
    if (run.rawAbort) run.rawAbort.abort();
    clearInterval(run.ticker);
    run.es.state = run.es.state === "open" ? "stopped by hand" : run.es.state;
    run.raw.state = run.raw.state === "open" ? "stopped by hand" : run.raw.state;
    $("start").disabled = false; $("stop").disabled = true; $("copy").disabled = false;
    render();
  };

  $("copy").onclick = function () {
    var p = run.params, rows = [run.es, run.raw].map(function (r) {
      return "| " + [r.name, r.opened === null ? "—" : fmt(r.opened) + " s", r.frames, r.pings,
                     r.events, fmt(r.maxGap) + " s", fmt(r.worstDrift, 2) + " s",
                     r.endedAt === null ? "—" : fmt(r.endedAt) + " s", r.state].join(" | ") + " |";
    }).join("\n");
    var md = "## SSE probe result\n\n"
      + "- **URL:** `" + location.origin + run.url + "`\n"
      + "- **started:** " + run.startedAt + "\n"
      + "- **browser:** `" + navigator.userAgent + "`\n"
      + "- **settings:** " + p.seconds + " s total, keepalive " + p.keepalive
      + " s, first event at " + p.first_event + " s, event every " + p.every + " s\n\n"
      + "| reader | opened | frames | pings | events | max gap | worst drift | ended at | state |\n"
      + "|---|---|---|---|---|---|---|---|---|\n" + rows + "\n\n"
      + "**Verdict**\n\n```\n" + (run.verdictText || "") + "\n```\n";
    navigator.clipboard.writeText(md).then(function () {
      $("copy").textContent = "Copied — paste it to Claude";
      setTimeout(function () { $("copy").textContent = "Copy result as Markdown"; }, 2500);
    });
  };
})();
</script>
</body></html>
"""
