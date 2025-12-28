# -*- coding: utf-8 -*-
"""Lightweight log web UI (no third-party deps).

Features
- Server-Sent Events (SSE) realtime logs
- Two-column layout (filters+stats | grouped logs)
- Filter by SKIP/ERROR/INFO/DRY, show keyword, season
- Counters per action and level
- Collapsible groups by show
- Small charts (canvas) for action counts
- Optional token auth via URL query (?token=xxx) or header X-Token

This file intentionally avoids extra dependencies so it can run on a bare Ubuntu VPS.
"""

from __future__ import annotations

import json
import os
import queue
import re
import threading
import time
from dataclasses import dataclass, asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Type
from urllib.parse import urlparse, parse_qs

_DEFAULT_PORT = 53943


def _cn2int(s: str) -> int:
    """Convert a small set of Chinese numerals to int (1-99-ish).

    Supports: 一二三四五六七八九十, and mixed digits.
    """
    if not s:
        raise ValueError("empty")
    if s.isdigit():
        return int(s)
    table = {"零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
    if s == "十":
        return 10
    if "十" in s:
        left, _, right = s.partition("十")
        a = table.get(left, 1) if left else 1
        b = table.get(right, 0) if right else 0
        return a * 10 + b
    # simple additive (rare): 二三 -> 23 not intended; fall back to per char
    val = 0
    for ch in s:
        if ch in table:
            val = val * 10 + table[ch]
    if val <= 0:
        raise ValueError(f"cannot parse: {s}")
    return val


@dataclass
class LogEvent:
    id: int
    ts: float
    level: str        # INFO/DRY/SKIP/ERROR/...
    action: str       # rename/move/skip/error/...
    # Optional structured fields (can be empty for generic log lines)
    show: str = ""
    season: str = ""
    message: str = ""
    src: str = ""
    dst: str = ""


class LogHub:
    """Collect logs, write to file, and feed the web UI.

    API expected by renamer.py:
      - LogHub(log_file=..., also_print=True, keep=N)
      - emit(level, message)
      - close()
      - subscribe()/snapshot()/stats() for the UI
    """

    def __init__(self, log_file: str = '', also_print: bool = True, keep: int = 500):
        self.log_file = log_file
        self.also_print = also_print
        self.keep = max(1, int(keep))
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self.running = False
        self._events: List[LogEvent] = []
        self._seq: int = 0  # monotonically increasing event id
        self._subscribers: List[queue.Queue] = []
        self._counts_level: Dict[str, int] = {}
        self._counts_action: Dict[str, int] = {}
        # lightweight "context" so the UI can group logs by series
        self._ctx_show: str = ""
        self._ctx_season: str = ""
        self._fh = None
        if self.log_file:
            os.makedirs(os.path.dirname(self.log_file) or '.', exist_ok=True)
            self._fh = open(self.log_file, 'a', encoding='utf-8', buffering=1)

    def _next_id(self) -> int:
        """Return a monotonically increasing event id (thread-safe)."""
        with self._lock:
            self._seq += 1
            return self._seq



    def _infer_season(self, text: str) -> str:
        if not text:
            return ""
        m = re.search(r"(?i)\bS(\d{1,2})\b", text)
        if m:
            try:
                return f"S{int(m.group(1)):02d}"
            except Exception:
                return ""
        # 第X季
        m = re.search(r"第\s*([一二三四五六七八九十\d]{1,3})\s*季", text)
        if m:
            raw = m.group(1)
            try:
                return f"S{_cn2int(raw):02d}"
            except Exception:
                return ""
        return ""

    def _infer_structured_fields(self, message: str) -> Dict[str, str]:
        """Best-effort parse of show/season/src/dst from a plain log line."""
        msg = message or ""

        # Context: === PROCESS: /path/to/series ===
        m = re.search(r"===\s*PROCESS:\s*(.+?)\s*===", msg)
        if m:
            p = m.group(1).strip().rstrip("/")
            show = os.path.basename(p)
            self._ctx_show = show
            self._ctx_season = ""
            return {"show": show, "season": "", "src": p, "dst": ""}

        show = self._ctx_show
        season = self._infer_season(msg) or self._ctx_season

        # If we can detect a season on this line, keep it in context.
        if season and season != self._ctx_season:
            self._ctx_season = season

        src = ""
        dst = ""

        # rename /path/file.ext -> newname.ext
        m = re.search(r"\brename\s+(?P<src>.+?)\s*->\s*(?P<dst>.+)$", msg)
        if m:
            src = m.group("src").strip()
            dst = m.group("dst").strip()

        # move [name] : src_dir -> dst_dir
        m = re.search(r"\bmove\s+\[(?P<name>.+?)\]\s*:\s*(?P<src>.+?)\s*->\s*(?P<dst>.+)$", msg)
        if m:
            name = m.group("name").strip()
            src_dir = m.group("src").strip()
            dst_dir = m.group("dst").strip()
            src = f"{src_dir.rstrip('/')}/{name}" if src_dir else name
            dst = f"{dst_dir.rstrip('/')}/{name}" if dst_dir else name

        # mkdir /path
        m = re.search(r"\bmkdir\s+(?P<dst>.+)$", msg)
        if m and not dst:
            dst = m.group("dst").strip()

        return {"show": show, "season": season, "src": src, "dst": dst}

    def _infer_action(self, msg: str) -> str:
        m = msg.lower()
        if '[dry]' in m and 'rename' in m:
            return 'rename'
        if '[dry]' in m and 'move' in m:
            return 'move'
        if 'rename ' in m:
            return 'rename'
        if ' move ' in m or m.startswith('move '):
            return 'move'
        if '[skip]' in m:
            return 'skip'
        if '[error]' in m:
            return 'error'
        return ''

    def emit(self, level: str, message: str) -> None:
        ts = _now_ts()
        line = f"{ts} | {level:<6} | {message}"
        if self._fh:
            try:
                self._fh.write(line + "\n")
            except Exception:
                pass
        if self.also_print:
            try:
                print(line)
            except Exception:
                pass
        fields = self._infer_structured_fields(message)
        ev = LogEvent(
            id=self._next_id(),
            ts=ts,
            level=level,
            message=message,
            action=self._infer_action(message),
            show=fields.get("show", ""),
            season=fields.get("season", ""),
            src=fields.get("src", ""),
            dst=fields.get("dst", ""),
        )
        self.push(ev)

    def push(self, ev: LogEvent) -> None:
        with self._lock:
            self._events.append(ev)
            if len(self._events) > self.keep:
                self._events = self._events[-self.keep :]
            self._counts_level[ev.level] = self._counts_level.get(ev.level, 0) + 1
            if ev.action:
                self._counts_action[ev.action] = self._counts_action.get(ev.action, 0) + 1
            for q in list(self._subscribers):
                try:
                    q.put_nowait(ev)
                except Exception:
                    pass

    def snapshot(self, limit: int = 2000, since: int = 0) -> List[LogEvent]:
        with self._lock:
            if since and since > 0:
                evs = [e for e in self._events if e.id > since]
                return evs[-limit:]
            return list(self._events[-limit:])

    def stats(self) -> Dict[str, object]:
        with self._lock:
            return {
                "counts_level": dict(self._counts_level),
                "counts_action": dict(self._counts_action),
                "total": len(self._events),
                "running": bool(getattr(self, "running", False)),
                "stop_requested": self.stop_requested(),
            }

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue()
        with self._lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def request_stop(self) -> None:
        """Request the running job to stop gracefully.

        The renamer loop checks `hub.stop_requested()` periodically.
        """
        try:
            self._stop_event.set()
            self.emit("WARN", "[STOP] stop requested")
        except Exception:
            self._stop_event.set()

    def stop_requested(self) -> bool:
        return bool(self._stop_event.is_set())

    def close(self):
        if self._fh:
            try:
                self._fh.close()
            except Exception:
                pass
def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _token_ok(handler: BaseHTTPRequestHandler, token: str) -> bool:
    if not token:
        return True
    # header
    t = handler.headers.get("X-Token") or ""
    if t == token:
        return True
    # query
    qs = parse_qs(urlparse(handler.path).query)
    return (qs.get("token") or [""])[0] == token


def make_handler(hub: LogHub, token: str) -> Type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "LogUI/2.0"

        def _send(self, code: int, body: bytes, ctype: str = "text/plain; charset=utf-8"):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def _auth_or_403(self) -> bool:
            if _token_ok(self, token):
                return True
            self._send(403, b"Forbidden\n")
            return False

        def do_OPTIONS(self):
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Token")
            self.end_headers()

        def do_GET(self):
            if self.path.startswith("/api/") or self.path.startswith("/events") or self.path.startswith("/export"):
                if not self._auth_or_403():
                    return

            if self.path.startswith("/api/stop"):
                if not self._auth_or_403():
                    return
                try:
                    hub.request_stop()
                except Exception:
                    pass
                self._send(200, json.dumps({"ok": True, "stopping": True}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                return

            if self.path.startswith("/api/stats"):
                st = hub.stats()
                counts_action = st.get("counts_action") or {}
                counts_level = st.get("counts_level") or {}
                payload = dict(st)
                payload.update({
                    "rename": int(counts_action.get("rename", 0) or 0),
                    "move": int(counts_action.get("move", 0) or 0),
                    "skip": int((counts_action.get("skip", 0) or 0) + (counts_action.get("dry", 0) or 0)),
                    "dry": int(counts_action.get("dry", 0) or 0),
                    "error": int(counts_level.get("ERROR", 0) or 0),
                })
                self._send(200, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                return

            if self.path.startswith("/api/events"):
                # Supports incremental polling: /api/events?since=<id>
                # NOTE: avoid NameError: use imported helpers
                parsed = urlparse(self.path)
                qs = parse_qs(parsed.query or "")
                since = 0
                try:
                    since = int((qs.get("since") or ["0"])[0] or "0")
                except Exception:
                    since = 0
                evs = [asdict(e) for e in hub.snapshot(limit=5000, since=since)]
                self._send(200, json.dumps({"events": evs}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                return

            if self.path.startswith("/export.csv"):
                rows = ["ts,level,action,show,season,message,src,dst"]
                for e in hub.snapshot(limit=10000):
                    def esc(x: str) -> str:
                        x = (x or "").replace('"', '""')
                        return f'"{x}"'
                    rows.append(",".join([esc(e.ts), esc(e.level), esc(e.action), esc(e.show), esc(e.season), esc(e.message), esc(e.src), esc(e.dst)]))
                self._send(200, ("\n".join(rows)).encode("utf-8"), "text/csv; charset=utf-8")
                return

            if self.path.startswith("/events"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()

                q = hub.subscribe()
                try:
                    # initial ping
                    self.wfile.write(b":ok\n\n")
                    self.wfile.flush()
                    while True:
                        try:
                            ev = q.get(timeout=15)
                            data = json.dumps(asdict(ev), ensure_ascii=False).encode("utf-8")
                            self.wfile.write(b"data: " + data + b"\n\n")
                            self.wfile.flush()
                        except queue.Empty:
                            self.wfile.write(b":ping\n\n")
                            self.wfile.flush()
                except Exception:
                    pass
                finally:
                    hub.unsubscribe(q)
                return

            # default: HTML
            if not self._auth_or_403():
                return

            ui_token = token or ""
            self._send(200, _INDEX_HTML.replace("__TOKEN__", ui_token).encode("utf-8"), "text/html; charset=utf-8")

        def log_message(self, fmt, *args):
            # keep quiet
            return

    return Handler


_INDEX_HTML = r"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>EmbyRename - 实时日志</title>
  <style>
    :root{
      --bg:#f6f7f9;
      --card:#ffffff;
      --card2:#ffffff;
      --text:#111827;
      --muted:#6b7280;
      --border:#e5e7eb;
      --accent:#2563eb;
      --ok:#16a34a;
      --warn:#d97706;
      --err:#dc2626;
      --skip:#7c3aed;
      --dry:#db2777;
    }
    *{box-sizing:border-box}
    body{margin:0;font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,"PingFang SC","Microsoft YaHei",sans-serif;background:radial-gradient(1200px 600px at 10% 10%, rgba(96,165,250,.12), transparent 60%), var(--bg);color:var(--text)}
    header{position:sticky;top:0;z-index:20;background:rgba(255,255,255,.9);backdrop-filter: blur(12px);border-bottom:1px solid var(--border)}
    .wrap{max-width:1200px;margin:0 auto;padding:14px 14px}
    .title{display:flex;gap:10px;align-items:center;justify-content:space-between}
    .title h1{font-size:16px;margin:0}
    .status{display:flex;gap:10px;align-items:center;color:var(--muted);font-size:12px}
    .pill{display:inline-flex;gap:6px;align-items:center;border:1px solid var(--border);padding:6px 10px;border-radius:999px;background:rgba(255,255,255,.85)}
    .dot{width:8px;height:8px;border-radius:999px;background:var(--muted)}
    .dot.ok{background:var(--ok)}
    .dot.err{background:var(--err)}
    button{appearance:none;border:1px solid var(--border);background:rgba(255,255,255,.9);color:var(--text);padding:8px 10px;border-radius:10px;cursor:pointer;font-size:12px}
    button:hover{border-color:rgba(96,165,250,.55)}
    button.primary{border-color:rgba(37,99,235,.35);background:rgba(37,99,235,.10)}
    main{max-width:1200px;margin:0 auto;padding:14px;display:grid;grid-template-columns: 340px 1fr;gap:14px}
    @media (max-width: 980px){main{grid-template-columns:1fr}}
    .card{background:linear-gradient(180deg, rgba(255,255,255,1), rgba(249,250,251,1));border:1px solid var(--border);border-radius:16px;overflow:hidden}
    .card h2{font-size:13px;margin:0;padding:12px 12px;border-bottom:1px solid var(--border);color:var(--muted)}
    .card .content{padding:12px}
    .grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}
    .stat{padding:10px;border:1px solid var(--border);border-radius:14px;background:rgba(249,250,251,1)}
    .stat .k{color:var(--muted);font-size:12px}
    .stat .v{font-size:22px;margin-top:4px}
    .filters{display:flex;flex-wrap:wrap;gap:8px;padding:12px;border-bottom:1px solid var(--border);background:rgba(249,250,251,1)}
    select,input{appearance:none;border:1px solid var(--border);background:rgba(255,255,255,.95);color:var(--text);padding:8px 10px;border-radius:10px;font-size:12px;outline:none}
    input{min-width:160px}
    .logbox{height: calc(100vh - 260px);overflow:auto;padding:12px}
    @media (max-width: 980px){.logbox{height: 64vh}}
    details.group{border:1px solid var(--border);border-radius:14px;background:rgba(255,255,255,1);margin-bottom:10px;overflow:hidden}
    details.group[open]{background:rgba(249,250,251,1)}
    summary{list-style:none;cursor:pointer;display:flex;justify-content:space-between;align-items:center;padding:10px 12px;gap:10px}
    summary::-webkit-details-marker{display:none}
    .gtitle{display:flex;gap:10px;align-items:center;min-width:0}
    .gtitle .name{font-weight:650;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:680px}
    .gmeta{font-size:12px;color:var(--muted);white-space:nowrap}
    .badges{display:flex;gap:6px;align-items:center}
    .badge{font-size:11px;padding:2px 8px;border-radius:999px;border:1px solid var(--border);color:var(--muted)}
    .badge.err{border-color:rgba(248,113,113,.5);color:var(--err)}
    .badge.warn{border-color:rgba(251,191,36,.5);color:var(--warn)}
    .badge.ok{border-color:rgba(52,211,153,.5);color:var(--ok)}
    .badge.skip{border-color:rgba(167,139,250,.5);color:var(--skip)}
    .badge.dry{border-color:rgba(251,113,133,.5);color:var(--dry)}
    ul.gitems{margin:0;padding:0;list-style:none;border-top:1px solid var(--border)}
    li.line{display:flex;gap:10px;align-items:flex-start;padding:7px 12px;border-bottom:1px solid rgba(148,163,184,.10);font-size:12px}
    li.line:last-child{border-bottom:none}
    .ts{color:var(--muted);min-width:135px}
    .lvl{min-width:62px;font-weight:650}
    .msg{white-space:pre-wrap;word-break:break-word}
    .lv-info .lvl{color:var(--ok)}
    .lv-warn .lvl{color:var(--warn)}
    .lv-error .lvl{color:var(--err)}
    .lv-skip .lvl{color:var(--skip)}
    .lv-dry .lvl{color:var(--dry)}
    .hint{color:var(--muted);font-size:12px;line-height:1.5}
    a{color:var(--accent);text-decoration:none}
    a:hover{text-decoration:underline}
  </style>
</head>
<body>
<header>
  <div class="wrap">
    <div class="title">
      <div style="display:flex;gap:10px;align-items:center">
        <h1>EmbyRename - 实时日志</h1>
        <span class="pill" id="pillConn"><span class="dot" id="dotConn"></span><span id="connText">连接中…</span></span>
        <span class="pill" id="pillRun"><span class="dot" id="dotRun"></span><span id="runText">状态：未知</span></span>
      </div>
      <div class="status">
        <button class="primary" id="btnStop">请求停止任务</button>
        <button id="btnRefresh">刷新</button>
      </div>
    </div>
  </div>
</header>

<main>
  <section class="card">
    <h2>统计</h2>
    <div class="content">
      <div class="grid2">
        <div class="stat"><div class="k">已重命名</div><div class="v" id="stRen">0</div></div>
        <div class="stat"><div class="k">已移动</div><div class="v" id="stMov">0</div></div>
        <div class="stat"><div class="k">已跳过</div><div class="v" id="stSkip">0</div></div>
        <div class="stat"><div class="k">错误</div><div class="v" id="stErr">0</div></div>
      </div>
      <div style="margin-top:10px" class="hint">
        提示：日志会按<strong>剧名</strong>分组显示，默认折叠。点击每个分组标题可展开/折叠。
      </div>
      <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
        <button id="btnExpandAll">展开全部</button>
        <button id="btnCollapseAll">折叠全部</button>
        <button id="btnClearLocal">清空本页缓存</button>
      </div>
    </div>
  </section>

  <section class="card">
    <h2>实时日志</h2>
    <div class="filters">
      <select id="fLevel">
        <option value="ALL">全部级别</option>
        <option value="ERROR">ERROR</option>
        <option value="WARN">WARN</option>
        <option value="INFO">INFO</option>
        <option value="SKIP">SKIP</option>
        <option value="DRY">DRY</option>
      </select>
      <input id="fSeason" placeholder="Season 过滤：如 S01" />
      <input id="fShow" placeholder="Show 过滤：如 暗河传" />
      <input id="fKeyword" placeholder="关键词过滤：如 rename / tmdb" />
      <button id="btnReset">重置过滤</button>
    </div>
    <div class="logbox" id="logGroups"></div>
  </section>
</main>

<script>
(function(){
  const params = new URLSearchParams(location.search);
  const token = params.get('token') || '';
  const headers = token ? {'X-Token': token} : {};

  const dotConn = document.getElementById('dotConn');
  const connText = document.getElementById('connText');
  const dotRun = document.getElementById('dotRun');
  const runText = document.getElementById('runText');

  const stRen = document.getElementById('stRen');
  const stMov = document.getElementById('stMov');
  const stSkip = document.getElementById('stSkip');
  const stErr = document.getElementById('stErr');

  const groupsEl = document.getElementById('logGroups');
  const fLevel = document.getElementById('fLevel');
  const fSeason = document.getElementById('fSeason');
  const fShow = document.getElementById('fShow');
  const fKeyword = document.getElementById('fKeyword');

  const btnStop = document.getElementById('btnStop');
  const btnRefresh = document.getElementById('btnRefresh');
  const btnExpandAll = document.getElementById('btnExpandAll');
  const btnCollapseAll = document.getElementById('btnCollapseAll');
  const btnReset = document.getElementById('btnReset');
  const btnClearLocal = document.getElementById('btnClearLocal');

  function escapeHtml(s){
    return String(s||'')
      .replaceAll('&','&amp;')
      .replaceAll('<','&lt;')
      .replaceAll('>','&gt;')
      .replaceAll('"','&quot;')
      .replaceAll("'",'&#039;');
  }

  function getShow(ev){
    return ev.show || '(未命名)';
  }

  function levelKey(lv){
    const u = String(lv||'INFO').toUpperCase();
    if(u === 'WARNING') return 'WARN';
    return u;
  }

  function levelClass(lv){
    const u = levelKey(lv);
    if(u === 'ERROR') return 'lv-error';
    if(u === 'WARN') return 'lv-warn';
    if(u === 'SKIP') return 'lv-skip';
    if(u === 'DRY') return 'lv-dry';
    return 'lv-info';
  }

  // Event store (client-side)
  let events = [];
  let lastId = 0;

  // show -> group state
  const groups = new Map();

  function mkGroup(show){
    const details = document.createElement('details');
    details.className = 'group';
    details.open = false; // default collapsed
    details.dataset.show = show;

    const summary = document.createElement('summary');
    summary.innerHTML = `
      <div class="gtitle">
        <span class="name">${escapeHtml(show)}</span>
      </div>
      <div class="badges">
        <span class="badge err">ERR <span data-k="ERROR">0</span></span>
        <span class="badge warn">WARN <span data-k="WARN">0</span></span>
        <span class="badge ok">OK <span data-k="INFO">0</span></span>
        <span class="badge skip">SKIP <span data-k="SKIP">0</span></span>
        <span class="badge dry">DRY <span data-k="DRY">0</span></span>
        <span class="badge">ALL <span data-k="ALL">0</span></span>
      </div>
    `;
    details.appendChild(summary);

    const ul = document.createElement('ul');
    ul.className = 'gitems';
    details.appendChild(ul);

    groupsEl.prepend(details);

    const st = {
      show,
      details,
      summary,
      ul,
      counts: {ALL:0, INFO:0, WARN:0, ERROR:0, SKIP:0, DRY:0, OTHER:0},
    };
    groups.set(show, st);
    return st;
  }

  function updateBadges(st){
    const spans = st.summary.querySelectorAll('span[data-k]');
    for(const sp of spans){
      const k = sp.getAttribute('data-k');
      sp.textContent = String(st.counts[k] || 0);
    }
  }

  function passes(ev){
    const lv = levelKey(ev.level);
    if(fLevel.value !== 'ALL' && lv !== fLevel.value) return false;
    if(fSeason.value && !String(ev.season||'').includes(fSeason.value)) return false;
    const show = getShow(ev);
    if(fShow.value && !show.includes(fShow.value)) return false;
    if(fKeyword.value){
      const kw = fKeyword.value;
      const hay = show + ' ' + (ev.message||ev.msg||'') + ' ' + (ev.path||'') + ' ' + (ev.src||'') + ' ' + (ev.dst||'');
      if(!hay.includes(kw)) return false;
    }
    return true;
  }

  function addEventToDom(ev){
    const show = getShow(ev);
    const st = groups.get(show) || mkGroup(show);

    const lv = levelKey(ev.level);
    st.counts.ALL += 1;
    if(st.counts[lv] !== undefined) st.counts[lv] += 1;
    else st.counts.OTHER += 1;
    updateBadges(st);

    const ts = ev.ts ? new Date(ev.ts*1000) : null;
    const tsText = ts ? ts.toLocaleString() : '';

    const li = document.createElement('li');
    li.className = 'line ' + levelClass(lv);
    li.innerHTML = `
      <span class="ts">${escapeHtml(tsText)}</span>
      <span class="lvl">${escapeHtml(lv)}</span>
      <span class="msg">${escapeHtml(ev.message||ev.msg||'')}</span>
    `;
    st.ul.appendChild(li);

    // per-group cap to avoid DOM explosion
    const MAX_PER_GROUP = 1600;
    while(st.ul.children.length > MAX_PER_GROUP){
      st.ul.removeChild(st.ul.firstChild);
    }
  }

  function rebuild(){
    const openState = new Map();
    for(const [show, st] of groups.entries()){
      openState.set(show, st.details.open);
    }
    groups.clear();
    groupsEl.innerHTML = '';

    for(const ev of events){
      if(passes(ev)) addEventToDom(ev);
    }

    for(const [show, st] of groups.entries()){
      if(openState.has(show)) st.details.open = openState.get(show);
    }
  }

  async function refreshStats(){
    try{
      const res = await fetch('/api/stats', {headers});
      if(!res.ok) throw new Error('stats http '+res.status);
      const js = await res.json();
      stRen.textContent = js.rename || 0;
      stMov.textContent = js.move || 0;
      stSkip.textContent = js.skip || 0;
      stErr.textContent = js.error || 0;

      dotConn.className = 'dot ok';
      connText.textContent = '已连接';
      dotRun.className = js.running ? 'dot ok' : 'dot';
      runText.textContent = js.running ? '状态：运行中' : '状态：未运行/已结束';
    }catch(e){
      dotConn.className = 'dot err';
      connText.textContent = '连接失败';
      dotRun.className = 'dot';
      runText.textContent = '状态：未知';
    }
  }

  async function poll(){
    try{
      const res = await fetch('/api/events?since=' + encodeURIComponent(String(lastId||0)), {headers});
      if(!res.ok) throw new Error('events http '+res.status);
      const js = await res.json();

      if(Array.isArray(js.events)){
        for(const ev of js.events){
          events.push(ev);
          lastId = Math.max(lastId, ev.id || 0);
          if(passes(ev)) addEventToDom(ev);
        }
      }

      // keep client-side buffer bounded too
      const MAX_LOCAL = 6000;
      if(events.length > MAX_LOCAL){
        events = events.slice(events.length - MAX_LOCAL);
      }

      dotConn.className = 'dot ok';
      connText.textContent = '已连接';
    }catch(e){
      dotConn.className = 'dot err';
      connText.textContent = '连接失败';
    }
  }

  // Controls
  btnStop.addEventListener('click', async ()=>{
    btnStop.disabled = true;
    try{
      await fetch('/api/stop', {method:'POST', headers});
    }catch(e){}
    setTimeout(()=>{btnStop.disabled = false;}, 1200);
  });
  btnRefresh.addEventListener('click', ()=> location.reload());
  btnExpandAll.addEventListener('click', ()=>{
    for(const st of groups.values()) st.details.open = true;
  });
  btnCollapseAll.addEventListener('click', ()=>{
    for(const st of groups.values()) st.details.open = false;
  });
  btnReset.addEventListener('click', ()=>{
    fLevel.value = 'ALL';
    fSeason.value = '';
    fShow.value = '';
    fKeyword.value = '';
    rebuild();
  });
  btnClearLocal.addEventListener('click', ()=>{
    events = [];
    lastId = 0;
    groups.clear();
    groupsEl.innerHTML = '';
  });

  fLevel.addEventListener('change', rebuild);
  fSeason.addEventListener('input', ()=>{
    // debounce-ish
    window.clearTimeout(window.__tSeason);
    window.__tSeason = window.setTimeout(rebuild, 180);
  });
  fShow.addEventListener('input', ()=>{
    window.clearTimeout(window.__tShow);
    window.__tShow = window.setTimeout(rebuild, 180);
  });
  fKeyword.addEventListener('input', ()=>{
    window.clearTimeout(window.__tKw);
    window.__tKw = window.setTimeout(rebuild, 180);
  });

  // boot
  refreshStats();
  poll();
  setInterval(refreshStats, 1800);
  setInterval(poll, 1200);
})();
</script>
</body>
</html>

"""



class LiveLog:
    """Run log server in a background thread."""

    def __init__(self, hub: LogHub, host: str = '127.0.0.1', port: int = _DEFAULT_PORT, token: str = ''):
        self.hub = hub
        self.host = host
        self.port = port
        self.token = token or ''
        self._http: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        handler = make_handler(self.hub, self.token)
        self._http = ThreadingHTTPServer((self.host, self.port), handler)
        # If port=0 (ephemeral), update to the actual bound port
        self.port = int(self._http.server_address[1])
        t = threading.Thread(target=self._http.serve_forever, daemon=True)
        t.start()
        self._thread = t

    def stop(self):
        if self._http:
            try:
                self._http.shutdown()
            except Exception:
                pass

    # --- Compatibility helpers ---
    # renamer.py historically treats `log` as a list-like sink and calls
    # `log.append("...")`.  In the web-UI mode `log` is a LiveLog instance,
    # so we provide an `append()` method that forwards to LogHub.emit().
    def append(self, message: str) -> None:
        try:
            msg = "" if message is None else str(message)
        except Exception:
            msg = "<unprintable>"

        # Try to infer level from a leading tag like "[DRY]" / "[SKIP]" / "[ERROR]".
        level = "INFO"
        m = re.match(r"^\[(?P<tag>[A-Za-z]+)\]", msg.strip())
        if m:
            tag = m.group("tag").upper()
            if tag in {"INFO", "DRY", "SKIP", "ERROR", "WARN", "WARNING"}:
                level = "WARN" if tag == "WARNING" else tag
            elif tag == "AI":
                level = "INFO"

        self.hub.emit(level, msg)

    def extend(self, items) -> None:
        for it in items or []:
            self.append(it)


def start_log_server(hub: LogHub, host: str = '127.0.0.1', port: int = _DEFAULT_PORT, token: str = None) -> LiveLog:
    """Start the log web UI (non-blocking)."""
    srv = LiveLog(hub=hub, host=host, port=port, token=token or '')
    srv.start()
    return srv
