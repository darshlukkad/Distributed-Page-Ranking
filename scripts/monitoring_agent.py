#!/usr/bin/env python3
"""
monitoring_agent.py — system metrics agent for each cluster machine.

Run on every machine alongside the worker process:
    python3 scripts/monitoring_agent.py --id 0 --http-port 9200 --log-dir demo_out/

HTTP endpoints:
    GET /metrics  → current snapshot (JSON)
    GET /history  → last 300 samples (JSON array)
    GET /health   → liveness probe

Query from coordinator or any machine:
    curl http://192.168.1.11:9200/metrics | python3 -m json.tool
"""

import argparse
import csv
import http.server
import json
import os
import platform
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path


# ── metric collection ─────────────────────────────────────────────────────────

def _read_cpu_macos() -> float:
    """Return CPU busy % on macOS via top."""
    try:
        out = subprocess.check_output(
            ["top", "-l", "1", "-n", "0", "-stats", "cpu"],
            timeout=3, stderr=subprocess.DEVNULL, text=True,
        )
        for line in out.splitlines():
            if "CPU usage" in line:
                # "CPU usage: 10.0% user, 5.0% sys, 85.0% idle"
                idle_part = [p for p in line.split(",") if "idle" in p]
                if idle_part:
                    idle = float(idle_part[0].strip().split("%")[0])
                    return round(100.0 - idle, 1)
    except Exception:
        pass
    return 0.0


_cpu_prev: dict = {}


def _read_cpu_linux() -> float:
    """Return CPU busy % on Linux via /proc/stat."""
    global _cpu_prev
    try:
        with open("/proc/stat") as f:
            parts = f.readline().split()
        vals = [int(p) for p in parts[1:]]
        idle = vals[3]
        total = sum(vals)
        prev_idle  = _cpu_prev.get("idle",  idle)
        prev_total = _cpu_prev.get("total", total)
        d_idle  = idle  - prev_idle
        d_total = total - prev_total
        _cpu_prev = {"idle": idle, "total": total}
        if d_total == 0:
            return 0.0
        return round(100.0 * (1.0 - d_idle / d_total), 1)
    except Exception:
        return 0.0


def cpu_percent() -> float:
    if platform.system() == "Darwin":
        return _read_cpu_macos()
    return _read_cpu_linux()


def memory_mb() -> dict:
    """Return dict with used_mb and total_mb."""
    if platform.system() == "Darwin":
        try:
            out = subprocess.check_output(["vm_stat"], timeout=3, text=True)
            pages: dict[str, int] = {}
            for line in out.splitlines():
                if ":" in line:
                    k, v = line.split(":", 1)
                    try:
                        pages[k.strip()] = int(v.strip().rstrip("."))
                    except ValueError:
                        pass
            page_size = 4096
            free     = pages.get("Pages free", 0)
            active   = pages.get("Pages active", 0)
            inactive = pages.get("Pages inactive", 0)
            wired    = pages.get("Pages wired down", 0)
            spec     = pages.get("Pages speculative", 0)
            total_pages = free + active + inactive + wired + spec
            used_pages  = active + wired
            return {
                "used_mb":  used_pages  * page_size // (1024 * 1024),
                "total_mb": total_pages * page_size // (1024 * 1024),
            }
        except Exception:
            pass
        return {"used_mb": 0, "total_mb": 0}
    else:
        try:
            info: dict[str, int] = {}
            with open("/proc/meminfo") as f:
                for line in f:
                    k, v = line.split(":", 1)
                    info[k.strip()] = int(v.strip().split()[0])
            total = info.get("MemTotal", 0)
            avail = info.get("MemAvailable", 0)
            return {"used_mb": (total - avail) // 1024, "total_mb": total // 1024}
        except Exception:
            return {"used_mb": 0, "total_mb": 0}


def net_bytes() -> dict:
    """Return cumulative bytes sent/received across all interfaces."""
    if platform.system() == "Darwin":
        try:
            out = subprocess.check_output(
                ["netstat", "-ib"], timeout=3, text=True, stderr=subprocess.DEVNULL
            )
            sent = recv = 0
            for line in out.splitlines()[1:]:
                parts = line.split()
                if len(parts) >= 10:
                    try:
                        recv += int(parts[6])
                        sent += int(parts[9])
                    except (ValueError, IndexError):
                        pass
            return {"sent_bytes": sent, "recv_bytes": recv}
        except Exception:
            pass
        return {"sent_bytes": 0, "recv_bytes": 0}
    else:
        try:
            sent = recv = 0
            with open("/proc/net/dev") as f:
                for line in f.readlines()[2:]:
                    parts = line.split()
                    if len(parts) >= 10:
                        recv += int(parts[1])
                        sent += int(parts[9])
            return {"sent_bytes": sent, "recv_bytes": recv}
        except Exception:
            return {"sent_bytes": 0, "recv_bytes": 0}


# ── agent state ───────────────────────────────────────────────────────────────

class Agent:
    HISTORY_LIMIT = 300

    def __init__(self, worker_id: int, log_path: Path):
        self.worker_id = worker_id
        self.log_path  = log_path
        self.lock      = threading.Lock()
        self.history: list[dict] = []
        self._prev_net = net_bytes()
        self._prev_ts  = time.monotonic()

    def collect(self) -> dict:
        now_ts  = time.monotonic()
        now_net = net_bytes()
        elapsed = max(now_ts - self._prev_ts, 0.001)

        sent_rate = (now_net["sent_bytes"] - self._prev_net["sent_bytes"]) / elapsed
        recv_rate = (now_net["recv_bytes"] - self._prev_net["recv_bytes"]) / elapsed
        self._prev_net = now_net
        self._prev_ts  = now_ts

        mem = memory_mb()
        snapshot = {
            "ts":             datetime.utcnow().isoformat() + "Z",
            "worker_id":      self.worker_id,
            "cpu_pct":        cpu_percent(),
            "mem_used_mb":    mem["used_mb"],
            "mem_total_mb":   mem["total_mb"],
            "net_send_kbps":  round(sent_rate / 1024, 1),
            "net_recv_kbps":  round(recv_rate / 1024, 1),
        }
        with self.lock:
            self.history.append(snapshot)
            if len(self.history) > self.HISTORY_LIMIT:
                self.history.pop(0)
        return snapshot

    def snapshot(self) -> dict:
        with self.lock:
            return self.history[-1] if self.history else {}

    def all_history(self) -> list:
        with self.lock:
            return list(self.history)


# ── HTTP server ───────────────────────────────────────────────────────────────

def make_handler(agent: Agent):
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            pass  # suppress per-request access log

        def _send_json(self, data):
            body = json.dumps(data, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/metrics":
                self._send_json(agent.snapshot())
            elif self.path == "/history":
                self._send_json(agent.all_history())
            elif self.path == "/health":
                self._send_json({"status": "ok", "worker_id": agent.worker_id})
            else:
                self.send_response(404)
                self.end_headers()

    return Handler


# ── collection loop ───────────────────────────────────────────────────────────

def collection_loop(agent: Agent, interval: float, csv_writer):
    while True:
        snap = agent.collect()
        csv_writer.writerow([
            snap["ts"], snap["worker_id"],
            snap["cpu_pct"], snap["mem_used_mb"], snap["mem_total_mb"],
            snap["net_send_kbps"], snap["net_recv_kbps"],
        ])
        time.sleep(interval)


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Per-machine monitoring agent.")
    parser.add_argument("--id",        required=True, type=int, help="Worker ID (0-based)")
    parser.add_argument("--http-port", default=9200,  type=int, help="HTTP port for metrics queries")
    parser.add_argument("--interval",  default=1.0,   type=float, help="Collection interval in seconds")
    parser.add_argument("--log-dir",   default="demo_out", help="Directory for agent CSV log")
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"agent_{args.id}.csv"

    agent = Agent(worker_id=args.id, log_path=log_path)

    with open(log_path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["ts", "worker_id", "cpu_pct", "mem_used_mb",
                         "mem_total_mb", "net_send_kbps", "net_recv_kbps"])

        # Start collection in background thread
        t = threading.Thread(
            target=collection_loop,
            args=(agent, args.interval, writer),
            daemon=True,
        )
        t.start()

        # Serve HTTP on main thread
        handler = make_handler(agent)
        port = args.http_port + args.id  # W0→9200, W1→9201, W2→9202, W3→9203
        server = http.server.HTTPServer(("0.0.0.0", port), handler)
        print(f"[agent W{args.id}] listening on :{port}  log={log_path}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
