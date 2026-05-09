#!/usr/bin/env python3
"""
monitor.py — central live dashboard that polls all per-machine agents.

Run on any machine (coordinator or laptop) while the cluster is running:
    python3 scripts/monitor.py --config cluster.conf --agent-port 9200

Reads cluster.conf for worker IPs, polls each agent's /metrics endpoint
every second, and renders a live terminal table.

Press Ctrl-C to exit.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from configparser import ConfigParser
from pathlib import Path


# ── config parsing ────────────────────────────────────────────────────────────

def load_worker_hosts(config_path: str) -> list[tuple[int, str]]:
    """Return [(worker_id, host), ...] from cluster.conf."""
    raw = Path(config_path).read_text()
    # Prepend a dummy section header so ConfigParser can parse the globals
    cp = ConfigParser()
    cp.read_string("[__global__]\n" + raw)

    workers = []
    for section in cp.sections():
        if section.startswith("worker_"):
            wid = int(section.split("_", 1)[1])
            host = cp.get(section, "host").strip()
            workers.append((wid, host))
    workers.sort()
    return workers


# ── HTTP fetch ────────────────────────────────────────────────────────────────

def fetch_metrics(host: str, port: int, timeout: float = 1.0) -> dict | None:
    url = f"http://{host}:{port}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


# ── terminal rendering ────────────────────────────────────────────────────────

CLEAR  = "\033[2J\033[H"
BOLD   = "\033[1m"
RESET  = "\033[0m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
CYAN   = "\033[36m"


def bar(value: float, max_val: float, width: int = 20) -> str:
    filled = int(round(value / max_val * width)) if max_val > 0 else 0
    filled = min(filled, width)
    color = GREEN if value < 60 else (YELLOW if value < 85 else RED)
    return color + "█" * filled + RESET + "░" * (width - filled)


def render(workers: list[tuple[int, str]], snapshots: dict[int, dict | None],
           agent_base_port: int, iteration_count: int):
    lines = []
    lines.append(f"{BOLD}{'─'*72}{RESET}")
    lines.append(f"{BOLD}  Distributed PageRank — Cluster Monitor{RESET}   "
                 f"(refresh 1s | Ctrl-C to exit)")
    lines.append(f"{'─'*72}")
    lines.append(
        f"  {'Worker':<8} {'CPU%':>6}  {'CPU bar':<22}  "
        f"{'Mem used':>10}  {'Net send':>10}  {'Net recv':>10}"
    )
    lines.append(f"{'─'*72}")

    for wid, host in workers:
        snap = snapshots.get(wid)
        if snap is None:
            lines.append(f"  W{wid:<7} {RED}OFFLINE{RESET}")
            continue

        cpu   = snap.get("cpu_pct", 0.0)
        mu    = snap.get("mem_used_mb", 0)
        mt    = snap.get("mem_total_mb", 1)
        send  = snap.get("net_send_kbps", 0.0)
        recv  = snap.get("net_recv_kbps", 0.0)
        cpu_b = bar(cpu, 100)

        lines.append(
            f"  W{wid:<7} {cpu:>5.1f}%  {cpu_b}  "
            f"{mu:>6}MB/{mt:<6}MB  "
            f"{send:>8.1f}KB/s  {recv:>8.1f}KB/s"
        )

    lines.append(f"{'─'*72}")
    lines.append(f"  {CYAN}Snapshots polled: {iteration_count}{RESET}   "
                 f"Agents: http://<host>:{agent_base_port}+id/metrics")
    return "\n".join(lines)


# ── main loop ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Live cluster monitoring dashboard.")
    parser.add_argument("--config",     default="cluster.local.conf",
                        help="Path to cluster.conf")
    parser.add_argument("--agent-port", default=9200, type=int,
                        help="Base HTTP port of monitoring agents (W0=base, W1=base+1, ...)")
    parser.add_argument("--interval",   default=1.0,  type=float,
                        help="Poll interval in seconds")
    args = parser.parse_args()

    workers = load_worker_hosts(args.config)
    if not workers:
        print("No [worker_N] sections found in config.", file=sys.stderr)
        sys.exit(1)

    print(f"Monitoring {len(workers)} workers. Press Ctrl-C to exit.", flush=True)
    time.sleep(0.5)

    iteration_count = 0
    while True:
        snapshots: dict[int, dict | None] = {}
        for wid, host in workers:
            port = args.agent_port + wid
            snapshots[wid] = fetch_metrics(host, port)

        iteration_count += 1
        output = CLEAR + render(workers, snapshots, args.agent_port, iteration_count)
        print(output, end="", flush=True)
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nMonitor stopped.")
