#!/usr/bin/env python3
"""Run identical grouped Top-K SQL through OFF, LIST, and DIRECT paths."""

import argparse
import csv
import io
import os
import statistics
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / ".work/duckdb/build/demand-release/duckdb"
DRIVER = ROOT / ".work/bench"


def query(k, width):
    payload = "struct_pack(id:=logical_id" + "".join(f",v{i}:=v{i}" for i in range(width)) + ")"
    return ("SELECT COUNT(*) groups,SUM(hash(top)) ordered_payload_hash FROM "
            f"(SELECT group_id,arg_max({payload},score,{k}) top "
            "FROM payload GROUP BY group_id)")


def environment(mode):
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("DUCKDB_DEMAND_"):
            del env[key]
    env.update(DUCKDB_DEMAND_SCOUT_TOPK="0" if mode == "OFF" else "1",
               DUCKDB_DEMAND_FUSED_HANDOFF="1" if mode == "DIRECT" else "0",
               DUCKDB_DEMAND_ASSUME_UNIQUE_SCORE="1")
    return env


def create_demo(db, rows, groups):
    if db.exists():
        return
    db.parent.mkdir(parents=True, exist_ok=True)
    stride = max(1, 65536 // groups + 1)
    if groups * stride > 1_000_000:
        raise ValueError("demo group domain exceeds the operator's 1,000,000-value limit")
    fields = ", ".join(f"CAST((i*{j+3})%1009 AS DOUBLE) v{j}" for j in range(6))
    sql = ("CREATE TABLE payload AS SELECT CAST((i%" + str(groups) + ")*" + str(stride)
           + " AS INTEGER) AS group_id, "
           "CAST(i AS BIGINT) logical_id, CAST(i AS BIGINT) score, " + fields
           + f" FROM range({rows}) t(i)")
    subprocess.run([str(CLI), "-no-agent", str(db), "-c", sql], check=True)


def run_case(db, k, width, threads):
    sql = query(k, width)
    for mode in ("LIST", "DIRECT"):
        plan = subprocess.run([str(CLI), "-no-agent", "-readonly", str(db), "-c",
                               f"SET threads={threads}; EXPLAIN " + sql],
                              env=environment(mode), capture_output=True, text=True, check=True)
        if "Demand Scout Topk" not in plan.stdout or f"Winner Handoff: {mode}" not in plan.stdout:
            raise RuntimeError(f"{mode} path not selected; check schema and supported SQL shape")
    query_file = ROOT / ".work/query.sql"
    query_file.write_text(sql + "\n")
    rows = []
    for mode in ("OFF", "LIST", "DIRECT", "DIRECT", "LIST", "OFF"):
        result = subprocess.run([str(DRIVER), str(db), str(query_file), str(threads), "1", "2"],
                                env=environment(mode), capture_output=True, text=True, check=True)
        for item in csv.DictReader(io.StringIO(result.stdout)):
            rows.append(dict(k=k, width=width, threads=threads, mode=mode, **item))
    if len({(row["groups"], row["ordered_payload_hash"]) for row in rows}) != 1:
        raise RuntimeError(f"result mismatch for K={k}, W={width}, T={threads}")
    timings = {mode: statistics.median(float(row["ms"]) for row in rows if row["mode"] == mode)
               for mode in ("OFF", "LIST", "DIRECT")}
    print(f"K={k} W={width} T={threads}: "
          f"OFF {timings['OFF']:.2f} ms, LIST {timings['LIST']:.2f} ms, "
          f"DIRECT {timings['DIRECT']:.2f} ms, OFF/DIRECT {timings['OFF']/timings['DIRECT']:.2f}x")
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, help="existing payload database; must satisfy the unique-score contract")
    parser.add_argument("--rows", type=int, default=1_000_000, help="demo rows if --database is omitted")
    parser.add_argument("--groups", type=int, default=50_000, help="demo groups if --database is omitted")
    parser.add_argument("--full", action="store_true", help="K=3/5/10, W=1/6, T=4/8; default is one quick case")
    args = parser.parse_args()
    if args.rows <= 0 or args.groups <= 0 or args.groups > args.rows:
        parser.error("require 0 < groups <= rows")
    if not CLI.exists() or not DRIVER.exists():
        parser.error("run scripts/build.sh first")
    db = args.database or ROOT / f".work/demo_n{args.rows}_g{args.groups}.duckdb"
    if args.database is None:
        create_demo(db, args.rows, args.groups)
    cases = [(k, width, threads) for k in (3, 5, 10)
             for width in (1, 6) for threads in (4, 8)] if args.full else [(10, 6, 4)]
    results = []
    for k, width, threads in cases:
        results.extend(run_case(db, k, width, threads))
    output = ROOT / ".work/latest_timings.csv"
    with output.open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    print(f"Raw timings: {output}")


if __name__ == "__main__":
    main()
