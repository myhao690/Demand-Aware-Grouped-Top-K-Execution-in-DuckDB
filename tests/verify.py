#!/usr/bin/env python3
"""Exact output and planner fallback checks for the supported query subset."""

import os
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / ".work/duckdb/build/demand-release/duckdb"


def query(k):
    return ("SELECT group_id,arg_max(struct_pack(id:=logical_id,v0:=v0,v1:=v1),"
            f"score,{k}) top FROM payload GROUP BY group_id ORDER BY group_id")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def run(db, sql, mode="OFF", unique=True):
    env = os.environ.copy()
    for key in list(env):
        if key.startswith("DUCKDB_DEMAND_"):
            del env[key]
    env.update(DUCKDB_DEMAND_SCOUT_TOPK="0" if mode == "OFF" else "1",
               DUCKDB_DEMAND_FUSED_HANDOFF="1" if mode == "DIRECT" else "0",
               DUCKDB_DEMAND_ASSUME_UNIQUE_SCORE="1" if unique else "0")
    result = subprocess.run([str(CLI), "-no-agent", "-csv", str(db), "-c", sql],
                            env=env, capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise RuntimeError(f"{mode}: {result.stderr}")
    return result.stdout


def main():
    with tempfile.TemporaryDirectory(prefix="demand-topk-test-") as directory:
        db = Path(directory) / "test.duckdb"
        run(db, "CREATE TABLE payload AS SELECT "
            "CASE WHEN i=0 THEN -200000 WHEN i=1 THEN 200000 "
            "WHEN i%97=0 THEN NULL ELSE CAST((i%11)-5 AS INTEGER) END group_id, "
            "CASE WHEN i%17=0 THEN NULL ELSE 100000+i END logical_id, "
            "CASE WHEN i%13=0 THEN NULL ELSE CAST(i AS BIGINT) END score, "
            "CASE WHEN i%7=0 THEN NULL ELSE CAST(i%29 AS DOUBLE) END v0, "
            "CAST(i%31 AS DOUBLE) v1 FROM range(5000) t(i)")

        for k in (1, 3, 5, 10):
            outputs = [run(db, query(k), mode) for mode in ("OFF", "LIST", "DIRECT")]
            require(outputs[0] == outputs[1] == outputs[2], f"K={k}: output mismatch")
            for mode in ("LIST", "DIRECT"):
                plan = run(db, "EXPLAIN " + query(k), mode)
                require("Demand Scout Topk" in plan, f"K={k}: native plan not selected")
                require(f"Winner Handoff: {mode}" in plan, f"K={k}: wrong handoff")
            print(f"K={k}: exact OFF/LIST/DIRECT output and plans agree")

        for k in (3, 10):
            cases = (
                "BEGIN; INSERT INTO payload VALUES (300000,999999,999999,1.0,2.0); ",
                "BEGIN; UPDATE payload SET group_id=-250000, logical_id=888888, v0=NULL "
                "WHERE score=4999; DELETE FROM payload WHERE score=4998; ",
            )
            for prefix in cases:
                outputs = [run(db, prefix + query(k), mode) for mode in ("OFF", "DIRECT")]
                require(outputs[0] == outputs[1], f"K={k}: transaction-local mismatch")
            print(f"K={k}: transaction-local insert/update/delete outputs agree")

        for name, sql, unique in (
            ("no unique-score contract", query(3), False),
            ("filtered scan", query(3).replace("FROM payload", "FROM payload WHERE v0>0"), True),
            ("K=11", query(11), True),
        ):
            plan = run(db, "EXPLAIN " + sql, "DIRECT", unique)
            require("Demand Scout Topk" not in plan, f"{name}: expected standard plan")
            print(f"{name}: standard-plan fallback")


if __name__ == "__main__":
    main()
