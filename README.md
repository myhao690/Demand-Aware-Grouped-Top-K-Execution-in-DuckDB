# Demand-Aware Grouped Top-K Execution in DuckDB

This repository contains a research patch for grouped `arg_max` queries with structured payloads. It changes DuckDB's physical plan; it is **not** a loadable extension. The patch is based on upstream DuckDB commit `f2b0bad0eb3e47e2060f1f07af9d2069eaf77ea1`.

## What it does

For a query such as

```sql
SELECT group_id,
       arg_max(struct_pack(id := logical_id, v0 := v0, v1 := v1), score, 10)
FROM payload
GROUP BY group_id;
```

the ordinary aggregate carries payload values while maintaining each group's Top-K state. The patched path delays that work:

```text
scan(group, score, rowid)
    → SCOUT_TOPK: compact (score, rowid) winners per group
    → direct handoff of finalized winner state
    → DEMAND_BURST: bucket winner rowids by storage block, fetch payload
    → LIST<STRUCT> result
```

Scout supports three key-stage topologies:

| Topology | Key reads | Top-K state |
| --- | --- | --- |
| LOCAL | Once, partitioned across threads | Per-thread state, then merge |
| OWNED | Once per owner | One owner per group, no merge |
| EXCHANGE | Once, then bounded routing | One owner per group, no merge |

OWNED is force-only. Burst fetches storage-block tasks in parallel and assembles the result in typed vectors. The direct handoff avoids building an intermediate `LIST<BIGINT>` of winner IDs. That LIST path remains available as an ablation.

This is an opt-in specialization, not a replacement for DuckDB's general `arg_max`. The earlier field-pruning and candidate-encoding experiments motivated it but are not part of this patch.

## Results

The table reports the geometric mean of `OFF / DIRECT` over payload widths 1 and 6 and thread counts 4 and 8. Values below 1 mean the patched path is slower. All three modes (OFF, LIST, DIRECT) used identical SQL and gave matching result summaries in the 48 tested configurations.

| Numeric projection | K=3 | K=5 | K=10 |
| --- | ---: | ---: | ---: |
| TPC-H SF1 lineitem | 2.40× | 2.01× | 1.63× |
| TPC-H SF3 lineitem | 2.29× | 2.20× | 1.68× |
| Dutch railway snapshot | 0.81× | 0.79× | 0.88× |
| Bus realtime snapshot | 1.08× | 1.00× | 0.83× |

For SF1 at K=3, six DOUBLE payload fields, and four threads, OFF took 288.80 ms, LIST 133.74 ms, and DIRECT 92.87 ms. At K=10 the corresponding times were 456.86, 330.87, and 225.69 ms. These are end-to-end query times, not operator-only measurements. The railway and bus cases show why the path should not be enabled unconditionally.

The three topologies were also forced in a separate DIRECT-handoff matrix. At K=10, six payload fields, and four threads, SF1 took 233.05 ms with LOCAL, 342.30 ms with OWNED, and 216.54 ms with EXCHANGE; standard DuckDB took 450.65 ms. OWNED was never the fastest specialized topology in the 48 measured configurations. The likely cause is that repeated full MVCC-visible key scans outweigh the merge work they remove; the measurements do not isolate every component of that cost. These native-SQL results should not be confused with results from an earlier standalone scanner. The [topology summary](results/topology_summary.csv) and [individual timings](results/topology_timings.csv) retain the negative cases.

The numbers above come from the clean patch and build described here, on an Apple M4 Pro with 48 GiB of memory. They are **controlled numeric projections**, not standard TPC-H queries. See [evaluation notes](docs/evaluation.md) and the [per-case summary](results/summary.csv); [individual timings](results/timings.csv) are included. The railway and bus databases are not redistributed.

## Build and check

Requires Git, CMake, Ninja, a C++17 compiler, and Python 3. The build script downloads the pinned DuckDB source into the ignored `.work/` directory, applies the integration patch, copies the four operator files, and builds the CLI and benchmark driver.

```sh
./scripts/build.sh
python3 tests/verify.py
python3 bench/run.py                 # self-contained 1M-row example
python3 bench/run.py --full          # K=3/5/10, width=1/6, threads=4/8
python3 bench/run.py --topology-matrix  # quick forced-topology comparison
python3 bench/run.py --full --topology-matrix --database path/to/payload.duckdb
```

The specialized path is selected only when `DUCKDB_DEMAND_SCOUT_TOPK=1` and the caller sets `DUCKDB_DEMAND_ASSUME_UNIQUE_SCORE=1`. The latter is a **promise, not a runtime uniqueness check**: scores must be unique within each group. `DUCKDB_DEMAND_FUSED_HANDOFF=1` selects DIRECT; `=0` selects the intermediate LIST path. `DUCKDB_DEMAND_SCOUT_FORCE=LOCAL|OWNED|EXCHANGE` forces a topology; without it, the current sampler chooses only LOCAL or EXCHANGE. Without the opt-in, DuckDB uses its normal plan.

## Code and scope

- [`src/physical_demand_scout_topk.cpp`](src/physical_demand_scout_topk.cpp): compact winner state and parallel key processing.
- [`src/physical_demand_burst.cpp`](src/physical_demand_burst.cpp): direct handoff, block-bucketed payload retrieval, and result assembly.
- [`patches/duckdb-integration.patch`](patches/duckdb-integration.patch): planner match, CMake entry, and a storage accessor; no complete DuckDB tree is copied here.
- [`tests/verify.py`](tests/verify.py): exact OFF/LIST/DIRECT comparisons and fallback checks.

The planner currently matches a single INTEGER grouping column, a BIGINT or UBIGINT score, a `STRUCT` containing one BIGINT ID and up to six DOUBLE fields, a constant `1 ≤ K ≤ 10`, and an unfiltered table scan with a group domain of at most one million values. NULLs and transaction-local rows are covered by focused tests; arbitrary SQL shapes, score ties, and automatic path selection are not claimed. The path can still read full compressed storage blocks when winners are dispersed.

The original code is MIT-licensed. DuckDB's license is retained in [`licenses/DuckDB-LICENSE`](licenses/DuckDB-LICENSE).
