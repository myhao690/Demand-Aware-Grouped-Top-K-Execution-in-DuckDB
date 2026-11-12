# Evaluation notes

All times in `results/summary.csv` and `results/topology_summary.csv` are from the code in this repository applied to
DuckDB commit `f2b0bad0eb3e47e2060f1f07af9d2069eaf77ea1`. The machine was
an Apple M4 Pro with 48 GiB of memory (macOS, arm64). These are controlled
`arg_max` queries, not TPC-H benchmark queries.

## Data

Each input has a table named `payload` with an INTEGER `group_id`, BIGINT
`logical_id` and `score`, and six DOUBLE columns `v0`–`v5`. The score is unique
within each group; the experimental path relies on that condition.

| Source | Rows | Groups | Projection |
| --- | ---: | ---: | --- |
| TPC-H SF1 lineitem | 6,001,215 | 200,000 | group by part; extended price plus order/line tie-break as score |
| TPC-H SF3 lineitem | 17,996,609 | 600,000 | same projection at larger scale |
| Dutch railway snapshot | 1,902,543 | 223,931 | group by service; arrival delay plus stop ID as score |
| Bus realtime snapshot | 630,103 | 27,971 | group by journey; observed arrival time plus row ID as score |

TPC-H data can be generated with DuckDB's `tpch` extension (`CALL dbgen(sf=1)`
or `CALL dbgen(sf=3)`). For SF1 the projection uses
`CAST(l_partkey AS INTEGER)` for `group_id`, `l_orderkey*10+l_linenumber` for
`logical_id`, and
`CAST(l_extendedprice*100 AS BIGINT)*100000000 + logical_id` for `score`.
SF3 uses a multiplier of `1000000000`. The six payload columns are quantity,
extended price, discount, tax, ship date, and receipt date, cast to DOUBLE;
dates are days since 1970-01-01. These numeric projections match the current
operator's supported types.

The railway and bus databases are not included. In particular, the local bus
snapshot had no explicit redistribution license. Their rows in the result CSV
document the tested boundary but are not independently reproducible from this
repository alone. `bench/run.py` creates a small synthetic database without
external data for checking the build and experiment path.

## Procedure

For each source, K ∈ {3, 5, 10}, payload width ∈ {1, 6} DOUBLE fields, and
threads ∈ {4, 8}, the same SQL is run with three modes:

- **OFF:** standard DuckDB aggregation;
- **LIST:** Scout–Burst with an intermediate `LIST<BIGINT>` of winner row IDs;
- **DIRECT:** Scout's finalized winner state is read directly by Burst.

Each case runs OFF, LIST, DIRECT, DIRECT, LIST, OFF. Every session has one
warm-up and two timed executions. The published time is the median of four
timed executions per mode. The C++ driver measures `Connection::Query` through
the first result `Fetch`, not process startup or database opening. `EXPLAIN`
must show the specialized plan before timing. Results are compared using group
count and `SUM(hash(top))`; the focused test in `tests/verify.py` additionally
compares exact output on NULLs, negative group IDs, transaction-local rows,
and K = 1, 3, 5, 10.

`results/timings.csv` contains the individual observations. The summary is
`results/summary.csv`. Differences of a few milliseconds on the smaller bus
queries should not be read as stable improvements. The specialized operator is
opt-in rather than chosen by the optimizer: the negative cases are as important
as the positive ones.

## Parallel topology comparison

`results/topology_summary.csv` compares OFF, forced LOCAL, forced OWNED, and
forced EXCHANGE on the same 48 source/K/width/thread configurations. All three
specialized arms use DIRECT winner handoff. The run order is OFF, LOCAL, OWNED,
EXCHANGE, EXCHANGE, OWNED, LOCAL, OFF; each process runs one warm-up and two
timed queries, giving four timed observations per arm. The published time is
their median. `results/topology_timings.csv` contains all 768 observations.
The runner checks the runtime route trace, not just EXPLAIN, and checks that
group count and `SUM(hash(top))` match across all arms.

OWNED uses one full `DataTable::Scan` of group, physical row ID, and score per
owner on the query's transaction snapshot. Only the matching owner updates a
given group's shared Top-K state, so there is no LOCAL-style state merge. The
ordinary key scan is replaced with a dummy source in this mode. Exact tests
also cover transaction-local INSERT, UPDATE, DELETE, NULLs, and signed and
unsigned scores. The automatic selector remains a two-way LOCAL/EXCHANGE
choice; OWNED is available only by force.

OWNED did not beat LOCAL or EXCHANGE in any of these 48 configurations. This
does not invalidate earlier standalone experiments: that scanner used direct
column loops on a synthetic fixture rather than repeated MVCC-visible SQL
scans. The two implementations have different scan costs, so the earlier
speedups cannot be transferred to the native operator without measurement.
