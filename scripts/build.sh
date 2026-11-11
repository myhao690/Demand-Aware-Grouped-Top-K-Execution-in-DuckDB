#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
source_dir="$root/.work/duckdb"
build_dir="$source_dir/build/demand-release"
upstream_commit=f2b0bad0eb3e47e2060f1f07af9d2069eaf77ea1

mkdir -p "$root/.work"
if [[ ! -d "$source_dir/.git" ]]; then
  git clone https://github.com/duckdb/duckdb.git "$source_dir"
  git -C "$source_dir" checkout --detach "$upstream_commit"
fi

if [[ $(git -C "$source_dir" rev-parse HEAD) != "$upstream_commit" ]]; then
  echo "Expected DuckDB $upstream_commit in $source_dir" >&2
  exit 1
fi
if [[ -z $(git -C "$source_dir" status --porcelain) ]]; then
  git -C "$source_dir" apply --check "$root/patches/duckdb-integration.patch"
  git -C "$source_dir" apply "$root/patches/duckdb-integration.patch"
  cp "$root/src/physical_demand_scout_topk.cpp" "$root/src/physical_demand_burst.cpp" \
     "$source_dir/src/execution/operator/aggregate/"
  cp "$root/src/physical_demand_scout_topk.hpp" "$root/src/physical_demand_burst.hpp" \
     "$source_dir/src/include/duckdb/execution/operator/aggregate/"
else
  expected=$(printf '%s\n' src/execution/operator/aggregate/CMakeLists.txt \
    src/execution/physical_plan/plan_aggregate.cpp \
    src/include/duckdb/storage/table/row_group.hpp)
  if [[ $(git -C "$source_dir" diff --name-only | sort) != "$expected" ]] || \
     ! git -C "$source_dir" apply --reverse --check "$root/patches/duckdb-integration.patch"; then
    echo "DuckDB checkout has changes other than this patch" >&2
    exit 1
  fi
  for name in physical_demand_scout_topk physical_demand_burst; do
    cmp "$root/src/$name.cpp" "$source_dir/src/execution/operator/aggregate/$name.cpp"
    cmp "$root/src/$name.hpp" "$source_dir/src/include/duckdb/execution/operator/aggregate/$name.hpp"
  done
fi

cmake -S "$source_dir" -B "$build_dir" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_SHELL=ON -DBUILD_UNITTESTS=OFF
cmake --build "$build_dir" --target shell -j "${JOBS:-4}"
if [[ $(uname -s) == Darwin ]]; then
  library=libduckdb.dylib
else
  library=libduckdb.so
fi
cmake --build "$build_dir" --target "src/$library" -j "${JOBS:-4}"

"${CXX:-c++}" -std=c++17 -O2 -I "$source_dir/src/include" \
  "$root/bench/bench.cpp" -L "$build_dir/src" -lduckdb \
  -Wl,-rpath,"$build_dir/src" -o "$root/.work/bench"
echo "Built DuckDB: $build_dir/duckdb"
echo "Built benchmark: $root/.work/bench"
