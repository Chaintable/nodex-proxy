#!/usr/bin/env bash
# Build and benchmark the same source with PGO explicitly off/on. No deployment
# or external upstreams: HTTP benchmarks use their local mock servers.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  printf 'Usage: %s CPU_PROFILE [NEW_RESULTS_DIRECTORY]\n' "$0" >&2
  exit 2
fi
profile=$(realpath "$1")
[[ -r "$profile" ]] || { printf 'Cannot read profile: %s\n' "$profile" >&2; exit 2; }
if [[ $# -eq 2 ]]; then
  mkdir "$2"
  results=$(realpath "$2")
else
  results=$(mktemp -d "${TMPDIR:-/tmp}/nodex-pgo.XXXXXXXX")
fi
cd "$(dirname "$0")/.."

rounds=${PGO_ROUNDS:-5}
benchtime=${PGO_BENCHTIME:-10000x}
procs=${GOMAXPROCS:-2}
runner=(env "GOMAXPROCS=$procs")
if [[ -n ${BENCH_CPUS:-} ]]; then
  runner+=(taskset -c "$BENCH_CPUS")
fi
[[ "$rounds" =~ ^[1-9][0-9]*$ ]] || { printf 'Invalid PGO_ROUNDS\n' >&2; exit 2; }
{
  go version
  go env GOOS GOARCH GOAMD64 GOARM64
  uname -a
  git rev-parse HEAD 2>/dev/null || printf 'source archive (no git metadata)\n'
  sha256sum "$profile"
  printf 'GOMAXPROCS=%s rounds=%s benchtime=%s BENCH_CPUS=%s\n' "$procs" "$rounds" "$benchtime" "${BENCH_CPUS:-unpinned}"
} > "$results/environment.txt"

for variant in off pgo; do
  pgo=off
  [[ "$variant" == off ]] || pgo="$profile"
  GOMAXPROCS="$procs" go build -p 2 -pgo="$pgo" -o "$results/proxy-$variant" ./cmd/proxy
  for package in jsonrpc lb; do
    GOMAXPROCS="$procs" go test -p 2 -pgo="$pgo" -c -o "$results/$package-$variant.test" "./$package"
  done
done

# Alternate AB/BA ordering to reduce temperature and host-load drift. CPU usage
# is reported separately from wall time by the Linux end-to-end benchmarks.
for ((round=1; round<=rounds; round++)); do
  variants=(off pgo)
  ((round % 2 != 0)) || variants=(pgo off)
  for variant in "${variants[@]}"; do
    "${runner[@]}" "$results/lb-$variant.test" -test.run '^$' \
      -test.bench '^BenchmarkServeHTTP(BlockContext)?CPU$' \
      -test.benchmem -test.benchtime="$benchtime" -test.count=1 >> "$results/cpu-$variant.txt"
  done
done
for variant in off pgo; do
  "${runner[@]}" "$results/jsonrpc-$variant.test" -test.run '^$' \
    -test.bench '^Benchmark(ParseRequest|RequestDecodeOwnership)$' \
    -test.benchmem -test.benchtime=500ms -test.count="$rounds" > "$results/parse-$variant.txt"
  "${runner[@]}" "$results/lb-$variant.test" -test.run '^$' \
    -test.bench '^BenchmarkBlockContext(LargeParams|Selection)$' \
    -test.benchmem -test.benchtime=500ms -test.count="$rounds" > "$results/block-context-$variant.txt"
done
printf 'Results: %s\n' "$results"
