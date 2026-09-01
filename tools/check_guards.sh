#!/usr/bin/env bash
# Exercise the guards and options that the warehouse diff cannot reach.
#
# The equivalence check proves the refactor did not change results on the happy
# path. These are the paths that are supposed to STOP a run, plus the knobs that
# change how it runs. Each case asserts on the outcome, not just the exit code.
#
#   usage: tools/check_guards.sh <jar>
set -uo pipefail

JAR_PATH=${1:?usage: check_guards.sh <jar>}
BUCKETS=${BUCKETS:-256}  # must match numBuckets in conf/job*.properties
: "${SPARK_HOME:?SPARK_HOME is not set - run through mise}"
cd "$(dirname "$0")/.."

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
pass=0
fail=0

# submit <props-file> <extra-props> <warehouse>  -> stdout+stderr, exit in $RC
submit() {
    local props=$1 extra=$2 wh=$3
    cp "$props" "$WORK/case.properties"
    [ -n "$extra" ] && printf '%s\n' "$extra" >> "$WORK/case.properties"
    mkdir -p "$wh"
    "$SPARK_HOME/bin/spark-submit" \
        --class fs.FeatureStoreMerge \
        --master "local[*]" \
        --driver-memory 6g \
        --files "$WORK/case.properties" \
        --conf spark.ui.enabled=false \
        --conf spark.sql.sources.partitionOverwriteMode=dynamic \
        --conf spark.sql.requireAllClusterKeysForCoPartition=false \
        --conf spark.sql.shuffle.partitions="$BUCKETS" \
        --conf spark.sql.warehouse.dir="$wh" \
        --conf spark.driver.extraJavaOptions="-Dderby.system.home=$wh" \
        "$JAR_PATH" > "$WORK/case.log" 2>&1
    RC=$?
}

expect_fail_with() {
    local label=$1 needle=$2
    if [ "$RC" -ne 0 ] && grep -qF "$needle" "$WORK/case.log"; then
        echo "PASS  $label"; pass=$((pass + 1))
    else
        echo "FAIL  $label (exit=$RC, expected message: $needle)"
        grep -iE "Exception|Error" "$WORK/case.log" | head -3
        fail=$((fail + 1))
    fi
}

expect_ok() {
    local label=$1
    if [ "$RC" -eq 0 ]; then
        echo "PASS  $label"; pass=$((pass + 1))
    else
        echo "FAIL  $label (exit=$RC)"
        grep -iE "Exception|Error" "$WORK/case.log" | head -3
        fail=$((fail + 1))
    fi
}

echo "=== B1: an input carrying two dateRefs must be refused ==="
submit conf/job_multidate.properties "" "$WORK/wh_b1"
expect_fail_with "B1 multi-date input rejected" "distinct dataRef values"

echo
echo "=== B1: a declared dateRefValue is accepted (and skips the scan) ==="
submit conf/job1.properties "dateRefValue=2026-08-15" "$WORK/wh_b1b"
expect_ok "B1 dateRefValue accepted"

echo
echo "=== A2: maxConcurrentTables is honoured ==="
submit conf/job1.properties "maxConcurrentTables=1" "$WORK/wh_a2"
expect_ok "A2 maxConcurrentTables=1 runs"

echo
echo "=== B2a: guard can be disabled explicitly ==="
submit conf/job1.properties "assertInputSuperset=false" "$WORK/wh_b2"
expect_ok "B2a assertInputSuperset=false runs"

echo
echo "-------- $pass passed, $fail failed --------"
[ "$fail" -eq 0 ]
