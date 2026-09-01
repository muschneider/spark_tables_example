#!/usr/bin/env bash
# Run the five fixture jobs, in order, against one jar and one warehouse.
#
# Order matters: job1 creates the tables, job2 adds a second date partition,
# job3 merges into job1's date, job4 adds a third date, job5 merges a renamed
# column. Running them out of order exercises different branches.
#
#   usage: tools/run_jobs.sh <jar> <warehouse-dir> [extra --conf args...]
#
# EXTRA_PROPS appends lines to each job's .properties. The job reads its config
# from that file, not from --conf, so this is the only way to override it:
#
#   EXTRA_PROPS='assertInputSuperset=false' tools/run_jobs.sh ... 
#   JOBS='5' EXTRA_PROPS='...' tools/run_jobs.sh ...      # a subset of jobs
#
# The three Spark flags the merge depends on (dynamic partition overwrite,
# co-partition relaxation, shuffle width) are NOT set by the job; they are
# passed below. Dropping partitionOverwriteMode=dynamic in particular turns
# job3's merge into a full-table overwrite and destroys job2's partition.
set -euo pipefail

JAR_PATH=${1:?usage: run_jobs.sh <jar> <warehouse> [extra confs...]}
WAREHOUSE_DIR=${2:?usage: run_jobs.sh <jar> <warehouse> [extra confs...]}
shift 2
EXTRA=("$@")
JOBS=${JOBS:-"1 2 3 4 5"}
EXTRA_PROPS=${EXTRA_PROPS:-}
BUCKETS=${BUCKETS:-256}  # must match numBuckets in conf/job*.properties

: "${SPARK_HOME:?SPARK_HOME is not set - run through mise}"
cd "$(dirname "$0")/.."

# Only wipe the warehouse for a full run; a subset is meant to build on it.
if [ "$JOBS" = "1 2 3 4 5" ]; then
    rm -rf "$WAREHOUSE_DIR"
fi
mkdir -p "$WAREHOUSE_DIR"

rc=0

PROPS_DIR=$(mktemp -d)
trap 'rm -rf "$PROPS_DIR"' EXIT

for n in $JOBS; do
    printf '\n=== job%s -> %s ===\n' "$n" "$WAREHOUSE_DIR"
    cp "conf/job${n}.properties" "$PROPS_DIR/job${n}.properties"
    if [ -n "$EXTRA_PROPS" ]; then
        printf '%s\n' "$EXTRA_PROPS" >> "$PROPS_DIR/job${n}.properties"
    fi
    "$SPARK_HOME/bin/spark-submit" \
        --class fs.FeatureStoreMerge \
        --master "local[*]" \
        --driver-memory 6g \
        --files "$PROPS_DIR/job${n}.properties" \
        --conf spark.ui.enabled=false \
        --conf spark.scheduler.mode=FAIR \
        --conf spark.sql.sources.partitionOverwriteMode=dynamic \
        --conf spark.sql.requireAllClusterKeysForCoPartition=false \
        --conf spark.sql.shuffle.partitions="$BUCKETS" \
        --conf spark.sql.warehouse.dir="$WAREHOUSE_DIR" \
        --conf spark.driver.extraJavaOptions="-Dderby.system.home=$WAREHOUSE_DIR" \
        "${EXTRA[@]}" \
        "$JAR_PATH" > "$PROPS_DIR/job${n}.log" 2>&1 || rc=$?

    # surface the interesting lines; keep the full log for a failure
    grep -iE "WARN fs\.|skipping" "$PROPS_DIR/job${n}.log" || true
    if [ "${rc:-0}" -ne 0 ]; then
        printf 'job%s FAILED (exit %s)\n' "$n" "$rc"
        grep -iE "RuntimeException|Caused by|absent from the input|distinct .* values" "$PROPS_DIR/job${n}.log" | head -5 || true
        printf '\nFAILED: %s\n' "$WAREHOUSE_DIR"
        exit "$rc"
    fi
done

printf '\ndone: %s\n' "$WAREHOUSE_DIR"
