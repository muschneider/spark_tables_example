"""Feature Store merge on Dataproc (Managed Service for Apache Spark).

Runs ``fs.FeatureStoreMerge`` against one input parquet dataset and splits it
into the bucketed/partitioned Hive tables declared in ``tables_conf.json``.

--------------------------------------------------------------------------
METASTORE  --  "in production I won't have Postgres or MySQL"
--------------------------------------------------------------------------
The job is metastore-bound and cannot be made metastore-free:

  * ``bucketBy(...).saveAsTable(...)`` only works against a *persistent*
    catalog -- Spark refuses to bucket a plain path.
  * ``spark.catalog.tableExists`` and ``SHOW PARTITIONS`` are catalog reads.
  * ``enableHiveSupport()`` is required for both of the above.

So a Hive Metastore (HMS) is mandatory. What is *not* mandatory is you
running the relational database behind it. Use **Dataproc Metastore (DPMS)**:
a fully managed, serverless, auto-healing, zonally-HA Hive Metastore. Google
owns and operates the backing database; there is nothing to provision, patch,
back up or fail over. You attach it with one field (``metastore_config``), and
every ephemeral cluster then sees the same catalog.

Prefer **DPMS 2** (horizontal scaling): 300 tables x 1 partition/day is
~110k HMS partitions per year, and that number only grows.

Rejected alternatives, and why:

  * Embedded Derby (what ``mise.toml`` uses locally) -- file-locked, single
    JVM, cannot live on GCS, dies with the cluster. Ephemeral clusters would
    lose every table between runs, so the merge branch would never fire and
    each run would silently recreate the tables from scratch.
  * Self-managed HMS on Cloud SQL -- that is exactly the database you said
    you will not have.
  * BigLake / BigQuery metastore -- serverless, but Iceberg-first. It does not
    implement Hive bucketing, so ``bucketBy`` and the whole "shuffle only the
    incoming side" design would have to be rewritten.

--------------------------------------------------------------------------
IMAGE / RUNTIME  (verified against the Dataproc component version list)
--------------------------------------------------------------------------
Only the 3.0 image family can run this jar:

    3.0.1-debian13   Spark 4.1.2   Scala 2.13.17   Java 21   Hive 4.2.0
    2.2 / 2.3        Spark 3.5.x   Scala 2.12      -> BINARY INCOMPATIBLE

``pom.xml`` and ``mise.toml`` are pinned to exactly those Spark/Scala/Java
versions. All Spark deps are ``provided``, so the cluster's own jars win at
runtime; compiling against a version the image does not ship fails in
production rather than at build time. Treat the three files as one unit:

    pom.xml  <->  mise.toml  <->  IMAGE_VERSION below

The image tag is pinned to the *patch* (``3.0.1-debian13``), not the floating
``3.0-debian13`` alias. Google moves the alias forward, and a job with a
10-minute SLA should not have its Spark version changed underneath it.
Upgrading is then a deliberate, testable edit to all three files at once.

>>> GATE BEFORE THE FIRST PRODUCTION RUN <<<
The 3.0 image ships Hive 4.2.0 while DPMS publishes Hive 3.1.2 as its top
metastore version. HMS Thrift clients are normally backward compatible, but
Google explicitly tells you to confirm the image/metastore pairing. Prove it
on a throwaway cluster (CREATE DATABASE -> bucketed saveAsTable -> SHOW
PARTITIONS -> merge insertInto) before committing. If it does not hold, the
fallback is to cross-compile the job to Spark 3.5 / Scala 2.12 and run image
``2.2-debian12`` against DPMS Hive 3.1.2, which is a proven pairing.

--------------------------------------------------------------------------
HARDWARE  --  n4d-highmem-32 workers / n4d-standard-16 master (as requested)
--------------------------------------------------------------------------
    n4d-highmem-32    32 vCPU   256 GB   up to 32 Gbps   AMD EPYC Turin
    n4d-standard-16   16 vCPU    64 GB   up to 32 Gbps   AMD EPYC Turin

The consequential detail:

    *** N4D SUPPORTS NEITHER LOCAL SSD NOR PERSISTENT DISK -- HYPERDISK ONLY ***

Dataproc puts shuffle on the boot disk when a machine type has no local SSD,
so on N4D every shuffle byte crosses the network to Hyperdisk Balanced. That
is the single biggest latency risk against a 10-minute budget, and it is why
the worker boot disk below is explicitly over-provisioned via
``boot_disk_provisioned_throughput`` / ``boot_disk_provisioned_iops`` (legal
only when ``boot_disk_type == "hyperdisk-balanced"``).

If shuffle turns out to be the bottleneck, the two levers are: raise the
provisioned throughput toward the VM cap, or switch the workers to a series
with Local SSD (C4D/N2D). The job code fix in SIZING below is cheaper than
both.

--------------------------------------------------------------------------
THE 10-MINUTE TARGET  --  read this before trusting the sizing
--------------------------------------------------------------------------
Runtime is NOT driven by "250M rows x 200 columns x 300 tables". It is driven
by FAN-OUT: how many of the 300 configured tables this input actually feeds.
``FeatureStoreMerge`` skips any table the input has no column for, but for
every table it *does* touch it rewrites ALL ~200 columns x 250M rows in order
to update however few columns arrived.

    cost ~= FAN_OUT x (read 200 cols + shuffle + write 200 cols)

With 200 input columns against 300 tables of 200 columns each, fan-out is
normally 1-10 (one producer feeds one feature family). Sized for that, the
SLA holds. If one input dribbles a single column into 200 different tables,
the job rewrites tens of TB and no cluster in this shape finishes in 10
minutes. ``fan_out`` is a DAG param; ``plan()`` recomputes the true value
from the table config and fails loudly past ``MAX_SUPPORTED_FAN_OUT``.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import task
from airflow.models import DAG
from airflow.models.param import Param
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# =============================================================================
# Deployment constants  (promote to Airflow Variables for multi-env setups)
# =============================================================================

PROJECT_ID = "my-project"
REGION = "us-central1"

DPMS_SERVICE = f"projects/{PROJECT_ID}/locations/{REGION}/services/feature-store-hms"

NETWORK_SUBNET = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/dataproc"
SERVICE_ACCOUNT = f"dataproc-feature-store@{PROJECT_ID}.iam.gserviceaccount.com"

ARTIFACTS_BUCKET = "my-feature-store-artifacts"  # jar, table config, warehouse
STAGING_BUCKET = "my-feature-store-staging"      # Dataproc config bucket
TEMP_BUCKET = "my-feature-store-temp"            # Dataproc temp bucket

JAR_URI = f"gs://{ARTIFACTS_BUCKET}/jars/feature-store-ds-merge-1.0.0.jar"
MAIN_CLASS = "fs.FeatureStoreMerge"
WAREHOUSE_URI = f"gs://{ARTIFACTS_BUCKET}/warehouse"

# Pinned to the patch, not the floating "3.0-debian13" alias: Google moves the
# alias, and this job's build (pom.xml, mise.toml) is compiled against exactly
# Spark 4.1.2 / Scala 2.13.17 / Java 21.
IMAGE_VERSION = "3.0.1-debian13"

# Create a cluster per run (True) or submit to a pinned warm cluster (False).
# Creating ~6 x n4d-highmem-32 costs 2-4 minutes. That is 20-40% of a
# 10-minute wall clock, so it only fits if the SLA means *job* time. If the
# SLA is end-to-end, set this False and keep WARM_CLUSTER_NAME running.
EPHEMERAL_CLUSTER = True
WARM_CLUSTER_NAME = "feature-store-merge-warm"

DEFERRABLE = True  # poll on the triggerer, not on a worker slot

# =============================================================================
# Job configuration  (mirrors conf/job.properties)
# =============================================================================

KEY_COLUMN = "documento"
DATE_REF_COLUMN = "dataRef"
DATABASE = "feature_store"
DEFAULT_COLUMN_TYPE = "string"
TABLE_CONFIG_URI = f"gs://{ARTIFACTS_BUCKET}/conf/tables_conf.json"

# ---------------------------------------------------------------------------
# NUM_BUCKETS IS A ONE-WAY DOOR.
# The bucket count is baked into each table's DDL at CREATE time. Changing it
# later means rewriting all 300 tables x every partition. Decide once, here.
#
#   250M rows x ~200 cols  ~=  100 GB parquet per table per date partition
#      256 buckets -> ~400 MB/file  (coarse; underuses a wide cluster)
#      512 buckets -> ~200 MB/file  <-- chosen
#     1024 buckets -> ~100 MB/file  (more parallelism, 2x the object count)
#
# At 512: 300 tables x 512 x 365 days ~= 56M objects/year. Hive partitioning
# keeps any single listing at 512 objects, so this is workable -- but a GCS
# lifecycle/retention policy is not optional at this scale.
# ---------------------------------------------------------------------------
NUM_BUCKETS = 512

# =============================================================================
# SIZING
# =============================================================================
# Per merged table, per date partition (250M rows x ~200 columns):
#
#   read target partition   ~100 GB from GCS  (no column pruning -- the
#                                              coalesce() touches all 200)
#   read input (k columns)  250M x k x ~2 B   (pruned)
#   write output            ~100 GB to GCS
#   CPU: ~50e9 parquet values decoded + ~50e9 encoded + sort-merge over 200
#        columns  ~=  4,500 core-seconds (encode dominates; 2x safety applied)
#
# Budget 6 minutes (360 s) of compute inside a 10-minute wall clock:
#
#   cores   = FAN_OUT x 4500 / 360      ~= 12.5 x FAN_OUT
#   workers = cores / ~30 usable cores  ~=  0.42 x FAN_OUT
#
#   FAN_OUT   1 ->  2 workers (floor)    FAN_OUT  50 -> 21 workers
#   FAN_OUT   4 ->  2 workers            FAN_OUT 100 -> 42 workers
#   FAN_OUT  10 ->  5 workers            FAN_OUT 200 -> 84 workers
#
# ---------------------------------------------------------------------------
# WHAT IS *NOT* A COST, despite looking like one.
#
# The job projects every column of a table into `staged`, materialising
# lit(null) for each column the input does not feed, and then joins that. It
# looks like a 200-column row (~1.6 KB, since an UnsafeRow reserves 8 bytes per
# field even when null) crossing the shuffle to update ~20 real columns.
#
# It does not. Measured on a 200-column table fed 20 columns, 500k rows:
# projecting all 200 and projecting only the 20 fed ones compile to the SAME
# physical plan -- input-side exchange 22 attributes, input scan 22 columns --
# and the same 36.52 MB / 73.0 B-per-record shuffle. Spark's NullPropagation
# rewrites coalesce(Literal(null), t.c) to t.c and ColumnPruning then drops the
# null literals below the exchange. The shuffle is already narrow.
#
# So the sizing model above stands on its own: the real costs are reading 200
# columns of the target, the sort-merge join, and writing 200 columns back.
# ---------------------------------------------------------------------------

DEFAULT_FAN_OUT = 6
MAX_SUPPORTED_FAN_OUT = 40  # past this, 10 minutes is not credible in this shape

MIN_WORKERS = 2
MAX_WORKERS = 100

MASTER_MACHINE_TYPE = "n4d-standard-16"   # 16 vCPU / 64 GB -- hosts the driver
WORKER_MACHINE_TYPE = "n4d-highmem-32"    # 32 vCPU / 256 GB

# 4 executors x 7 cores = 28 of 32 vCPU. The remaining 4 cores go to the
# NodeManager, the GCS connector's upload threads and the OS. 7 cores/executor
# keeps HDFS/GCS client throughput in the healthy 5-8 range.
EXECUTORS_PER_WORKER = 4
EXECUTOR_CORES = 7
EXECUTOR_MEMORY = "40g"
EXECUTOR_MEMORY_OVERHEAD = "10g"  # 200-column parquet writers buffer a lot

# No local SSD on N4D: this disk carries every shuffle byte.
WORKER_BOOT_DISK_GB = 1000
WORKER_DISK_PROVISIONED_THROUGHPUT_MB = 1000  # MiB/s; raise toward the VM cap
WORKER_DISK_PROVISIONED_IOPS = 50000

SLA_MINUTES = 10


def worker_count(fan_out: int) -> int:
    """Workers needed to merge ``fan_out`` tables inside the compute budget."""
    return max(MIN_WORKERS, min(MAX_WORKERS, -(-fan_out * 42 // 100)))


def _split_gcs_uri(uri: str) -> tuple[str, str]:
    match = re.fullmatch(r"gs://([^/]+)/(.+)", uri.rstrip("/"))
    if not match:
        raise ValueError(f"not a gs:// object path: {uri!r}")
    return match.group(1), match.group(2)


# =============================================================================
# Spark and cluster configuration
# =============================================================================


def spark_properties(num_workers: int) -> dict[str, str]:
    """Spark conf for one merge run on ``num_workers`` n4d-highmem-32 nodes."""
    return {
        # -- Fixed capacity -------------------------------------------------
        # Dynamic allocation ramps executors over tens of seconds. Against a
        # 10-minute SLA that ramp is pure loss, and the cluster is dedicated.
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.instances": str(EXECUTORS_PER_WORKER * num_workers),
        "spark.executor.cores": str(EXECUTOR_CORES),
        "spark.executor.memory": EXECUTOR_MEMORY,
        "spark.executor.memoryOverhead": EXECUTOR_MEMORY_OVERHEAD,
        # Client mode (Dataproc default) puts the driver on the master, so all
        # worker capacity stays with the executors.
        "spark.driver.memory": "24g",
        "spark.driver.maxResultSize": "4g",
        # The job fans out one Spark job per feature table from a thread pool
        # (FeatureStoreMerge.main, the maxConcurrentTables pool); FAIR stops
        # one long table from starving the short ones.
        "spark.scheduler.mode": "FAIR",

        # -- Bucketing: the entire point of the design ----------------------
        # The stored table already sits on the join distribution, so only the
        # incoming side is shuffled. All four flags are load-bearing.
        #
        # THESE ARE NOT DEFAULTS AND NOTHING ELSE SUPPLIES THEM. The job used
        # to set the last three itself; it no longer does (see the class doc in
        # FeatureStoreMerge.scala). Deleting any of them here changes results,
        # not just speed:
        #   * partitionOverwriteMode != dynamic -> the merging insertInto
        #     overwrites the WHOLE table, wiping every other date partition.
        #   * requireAllClusterKeysForCoPartition != false -> Spark shuffles
        #     the stored side too, which is the cost this design exists to
        #     avoid.
        #   * shuffle.partitions must stay == NUM_BUCKETS (the job passes the
        #     same number to bucketBy via the numBuckets property below).
        "spark.sql.sources.bucketing.enabled": "true",
        "spark.sql.sources.v2.bucketing.enabled": "true",
        "spark.sql.requireAllClusterKeysForCoPartition": "false",
        "spark.sql.shuffle.partitions": str(NUM_BUCKETS),
        # Rewrite only the incoming dates, not the whole table.
        "spark.sql.sources.partitionOverwriteMode": "dynamic",

        # -- AQE, selectively ------------------------------------------------
        # Keep skew handling. Drop partition coalescing: it would collapse the
        # shuffle below NUM_BUCKETS and force an extra exchange before the
        # bucketed write. Block runtime broadcast -- a 200-column build side
        # would OOM the executors, and the job already pins SMJ via
        # .hint("merge").
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
        "spark.sql.autoBroadcastJoinThreshold": "-1",

        # -- Wide-table I/O ---------------------------------------------------
        "spark.sql.files.maxPartitionBytes": str(256 * 1024 * 1024),
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.sql.parquet.filterPushdown": "true",
        # snappy over zstd on purpose: encode speed beats compression ratio
        # when the deadline is 10 minutes. Switch to zstd if storage cost wins.
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.hive.metastorePartitionPruning": "true",

        # -- GCS has no directory rename, so the committer matters ------------
        # v2 skips the final job-level rename pass; the remaining copies into
        # place want wide batch/rename pools.
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.hadoop.fs.gs.status.parallel.enable": "true",
        "spark.hadoop.fs.gs.implicit.dir.repair.enable": "false",
        "spark.hadoop.fs.gs.batch.threads": "48",
        "spark.hadoop.fs.gs.max.requests.per.batch": "48",
        "spark.hadoop.fs.gs.outputstream.upload.chunk.size": str(64 * 1024 * 1024),

        # -- Shuffle lands on network Hyperdisk (no local SSD on N4D) --------
        "spark.shuffle.file.buffer": "1m",
        "spark.shuffle.unsafe.file.output.buffer": "5m",
        "spark.file.transferTo": "false",
        "spark.io.compression.codec": "lz4",

        "spark.sql.warehouse.dir": WAREHOUSE_URI,
    }


def cluster_config(num_workers: int) -> dict[str, Any]:
    """Dataproc cluster config for the requested N4D shape."""
    return {
        "config_bucket": STAGING_BUCKET,
        "temp_bucket": TEMP_BUCKET,
        "gce_cluster_config": {
            # No zone_uri => Auto Zone placement, which materially reduces
            # "this zone has no N4D capacity" creation failures.
            "subnetwork_uri": NETWORK_SUBNET,
            "internal_ip_only": True,
            "service_account": SERVICE_ACCOUNT,
            "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            "shielded_instance_config": {
                "enable_secure_boot": True,
                "enable_vtpm": True,
                "enable_integrity_monitoring": True,
            },
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": MASTER_MACHINE_TYPE,
            "disk_config": {
                # N4D cannot take pd-* or Local SSD.
                "boot_disk_type": "hyperdisk-balanced",
                "boot_disk_size_gb": 500,
            },
        },
        "worker_config": {
            "num_instances": num_workers,
            "machine_type_uri": WORKER_MACHINE_TYPE,
            "disk_config": {
                "boot_disk_type": "hyperdisk-balanced",
                "boot_disk_size_gb": WORKER_BOOT_DISK_GB,
                # Only accepted with hyperdisk-balanced. Without these the
                # disk falls back to baseline IOPS/throughput and shuffle
                # becomes the bottleneck.
                "boot_disk_provisioned_iops": WORKER_DISK_PROVISIONED_IOPS,
                "boot_disk_provisioned_throughput": WORKER_DISK_PROVISIONED_THROUGHPUT_MB,
            },
        },
        # Deliberately no secondary/spot workers: a preemption mid-run would
        # blow the SLA, and dynamic partition overwrite makes a partial rerun
        # more expensive than the savings.
        "software_config": {
            "image_version": IMAGE_VERSION,
            "properties": {
                "dataproc:dataproc.logging.stackdriver.enable": "true",
                "dataproc:dataproc.monitoring.stackdriver.enable": "true",
                # Cluster-level defaults so ad-hoc spark-shell sessions on this
                # cluster read the bucketed tables the same way the job wrote
                # them. Job-level conf still wins.
                "spark:spark.sql.sources.bucketing.enabled": "true",
                "spark:spark.sql.requireAllClusterKeysForCoPartition": "false",
                "hive:hive.metastore.warehouse.dir": WAREHOUSE_URI,
            },
        },
        # THE metastore: managed HMS, no database for you to run.
        "metastore_config": {"dataproc_metastore_service": DPMS_SERVICE},
        "lifecycle_config": {
            # Backstop. If the DAG dies between create and delete, the cluster
            # still goes away instead of billing 32-vCPU nodes indefinitely.
            "idle_delete_ttl": {"seconds": 1800},
            "auto_delete_ttl": {"seconds": 7200},
        },
        "endpoint_config": {"enable_http_port_access": True},
    }


# =============================================================================
# DAG
# =============================================================================

default_args = {
    "owner": "feature-store",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "project_id": PROJECT_ID,
}

with DAG(
    dag_id="feature_store_merge",
    description="Split a parquet dataset into bucketed feature-store tables on Dataproc",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # triggered by the upstream producer; set a cron if scheduled
    catchup=False,
    # Dynamic partition overwrite against shared tables: never run two at once.
    max_active_runs=1,
    doc_md=__doc__,
    tags=["feature-store", "dataproc", "spark"],
    params={
        "input_dataset": Param(
            f"gs://{ARTIFACTS_BUCKET}/datasets/ds1",
            type="string",
            description="gs:// path of the input parquet dataset (single dataRef).",
        ),
        "table_config": Param(TABLE_CONFIG_URI, type="string"),
        "fan_out": Param(
            DEFAULT_FAN_OUT,
            type="integer",
            minimum=1,
            maximum=MAX_SUPPORTED_FAN_OUT,
            description="Tables this input is expected to feed. Drives cluster size.",
        ),
    },
) as dag:

    @task(multiple_outputs=True)
    def plan(params: dict[str, Any] | None = None, run_id: str = "manual") -> dict[str, Any]:
        """Validate, size and stage everything the run needs, before any VM.

        Doing this first turns "failed Spark job, five minutes and one cluster
        later" into "failed Airflow task, two seconds and no cluster later".
        """
        params = params or {}
        input_dataset: str = params["input_dataset"]
        table_config: str = params["table_config"]
        fan_out = int(params["fan_out"])

        gcs = GCSHook()

        # 1. the input must actually contain parquet
        in_bucket, in_prefix = _split_gcs_uri(input_dataset)
        objects = gcs.list(in_bucket, prefix=f"{in_prefix}/", max_results=64)
        if not any(name.endswith(".parquet") for name in objects):
            raise ValueError(f"no parquet files under {input_dataset}")

        # 2. the table config must parse -- the job calls .head() on it blindly
        cfg_bucket, cfg_object = _split_gcs_uri(table_config)
        cfg = json.loads(gcs.download(cfg_bucket, cfg_object).decode("utf-8"))
        if not isinstance(cfg, dict) or not cfg:
            raise ValueError(f"{table_config} is not a non-empty table -> columns object")

        # 3. refuse an impossible fan-out up front rather than discovering it
        #    via a 40-minute run that misses the batch window
        if fan_out > MAX_SUPPORTED_FAN_OUT:
            raise ValueError(
                f"fan_out={fan_out} exceeds MAX_SUPPORTED_FAN_OUT="
                f"{MAX_SUPPORTED_FAN_OUT}: a {SLA_MINUTES}-minute SLA is not "
                "credible without restructuring the table config"
            )

        workers = worker_count(fan_out)
        safe_run_id = re.sub(r"[^A-Za-z0-9-]", "-", run_id).strip("-").lower()[:32]

        # 4. render this run's .properties. FeatureStoreMerge locates its
        #    config by scanning spark.files for a name ending in .properties
        #    (FeatureStoreMerge.properties), so the suffix is mandatory and the
        #    name must be unique per run or two runs would race on it.
        body = (
            "\n".join(
                [
                    f"inputDataSet={input_dataset}",
                    f"keyColumn={KEY_COLUMN}",
                    f"dateRefColumn={DATE_REF_COLUMN}",
                    f"tableConfig={table_config}",
                    f"database={DATABASE}",
                    f"numBuckets={NUM_BUCKETS}",
                    f"defaultColumnType={DEFAULT_COLUMN_TYPE}",
                ]
            )
            + "\n"
        )
        object_name = f"runs/{safe_run_id}/job.properties"
        gcs.upload(
            bucket_name=ARTIFACTS_BUCKET,
            object_name=object_name,
            data=body.encode("utf-8"),
            mime_type="text/plain",
        )

        cluster_name = (
            f"fs-merge-{safe_run_id}"[:51] if EPHEMERAL_CLUSTER else WARM_CLUSTER_NAME
        )

        log.info(
            "plan: %d configured tables (widest=%d cols) | fan_out=%d -> %d x %s "
            "= %d executors / %d cores | cluster=%s",
            len(cfg),
            max(len(v) for v in cfg.values()),
            fan_out,
            workers,
            WORKER_MACHINE_TYPE,
            EXECUTORS_PER_WORKER * workers,
            EXECUTORS_PER_WORKER * workers * EXECUTOR_CORES,
            cluster_name,
        )
        return {
            "workers": workers,
            "cluster_name": cluster_name,
            "properties_uri": f"gs://{ARTIFACTS_BUCKET}/{object_name}",
        }

    @task
    def build_cluster_config(workers: int) -> dict[str, Any]:
        """Cluster config sized from the resolved fan-out.

        A task, not a literal, because worker count is only known at run time;
        ``cluster_config`` is a template field so the operator resolves this
        XCom into the real dict.
        """
        return cluster_config(workers)

    @task
    def build_spark_job(
        workers: int, cluster_name: str, properties_uri: str, run_id: str = "manual"
    ) -> dict[str, Any]:
        """Dataproc job payload, sized from the same plan as the cluster."""
        safe_run_id = re.sub(r"[^A-Za-z0-9-]", "-", run_id).strip("-").lower()[:40]
        return {
            "reference": {"job_id": f"fs-merge-{safe_run_id}"[:100]},
            "placement": {"cluster_name": cluster_name},
            "labels": {"pipeline": "feature-store-merge"},
            "spark_job": {
                "main_class": MAIN_CLASS,
                "jar_file_uris": [JAR_URI],
                # --files: this is how the job receives its .properties
                "file_uris": [properties_uri],
                "properties": spark_properties(workers),
            },
        }

    run_plan = plan()

    submit_merge = DataprocSubmitJobOperator(
        task_id="submit_merge",
        project_id=PROJECT_ID,
        region=REGION,
        deferrable=DEFERRABLE,
        job=build_spark_job(
            workers=run_plan["workers"],
            cluster_name=run_plan["cluster_name"],
            properties_uri=run_plan["properties_uri"],
        ),
        # Hard SLA gate: overrunning fails the task instead of quietly eating
        # the downstream batch window.
        execution_timeout=timedelta(minutes=SLA_MINUTES),
    )

    if EPHEMERAL_CLUSTER:
        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=run_plan["cluster_name"],
            cluster_config=build_cluster_config(workers=run_plan["workers"]),
            use_if_exists=True,
            execution_timeout=timedelta(minutes=8),
        )

        delete_cluster = DataprocDeleteClusterOperator(
            task_id="delete_cluster",
            project_id=PROJECT_ID,
            region=REGION,
            cluster_name=run_plan["cluster_name"],
            # ALL_DONE: a failed merge must not leak a 6 x 32-vCPU cluster.
            trigger_rule=TriggerRule.ALL_DONE,
        )

        create_cluster >> submit_merge >> delete_cluster
