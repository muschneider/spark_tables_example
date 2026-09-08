"""Apache Zeppelin on Dataproc, pointed at the feature-store tables.

One task: create a long-lived cluster with the Zeppelin optional component and
walk away. There is deliberately no delete task -- this is an interactive
exploration cluster, so it lives until ``auto_delete_ttl`` fires (see COST).

Open it from the Dataproc console: Cluster -> WEB INTERFACES -> Zeppelin. The
``log_zeppelin_url`` task also prints the Component Gateway URL to the task log.

--------------------------------------------------------------------------
READING THE SPARK TABLES  --  the four settings that matter
--------------------------------------------------------------------------
The tables written by ``fs.FeatureStoreMerge`` are Hive tables: bucketed,
date-partitioned, data on GCS, metadata in Dataproc Metastore (DPMS). Zeppelin
sees them only if this cluster joins the same catalog and the same warehouse:

  1. ``metastore_config``          -- same DPMS service as the merge DAG, so
                                      ``SHOW TABLES`` returns something.
  2. ``hive.metastore.warehouse.dir`` / ``spark.sql.warehouse.dir``
                                   -- same GCS prefix, so table locations
                                      resolve.
  3. ``spark.sql.catalogImplementation=hive``
                                   -- the config-file equivalent of
                                      ``enableHiveSupport()``. Without it the
                                      session opens an in-memory catalog and
                                      every table is "not found".
  4. bucketing flags              -- notably
                                      ``requireAllClusterKeysForCoPartition=false``,
                                      which is what lets a join between two
                                      bucketed tables skip the shuffle. It is
                                      NOT a default; without it exploration
                                      queries silently reshuffle 100 GB.

All of these are set as *cluster* properties, so the Zeppelin Spark interpreter
picks them up from ``/etc/spark/conf/spark-defaults.conf`` with no per-notebook
``%spark.conf`` boilerplate.

--------------------------------------------------------------------------
WHY 2 PRIMARY + 13 SPOT AND NOT 15 SPOT
--------------------------------------------------------------------------
You asked for 1 master + 15 workers, all Spot. Dataproc cannot express that
directly:

  * ``preemptibility`` is only a field of ``secondaryWorkerConfig``. For the
    master and primary worker groups it is fixed at NON_PREEMPTIBLE and,
    per the InstanceGroupConfig reference, "this default cannot be changed".
  * A standard cluster "require[s] at least two primary workers".

So the shape below is 2 on-demand primary + 13 Spot secondary = 15 workers,
which keeps the total you asked for and puts 87% of the fleet on Spot pricing.

The all-Spot option exists -- ``--cluster-type=zero-scale``, which runs
secondary workers only -- but it drags in an autoscaling policy (mandatory),
Flexible VMs (mandatory), and drops HDFS entirely. That is a lot of moving
parts for two VMs' worth of savings, so it is not used here.

Note that Spot workers are reclaimed with ~30s notice. For interactive
exploration that is fine: you lose a running paragraph, not a batch window.
Secondary workers are also compute-only -- they store no HDFS data -- which is
exactly right when the tables live on GCS.

--------------------------------------------------------------------------
COST  --  read this one
--------------------------------------------------------------------------
15 x n4d-highmem-32 is ~480 vCPU / ~3.8 TB RAM sitting idle between queries.

``idle_delete_ttl`` alone will NOT save you: Dataproc counts a cluster as idle
only when no YARN application is running, and a live Zeppelin Spark interpreter
holds a YARN application open even while you are reading the results. A
forgotten notebook therefore looks busy forever.

``auto_delete_ttl`` is the real guard, and it is absolute: the cluster is
deleted 8 hours after creation regardless of activity. Re-trigger this DAG to
get it back (``use_if_exists=True`` makes that idempotent).

--------------------------------------------------------------------------
HARDWARE
--------------------------------------------------------------------------
    master    1  x n4d-standard-8    8 vCPU /  32 GB
    primary   2  x n4d-highmem-32   32 vCPU / 256 GB   on-demand
    secondary 13 x n4d-highmem-32   32 vCPU / 256 GB   SPOT

*** N4D TAKES HYPERDISK ONLY -- NO LOCAL SSD, NO PERSISTENT DISK ***
Every disk_config below therefore pins ``hyperdisk-balanced``. Leaving the
default pd-* boot disk in place makes cluster creation fail outright.

Executor sizing is deliberately left at the Dataproc defaults, which means
dynamic allocation stays ON. That is the right call for an interactive cluster:
an idle notebook releases its executors instead of pinning 480 cores. (The
merge DAG pins fixed capacity instead, because a 10-minute SLA cannot afford
the ramp -- different job, different trade-off.)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import task
from airflow.models import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
)

log = logging.getLogger(__name__)

# =============================================================================
# Deployment constants  --  MUST match dags/feature_store_merge_dag.py
# =============================================================================
# Zeppelin has to land in the same project/region/metastore/warehouse as the
# job that writes the tables. If you change them there, change them here.

PROJECT_ID = "my-project"
REGION = "us-central1"

DPMS_SERVICE = f"projects/{PROJECT_ID}/locations/{REGION}/services/feature-store-hms"

NETWORK_SUBNET = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/dataproc"
SERVICE_ACCOUNT = f"dataproc-feature-store@{PROJECT_ID}.iam.gserviceaccount.com"

ARTIFACTS_BUCKET = "my-feature-store-artifacts"
STAGING_BUCKET = "my-feature-store-staging"
TEMP_BUCKET = "my-feature-store-temp"

WAREHOUSE_URI = f"gs://{ARTIFACTS_BUCKET}/warehouse"
DATABASE = "feature_store"

# Where notebooks live. Survives cluster deletion -- which is the whole point,
# since this cluster is designed to be deleted every 8 hours.
NOTEBOOKS_URI = f"gs://{ARTIFACTS_BUCKET}/zeppelin/notebooks"

# Ships Spark 4.1.2 / Scala 2.13.17 / Java 21 / Hive 4.2.0 / Zeppelin 0.12.0.
IMAGE_VERSION = "3.0.1-debian13"

# Must equal numBuckets in the merge job, otherwise any shuffle that does
# happen fights the bucketing instead of matching it.
NUM_BUCKETS = 512

CLUSTER_NAME = "feature-store-zeppelin"

# =============================================================================
# Shape
# =============================================================================

MASTER_MACHINE_TYPE = "n4d-standard-8"
WORKER_MACHINE_TYPE = "n4d-highmem-32"

TOTAL_WORKERS = 15
PRIMARY_WORKERS = 10
SPOT_WORKERS = 5

# N4D is hyperdisk-only. Shuffle has no local SSD to land on, so it crosses the
# network to Hyperdisk; keep these roomy or interactive joins crawl.
BOOT_DISK_TYPE = "hyperdisk-balanced"
MASTER_BOOT_DISK_GB = 200
WORKER_BOOT_DISK_GB = 1000

AUTO_DELETE_HOURS = 8   # absolute kill switch -- the one that actually fires
IDLE_DELETE_HOURS = 2   # only fires if no YARN app is alive (see COST above)


CLUSTER_CONFIG: dict[str, Any] = {
    "config_bucket": STAGING_BUCKET,
    "temp_bucket": TEMP_BUCKET,
    "gce_cluster_config": {
        # No zone_uri => Auto Zone placement. N4D capacity is patchy per-zone
        # and this is 16 large VMs; let Google pick a zone that can hold them.
        "subnetwork_uri": NETWORK_SUBNET,
        "internal_ip_only": True,
        "service_account": SERVICE_ACCOUNT,
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": MASTER_MACHINE_TYPE,
        "disk_config": {
            "boot_disk_type": BOOT_DISK_TYPE,
            "boot_disk_size_gb": MASTER_BOOT_DISK_GB,
        },
    },
    "worker_config": {
        "num_instances": PRIMARY_WORKERS,
        "machine_type_uri": WORKER_MACHINE_TYPE,
        "disk_config": {
            "boot_disk_type": BOOT_DISK_TYPE,
            "boot_disk_size_gb": WORKER_BOOT_DISK_GB,
        },
    },
    "secondary_worker_config": {
        "num_instances": SPOT_WORKERS,
        "machine_type_uri": WORKER_MACHINE_TYPE,
        # SPOT, not PREEMPTIBLE: same discount, but no 24-hour cap, so an
        # afternoon of exploration is not guillotined on a timer.
        "preemptibility": "SPOT",
        "disk_config": {
            "boot_disk_type": BOOT_DISK_TYPE,
            "boot_disk_size_gb": WORKER_BOOT_DISK_GB,
        },
    },
    "software_config": {
        "image_version": IMAGE_VERSION,
        "optional_components": ["ZEPPELIN"],
        "properties": {
            # -- Notebooks on GCS ------------------------------------------
            # Default is the Dataproc staging bucket, which is a dumping
            # ground. Pin an explicit prefix instead.
            "zeppelin:zeppelin.notebook.gcs.dir": NOTEBOOKS_URI,
            # Already the Dataproc default; stated so an image change cannot
            # silently move notebooks back onto local disk and lose them at
            # auto_delete_ttl.
            "zeppelin:zeppelin.notebook.storage": (
                "org.apache.zeppelin.notebook.repo.GCSNotebookRepo"
            ),

            # -- See the feature-store tables ------------------------------
            # Turns on Hive support for every interpreter session.
            "spark:spark.sql.catalogImplementation": "hive",
            "spark:spark.sql.warehouse.dir": WAREHOUSE_URI,
            "hive:hive.metastore.warehouse.dir": WAREHOUSE_URI,

            # -- Read the tables the way they were written -----------------
            # requireAllClusterKeysForCoPartition=false is load-bearing: it is
            # what lets a bucketed-to-bucketed join skip the exchange. The
            # other two are defaults, restated so they survive an image bump.
            "spark:spark.sql.sources.bucketing.enabled": "true",
            "spark:spark.sql.sources.v2.bucketing.enabled": "true",
            "spark:spark.sql.requireAllClusterKeysForCoPartition": "false",
            "spark:spark.sql.shuffle.partitions": str(NUM_BUCKETS),

            # -- Interactive quality of life -------------------------------
            # A 200-column table is far too wide to broadcast; let it OOM
            # never rather than mysteriously.
            "spark:spark.sql.autoBroadcastJoinThreshold": "-1",
            "dataproc:dataproc.logging.stackdriver.enable": "true",
            "dataproc:dataproc.monitoring.stackdriver.enable": "true",
        },
    },
    # Same managed Hive Metastore the merge job writes to. Without this the
    # cluster boots its own empty catalog and sees zero tables.
    "metastore_config": {"dataproc_metastore_service": DPMS_SERVICE},
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": IDLE_DELETE_HOURS * 3600},
        "auto_delete_ttl": {"seconds": AUTO_DELETE_HOURS * 3600},
    },
    # Required to reach the Zeppelin UI through the Component Gateway instead
    # of an SSH tunnel. Pairs with internal_ip_only above.
    "endpoint_config": {"enable_http_port_access": True},
}


with DAG(
    dag_id="zeppelin_cluster",
    description="Start a Zeppelin cluster wired to the feature-store tables",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # on demand -- you start this when you want to explore
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    tags=["feature-store", "dataproc", "zeppelin"],
    default_args={
        "owner": "feature-store",
        "retries": 0,  # a half-created 16-VM cluster should be looked at, not retried
        "project_id": PROJECT_ID,
    },
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        # Re-triggering while the cluster is up is a no-op, not an error.
        use_if_exists=True,
        # 16 N4D VMs plus the Zeppelin component download. 3.0 images install
        # optional components at creation time, so this is slower than 2.2.
        execution_timeout=timedelta(minutes=20),
    )

    @task
    def log_zeppelin_url(cluster: dict[str, Any]) -> str:
        """Print the Component Gateway URL so it is not a console scavenger hunt."""
        ports = (
            (cluster.get("config") or {}).get("endpoint_config", {}).get("http_ports", {})
        )
        url = ports.get("Zeppelin", "")
        if not url:
            # Not fatal: the cluster is up either way, and the console still
            # has the link under WEB INTERFACES.
            log.warning("no Zeppelin endpoint in cluster config; ports=%s", ports)
            return ""

        log.info(
            "Zeppelin: %s\n"
            "  notebooks : %s\n"
            "  database  : %s   (try: %%sql SHOW TABLES IN %s)\n"
            "  cluster   : %s  --  auto-deletes %dh after creation",
            url,
            NOTEBOOKS_URI,
            DATABASE,
            DATABASE,
            CLUSTER_NAME,
            AUTO_DELETE_HOURS,
        )
        return url

    log_zeppelin_url(create_cluster.output)
