package fs

import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{File, FileInputStream}
import java.util.Properties
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** Splits a parquet dataset into feature tables, one per entry of `tableConfig`.
  *
  * Every table is PARTITIONED BY dateRefColumn and BUCKETED BY keyColumn, so the merge below shuffles only the incoming dataset: the stored
  * table already sits on the join distribution. Tables are built concurrently; each one scans only the parquet columns it needs.
  *
  * A configured table the input feeds no column of is skipped: the dataset has nothing to say about it, and writing it would only
  * materialize NULLs.
  *
  * Optional properties, all with production-safe defaults:
  *   - `maxConcurrentTables` (8) how many tables to build at once
  *   - `dateRefValue` (derived) the partition being loaded, when the caller already knows it
  *   - `assertInputSuperset` (true) verify the merge cannot drop stored keys
  *
  * REQUIRED SPARK CONF, set at submit time -- this job does not set them itself:
  *   - `spark.sql.sources.partitionOverwriteMode=dynamic` rewrite only the incoming dates. Without it the overwriting `insertInto` below
  *     replaces the WHOLE table, silently destroying every other date partition.
  *   - `spark.sql.requireAllClusterKeysForCoPartition=false` bucketing on the key alone serves the (key, date) join. Without it Spark
  *     refuses the co-partitioning and shuffles the stored side too, which is the one thing this design exists to avoid.
  *   - `spark.sql.shuffle.partitions=<numBuckets>` must equal the `numBuckets` property. A mismatch costs an extra exchange before the
  *     bucketed write.
  *
  * See `mise.toml` (local), `tools/run_jobs.sh` (fixtures) and `dags/feature_store_merge_dag.py` (`spark_properties`, production).
  */
object FeatureStoreMerge {

    private val log = LoggerFactory.getLogger(getClass)

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("feature-store-ds-merge").enableHiveSupport().getOrCreate()

        val p = new Properties()
        val stream = new FileInputStream(properties(spark))
        try p.load(stream)
        finally stream.close()

        val key = p.getProperty("keyColumn")
        val date = p.getProperty("dateRefColumn")
        val db = p.getProperty("database", "feature_store")
        val buckets = p.getProperty("numBuckets", "256").toInt
        val fallback = DataType.fromDDL(p.getProperty("defaultColumnType", "string"))

        // One Spark job per table runs concurrently. Unbounded, a wide fan-out lands one query plan, one `buckets`-way task set and one
        // metastore conversation per configured table on the driver at the same time; the scheduler's event loop is single threaded.
        // Each table already parallelizes across the cluster, so concurrency here only fills straggler tails.
        val lanes = optional(p, "maxConcurrentTables").fold(8)(_.toInt)
        val assertSuperset = optional(p, "assertInputSuperset").fold(true)(_.toBoolean)

        spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")

        val src = spark.read.parquet(p.getProperty("inputDataSet"))
        val srcCols = src.columns.map(_.toLowerCase).toSet

        val dates = dateRefs(src, date, optional(p, "dateRefValue"))

        val cfg = spark.read.option("multiLine", value = true).json(p.getProperty("tableConfig")).head()
        val configured = cfg.schema.fieldNames.map { t =>
            t -> cfg.getAs[collection.Seq[String]](t).toList.filterNot(c => c.equalsIgnoreCase(key) || c.equalsIgnoreCase(date))
        }.toList

        // the input must feed at least one column, otherwise the write would only materialize NULLs
        val (tables, skipped) = configured.partition { case (_, cols) => cols.exists(c => srcCols(c.toLowerCase)) }
        skipped.foreach { case (t, cols) =>
            log.warn(s"skipping $db.$t: the input has none of its ${cols.size} configured columns")
        }
        log.info(s"merging ${dates.mkString(",")} into ${tables.size} table(s), $lanes at a time")

        val pool = Executors.newFixedThreadPool((lanes min tables.size) max 1)
        implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)

        try
            Await.result(
                Future.traverse(tables) { case (table, cols) =>
                    Future {
                        val name = s"$db.$table"
                        val target = if (spark.catalog.tableExists(name)) Some(spark.table(name)) else None
                        val fed = cols.filter(c => srcCols(c.toLowerCase))
                        val isFed = fed.map(_.toLowerCase).toSet

                        // every configured column, the absent ones as NULL typed after the stored table when it exists. Only for the paths
                        // with no stored row to read those columns from.
                        lazy val widened = src.select(cols.map { c =>
                            if (isFed(c.toLowerCase)) col(c)
                            else lit(null).cast(target.flatMap(_.schema.find(_.name.equalsIgnoreCase(c))).fold(fallback)(_.dataType)).as(c)
                        } ++ Seq(col(key), col(date)): _*)

                        target match {
                            case None =>
                                widened
                                    .repartition(buckets, col(key)) // one file per bucket
                                    .write
                                    .partitionBy(date)
                                    .bucketBy(buckets, key)
                                    .sortBy(key)
                                    .saveAsTable(name)

                            case Some(t) if stored(spark, name, date, dates) =>
                                // Only the columns the input actually feeds cross the shuffle: for a column it does not feed,
                                // coalesce(null, t.c) is just t.c, so it is read from the target instead -- which never moves, being
                                // already bucketed on the join key.
                                //
                                // MEASURED: this is NOT a speed-up. Spark already derives it. NullPropagation rewrites
                                // coalesce(Literal(null), t.c) to t.c, and ColumnPruning then drops the unreferenced null literals from
                                // the projection under the exchange. Projecting all 200 columns and projecting only the 20 fed ones
                                // produce the same physical plan -- input-side exchange 22 attributes, input scan 22 columns -- and the
                                // same shuffle bytes (36.52 MB, 73.0 B/record, 500k rows, 20-of-200 fed). Written out explicitly only so
                                // the intent is visible in the source. Do not "optimize" the wide form back in expecting a win.
                                val staged = src.select(fed.map(col) ++ Seq(col(key), col(date)): _*)
                                val slice = t.where(col(date).isin(dates: _*))
                                if (assertSuperset) requireSuperset(staged, slice, key, name, dates)

                                // the input drives the join (it never has fewer rows) and its non-null values win
                                staged
                                    .as("s")
                                    .join(slice.as("t").hint("merge"), Seq(key, date), "left")
                                    .select(cols.map { c =>
                                        if (isFed(c.toLowerCase)) coalesce(col(s"s.$c"), col(s"t.$c")).as(c) else col(s"t.$c").as(c)
                                    } ++ Seq(col(key), col(date)): _*)
                                    .write
                                    .mode("overwrite")
                                    .insertInto(name)

                            case _ =>
                                widened.repartition(buckets, col(key)).write.mode("overwrite").insertInto(name)
                        }
                    }
                },
                Duration.Inf
            )
        finally {
            pool.shutdown()
            spark.stop()
        }
    }

    /** A property that may be absent; blank counts as absent. */
    private def optional(p: Properties, name: String): Option[String] =
        Option(p.getProperty(name)).map(_.trim).filter(_.nonEmpty)

    /** The dateRef the input carries.
      *
      * The caller normally already knows it and declares it via `dateRefValue`, which costs nothing. Otherwise the column is scanned and
      * exactly one value is required: this job merges a single partition per run, so a second dateRef in the same input would be written
      * without ever being merged, replacing everything the stored partition holds outside this input's columns with NULLs.
      */
    private def dateRefs(src: DataFrame, date: String, declared: Option[String]): List[String] =
        declared.map(List(_)).getOrElse {
            src.select(date).distinct().collect().flatMap(r => Option(r.get(0))).map(_.toString).toList match {
                case one :: Nil => List(one)
                case Nil        => sys.error(s"the input carries no $date value")
                case many =>
                    sys.error(
                        s"the input carries ${many.size} distinct $date values (${many.sorted.mkString(", ")}); this job merges one " +
                            "partition per run -- split the input, or set dateRefValue to name the one being loaded"
                    )
            }
        }

    /** The `*.properties` shipped by `spark-submit --files`, whatever it is named. */
    private def properties(spark: SparkSession): File = {
        val name = spark.sparkContext.getConf
            .get("spark.files", "")
            .split(',')
            .map(_.split('/').last)
            .find(_.endsWith(".properties"))
            .getOrElse(sys.error("spark-submit needs --files <some>.properties"))
        val shipped = new File(name) // cluster mode lands it in the working directory
        if (shipped.isFile) shipped else new File(SparkFiles.get(name))
    }

    /** Metastore-only lookup: is any incoming date already materialized?
      *
      * Asks about the one partition in question. A bare `SHOW PARTITIONS` returns the table's whole history, which grows by a row per day
      * per table and is read once per table per run.
      */
    private def stored(spark: SparkSession, table: String, date: String, dates: Seq[String]): Boolean =
        dates.exists { d =>
            // an absent partition either throws NoSuchPartitionException or comes back empty, depending on the catalog
            try spark.sql(s"SHOW PARTITIONS $table PARTITION($date = '${d.replace("'", "''")}')").take(1).nonEmpty
            catch { case _: AnalysisException => false }
        }

    /** Fails unless the input carries every key the stored partition already holds.
      *
      * The merge is a LEFT join from the input, so a stored key the input lacks is dropped from the join, and the dynamic partition
      * overwrite that follows makes the loss permanent. The join is only correct while the input is a superset, which nothing else checks.
      *
      * Compares distinct keys, not row counts: an input may legitimately repeat a key. The target is bucketed on the key, so its distinct is
      * partition-local, and only one narrow column of the input is shuffled -- cheap next to the wide merge it guards.
      */
    private def requireSuperset(staged: DataFrame, slice: DataFrame, key: String, table: String, dates: Seq[String]): Unit = {
        val orphans = slice.select(col(key)).distinct().join(staged.select(col(key)).distinct(), Seq(key), "left_anti").count()
        if (orphans > 0)
            sys.error(
                s"$table ${dates.mkString(",")}: $orphans stored key(s) absent from the input, which a LEFT join would drop. Fix the " +
                    "input, or set assertInputSuperset=false to accept the loss."
            )
    }
}
