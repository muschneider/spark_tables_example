// Diff two feature-store warehouses table by table.
//
// A1 rewrites the merge's physical plan and is claimed to be
// semantics-preserving. This proves it on real data instead of asserting it:
// for every table, in every date partition, the two warehouses must agree
// row for row.
//
// Counts alone are not enough -- a wrong coalesce direction preserves row
// count while corrupting values -- so this runs exceptAll in BOTH directions.
//
//   BASE=/tmp/warehouse_baseline PATCHED=/tmp/warehouse_patched \
//     spark-shell -i tools/compare_warehouses.scala

import org.apache.spark.sql.DataFrame

val baseDir = sys.env.getOrElse("BASE", "/tmp/warehouse_baseline")
val patchedDir = sys.env.getOrElse("PATCHED", "/tmp/warehouse_patched")
val tables = Seq("vps_credito", "vps_debito", "vps_consulta", "vps_similaridade", "vps_geo")
val db = "feature_store"

/** Read a table straight off disk. Going through the warehouse path rather
  * than a catalog avoids needing both Derby metastores live at once.
  */
def read(root: String, table: String): Option[DataFrame] = {
    val path = s"$root/$db.db/$table"
    if (!new java.io.File(path).isDirectory) None
    else
        Some(
            spark.read
                .option("basePath", path)
                .parquet(path)
        )
}

def sortedCols(df: DataFrame): Seq[String] = df.columns.toSeq.sorted

var failures = List.empty[String]
def fail(msg: String): Unit = { failures ::= msg; println(s"  FAIL  $msg") }

println(s"\nbaseline = $baseDir")
println(s"patched  = $patchedDir\n")

tables.foreach { t =>
    println(s"--- $db.$t ---")
    (read(baseDir, t), read(patchedDir, t)) match {
        case (None, None) =>
            println("  both absent (input never fed this table)")

        case (Some(_), None) => fail(s"$t: present in baseline, missing in patched")
        case (None, Some(_)) => fail(s"$t: missing in baseline, present in patched")

        case (Some(b0), Some(p0)) =>
            val bc = sortedCols(b0)
            val pc = sortedCols(p0)
            if (bc != pc) {
                fail(s"$t: schema differs\n    baseline=$bc\n    patched =$pc")
            } else {
                // align column order so exceptAll compares like with like
                val b = b0.select(bc.map(col): _*)
                val p = p0.select(bc.map(col): _*)
                val bn = b.count()
                val pn = p.count()
                val onlyBase = b.exceptAll(p).count()
                val onlyPatched = p.exceptAll(b).count()

                println(f"  rows baseline=$bn%-9d patched=$pn%-9d  onlyBase=$onlyBase%-6d onlyPatched=$onlyPatched%-6d")

                // per-partition row counts, so a mismatch says WHERE
                val byDate = b.groupBy("dataRef").count().withColumnRenamed("count", "base")
                    .join(p.groupBy("dataRef").count().withColumnRenamed("count", "patched"), Seq("dataRef"), "full_outer")
                    .orderBy("dataRef")
                byDate.collect().foreach { r =>
                    println(f"    ${r.getAs[String]("dataRef")}%-12s base=${r.get(1)}%-9s patched=${r.get(2)}%-9s")
                }

                if (bn != pn) fail(s"$t: row count $bn vs $pn")
                if (onlyBase != 0 || onlyPatched != 0)
                    fail(s"$t: $onlyBase rows only in baseline, $onlyPatched only in patched")
                if (bn == pn && onlyBase == 0 && onlyPatched == 0) println("  IDENTICAL")
            }
    }
}

println("\n" + "=" * 60)
if (failures.isEmpty) println("RESULT: all tables identical -- refactor is semantics-preserving")
else {
    println(s"RESULT: ${failures.size} MISMATCH(ES)")
    failures.reverse.foreach(f => println(s"  - $f"))
}
println("=" * 60)

System.exit(if (failures.isEmpty) 0 else 1)
