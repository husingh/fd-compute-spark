package com.fd.compute

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable

/**
 * Ports the legacy customFilter.jar (RegexFilterNew + ExpansionFilter) logic
 * to Scala so the Spark job can filter S3 input by date range + cpcode/metro
 * pattern, instead of processing every file under the input prefix.
 *
 * Directory layout under basePath (e.g.
 *   s3a://.../cw_storelog_v2_1mi_classic/footprint/fd_input/):
 *
 *   <basePath>/YYYY-MM-DD/<arlId>/<metro>/<files...>
 *
 * Config keys (driver-side; read from SparkConf):
 *   fd.computation.enableFiltering : "true" | "false"
 *   fd.computation.date.start      : "yyyy-MM-dd"
 *   fd.computation.date.end        : "yyyy-MM-dd" (inclusive)
 *   file.pattern                   : comma-separated entries of the form
 *                                    "arl:metro[:metro2:...]"
 *                                    e.g. "1263019:cw-s-sg,1285720:cw-s-sg"
 *   fd.computation.filter.type     : "cw" (default) | "expansion"
 */
object FdInputFileFilter {

  /** Build a filter from SparkConf; returns None when filtering is disabled. */
  def fromSparkConf(
    conf:     org.apache.spark.SparkConf,
    basePath: String
  ): Option[FdInputFileFilter] = {
    val enabled = conf.getOption("fd.computation.enableFiltering")
      .orElse(sys.props.get("fd.computation.enableFiltering"))
      .exists(_.equalsIgnoreCase("true"))
    if (!enabled) return None

    def req(k: String): String =
      conf.getOption(k).orElse(sys.props.get(k)).getOrElse {
        throw new IllegalStateException(s"fd.computation filtering enabled but '$k' not set")
      }

    val filterType = conf.getOption("fd.computation.filter.type")
      .orElse(sys.props.get("fd.computation.filter.type"))
      .getOrElse("cw")
      .toLowerCase

    Some(new FdInputFileFilter(
      basePath    = basePath.stripSuffix("/") + "/",
      startDate   = req("fd.computation.date.start"),
      endDate     = req("fd.computation.date.end"),
      filePattern = req("file.pattern"),
      filterType  = filterType
    ))
  }
}

class FdInputFileFilter(
  val basePath:    String,
  val startDate:   String,
  val endDate:     String,
  val filePattern: String,
  val filterType:  String = "cw"
) extends Serializable {

  require(filterType == "cw" || filterType == "expansion",
    s"unknown fd.computation.filter.type='$filterType' (expected 'cw' or 'expansion')")

  // SimpleDateFormat is NOT thread-safe; we only use it from the driver during listing.
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val start: Date = dateFormat.parse(startDate)
  val end:   Date = dateFormat.parse(endDate)

  // For "cw":  arlSet = first token of every comma entry
  //           comboSet = "tok[0]:tok[i]" for i >= 1 of every comma entry
  // For "expansion": same parse but the entry layout is different in the legacy
  //           ExpansionFilter.jar (it reads metro/map/network/traffic sets from
  //           a separate filter file in HDFS). For now we apply the same logic;
  //           callers should supply a flattened "expansion-style" file.pattern.
  val arlSet:   mutable.HashSet[String] = mutable.HashSet.empty[String]
  val comboSet: mutable.HashSet[String] = mutable.HashSet.empty[String]
  filePattern.split(",").foreach { entry =>
    val tok = entry.trim.split(":")
    if (tok.length >= 1 && tok(0).nonEmpty) {
      arlSet += tok(0)
      var i = 1
      while (i < tok.length) {
        comboSet += s"${tok(0)}:${tok(i)}"
        i += 1
      }
    }
  }

  /**
   * Accept/reject a leaf file path under basePath. Equivalent to running the
   * legacy Hadoop PathFilter at every directory level of the tree, applied to
   * a single leaf path:
   *
   *   parts[0] = "YYYY-MM-DD"  — must be in [start, end]
   *   parts[1] = "<arlId>"     — must be in arlSet
   *   parts[2] = "<metro>"     — "arlId:metro" must be in comboSet
   *   parts[3..] = filename(s) — accepted by virtue of dirs being accepted
   *
   * Paths that don't sit under basePath, or don't have at least 1 component,
   * are rejected so we don't accidentally pull unrelated S3 objects.
   */
  def accept(fullPath: String): Boolean = {
    val rel = fullPath.stripPrefix(basePath).stripPrefix("/")
    if (rel.isEmpty) return false
    val parts = rel.split("/")

    // Level 1: date
    val datePart = parts(0)
    val d = try dateFormat.parse(datePart) catch { case _: Throwable => return false }
    if (d.compareTo(start) < 0 || d.compareTo(end) > 0) return false

    // Level 2: arl id
    if (parts.length >= 2) {
      if (!arlSet.contains(parts(1))) return false
    }

    // Level 3: metro / pattern second token
    if (parts.length >= 3) {
      if (!comboSet.contains(s"${parts(1)}:${parts(2)}")) return false
    }

    true
  }

  override def toString: String =
    s"FdInputFileFilter(type=$filterType, base=$basePath, dates=$startDate..$endDate, " +
      s"arlSet=${arlSet.size}, comboSet=${comboSet.size})"
}
