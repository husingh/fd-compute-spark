package com.fd.compute

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Spark job for the stack_distance map phase.
 *
 * Reads raw CDN log files, hands each partition to the C++ mapper via JNI
 * in batches of ~10 000 lines, and writes the filtered+formatted output as
 * text files ready for the reduce phase.
 *
 * Output format per line (colon-separated, as emitted by C++ mapper):
 *   serial:timestamp:md5:size:bytes:cpcode[:trueSerial:max_age]
 *
 * Config is passed to the C++ code entirely through environment variables
 * (FD_MAPREDUCE_*) set via spark.executorEnv.* — no config crosses the JNI
 * boundary.  See run_map_job.sh for the full list.
 *
 * Usage (spark-submit):
 *   spark-submit --class com.fd.compute.StackDistanceMapJob fd-compute-spark-0.1.jar \
 *     <inputPath> <outputPath> [numPartitions]
 *
 * Usage (sbt, local testing):
 *   sbt "runMain com.fd.compute.StackDistanceMapJob logs/test.log /tmp/fd_out 4"
 */
object StackDistanceMapJob {

  /** Number of raw log lines sent to C++ per JNI call. */
  val BATCH_SIZE = 10000

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: StackDistanceMapJob <inputPath> <outputPath> [numPartitions]"
      )
      System.exit(1)
    }

    val inputPath     = args(0)
    val outputPath    = args(1)
    val numPartitions = if (args.length > 2) args(2).toInt else 64

    val conf = new SparkConf().setAppName("StackDistanceMapJob")

    // Allow running locally with `sbt run` without an explicit --master
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    try {
      val count = run(sc, inputPath, outputPath, numPartitions)
      println(s"Map job complete. Emitted $count lines to $outputPath")
    } finally {
      sc.stop()
    }
  }

  /**
   * Core map logic — separated from main() for testability.
   *
   * @return  Total number of output (key, line) pairs written.
   */
  def run(
    sc:            SparkContext,
    inputPath:     String,
    outputPath:    String,
    numPartitions: Int
  ): Long = {

    // Read raw log files.  textFile() handles gzipped files transparently.
    // repartition() controls how many C++ mapper instances run in parallel.
    val rawLines: RDD[String] =
      sc.textFile(inputPath).repartition(numPartitions)

    // Map phase — one C++ mapper per Spark partition
    val mapped: RDD[String] = rawLines.mapPartitions { iter =>

      // ------------------------------------------------------------------
      // Init: one call per partition.
      //   - readConfig() inside C++ picks up FD_MAPREDUCE_* from env vars
      //   - Ghost-IP filtering by filename is not used in Spark mode;
      //     filter input paths upstream if needed.
      // ------------------------------------------------------------------
      val handle = FDComputeNative.mapperInit("")

      // ------------------------------------------------------------------
      // Process: feed lines in batches of BATCH_SIZE
      // ------------------------------------------------------------------
      val outputLines: Iterator[String] = iter
        .grouped(BATCH_SIZE)
        .flatMap { batch =>

          // One JNI call per batch — C++ does all parsing + filtering
          val output = FDComputeNative.mapperProcessBatch(
            handle,
            batch.mkString("\n")
          )

          if (output == null || output.isEmpty) {
            Iterator.empty
          } else {
            // C++ emits colon-separated lines: serial:timestamp:md5:...
            // Filter out blank lines only.
            output.linesIterator.filter(_.nonEmpty)
          }
        }

      // ------------------------------------------------------------------
      // Lazy finalizing iterator — calls mapperFinalize only when the
      // output is fully consumed, avoiding materializing everything into
      // a List (which caused OOM on large partitions).
      // ------------------------------------------------------------------
      new Iterator[String] {
        private var done = false

        def hasNext: Boolean = {
          val hn = outputLines.hasNext
          if (!hn && !done) {
            done = true
            FDComputeNative.mapperFinalize(handle) // frees C++ MapperCtx
          }
          hn
        }

        def next(): String = outputLines.next()
      }
    }

    // ------------------------------------------------------------------
    // Simple reduce: count lines emitted per key (field 0 = serial/bucket)
    // This shuffles the map output exactly as the real reducer would,
    // letting us verify the map job works end-to-end without JNI reducer.
    // Output: "<key>\t<lineCount>"
    // ------------------------------------------------------------------
    val counts: RDD[String] = mapped
      .map { line =>
        val key = line.substring(0, line.indexOf(':'))
        (key, 1L)
      }
      .reduceByKey(_ + _)            // shuffle: all lines for same key → same task
      .sortBy(_._2, ascending = false)
      .map { case (key, cnt) => s"$key\t$cnt" }

    // Remove previous output if it exists
    val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    val outPath = new org.apache.hadoop.fs.Path(outputPath)
    if (fs.exists(outPath)) fs.delete(outPath, true)

    counts.saveAsTextFile(outputPath)

    val count = mapped.count()

    println(s"\n--- Lines emitted by C++ mapper per key (top 20) ---")
    counts.take(20).foreach(println)

    count
  }
}
