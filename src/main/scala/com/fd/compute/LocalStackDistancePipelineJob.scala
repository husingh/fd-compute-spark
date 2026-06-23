package com.fd.compute

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Partitioner
import org.apache.spark.TaskContext

import java.io._
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.zip.GZIPInputStream

import scala.collection.JavaConverters._

/**
 * Local-mode stack_distance pipeline.
 *
 * Reads already-decrypted .gz files from a local directory (e.g. downloaded
 * by download_fd_input.py), runs the C++ mapper + reducer via JNI, and writes
 * stdtime.* output files to a local output directory.
 *
 * No S3 / SSE-C / Kubernetes required. Use run_local_job.sh to launch.
 *
 * Usage:
 *   spark-submit --class com.fd.compute.LocalStackDistancePipelineJob \
 *     fd-compute-spark-assembly.jar \
 *     <inputDir> <outputDir> <numMappers> <numReducers>
 *
 * Data flow (mirrors StackDistancePipelineJob without S3):
 *   local .gz files
 *     → parallelize file paths → mapPartitions (decompress per file)
 *     → repartition(numMappers) → mapPartitions (C++ mapper via JNI)
 *     → repartitionAndSortWithinPartitions (shuffle, key+timestamp sort)
 *     → foreachPartition (C++ reducer via JNI)
 *     → stdtime.* files copied to outputDir
 */
object LocalStackDistancePipelineJob {

  def mapperBatchSize:  Int = sys.env.getOrElse("FD_MAPPER_BATCH_SIZE",  "10000").toInt
  def reducerBatchSize: Int = sys.env.getOrElse("FD_REDUCER_BATCH_SIZE", "10000").toInt

  class SerialKeyPartitioner(val numPartitions: Int) extends Partitioner {
    require(numPartitions > 0)
    def getPartition(key: Any): Int = key match {
      case (k: String, _: Double) => (k.toInt % numPartitions + numPartitions) % numPartitions
      case _ => 0
    }
  }

  /** Recursively list all .gz files under dir (includes .gz.keyN from SSE-C downloads). */
  def listLocalFiles(dir: String): Seq[String] =
    Files.walk(Paths.get(dir))
      .iterator().asScala
      .filter(p => Files.isRegularFile(p) && p.toString.contains(".gz"))
      .map(_.toString)
      .toSeq
      .sorted

  /** Lazy line iterator over a local gzip file (already decrypted). */
  def readLocalGzipFile(path: String): Iterator[String] = {
    val raw    = new FileInputStream(path)
    val gz     = new GZIPInputStream(raw)
    val reader = new BufferedReader(new InputStreamReader(gz, "UTF-8"))
    new Iterator[String] {
      private var line: String = reader.readLine()
      def hasNext: Boolean     = line != null
      def next(): String = {
        val cur = line
        line = reader.readLine()
        if (line == null) { try { reader.close() } catch { case _: Exception => } }
        cur
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        "Usage: LocalStackDistancePipelineJob <inputDir> <outputDir> <numMappers> <numReducers>"
      )
      System.exit(1)
    }

    val inputDir    = args(0)
    val outputDir   = args(1)
    val numMappers  = args(2).toInt
    val numReducers = args(3).toInt

    val conf = new SparkConf().setAppName("LocalStackDistancePipelineJob")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val sc = new SparkContext(conf)
    try {
      run(sc, inputDir, outputDir, numMappers, numReducers)
      println(s"Pipeline complete. Output in: $outputDir")
    } finally {
      sc.stop()
    }
  }

  def run(
    sc:          SparkContext,
    inputDir:    String,
    outputDir:   String,
    numMappers:  Int,
    numReducers: Int
  ): Unit = {

    val allFiles = listLocalFiles(inputDir)
    println(s"Found ${allFiles.size} file(s) under $inputDir")
    allFiles.foreach(f => println(s"  $f"))

    new File(outputDir).mkdirs()

    // -------------------------------------------------------------------------
    // Stage 1a: parallelize file list → decompress each .gz into raw log lines
    // -------------------------------------------------------------------------
    val rawLines = sc.parallelize(allFiles, allFiles.size)
      .mapPartitions { fileIter =>
        fileIter.flatMap(readLocalGzipFile)
      }

    // -------------------------------------------------------------------------
    // Stage 1b: repartition → C++ mapper (one instance per Spark partition)
    // -------------------------------------------------------------------------
    val mapOutput = rawLines
      .repartition(numMappers)
      .mapPartitions { iter =>
        FDComputeNative.ensureLoaded("")
        val mapperEnvOverrides = sys.env
          .filter { case (k, _) => k.startsWith("FD_MAPREDUCE_") }
          .map    { case (k, v) => s"$k=$v" }
          .mkString("\n")
        if (mapperEnvOverrides.nonEmpty)
          println(s"[mapper] Env overrides for C++:\n${mapperEnvOverrides.linesIterator.map("  " + _).mkString("\n")}")
        val handle = FDComputeNative.mapperInit("", mapperEnvOverrides)

        val outputLines = iter
          .grouped(mapperBatchSize)
          .flatMap { batch =>
            val out = FDComputeNative.mapperProcessBatch(handle, batch.mkString("\n"))
            if (out == null || out.isEmpty) Iterator.empty
            else out.linesIterator.filter(_.nonEmpty)
          }

        new Iterator[String] {
          private var done = false
          def hasNext: Boolean = {
            val hn = outputLines.hasNext
            if (!hn && !done) { done = true; FDComputeNative.mapperFinalize(handle) }
            hn
          }
          def next(): String = outputLines.next()
        }
      }

    // -------------------------------------------------------------------------
    // DEBUG: FD_DEBUG_DUMP=true — print mapper output summary and save to disk.
    // Reducer does NOT run. Re-run without FD_DEBUG_DUMP to do the full pipeline.
    // -------------------------------------------------------------------------
    val debugDump = sys.env.getOrElse("FD_DEBUG_DUMP", "false").toLowerCase == "true"
    if (debugDump) {
      val debugDir    = sys.env.getOrElse("FD_DEBUG_DIR", s"$outputDir/debug")
      val mapOutPath  = s"$debugDir/map_output"
      println(s"[DEBUG] FD_DEBUG_DUMP=true — sampling mapper output (reducer will NOT run)")
      val cached    = mapOutput.cache()
      val lineCount = cached.count()
      val sample    = cached.take(20)
      println(s"\n[DEBUG] ===== Mapper Output Summary =====")
      println(s"[DEBUG] Total output lines : $lineCount")
      println(s"[DEBUG] Format: serial:timestamp:md5:size:bytes:cpcode[:trueSerial:max_age]")
      println(s"[DEBUG] First ${sample.length} lines:")
      sample.foreach(l => println(s"[DEBUG]   $l"))
      // Remove existing output dir so saveAsTextFile doesn't fail on re-runs
      val existing = new File(mapOutPath)
      if (existing.exists()) {
        def deleteDir(f: File): Unit = { if (f.isDirectory) Option(f.listFiles()).foreach(_.foreach(deleteDir)); f.delete() }
        deleteDir(existing)
      }
      cached.saveAsTextFile(mapOutPath)
      println(s"[DEBUG] Full output saved to: $mapOutPath")
      return
    }

    // -------------------------------------------------------------------------
    // Stage 2: shuffle + sort by (serialKey, timestamp)
    // -------------------------------------------------------------------------
    val keyed = mapOutput.map { line =>
      val firstColon  = line.indexOf(':')
      val secondColon = line.indexOf(':', firstColon + 1)
      val key = line.substring(0, firstColon)
      val ts  = line.substring(firstColon + 1, secondColon).toDouble
      ((key, ts), line)
    }

    implicit val pairOrdering: Ordering[(String, Double)] =
      Ordering.Tuple2(Ordering.String, Ordering.Double)

    val sorted = keyed.repartitionAndSortWithinPartitions(
      new SerialKeyPartitioner(numReducers)
    )

    // -------------------------------------------------------------------------
    // Stage 3: C++ reducer → stdtime.* → copied to outputDir
    // -------------------------------------------------------------------------
    val jobRunId = sys.env.getOrElse(
      "FD_JOB_RUN_ID",
      Option(org.apache.spark.SparkEnv.get).map(_.conf.getAppId).getOrElse("local")
    )
    val bcOutputDir   = sc.broadcast(outputDir)
    val bcJobRunId    = sc.broadcast(jobRunId)
    val bcNumReducers = sc.broadcast(numReducers)

    val partitionLogs: Array[String] = sorted.mapPartitions { iter =>
      val partitionId = TaskContext.get().partitionId()
      val msgs        = scala.collection.mutable.ArrayBuffer[String]()
      def log(msg: String): Unit = msgs += s"[P$partitionId] $msg"

      try {
        val numRed = bcNumReducers.value
        val outDir = bcOutputDir.value
        val runId  = bcJobRunId.value

        log(s"hostname=${java.net.InetAddress.getLocalHost.getHostName}")

        FDComputeNative.ensureLoaded("")
        log("ensureLoaded OK")

        val localTmpDir = s"/tmp/fd_compute/$runId/$partitionId"
        new File(localTmpDir).mkdirs()
        log(s"localTmpDir=$localTmpDir")

        // Forward all FD_MAPREDUCE_* env vars to C++ and pin the output dir
        val fdEnvOverrides  = sys.env
          .filter { case (k, _) => k.startsWith("FD_MAPREDUCE_") }
          .map    { case (k, v) => s"$k=$v" }
          .toSeq
        val envOverridesStr = (fdEnvOverrides :+ s"FD_MAPREDUCE_OUTPUT_DIR=$localTmpDir").mkString("\n")
        log(s"envOverrides: ${envOverridesStr.replace("\n", " | ")}")

        // Materialize lines first so we can skip reducerInit entirely for empty partitions
        val lines = iter.map(_._2).toVector
        val lineCount = lines.size
        log(s"lines in partition: $lineCount")

        // FD_DEBUG_REDUCER_INPUT=true writes each partition's input lines to
        // <FD_DEBUG_DIR>/reducer_input/part-<partitionId>.txt for inspection.
        if (sys.env.getOrElse("FD_DEBUG_REDUCER_INPUT", "false").toLowerCase == "true" && lineCount > 0) {
          val debugDir  = sys.env.getOrElse("FD_DEBUG_DIR", s"$outDir/debug")
          val inputDir  = new File(s"$debugDir/reducer_input")
          inputDir.mkdirs()
          val partFile  = new File(inputDir, f"part-$partitionId%05d.txt")
          val pw        = new java.io.PrintWriter(partFile)
          try { lines.foreach(pw.println) } finally { pw.close() }
          log(s"wrote ${lines.size} input lines to ${partFile.getAbsolutePath}")
        }

        if (lineCount > 0) {
          FDComputeNative.reducerInit(partitionId, numRed, envOverridesStr)
          log("reducerInit OK")

          var batchCount = 0
          lines.grouped(reducerBatchSize).foreach { batch =>
            batchCount += 1
            FDComputeNative.reducerProcessBatch(batch.mkString("\n"))
          }
          log(s"reducerProcessBatch done: $batchCount batches, $lineCount lines")

          FDComputeNative.reducerFinalize()
          log("reducerFinalize OK")
        } else {
          log("Empty partition — skipping reducer entirely")
        }

        if (lineCount > 0) {
          // Find stdtime.* files written by C++ to the tmp dir
          val tmpDirFile  = new File(localTmpDir)
          val allTmpFiles = Option(tmpDirFile.listFiles()).getOrElse(Array.empty)
          log(s"Total files in $localTmpDir: ${allTmpFiles.length}")
          allTmpFiles.foreach(f => log(s"  ${f.getName}  ${f.length} bytes"))

          val stdtimeFiles = allTmpFiles.filter(f => f.isFile && f.getName.startsWith("stdtime"))
          val serialInfoFiles = allTmpFiles.filter(f => f.isFile && f.getName.startsWith("serialInfo"))
          val outputFiles = stdtimeFiles ++ serialInfoFiles
          log(s"stdtime files to copy: ${stdtimeFiles.length}, serialInfo files to copy: ${serialInfoFiles.length}")

          if (stdtimeFiles.isEmpty) {
            log(s"WARNING: no stdtime.* files found in $localTmpDir")
          } else {
            val outDirFile = new File(outDir)
            outDirFile.mkdirs()
            outputFiles.foreach { f =>
              val dest = new File(outDir, f.getName)
              Files.copy(f.toPath, dest.toPath, StandardCopyOption.REPLACE_EXISTING)
              log(s"Copied ${f.getName} (${f.length} bytes) → ${dest.getAbsolutePath}")
            }
            log(s"All ${outputFiles.length} file(s) saved to $outDir/")
          }

          // Clean up tmp dir
          try {
            allTmpFiles.foreach(_.delete())
            tmpDirFile.delete()
          } catch { case _: Exception => }
        }

      } catch {
        case e: Exception =>
          msgs += s"[P$partitionId] EXCEPTION ${e.getClass.getName}: ${e.getMessage}"
          msgs += e.getStackTrace.take(15).map(f => s"[P$partitionId]   at $f").mkString("\n")
      }

      msgs.iterator
    }.collect()

    println("=== Executor Partition Logs ===")
    partitionLogs.foreach(println)
    println("=== End Executor Logs ===")
  }
}
