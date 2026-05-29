package com.fd.compute

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Partitioner
import org.apache.spark.TaskContext

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.GZIPInputStream
import java.security.MessageDigest
import java.util.Base64

import scala.io.Source
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * End-to-end stack_distance pipeline: map + reduce in a single Spark job.
 *
 * Reads SSE-C encrypted, gzip-compressed log files from Linode Object Storage
 * (S3-compatible via S3A). Decrypts using per-file key index (.keyN suffix).
 * Decompresses gzip. Feeds raw lines to the C++ mapper via JNI.
 *
 * Data flows (no intermediate files):
 *   S3 encrypted .gz files
 *     → list files on driver
 *     → parallelize file paths → mapPartitions (decrypt + decompress per file)
 *     → repartition(numMappers) → mapPartitions (C++ mapper via JNI)
 *     → repartitionAndSortWithinPartitions (Spark shuffle, key+timestamp sort)
 *     → foreachPartition (C++ reducer via JNI)
 *     → stdtime.<ext> files in FD_MAPREDUCE_OUTPUT_DIR
 *
 * Usage:
 *   spark-submit --class com.fd.compute.StackDistancePipelineJob \
 *     fd-compute-spark_2.13-0.1.jar \
 *     <s3Bucket> <s3Prefix> <encryptionKeysFile> <numMappers> <numReducers>
 */
object StackDistancePipelineJob {

  // These can all be overridden via environment variables without rebuilding.
  def mapperBatchSize:  Int    = sys.env.getOrElse("FD_MAPPER_BATCH_SIZE",  "10000").toInt
  def reducerBatchSize: Int    = sys.env.getOrElse("FD_REDUCER_BATCH_SIZE", "10000").toInt
  def s3Endpoint:       String = sys.env.getOrElse("S3_ENDPOINT", "us-ord-10.linodeobjects.com")

  // -------------------------------------------------------------------------
  // Partitioner: routes all lines with the same serialKey to one reducer
  // -------------------------------------------------------------------------
  class SerialKeyPartitioner(val numPartitions: Int) extends Partitioner {
    require(numPartitions > 0)
    def getPartition(key: Any): Int = key match {
      case (k: String, _: Double) => (k.hashCode & Int.MaxValue) % numPartitions
      case _ => 0
    }
  }

  // -------------------------------------------------------------------------
  // S3 / SSE-C helpers
  // -------------------------------------------------------------------------

  /**
   * Parse encryption_keys.json: {"1": "<base64>", "2": "<base64>"}
   * Returns Map[keyIndex -> base64Key].
   * Uses only standard library — no external JSON dependency needed on executors.
   */
  def loadEncryptionKeys(path: String): Map[String, String] = {
    val src  = Source.fromFile(path)
    val json = try src.mkString finally src.close()
    // Strip braces, quotes, whitespace; split on commas; split each pair on first colon
    json.replaceAll("[{}\"\\s]", "")
      .split(",")
      .flatMap { pair =>
        val idx = pair.indexOf(':')
        if (idx < 0) None
        else Some(pair.substring(0, idx).trim -> pair.substring(idx + 1).trim)
      }
      .toMap
  }

  /**
   * Extract .keyN suffix from an S3 path.
   * "s3a://bucket/prefix/file.gz.key2" → "2"
   * "s3a://bucket/prefix/file.gz"      → "1" (default)
   */
  def keyIndexOf(s3Path: String): String = {
    val m = """\.key(\d+)$""".r.findFirstMatchIn(s3Path)
    m.map(_.group(1)).getOrElse("1")
  }

  /**
   * Build a Hadoop Configuration for S3A with SSE-C decryption.
   * Called on executors (serializable pieces only — String args).
   */
  def s3aConf(accessKey: String, secretKey: String, sseKeyB64: String): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.s3a.impl",                               "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.endpoint",                           s3Endpoint)
    conf.set("fs.s3a.path.style.access",                  "true")
    conf.set("fs.s3a.connection.ssl.enabled",             "true")
    conf.set("fs.s3a.access.key",                         accessKey)
    conf.set("fs.s3a.secret.key",                         secretKey)
    conf.set("fs.s3a.server-side-encryption-algorithm",   "SSE-C")
    conf.set("fs.s3a.server-side-encryption.key",         sseKeyB64)
    // Required by the SSE-C protocol: MD5 of the raw key bytes
    val keyBytes = Base64.getDecoder.decode(sseKeyB64)
    val md5B64   = Base64.getEncoder.encodeToString(
                     MessageDigest.getInstance("MD5").digest(keyBytes))
    conf.set("fs.s3a.server-side-encryption-key-md5", md5B64)
    // Required by S3A internally even when no local staging is used
    conf.set("hadoop.tmp.dir", System.getProperty("java.io.tmpdir", "/tmp"))
    conf
  }

  /**
   * List all files recursively under s3a://bucket/prefix using the given conf.
   */
  def listS3Files(bucket: String, prefix: String, conf: Configuration): Seq[String] = {
    val basePath = new Path(s"s3a://$bucket/$prefix")
    val fs       = FileSystem.get(basePath.toUri, conf)
    val buf      = mutable.ArrayBuffer[String]()
    try {
      val iter = fs.listFiles(basePath, /* recursive = */ true)
      while (iter.hasNext) {
        val status = iter.next()
        val p = status.getPath.toString
        // Skip directory placeholder entries (end with /) and non-gz files
        if (!p.endsWith("/") && p.contains(".gz")) buf += p
      }
    } finally fs.close()
    buf.toSeq
  }

  /**
   * Open an SSE-C encrypted, gzip-compressed S3 file and return a lazy line iterator.
   * S3A handles decryption transparently via the SSE-C headers in the Hadoop conf.
   */
  def readEncryptedGzipFile(s3Path: String, conf: Configuration): Iterator[String] = {
    val path   = new Path(s3Path)
    val fs     = FileSystem.get(path.toUri, conf)
    val raw    = fs.open(path)
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

  // -------------------------------------------------------------------------
  // Main
  // -------------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println(
        "Usage: StackDistancePipelineJob " +
        "<s3Bucket> <s3Prefix> <encryptionKeysFile> <numMappers> <numReducers>"
      )
      System.exit(1)
    }

    val s3Bucket           = args(0)
    val s3Prefix           = args(1).stripSuffix("/")
    val encryptionKeysFile = args(2)
    val numMappers         = args(3).toInt
    val numReducers        = args(4).toInt
    val s3OutputPrefix     = if (args.length > 5) args(5).stripSuffix("/") else "fd_output"

    val conf = new SparkConf().setAppName("StackDistancePipelineJob")
    if (!conf.contains("spark.master")) conf.setMaster("local[1]")

    val sc = new SparkContext(conf)
    try {
      run(sc, s3Bucket, s3Prefix, encryptionKeysFile, numMappers, numReducers, s3OutputPrefix)
      println(s"Pipeline complete. stdtime files uploaded to s3://$s3Bucket/$s3OutputPrefix/")
    } finally {
      sc.stop()
    }
  }

  // -------------------------------------------------------------------------
  // Pipeline
  // -------------------------------------------------------------------------

  def run(
    sc:                 SparkContext,
    s3Bucket:           String,
    s3Prefix:           String,
    encryptionKeysFile: String,
    numMappers:         Int,
    numReducers:        Int,
    s3OutputPrefix:     String = "fd_output"
  ): Unit = {

    // Read credentials from env vars (set via spark.executorEnv.* in run script)
    val accessKey = sys.env.getOrElse("S3_ACCESS_KEY",
      throw new IllegalStateException("S3_ACCESS_KEY env var not set"))
    val secretKey = sys.env.getOrElse("S3_SECRET_KEY",
      throw new IllegalStateException("S3_SECRET_KEY env var not set"))

    // Load encryption keys on the driver (local file, not S3)
    val encryptionKeys: Map[String, String] = loadEncryptionKeys(encryptionKeysFile)
    println(s"Loaded ${encryptionKeys.size} encryption key(s) from $encryptionKeysFile")

    // List all files — use key "1" (no data read, listing never needs SSE-C key)
    val listConf  = s3aConf(accessKey, secretKey, encryptionKeys("1"))
    val allFiles  = listS3Files(s3Bucket, s3Prefix, listConf)
    println(s"Found ${allFiles.size} file(s) under s3://$s3Bucket/$s3Prefix")
    allFiles.foreach(f => println(s"  $f"))

    // Broadcast credentials and keys so executors can access S3
    val bcAccessKey      = sc.broadcast(accessKey)
    val bcSecretKey      = sc.broadcast(secretKey)
    val bcEncryptionKeys = sc.broadcast(encryptionKeys)

    // -----------------------------------------------------------------------
    // Stage 1a: Parallelize file list → each task reads one S3 file
    //           (decrypt SSE-C + decompress gzip → raw log lines)
    // -----------------------------------------------------------------------
    val rawLines = sc.parallelize(allFiles, allFiles.size)
      .mapPartitions { fileIter =>
        val ak   = bcAccessKey.value
        val sk   = bcSecretKey.value
        val keys = bcEncryptionKeys.value

        fileIter.flatMap { s3Path =>
          val keyIdx = keyIndexOf(s3Path)
          val sseKey = keys.getOrElse(keyIdx,
            throw new IllegalArgumentException(
              s"No encryption key for index '$keyIdx' in file: $s3Path"))
          val conf = s3aConf(ak, sk, sseKey)
          readEncryptedGzipFile(s3Path, conf)
        }
      }

    // -----------------------------------------------------------------------
    // Stage 1b: C++ mapper — repartition first so each mapper gets an
    //           even share of lines (not one partition per file)
    // -----------------------------------------------------------------------
    val mapOutput = rawLines
      .repartition(numMappers)
      .mapPartitions { iter =>

        val handle = FDComputeNative.mapperInit("")

        val outputLines = iter
          .grouped(mapperBatchSize)
          .flatMap { batch =>
            val out = FDComputeNative.mapperProcessBatch(handle, batch.mkString("\n"))
            if (out == null || out.isEmpty) Iterator.empty
            else out.linesIterator.filter(_.nonEmpty)
          }

        // Lazy wrapper: calls mapperFinalize only when all output is consumed
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

    // -----------------------------------------------------------------------
    // DEBUG: if FD_DEBUG_DUMP=true, save mapper output to local disk and stop.
    // Inspect /tmp/fd_debug/map_output/ to verify mapper format and content,
    // then re-run without FD_DEBUG_DUMP to proceed to the reducer.
    // -----------------------------------------------------------------------
    val debugDump = sys.env.getOrElse("FD_DEBUG_DUMP", "false").toLowerCase == "true"
    if (debugDump) {
      val debugDir = sys.env.getOrElse("FD_DEBUG_DIR", "/tmp/fd_debug")
      println(s"[DEBUG] FD_DEBUG_DUMP=true — saving mapper output to $debugDir/map_output/")
      println(s"[DEBUG] Reducer will NOT run. Re-run without FD_DEBUG_DUMP to do a full run.")
      mapOutput.saveAsTextFile(s"$debugDir/map_output")

      // Print a summary: line count, unique serial keys, sample lines
      val sample    = mapOutput.take(20)
      val lineCount = mapOutput.count()
      val uniqKeys  = mapOutput.map(_.takeWhile(_ != ':')).distinct().count()
      println(s"\n[DEBUG] ===== Mapper Output Summary =====")
      println(s"[DEBUG] Total output lines : $lineCount")
      println(s"[DEBUG] Unique serial keys : $uniqKeys")
      println(s"[DEBUG] Format: serial:timestamp:md5:size:bytes:cpcode[:trueSerial:max_age]")
      println(s"[DEBUG] First 20 lines:")
      sample.foreach(l => println(s"[DEBUG]   $l"))
      println(s"[DEBUG] Full output in: $debugDir/map_output/")
      return
    }

    // -----------------------------------------------------------------------
    // Stage 2: Shuffle + Sort
    // Composite key (serialKey, timestamp) — same serial → same reducer,
    // sorted by timestamp within each reducer partition.
    // -----------------------------------------------------------------------
    val keyed = mapOutput.map { line =>
      val firstColon  = line.indexOf(':')
      val secondColon = line.indexOf(':', firstColon + 1)
      val key = line.substring(0, firstColon)
      val ts  = line.substring(firstColon + 1, secondColon).toDouble
      ((key, ts), line)
    }

    implicit def pairOrdering: Ordering[(String, Double)] =
      new Ordering[(String, Double)] {
        def compare(a: (String, Double), b: (String, Double)): Int = {
          val c = a._1.compareTo(b._1)
          if (c != 0) c else java.lang.Double.compare(a._2, b._2)
        }
      }

    val sorted = keyed.repartitionAndSortWithinPartitions(
      new SerialKeyPartitioner(numReducers)
    )

    // -----------------------------------------------------------------------
    // DEBUG: if FD_DEBUG_SORT=true, save the sorted shuffle output and stop.
    // Each file = one reducer partition. Lines are sorted by (serial, timestamp).
    // Verify: all lines in a file share the same serial keys, ordered by time.
    // -----------------------------------------------------------------------
    val debugSort = sys.env.getOrElse("FD_DEBUG_SORT", "false").toLowerCase == "true"
    if (debugSort) {
      val debugDir = sys.env.getOrElse("FD_DEBUG_DIR", "/tmp/fd_debug")
      println(s"[DEBUG] FD_DEBUG_SORT=true — saving sorted shuffle output to $debugDir/sort_output/")
      println(s"[DEBUG] Each part-NNNNN file = one reducer partition.")
      println(s"[DEBUG] Check: all lines in a file share the same serial keys, ordered by timestamp.")
      sorted.map { case ((key, ts), line) => line }
            .saveAsTextFile(s"$debugDir/sort_output")

      // Show a sample from each partition
      sorted.mapPartitionsWithIndex { (partId, iter) =>
        val lines = iter.take(5).map { case ((key, ts), line) =>
          s"partition=$partId  serial=$key  ts=$ts"
        }
        lines
      }.collect().foreach(l => println(s"[DEBUG]   $l"))
      println(s"[DEBUG] Full sorted output in: $debugDir/sort_output/")
      return
    }

    // -----------------------------------------------------------------------
    // Stage 3: Reduce — C++ reducer, one instance per Spark partition
    // Lines arrive sorted by (serialKey, timestamp) — exactly as Hadoop did.
    // -----------------------------------------------------------------------
    sorted.foreachPartition { iter =>
      val partitionId = TaskContext.get().partitionId()
      val native      = FDComputeNative

      native.reducerInit(partitionId, numReducers)

      iter
        .map(_._2)
        .grouped(reducerBatchSize)
        .foreach { batch =>
          native.reducerProcessBatch(batch.mkString("\n"))
        }

      native.reducerFinalize()
    }

    // -----------------------------------------------------------------------
    // Stage 4: Upload local stdtime.* output files to S3
    // -----------------------------------------------------------------------
    val localOutputDir = sys.env.getOrElse("FD_MAPREDUCE_OUTPUT_DIR", "/tmp/fd_output")
    val uploadConf     = s3aConf(accessKey, secretKey, encryptionKeys("1"))
    val s3Fs           = FileSystem.get(new java.net.URI(s"s3a://$s3Bucket"), uploadConf)

    val localDir = new java.io.File(localOutputDir)
    val outputFiles = Option(localDir.listFiles()).getOrElse(Array.empty)
      .filter(f => f.isFile && f.getName.startsWith("stdtime"))

    if (outputFiles.isEmpty) {
      println(s"WARNING: No stdtime.* files found in $localOutputDir to upload.")
    } else {
      outputFiles.foreach { f =>
        val dest = new Path(s"s3a://$s3Bucket/$s3OutputPrefix/${f.getName}")
        println(s"Uploading ${f.getName} → s3://$s3Bucket/$s3OutputPrefix/${f.getName}")
        val localIn = new java.io.FileInputStream(f)
        val s3Out   = s3Fs.create(dest, /*overwrite=*/true)
        try {
          val buf = new Array[Byte](64 * 1024)
          var n = localIn.read(buf)
          while (n > 0) { s3Out.write(buf, 0, n); n = localIn.read(buf) }
        } finally {
          localIn.close()
          s3Out.close()
        }
      }
      println(s"Uploaded ${outputFiles.length} file(s) to s3://$s3Bucket/$s3OutputPrefix/")
    }
  }
}
