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
import scala.collection.JavaConverters._

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
    // Hadoop's actual reference run uses a CUSTOM partitioner, not a hash:
    // -D mapred.partitioner.class=customPartition.SerialSpacePartitionerMapred
    // Decompiled customPartition.jar's SerialSpacePartitioner.getPartition()
    // bytecode confirms it does: Integer.parseInt(firstField) % numPartitions
    // — a direct numeric modulo on the serial, no hashing at all. (There's a
    // dead hashCode()/getPartition(int,int) helper pair in the class that is
    // never actually called from getPartition(K2,V2,int) — leftover from a
    // base implementation, not used.) The serial is always a small
    // non-negative int (bounded by FD_MAPREDUCE_URL_BUCKET), so plain Int
    // parsing and modulo is exact — no overflow/sign concerns.
    def getPartition(key: Any): Int = key match {
      case (k: String, _: Double, _: String) => k.toInt % numPartitions
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
  def s3aConf(accessKey: String, secretKey: String, sseKeyB64: String,
              withSseC: Boolean = true): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.s3a.impl",                "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.endpoint",            s3Endpoint)
    conf.set("fs.s3a.path.style.access",   "true")
    conf.set("fs.s3a.connection.ssl.enabled", "true")
    conf.set("fs.s3a.signing-algorithm",   "S3SignerType")   // Linode requires v2 signing
    conf.set("fs.s3a.checksum.validation", "false")          // Linode rejects AWS v4 checksum header
    conf.set("fs.s3a.access.key",          accessKey)
    conf.set("fs.s3a.secret.key",          secretKey)
    if (withSseC) {
      conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-C")
      conf.set("fs.s3a.server-side-encryption.key",       sseKeyB64)
      val keyBytes = Base64.getDecoder.decode(sseKeyB64)
      val md5B64   = Base64.getEncoder.encodeToString(
                       MessageDigest.getInstance("MD5").digest(keyBytes))
      conf.set("fs.s3a.server-side-encryption-key-md5", md5B64)
    }
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
        "<s3Bucket> <s3Prefix> <inputEncryptionKeysFile> <numMappers> <numReducers> " +
        "[s3OutputPrefix] [outputBucket] [outputEncryptionKeysFile]"
      )
      System.exit(1)
    }

    val s3Bucket                 = args(0)
    val s3Prefix                 = args(1).stripSuffix("/")
    val encryptionKeysFile       = args(2)
    val numMappers               = args(3).toInt
    val numReducers              = args(4).toInt
    val s3OutputPrefix           = if (args.length > 5) args(5).stripSuffix("/") else "fd_output"
    val outputBucket             = if (args.length > 6 && args(6).nonEmpty) args(6) else s3Bucket
    val outputEncryptionKeysFile = if (args.length > 7) args(7) else ""

    val conf = new SparkConf().setAppName("StackDistancePipelineJob")
    if (!conf.contains("spark.master")) conf.setMaster("local[1]")

    val sc = new SparkContext(conf)
    try {
      run(sc, s3Bucket, s3Prefix, encryptionKeysFile, numMappers, numReducers, s3OutputPrefix, outputBucket, outputEncryptionKeysFile)
      println(s"Pipeline complete. stdtime files uploaded to s3://$outputBucket/$s3OutputPrefix/")
    } finally {
      sc.stop()
    }
  }

  // -------------------------------------------------------------------------
  // Pipeline
  // -------------------------------------------------------------------------

  def run(
    sc:                        SparkContext,
    s3Bucket:                  String,
    s3Prefix:                  String,
    encryptionKeysFile:        String,
    numMappers:                Int,
    numReducers:               Int,
    s3OutputPrefix:            String = "fd_output",
    outputBucket:              String = "",   // if empty, defaults to s3Bucket
    outputEncryptionKeysFile:  String = ""    // if empty, falls back to input keys
  ): Unit = {

    val resolvedOutputBucket = if (outputBucket.nonEmpty) outputBucket else s3Bucket

    // Read credentials from env vars (set via spark.executorEnv.* in run script)
    val accessKey = sys.env.getOrElse("S3_ACCESS_KEY",
      throw new IllegalStateException("S3_ACCESS_KEY env var not set"))
    val secretKey = sys.env.getOrElse("S3_SECRET_KEY",
      throw new IllegalStateException("S3_SECRET_KEY env var not set"))
    // S3_OUTPUT_ACCESS_KEY / S3_OUTPUT_SECRET_KEY: separate output bucket creds.
    // Fall back to input creds when writing to the same bucket.
    val outputAccessKey = sys.env.getOrElse("S3_OUTPUT_ACCESS_KEY", accessKey)
    val outputSecretKey = sys.env.getOrElse("S3_OUTPUT_SECRET_KEY", secretKey)

    // Load encryption keys on the driver (local file, not S3)
    val encryptionKeys: Map[String, String] = loadEncryptionKeys(encryptionKeysFile)
    println(s"Loaded ${encryptionKeys.size} input encryption key(s) from $encryptionKeysFile")

    // Load output encryption keys — separate JSON, latest index used for upload.
    // Falls back to input keys if no output keys file is provided.
    val outputEncryptionKeys: Map[String, String] =
      if (outputEncryptionKeysFile.nonEmpty) {
        val m = loadEncryptionKeys(outputEncryptionKeysFile)
        println(s"Loaded ${m.size} output encryption key(s) from $outputEncryptionKeysFile")
        m
      } else {
        println("No output encryption keys file provided — falling back to input keys for upload")
        encryptionKeys
      }

    // List all files — listing never needs SSE-C headers
    val listConf  = s3aConf(accessKey, secretKey, encryptionKeys("1"), withSseC = false)
    val rawFiles  = listS3Files(s3Bucket, s3Prefix, listConf)
    println(s"Found ${rawFiles.size} file(s) under s3://$s3Bucket/$s3Prefix")

    // Optional date-range + file.pattern filter (replaces legacy customFilter.jar).
    // Enabled via sparkConf 'fd.computation.enableFiltering=true' plus
    // 'fd.computation.date.start', 'fd.computation.date.end', 'file.pattern'.
    val filterOpt = FdInputFileFilter.fromSparkConf(
      sc.getConf,
      basePath = s"s3a://$s3Bucket/$s3Prefix/"
    )
    val allFiles: Seq[String] = filterOpt match {
      case Some(f) =>
        println(s"Applying input filter: $f")
        val kept = rawFiles.filter(f.accept)
        println(s"After filter: ${kept.size} of ${rawFiles.size} file(s) kept " +
          s"(dates ${f.startDate}..${f.endDate}, " +
          s"${f.arlSet.size} arl id(s), ${f.comboSet.size} arl:metro combo(s))")
        kept
      case None =>
        println("Input filter disabled (fd.computation.enableFiltering!=true) — using all files")
        rawFiles
    }
    allFiles.foreach(f => println(s"  $f"))

    // Broadcast credentials and keys so executors can access S3
    val bcAccessKey             = sc.broadcast(accessKey)
    val bcSecretKey             = sc.broadcast(secretKey)
    val bcEncryptionKeys        = sc.broadcast(encryptionKeys)
    val bcOutputEncryptionKeys  = sc.broadcast(outputEncryptionKeys)
    // Broadcast output-specific values so executors can upload stdtime.* directly
    val bcOutputAccessKey = sc.broadcast(outputAccessKey)
    val bcOutputSecretKey = sc.broadcast(outputSecretKey)
    val bcOutputBucket    = sc.broadcast(resolvedOutputBucket)
    val bcS3OutputPrefix  = sc.broadcast(s3OutputPrefix)

    // -----------------------------------------------------------------------
    // Stage 1a: Parallelize file list → each task reads one S3 file
    //           (decrypt SSE-C + decompress gzip → raw log lines)
    // -----------------------------------------------------------------------
    val rawLines = sc.parallelize(allFiles, allFiles.size)
      .mapPartitions { fileIter =>
        val ak   = bcAccessKey.value
        val sk   = bcSecretKey.value
        val keys = bcEncryptionKeys.value

        // S3_INPUT_SSE_C_KEY env var is no longer needed — input keys are loaded
        // from the mounted input encryption-keys JSON (args(2)) and keyed by
        // the index embedded in each input file path.

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

        val ak   = bcAccessKey.value
        val sk   = bcSecretKey.value
        val keys = bcEncryptionKeys.value

        val libPath = s"${org.apache.spark.SparkFiles.getRootDirectory()}/libFDCompute.so"
        FDComputeNative.ensureLoaded(libPath)
        val handle = FDComputeNative.mapperInit("", "")

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
      // Cache so every action below reads the SAME materialized output,
      // instead of re-triggering mapperInit/mapperProcessBatch/mapperFinalize
      // from scratch per action (ruling out cross-call native-state drift
      // as a source of inconsistent counts between actions).
      mapOutput.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      mapOutput.saveAsTextFile(s"$debugDir/map_output")

      // Print a summary: line count, unique serial keys, unique md5s, sample lines
      val sample    = mapOutput.take(20)
      val lineCount = mapOutput.count()
      val uniqKeys  = mapOutput.map(_.takeWhile(_ != ':')).distinct().count()
      // Extract md5 = 3rd field (index 2) split by ':'
      val uniqMd5s  = mapOutput.map { line =>
        val fields = line.split(":", 4)
        if (fields.length >= 3) fields(2) else ""
      }.distinct().count()
      println(s"\n[DEBUG] ===== Mapper Output Summary =====")
      println(s"[DEBUG] Total output lines : $lineCount")
      println(s"[DEBUG] Unique serial keys : $uniqKeys")
      println(s"[DEBUG] Unique md5 hashes  : $uniqMd5s")
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
    // Third sort field (the full line) is a deterministic tiebreaker for
    // records that share the exact same (serial, timestamp) — without it,
    // relative order among tied records depends on shuffle fetch order and
    // isn't even stable across repeated Spark runs. This affects only the
    // order-sensitive header fields (total_mib, uniqueMissBytes) computed
    // from cache hit/miss simulation; count/bytes/period/max_id are simple
    // sums unaffected by order.
    val keyed = mapOutput.map { line =>
      val firstColon  = line.indexOf(':')
      val secondColon = line.indexOf(':', firstColon + 1)
      val key = line.substring(0, firstColon)
      val ts  = line.substring(firstColon + 1, secondColon).toDouble
      ((key, ts, line), line)
    }

    implicit val tripleOrdering: Ordering[(String, Double, String)] =
      Ordering.Tuple3(Ordering.String, Ordering.Double, Ordering.String)

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
      sorted.map { case ((key, ts, _), line) => line }
            .saveAsTextFile(s"$debugDir/sort_output")

      // Show a sample from each partition
      sorted.mapPartitionsWithIndex { (partId, iter) =>
        val lines = iter.take(5).map { case ((key, ts, _), line) =>
          s"partition=$partId  serial=$key  ts=$ts"
        }
        lines
      }.collect().foreach(l => println(s"[DEBUG]   $l"))
      println(s"[DEBUG] Full sorted output in: $debugDir/sort_output/")
      return
    }

    // -----------------------------------------------------------------------
    // Stage 3: Reduce — C++ reducer, one instance per Spark partition.
    // Lines arrive sorted by (serialKey, timestamp) — exactly as Hadoop did.
    // Uses mapPartitions + collect so ALL executor-side logs return to the
    // driver (executor pod logs vanish on completion; driver log is permanent).
    // -----------------------------------------------------------------------
    val partitionLogs: Array[String] = sorted.mapPartitions { iter =>
      val partitionId = TaskContext.get().partitionId()
      val msgs = scala.collection.mutable.ArrayBuffer[String]()

      def log(msg: String): Unit = msgs += s"[P$partitionId] $msg"

      try {
        val native    = FDComputeNative
        val ak          = bcOutputAccessKey.value
        val sk          = bcOutputSecretKey.value
        val encKeys     = bcEncryptionKeys.value
        val outEncKeys  = bcOutputEncryptionKeys.value
        val outBucket   = bcOutputBucket.value
        val outPrefix   = bcS3OutputPrefix.value

        log(s"hostname=${java.net.InetAddress.getLocalHost.getHostName}")
        log(s"outBucket=$outBucket  outPrefix=$outPrefix")

        val libPath = s"${org.apache.spark.SparkFiles.getRootDirectory()}/libFDCompute.so"
        log(s"Loading native lib from: $libPath  exists=${new java.io.File(libPath).exists()}")
        native.ensureLoaded(libPath)
        log("ensureLoaded OK")

        // Scope the output dir to this Spark application's run id so concurrent
        // pipelines can never collide on the same path. FD_JOB_RUN_ID is injected
        // by the YAML (driver + executor env). Falls back to Spark applicationId
        // (available on driver+executor) if not set.
        val jobRunId = sys.env.getOrElse(
          "FD_JOB_RUN_ID",
          Option(org.apache.spark.SparkEnv.get).map(_.conf.getAppId).getOrElse("local")
        )
        val localOutputDir = s"/tmp/fd_compute/$jobRunId"
        val localDir = new java.io.File(localOutputDir)
        localDir.mkdirs()
        log(s"jobRunId=$jobRunId  localOutputDir=$localOutputDir  exists=${localDir.exists()}  cwd=${new java.io.File(".").getAbsolutePath}")

        // This directory is shared by every reducer partition that runs on
        // this executor (it's keyed only by jobRunId, not by partitionId).
        // If a previous partition's end-of-task cleanup was skipped (e.g. an
        // exception thrown after upload but before cleanup), its stdtime.*
        // file is still sitting here when this partition starts, and this
        // partition's own "list files in dir" step below would pick it up
        // and re-upload it under its own name — producing duplicate content
        // across unrelated, genuinely-empty partitions. Wipe any leftovers
        // before this partition's reducerInit runs.
        val staleFiles = Option(localDir.listFiles()).getOrElse(Array.empty)
        if (staleFiles.nonEmpty) {
          log(s"WARNING: found ${staleFiles.length} stale file(s) left over from a previous partition in $localOutputDir — deleting before this run")
          staleFiles.foreach { f => val ok = f.delete(); log(s"  stale-cleanup: ${f.getName} deleted=$ok") }
        }

        log(s"reducerInit(partitionId=$partitionId, numReducers=$numReducers)")
        // Forward all FD_MAPREDUCE_* env vars to C++ via JNI envOverrides, and
        // explicitly set FD_MAPREDUCE_OUTPUT_DIR so C++ writes stdtime.* to the
        // same directory Scala will scan for uploads.
        val fdEnvOverrides = sys.env
          .filter { case (k, _) => k.startsWith("FD_MAPREDUCE_") }
          .map    { case (k, v) => s"$k=$v" }
          .toSeq
        val envOverridesStr = (fdEnvOverrides :+ s"FD_MAPREDUCE_OUTPUT_DIR=$localOutputDir").mkString("\n")
        log(s"envOverrides: ${envOverridesStr.replace("\n", " | ")}")
        native.reducerInit(partitionId, numReducers, envOverridesStr)
        log("reducerInit OK")

        var batchCount = 0
        var lineCount  = 0
        iter.map(_._2).grouped(reducerBatchSize).foreach { batch =>
          batchCount += 1
          lineCount  += batch.size
          native.reducerProcessBatch(batch.mkString("\n"))
        }
        log(s"reducerProcessBatch done: $batchCount batches, $lineCount lines total")

        native.reducerFinalize()
        log("reducerFinalize OK")

        // ── Inspect output directory ─────────────────────────────────────
        log(s"FD_MAPREDUCE_OUTPUT_DIR=$localOutputDir  FD_MAPREDUCE_HDFS_ODIR=${sys.env.getOrElse("FD_MAPREDUCE_HDFS_ODIR","<not set>")}")
        log(s"dir exists=${localDir.exists()}  isDirectory=${localDir.isDirectory}  abs=${localDir.getAbsolutePath}")

        val allFiles = Option(localDir.listFiles()).getOrElse(Array.empty)
        log(s"Total files in dir: ${allFiles.length}")
        allFiles.foreach { f => log(s"  file: ${f.getName}  size=${f.length}  isFile=${f.isFile}") }

        // Also check cwd — older C++ writes stdtime.* there if FD_MAPREDUCE_OUTPUT_DIR
        // didn't reach getenv().
        val cwdDir = new java.io.File(".")
        val cwdFiles = Option(cwdDir.listFiles()).getOrElse(Array.empty)
          .filter(f => f.isFile && f.getName.startsWith("stdtime"))
        if (cwdFiles.nonEmpty) {
          log(s"Found ${cwdFiles.length} stdtime file(s) in cwd ${cwdDir.getAbsolutePath} — will use these:")
          cwdFiles.foreach(f => log(s"  cwd/${f.getName}  size=${f.length}"))
        }

        // Also check /tmp for any stdtime files outside the expected dir
        val tmpStdtime = new java.io.File("/tmp").listFiles()
        if (tmpStdtime != null) {
          val found = tmpStdtime.filter(f => f.isFile && f.getName.startsWith("stdtime"))
          if (found.nonEmpty) {
            log(s"Found ${found.length} stdtime file(s) directly in /tmp (wrong dir!):")
            found.foreach(f => log(s"  /tmp/${f.getName}  size=${f.length}"))
          } else {
            log("No stdtime files found directly in /tmp either")
          }
        }

        val outputFilesPrimary = allFiles.filter(f => f.isFile && f.getName.startsWith("stdtime"))
        val outputFiles = if (outputFilesPrimary.nonEmpty) outputFilesPrimary else cwdFiles
        log(s"stdtime files to upload: ${outputFiles.length} (primary=${outputFilesPrimary.length}, cwdFallback=${cwdFiles.length})")

        if (outputFiles.isEmpty) {
          log(s"WARNING: No stdtime.* files found in $localOutputDir — nothing to upload")
        } else {
          // Use the highest-indexed key from the dedicated output encryption keys JSON.
          val outKeyIdx    = outEncKeys.keys.map(_.toInt).max.toString
          val uploadSseCKey = outEncKeys(outKeyIdx)
          log(s"Using output encryption key index=$outKeyIdx for upload")

          val uploadConf = s3aConf(ak, sk, uploadSseCKey)
          log(s"Creating S3 FileSystem for s3a://$outBucket ...")
          val s3Fs = FileSystem.get(new java.net.URI(s"s3a://$outBucket"), uploadConf)
          log("S3 FileSystem created OK")

          outputFiles.foreach { f =>
            val s3FileName = s"${f.getName}.key$outKeyIdx"
            val dest       = new Path(s"s3a://$outBucket/$outPrefix/$s3FileName")
            log(s"Uploading ${f.getName} (${f.length} bytes) → $dest")
            val localIn = new java.io.FileInputStream(f)
            val s3Out   = s3Fs.create(dest, /*overwrite=*/true)
            try {
              val buf = new Array[Byte](64 * 1024)
              var n = localIn.read(buf)
              var bytesWritten = 0L
              while (n > 0) { s3Out.write(buf, 0, n); bytesWritten += n; n = localIn.read(buf) }
              log(s"Uploaded ${f.getName} — $bytesWritten bytes written")
            } finally { localIn.close(); s3Out.close() }
          }
          log(s"All ${outputFiles.length} file(s) uploaded to s3://$outBucket/$outPrefix/")
        }

        // Clean up the per-run local dir to avoid leaving stdtime.* behind on
        // the executor's filesystem.
        try {
          val deleted = Option(localDir.listFiles()).getOrElse(Array.empty).count { f =>
            val ok = f.delete(); if (ok) log(s"cleanup: deleted ${f.getAbsolutePath}"); ok
          }
          if (localDir.exists() && localDir.delete()) log(s"cleanup: removed dir $localOutputDir ($deleted files)")
        } catch { case e: Exception => log(s"cleanup warning: ${e.getMessage}") }

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
    println("Pipeline complete. Executors uploaded stdtime files directly to S3.")
  }
}
