package com.fd.compute

/**
 * JNI wrapper for libFDCompute.dylib / libFDCompute.so.
 *
 * Covers the stack_distance mapper API exposed by stack_distance_map_jni.cpp.
 * Reducer methods are stubbed and will be wired up later.
 *
 * @native methods live in a class (not a Scala object) so the JVM resolves
 * the symbol as  Java_com_fd_compute_FDComputeNative_mapperInit  (no _00024
 * dollar suffix that a Scala object companion would introduce).
 */
class FDComputeNative {

  // -------------------------------------------------------------------------
  // Mapper — stack_distance map phase
  // -------------------------------------------------------------------------

  /**
   * Initialise the C++ mapper for one Spark partition.
   * Reads all FD_MAPREDUCE_* config directly from env vars via getenv().
   *
   * @param inputFilename  Source log filename (pass "" in Spark mode).
   * @param envOverrides   Newline-separated "KEY=localPath" pairs for any
   *                       FD_MAPREDUCE_*_FILE vars whose S3 files were
   *                       pre-downloaded to local temp paths by Spark.
   *                       Pass "" if no file vars are needed.
   * @return  Opaque handle (pointer to MapperCtx) to pass to subsequent calls.
   */
  @native def mapperInit(inputFilename: String, envOverrides: String): Long

  /**
   * Process a batch of raw CDN log lines.
   *
   * @param handle  Handle returned by mapperInit.
   * @param batch   Newline-separated raw log lines (≈10 000 lines per call).
   * @return        Newline-separated mapper output lines, each formatted as
   *                "<key>\t<formatted_line>".  Empty string if all lines
   *                were filtered out.
   */
  @native def mapperProcessBatch(handle: Long, batch: String): String

  /**
   * Finalise the mapper for this partition.
   * Logs allLinesCount / ignoredLinesCount etc. to executor stderr.
   * Frees the native MapperCtx.
   *
   * @param handle  Handle returned by mapperInit.
   */
  @native def mapperFinalize(handle: Long): Unit

  // -------------------------------------------------------------------------
  // Reducer — stack_distance reduce phase
  // -------------------------------------------------------------------------

  /**
   * Initialise the C++ reducer for one Spark partition.
   *
   * @param partitionId   Spark partition index (0-based).
   * @param numReducers   Total number of reduce partitions.
   * @param envOverrides  Newline-separated "KEY=localPath" pairs for any
   *                      FD_MAPREDUCE_*_FILE vars pre-downloaded from S3.
   *                      Pass "" if no file vars are needed.
   */
  @native def reducerInit(partitionId: Int, numReducers: Int, envOverrides: String): Unit

  /**
   * Process a batch of mapper output lines.
   *
   * @param batch  Newline-separated lines in the format:
   *               key:timestamp:md5:size:bytes:cpcode[:trueSerial:max_age]
   *               Lines within each key group must be sorted by timestamp.
   */
  @native def reducerProcessBatch(batch: String): Unit

  /**
   * Finalise the reducer for this partition.
   * Writes stdtime.<extension> to FD_MAPREDUCE_OUTPUT_DIR.
   */
  @native def reducerFinalize(): Unit
}

object FDComputeNative extends FDComputeNative {
  /**
   * Load the native library.
   *
   * Local dev (sbt run):
   *   - Resolved from lib/ via  -Djava.library.path=<project>/lib  in build.sbt
   *   - Copy:  cp fd_compute/libFDCompute.dylib fd-compute-spark/lib/
   *
   * spark-submit:
   *   - Include the .so in --files and set -Djava.library.path=.
   *   - e.g.  --files lib/libFDCompute.so#libFDCompute.so
   *           --conf spark.executor.extraJavaOptions=-Djava.library.path=.
   */
  System.loadLibrary("FDCompute")
}
