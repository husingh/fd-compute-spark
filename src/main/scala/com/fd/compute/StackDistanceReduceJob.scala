package com.fd.compute

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Partitioner
import org.apache.spark.TaskContext

/**
 * Spark job for the stack_distance reduce phase.
 *
 * Reads the colon-separated mapper output produced by StackDistanceMapJob,
 * shuffles lines so that all lines sharing the same key (field 0) land on
 * the same executor, sorts them by timestamp (field 1) within each key group,
 * and feeds them to the C++ reducer via JNI.
 *
 * The C++ reducer writes one stdtime.<extension> file per Spark partition to
 * FD_MAPREDUCE_OUTPUT_DIR (set via spark.executorEnv.FD_MAPREDUCE_OUTPUT_DIR).
 *
 * Shuffle strategy: repartitionAndSortWithinPartitions with a composite key
 * (serialKey, timestamp).  This is a single shuffle pass — more memory
 * efficient than groupByKey which would load all values per key into RAM.
 * Lines are routed by hashing serialKey, then sorted by (serialKey, timestamp)
 * within each partition, giving the C++ reducer per-key timestamp-ordered input.
 *
 * Usage:
 *   spark-submit --class com.fd.compute.StackDistanceReduceJob \
 *     fd-compute-spark_2.13-0.1.jar <mapOutputPath> <numReducers>
 */
object StackDistanceReduceJob {

  /** Lines sent to C++ per JNI call. */
  val BATCH_SIZE = 10000

  /** Partitions by the first element of a (String, Double) key. */
  class SerialKeyPartitioner(val numPartitions: Int) extends Partitioner {
    require(numPartitions > 0)
    def getPartition(key: Any): Int = key match {
      case (k: String, _: Double) => (k.hashCode & Int.MaxValue) % numPartitions
      case _ => 0
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        "Usage: StackDistanceReduceJob <mapOutputPath> <numReducers>"
      )
      System.exit(1)
    }

    val mapOutputPath = args(0)
    val numReducers   = args(1).toInt

    val conf = new SparkConf().setAppName("StackDistanceReduceJob")
    if (!conf.contains("spark.master")) conf.setMaster("local[1]")

    val sc = new SparkContext(conf)

    try {
      run(sc, mapOutputPath, numReducers)
      println(s"Reduce job complete. stdtime files written to FD_MAPREDUCE_OUTPUT_DIR.")
    } finally {
      sc.stop()
    }
  }

  def run(sc: SparkContext, mapOutputPath: String, numReducers: Int): Unit = {

    // -----------------------------------------------------------------------
    // 1. Read mapper output and build composite key (serialKey, timestamp).
    //    Partition by serialKey so all lines for a key go to the same task.
    //    Sort by (serialKey, timestamp) within each partition — this gives
    //    the C++ reducer per-key timestamp-ordered lines, matching Hadoop.
    // -----------------------------------------------------------------------
    val compositeRDD = sc.textFile(mapOutputPath)
      .filter(_.nonEmpty)
      .map { line =>
        val firstColon  = line.indexOf(':')
        val secondColon = line.indexOf(':', firstColon + 1)
        val key = line.substring(0, firstColon)
        val ts  = line.substring(firstColon + 1, secondColon).toDouble
        ((key, ts), line)
      }

    val partitioner = new SerialKeyPartitioner(numReducers)

    // Ordering for the composite key: sort by serialKey first, then timestamp.
    // Declared as def to avoid Scala's forward-reference restriction on implicit vals.
    implicit def pairOrdering: Ordering[(String, Double)] = new Ordering[(String, Double)] {
      def compare(a: (String, Double), b: (String, Double)): Int = {
        val c = a._1.compareTo(b._1)
        if (c != 0) c else java.lang.Double.compare(a._2, b._2)
      }
    }

    val sorted = compositeRDD.repartitionAndSortWithinPartitions(partitioner)

    // -----------------------------------------------------------------------
    // 2. Reduce phase — one C++ reducer session per Spark partition.
    //    foreachPartition is an action: Spark pulls records lazily from the
    //    shuffle, feeds them to C++ in batches, then calls reducerFinalize
    //    which writes the stdtime file.
    // -----------------------------------------------------------------------
    sorted.foreachPartition { iter =>
      val partitionId = TaskContext.get().partitionId()
      val native      = FDComputeNative

      // Init: reads FD_MAPREDUCE_* env vars, sets stdtime_extension from partitionId
      native.reducerInit(partitionId, numReducers, "")

      // Process in batches — value is the original mapper output line
      iter
        .map(_._2)                 // drop composite key, keep the line
        .grouped(BATCH_SIZE)
        .foreach { batch =>
          native.reducerProcessBatch(batch.mkString("\n"))
        }

      // Finalize: writes stdtime.<extension> to FD_MAPREDUCE_OUTPUT_DIR
      native.reducerFinalize()
    }
  }
}
