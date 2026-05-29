#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_pipeline_job.sh — end-to-end stack_distance pipeline (map + reduce)
#
# Reads SSE-C encrypted .gz log files from Linode S3, runs the C++ mapper
# and reducer via JNI, writes stdtime.* files to FD_MAPREDUCE_OUTPUT_DIR.
#
# Usage:
#   ./run_pipeline_job.sh
#   S3_PREFIX=raw-logs/BLR/f/gi3/O NUM_MAPPERS=4 ./run_pipeline_job.sh
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# S3 credentials — override via environment if needed
# ---------------------------------------------------------------------------
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-7O2KSN0FIDL9NY97JEF8}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-NCromtL8w1RvTo8Afa6hN7wHNDeZopNX8emHlpBd}"

# ---------------------------------------------------------------------------
# S3 input config
# ---------------------------------------------------------------------------
S3_BUCKET="${S3_BUCKET:-fds-e3testing-bucket-husingh}"
S3_PREFIX="${S3_PREFIX:-raw-logs/BLR/f/gi3/O}"
S3_OUTPUT_PREFIX="${S3_OUTPUT_PREFIX:-fd_output}"
ENCRYPTION_KEYS_FILE="${ENCRYPTION_KEYS_FILE:-$PROJECT_DIR/conf/encryption_keys.json}"

# ---------------------------------------------------------------------------
# Parallelism
# ---------------------------------------------------------------------------
NUM_MAPPERS="${NUM_MAPPERS:-4}"
NUM_REDUCERS="${NUM_REDUCERS:-8}"

# ---------------------------------------------------------------------------
# FD compute config — read by C++ via getenv() on each executor
# ---------------------------------------------------------------------------
export FD_MAPREDUCE_MAPRULES="${FD_MAPREDUCE_MAPRULES:-mm1}"
export FD_MAPREDUCE_NETWORK="${FD_MAPREDUCE_NETWORK:-FF}"
export FD_MAPREDUCE_OUTPUT_DIR="${FD_MAPREDUCE_OUTPUT_DIR:-/tmp/fd_output}"
export FD_MAPREDUCE_SERIAL_BUCKET="${FD_MAPREDUCE_SERIAL_BUCKET:-8}"
export FD_MAPREDUCE_START_TIME="${FD_MAPREDUCE_START_TIME:-0}"
export FD_MAPREDUCE_END_TIME="${FD_MAPREDUCE_END_TIME:-2147483647}"
export FD_MAPREDUCE_NODELEN="${FD_MAPREDUCE_NODELEN:-2000000}"

# ---------------------------------------------------------------------------
# Spark tuning — JNI batch sizes and S3 endpoint
# ---------------------------------------------------------------------------
export FD_MAPPER_BATCH_SIZE="${FD_MAPPER_BATCH_SIZE:-10000}"
export FD_REDUCER_BATCH_SIZE="${FD_REDUCER_BATCH_SIZE:-10000}"
export S3_ENDPOINT="${S3_ENDPOINT:-us-ord-10.linodeobjects.com}"

LIB_DIR="$PROJECT_DIR/lib"
mkdir -p "$FD_MAPREDUCE_OUTPUT_DIR"

# ---------------------------------------------------------------------------
# Locate spark-submit
# ---------------------------------------------------------------------------
if command -v spark-submit &>/dev/null; then
  SPARK_SUBMIT="spark-submit"
elif [ -f "/opt/homebrew/opt/apache-spark/bin/spark-submit" ]; then
  SPARK_SUBMIT="/opt/homebrew/opt/apache-spark/bin/spark-submit"
elif [ -f "/opt/homebrew/Cellar/apache-spark/$(ls /opt/homebrew/Cellar/apache-spark/ 2>/dev/null | tail -1)/bin/spark-submit" ]; then
  SPARK_SUBMIT="/opt/homebrew/Cellar/apache-spark/$(ls /opt/homebrew/Cellar/apache-spark/ 2>/dev/null | tail -1)/bin/spark-submit"
elif [ -n "${SPARK_HOME:-}" ] && [ -f "$SPARK_HOME/bin/spark-submit" ]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
else
  echo "ERROR: spark-submit not found. Install with: brew install apache-spark"
  exit 1
fi

# ---------------------------------------------------------------------------
# Locate the jar
# ---------------------------------------------------------------------------
JAR=$(find "$PROJECT_DIR/target" -name "fd-compute-spark*.jar" \
        -not -name "*javadoc*" 2>/dev/null | sort | tail -1)
if [ -z "$JAR" ]; then
  echo "ERROR: No jar found. Build with: sbt package"
  exit 1
fi

echo "========================================"
echo "  StackDistancePipelineJob"
echo "========================================"
echo "  spark-submit       : $SPARK_SUBMIT"
echo "  jar                : $JAR"
echo "  S3 bucket          : $S3_BUCKET"
echo "  S3 prefix          : $S3_PREFIX"
echo "  S3 output prefix   : $S3_OUTPUT_PREFIX"
echo "  Encryption keys    : $ENCRYPTION_KEYS_FILE"
echo "  Mappers            : $NUM_MAPPERS"
echo "  Reducers           : $NUM_REDUCERS"
echo "  FD output dir      : $FD_MAPREDUCE_OUTPUT_DIR"
echo "  FD maprules        : $FD_MAPREDUCE_MAPRULES"
echo "========================================"
echo ""

"$SPARK_SUBMIT" \
  --master "local[1]" \
  --class com.fd.compute.StackDistancePipelineJob \
  --packages "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --conf "spark.driver.bindAddress=127.0.0.1" \
  --conf "spark.driver.host=127.0.0.1" \
  --conf "spark.driver.memory=4g" \
  --conf "spark.executor.memory=4g" \
  --conf "spark.executorEnv.S3_ACCESS_KEY=$S3_ACCESS_KEY" \
  --conf "spark.executorEnv.S3_SECRET_KEY=$S3_SECRET_KEY" \
  --conf "spark.executorEnv.S3_ENDPOINT=$S3_ENDPOINT" \
  --conf "spark.executorEnv.FD_MAPPER_BATCH_SIZE=$FD_MAPPER_BATCH_SIZE" \
  --conf "spark.executorEnv.FD_REDUCER_BATCH_SIZE=$FD_REDUCER_BATCH_SIZE" \
  --conf "spark.executorEnv.FD_MAPREDUCE_MAPRULES=$FD_MAPREDUCE_MAPRULES" \
  --conf "spark.executorEnv.FD_MAPREDUCE_NETWORK=$FD_MAPREDUCE_NETWORK" \
  --conf "spark.executorEnv.FD_MAPREDUCE_OUTPUT_DIR=$FD_MAPREDUCE_OUTPUT_DIR" \
  --conf "spark.executorEnv.FD_MAPREDUCE_SERIAL_BUCKET=$FD_MAPREDUCE_SERIAL_BUCKET" \
  --conf "spark.executorEnv.FD_MAPREDUCE_START_TIME=$FD_MAPREDUCE_START_TIME" \
  --conf "spark.executorEnv.FD_MAPREDUCE_END_TIME=$FD_MAPREDUCE_END_TIME" \
  --conf "spark.executorEnv.FD_MAPREDUCE_NODELEN=$FD_MAPREDUCE_NODELEN" \
  --conf "spark.driver.extraJavaOptions=-Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=-Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  "$JAR" \
  "$S3_BUCKET" \
  "$S3_PREFIX" \
  "$ENCRYPTION_KEYS_FILE" \
  "$NUM_MAPPERS" \
  "$NUM_REDUCERS" \
  "$S3_OUTPUT_PREFIX"

echo ""
echo "Done. stdtime files uploaded to: s3://$S3_BUCKET/$S3_OUTPUT_PREFIX/"
