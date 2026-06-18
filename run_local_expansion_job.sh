#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_local_expansion_job.sh — run LocalStackDistancePipelineJob against
# local "expansion" (VCD/customer-map split) test data
#
# Reads already-decrypted .gz files from a local directory (fd_expansion_input)
# and writes stdtime.* output to a local directory. No S3/wasbs, no HDFS, no
# Kubernetes needed.
#
# Usage:
#   ./run_local_expansion_job.sh [inputDir] [outputDir]
#
# Defaults:
#   inputDir  = ./fd_expansion_input
#   outputDir = ./fd_expansion_local_output
#
# Override FD_MAPREDUCE_* env vars to match the run you want to reproduce.
# The defaults below mirror the "exp_3_ComputeFD" hadoop-streaming job:
#   FD_MAPREDUCE_VCD_MAP_NETWORK_FILE and FD_MAPREDUCE_ARL_VCD_TRANSLATION_FILE
#   pointed at hdfs://CustomerMaps/... in that job — here they're pointed at
#   the local copies in fd_expansion_configs/, since the C++ side opens them
#   with a plain fopen() (no HDFS/S3 client involved).
#
#   fd.computation.path.filter / mapreduce.input.pathFilter.class=ExpansionFilter
#   from that job filter which files Hadoop lists as input. LocalStackDistance-
#   PipelineJob has no such filtering — it processes every .gz file under
#   inputDir — so fd_expansion_input is expected to already be the
#   pre-filtered subset (this matches how fd_input_download is used by
#   run_local_job.sh).
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

INPUT_DIR="${1:-$PROJECT_DIR/fd_expansion_input}"
OUTPUT_DIR="${2:-$PROJECT_DIR/fd_expansion_local_output}"

# ---------------------------------------------------------------------------
# Parallelism — hadoop job used mapreduce.job.reduces=2320; lower NUM_REDUCERS
# for faster local iteration on the small test set.
# ---------------------------------------------------------------------------
NUM_MAPPERS="${NUM_MAPPERS:-4}"
NUM_REDUCERS="${NUM_REDUCERS:-2320}"

# ---------------------------------------------------------------------------
# FD compute config — mirrors the exp_3_ComputeFD hadoop-streaming job's
# mapreduce.map.env / mapreduce.reduce.env
# ---------------------------------------------------------------------------
export FD_JOB_RUN_ID="${FD_JOB_RUN_ID:-local-$(date +%Y%m%d-%H%M%S)}"
export FD_MAPREDUCE_NETWORK="${FD_MAPREDUCE_NETWORK:-freeflow}"
export FD_MAPREDUCE_RAM_SIZE="${FD_MAPREDUCE_RAM_SIZE:-64}"
export FD_MAPREDUCE_THINNING_PCT_JUMP="${FD_MAPREDUCE_THINNING_PCT_JUMP:-5}"
export FD_MAPREDUCE_START_TIME="${FD_MAPREDUCE_START_TIME:-1779840000}"
export FD_MAPREDUCE_END_TIME="${FD_MAPREDUCE_END_TIME:-1780012800}"
export FD_MAPREDUCE_PREWARM_PERIOD="${FD_MAPREDUCE_PREWARM_PERIOD:-0}"
export FD_MAPREDUCE_PREWARM_CONFIG_RELATIVE="${FD_MAPREDUCE_PREWARM_CONFIG_RELATIVE:-1}"
export FD_MAPREDUCE_SAMPLE_ON_ATTEMPT="${FD_MAPREDUCE_SAMPLE_ON_ATTEMPT:-0}"
export FD_MAPREDUCE_SAMPLE_BUCKETS="${FD_MAPREDUCE_SAMPLE_BUCKETS:-100}"
export FD_MAPREDUCE_WRITE_SERIAL_INFO_FILE="${FD_MAPREDUCE_WRITE_SERIAL_INFO_FILE:-1}"
export FD_MAPREDUCE_COMPUTE_BOTH_SERVED_AND_MISS_FDS="${FD_MAPREDUCE_COMPUTE_BOTH_SERVED_AND_MISS_FDS:-1}"
export FD_MAPREDUCE_IGNORE_PREFETCH_LINES="${FD_MAPREDUCE_IGNORE_PREFETCH_LINES:-1}"

# Expansion-specific: split FDs by VCD/map/network, using the customer map
# and arl->vcd translation files. Originally hdfs://CustomerMaps/...; here
# they're the local copies checked into fd_expansion_configs/.
export FD_MAPREDUCE_SPLIT_BY_VCD_MAP_NETWORK="${FD_MAPREDUCE_SPLIT_BY_VCD_MAP_NETWORK:-1}"
export FD_MAPREDUCE_VCD_MAP_NETWORK_FILE="${FD_MAPREDUCE_VCD_MAP_NETWORK_FILE:-$PROJECT_DIR/fd_expansion_configs/fd_compute_customer_map}"
export FD_MAPREDUCE_ARL_VCD_TRANSLATION_FILE="${FD_MAPREDUCE_ARL_VCD_TRANSLATION_FILE:-$PROJECT_DIR/fd_expansion_configs/arlid2vcd.conf}"

export FD_MAPREDUCE_SAMPLE_BUCKETS_OVERRIDE="${FD_MAPREDUCE_SAMPLE_BUCKETS_OVERRIDE:-mch-ff-lo:f:2:200^mm2:f:4:400^ch002:f:1:100^ch003:f:1:100^ch541:f:1:100^ch542:f:1:100^ch543:f:1:100^ch544:f:1:100^b:f:4:400^ch001:f:1:100^chsc00p:f:1:100^chsc07p:f:1:100^chsc08p:f:1:100^s136:f:1:100^chsc13p:f:1:100^chsc14p:f:1:100^mch-ff-so:f:4:400^-:f:1:100^c:f:1:100^d:f:1:100^da1:f:1:100^g2:f:1:100^g3:f:1:100^gi3:f:1:100^h:f:1:100^mm1:f:1:100^ms:f:1:100^v:f:1:100^w10:f:1:100^w12:f:1:100^w141:f:1:100^w143:f:1:100^w155:f:1:100^w16:f:1:100^w25:f:1:100^w27:f:1:100^w28:f:1:100^w38:f:1:100^w39:f:1:100^w4:f:1:100^w40:f:1:100^w41:f:1:100^w42:f:1:100^w43:f:1:100^w48:f:1:100^w5:f:1:100^w65:f:1:100^w66:f:1:100^w67:f:1:100^w68:f:1:100^w69:f:1:100^w80:f:1:100^w85:f:1:100^w86:f:1:100^z:f:1:100}"
export FD_MAPREDUCE_IGNORE_PREFETCH_OVERRIDE="${FD_MAPREDUCE_IGNORE_PREFETCH_OVERRIDE:-mch-ff-lo:f:false^ch002:f:false^ch003:f:false^ch541:f:false^ch542:f:false^ch543:f:false^ch544:f:false^ch001:f:false^chsc00p:f:false^chsc07p:f:false^chsc08p:f:false^s136:f:false^chsc13p:f:false^chsc14p:f:false^mch-ff-so:f:false}"

export FD_MAPREDUCE_OUTPUT_DIR="$OUTPUT_DIR"
export FD_DEBUG_DUMP="${FD_DEBUG_DUMP:-false}"
export FD_DEBUG_REDUCER_INPUT="${FD_DEBUG_REDUCER_INPUT:-false}"
export FD_DEBUG_DIR="${FD_DEBUG_DIR:-$OUTPUT_DIR/debug}"

LIB_DIR="$PROJECT_DIR/lib"
mkdir -p "$OUTPUT_DIR"

# ---------------------------------------------------------------------------
# Locate spark-submit
# ---------------------------------------------------------------------------
if command -v spark-submit &>/dev/null; then
  SPARK_SUBMIT="spark-submit"
elif [ -f "/opt/homebrew/opt/apache-spark/bin/spark-submit" ]; then
  SPARK_SUBMIT="/opt/homebrew/opt/apache-spark/bin/spark-submit"
elif [ -n "${SPARK_HOME:-}" ] && [ -f "$SPARK_HOME/bin/spark-submit" ]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
else
  echo "ERROR: spark-submit not found. Install with: brew install apache-spark"
  exit 1
fi

# ---------------------------------------------------------------------------
# Locate the jar
# ---------------------------------------------------------------------------
JAR=$(find "$PROJECT_DIR/target/scala-2.13" -name "fd-compute-spark_2.13-*.jar" \
        2>/dev/null | sort | tail -1)
if [ -z "$JAR" ]; then
  echo "ERROR: No jar found in target/scala-2.13/. Build with: sbt '++2.13.17 package'"
  exit 1
fi

echo "========================================"
echo "  LocalStackDistancePipelineJob (expansion)"
echo "========================================"
echo "  spark-submit : $SPARK_SUBMIT"
echo "  jar          : $JAR"
echo "  input dir    : $INPUT_DIR"
echo "  output dir   : $OUTPUT_DIR"
echo "  mappers      : $NUM_MAPPERS"
echo "  reducers     : $NUM_REDUCERS"
echo "  job run id   : $FD_JOB_RUN_ID"
echo "  network      : $FD_MAPREDUCE_NETWORK"
echo "  ram size     : $FD_MAPREDUCE_RAM_SIZE"
echo "  vcd map file : $FD_MAPREDUCE_VCD_MAP_NETWORK_FILE"
echo "  arl/vcd file : $FD_MAPREDUCE_ARL_VCD_TRANSLATION_FILE"
echo "========================================"
echo ""

"$SPARK_SUBMIT" \
  --master "local[4]" \
  --class com.fd.compute.LocalStackDistancePipelineJob \
  --conf "spark.driver.bindAddress=127.0.0.1" \
  --conf "spark.driver.host=127.0.0.1" \
  --conf "spark.driver.memory=4g" \
  --conf "spark.executor.memory=4g" \
  --conf "spark.driver.extraJavaOptions=-Duser.home=/tmp -Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=-Duser.home=/tmp -Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  --conf "spark.executorEnv.FD_JOB_RUN_ID=$FD_JOB_RUN_ID" \
  --conf "spark.executorEnv.FD_MAPREDUCE_NETWORK=$FD_MAPREDUCE_NETWORK" \
  --conf "spark.executorEnv.FD_MAPREDUCE_RAM_SIZE=$FD_MAPREDUCE_RAM_SIZE" \
  --conf "spark.executorEnv.FD_MAPREDUCE_THINNING_PCT_JUMP=$FD_MAPREDUCE_THINNING_PCT_JUMP" \
  --conf "spark.executorEnv.FD_MAPREDUCE_START_TIME=$FD_MAPREDUCE_START_TIME" \
  --conf "spark.executorEnv.FD_MAPREDUCE_END_TIME=$FD_MAPREDUCE_END_TIME" \
  --conf "spark.executorEnv.FD_MAPREDUCE_SAMPLE_BUCKETS=$FD_MAPREDUCE_SAMPLE_BUCKETS" \
  --conf "spark.executorEnv.FD_MAPREDUCE_SAMPLE_ON_ATTEMPT=$FD_MAPREDUCE_SAMPLE_ON_ATTEMPT" \
  --conf "spark.executorEnv.FD_MAPREDUCE_PREWARM_PERIOD=$FD_MAPREDUCE_PREWARM_PERIOD" \
  --conf "spark.executorEnv.FD_MAPREDUCE_PREWARM_CONFIG_RELATIVE=$FD_MAPREDUCE_PREWARM_CONFIG_RELATIVE" \
  --conf "spark.executorEnv.FD_MAPREDUCE_WRITE_SERIAL_INFO_FILE=$FD_MAPREDUCE_WRITE_SERIAL_INFO_FILE" \
  --conf "spark.executorEnv.FD_MAPREDUCE_COMPUTE_BOTH_SERVED_AND_MISS_FDS=$FD_MAPREDUCE_COMPUTE_BOTH_SERVED_AND_MISS_FDS" \
  --conf "spark.executorEnv.FD_MAPREDUCE_IGNORE_PREFETCH_LINES=$FD_MAPREDUCE_IGNORE_PREFETCH_LINES" \
  --conf "spark.executorEnv.FD_MAPREDUCE_SPLIT_BY_VCD_MAP_NETWORK=$FD_MAPREDUCE_SPLIT_BY_VCD_MAP_NETWORK" \
  --conf "spark.executorEnv.FD_MAPREDUCE_VCD_MAP_NETWORK_FILE=$FD_MAPREDUCE_VCD_MAP_NETWORK_FILE" \
  --conf "spark.executorEnv.FD_MAPREDUCE_ARL_VCD_TRANSLATION_FILE=$FD_MAPREDUCE_ARL_VCD_TRANSLATION_FILE" \
  --conf "spark.executorEnv.FD_MAPREDUCE_SAMPLE_BUCKETS_OVERRIDE=$FD_MAPREDUCE_SAMPLE_BUCKETS_OVERRIDE" \
  --conf "spark.executorEnv.FD_MAPREDUCE_IGNORE_PREFETCH_OVERRIDE=$FD_MAPREDUCE_IGNORE_PREFETCH_OVERRIDE" \
  --conf "spark.executorEnv.FD_MAPREDUCE_OUTPUT_DIR=$OUTPUT_DIR" \
  --conf "spark.executorEnv.FD_DEBUG_DUMP=$FD_DEBUG_DUMP" \
  --conf "spark.executorEnv.FD_DEBUG_REDUCER_INPUT=$FD_DEBUG_REDUCER_INPUT" \
  --conf "spark.executorEnv.FD_DEBUG_DIR=$FD_DEBUG_DIR" \
  "$JAR" \
  "$INPUT_DIR" \
  "$OUTPUT_DIR" \
  "$NUM_MAPPERS" \
  "$NUM_REDUCERS"

echo ""
echo "Done. Output files in: $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR" 2>/dev/null || true
