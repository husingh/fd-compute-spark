# fd-compute-spark

Spark implementation of the **stack distance** footprint descriptor computation.
Reads SSE-C encrypted, gzip-compressed CDN log files from Linode Object Storage,
runs the C++ mapper and reducer via JNI, and uploads the resulting `stdtime.*`
footprint descriptor files back to S3.

This replaces the original Hadoop MapReduce job while keeping all computation
logic in the existing C++ codebase — no algorithmic changes.

---

## How It Works

```
Linode Object Storage
  raw-logs/.../O/*.gz.keyN          (SSE-C encrypted, gzip compressed)
        │
        │  S3A + SSE-C decryption + gzip decompression
        ▼
  Raw CDN log lines
  (one line per object request: IP, timestamp, URL, size, bytes, cpcode, ...)
        │
        │  C++ mapper via JNI  (stack_distance.map.cpp)
        │  FD_MAPREDUCE_MAPRULES selects traffic classification
        ▼
  Mapper output lines
  serial:timestamp:md5:size:bytes:cpcode:trueSerial:max_age
        │
        │  Spark shuffle  (repartitionAndSortWithinPartitions)
        │  All lines with the same serial → same reducer partition
        │  Within each partition: sorted by (serial, timestamp)
        ▼
  Sorted, routed lines
        │
        │  C++ reducer via JNI  (stack_distance.reduce.cpp)
        │  Computes stack distance histogram per serial key
        ▼
  stdtime.<partition>  (local, then uploaded to S3)

Linode Object Storage
  fd_output/stdtime.0 … stdtime.N   (footprint descriptor files)
```

These `stdtime.*` files are then consumed by **fd-tools-spark** (mix_maprule)
to produce blended footprint descriptors for cache sizing.

---

## Repository Layout

```
fd-compute-spark/
├── compile_native.sh          ← Step 1: compile C++ on your target platform
├── run_pipeline_job.sh        ← Step 3: run the Spark job
├── build.sbt                  ← Scala 2.13 / Spark 4.1 build file
├── conf/
│   └── encryption_keys.json   ← SSE-C AES keys (one entry per key index)
├── cpp_src/                   ← C++ source files (all computation logic)
│   ├── stack_distance.map.cpp
│   ├── stack_distance.reduce.cpp
│   ├── stack_distance_map_jni.cpp
│   ├── stack_distance_reduce_jni.cpp
│   ├── splay.cpp / splay.h
│   ├── stack_distance.config.cpp / .h
│   └── replication.{map,reduce,merge}.cpp
├── lib/                       ← Native library (output of compile_native.sh)
│   ├── libFDCompute.so        ← Linux — used on Spark cluster
│   └── libFDCompute.dylib     ← macOS — used for local testing
│                              (sbt package bundles these into the jar
│                               under  native/  for runtime extraction)
└── src/main/scala/com/fd/compute/
    ├── StackDistancePipelineJob.scala   ← Production job (map + reduce in one)
    ├── FDComputeNative.scala            ← JNI interface + jar-resource loader
    ├── StackDistanceMapJob.scala        ← Debug: map phase only
    └── StackDistanceReduceJob.scala     ← Debug: reduce phase only
```

> **Build/scaffold files (also in the repo):**
> `project/plugins.sbt` (sbt-assembly), `project/build.properties` (sbt version).

> `StackDistanceMapJob` and `StackDistanceReduceJob` are **not used in production**.
> The pipeline job does everything. They exist for debugging each phase in isolation.

---

## Deliverables

| File | Portable? | Notes |
|---|---|---|
| `target/scala-2.12/fd-compute-spark_2.12-0.1.jar` | ⚠️ Bundles native lib for **the platform it was packaged on** | Pure Scala bytecode + the `.so`/`.dylib` produced by `compile_native.sh`, embedded as resources under `native/` |
| `run_pipeline_job.sh` | ✅ | Shell script, no changes needed |
| `conf/encryption_keys.json` | ✅ | Keep secret — contains AES keys |

> **The native library is bundled inside the jar.**
> At runtime, `FDComputeNative.ensureLoaded()` extracts `native/libFDCompute.{so,dylib}`
> from the jar to a temp file and calls `System.load()`. No `LD_LIBRARY_PATH` /
> `java.library.path` setup needed on the receiving cluster.
>
> ⚠️ The jar is platform-specific because the bundled `.so`/`.dylib` is.
> Re-run `./compile_native.sh && sbt package` on the target OS/arch (typically
> Linux x86_64) before distributing to the cluster. A macOS-built jar will
> fail with `dlopen` errors on Linux.

---

## Setup

### Step 1 — Compile the Native Library

Run on the **target platform** (the machine where Spark executors will run):

```bash
cd fd-compute-spark
./compile_native.sh
```

**Linux prerequisites:**
```bash
sudo apt-get install -y g++ openjdk-11-jdk libssl-dev zlib1g-dev
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

**macOS prerequisites:**
```bash
brew install openssl openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
```

Output: `lib/libFDCompute.so` (Linux) or `lib/libFDCompute.dylib` (macOS).

---

### Step 2 — Build the Scala Jar

```bash
cd fd-compute-spark
sbt package
# → target/scala-2.12/fd-compute-spark_2.12-0.1.jar
```

`sbt package` automatically copies whichever of `lib/libFDCompute.{so,dylib}`
exist into the jar under `native/`. You can verify with:

```bash
unzip -l target/scala-2.12/fd-compute-spark_2.12-0.1.jar | grep native/
#   native/libFDCompute.so      174752  ...
#   native/libFDCompute.dylib   159712  ...   (only on macOS dev boxes)
```

> Always run `compile_native.sh` first. If `lib/` is empty when `sbt package`
> runs, the resulting jar will not contain a native lib and the runtime loader
> will fall back to `LD_LIBRARY_PATH` / `java.library.path` (which most deploys
> won't have set).

---

### Step 3 — Run

```bash
cd fd-compute-spark
./run_pipeline_job.sh
```

Override any parameter inline:

```bash
S3_BUCKET=my-bucket \
S3_PREFIX=raw-logs/BLR/f/gi3/O \
S3_OUTPUT_PREFIX=fd_output/2026-03-09 \
NUM_MAPPERS=16 \
NUM_REDUCERS=1 \
FD_MAPREDUCE_MAPRULES=mm1 \
./run_pipeline_job.sh
```

---

## Configuration Reference

### S3 Credentials

| Variable | Description |
|---|---|
| `S3_ACCESS_KEY` | Linode Object Storage access key |
| `S3_SECRET_KEY` | Linode Object Storage secret key |

### S3 Input / Output

| Variable | Default | Description |
|---|---|---|
| `S3_BUCKET` | `fds-e3testing-bucket-husingh` | Bucket name |
| `S3_PREFIX` | `raw-logs/BLR/f/gi3/O` | Input prefix — scanned recursively for `*.gz.keyN` files |
| `S3_OUTPUT_PREFIX` | `fd_output` | Output prefix — `stdtime.*` files uploaded here |
| `S3_ENDPOINT` | `us-ord-10.linodeobjects.com` | S3-compatible endpoint. Change for a different Linode region or AWS. |
| `ENCRYPTION_KEYS_FILE` | `conf/encryption_keys.json` | JSON file mapping key index → base64 AES-256 SSE-C key |

### Spark Parallelism

| Variable | Default | Description |
|---|---|---|
| `NUM_MAPPERS` | `4` | Number of mapper partitions (C++ mapper tasks run in parallel) |
| `NUM_REDUCERS` | `1` | Number of reducer partitions — also the number of output `stdtime.*` files |

> ⚠️ The C++ reducer uses global state. Keep `--master local[1]` (single reducer thread) unless the C++ reducer is confirmed thread-safe for concurrent execution.

### JNI Tuning

| Variable | Default | Description |
|---|---|---|
| `FD_MAPPER_BATCH_SIZE` | `10000` | Raw log lines per JNI call to C++ mapper |
| `FD_REDUCER_BATCH_SIZE` | `10000` | Sorted lines per JNI call to C++ reducer |

### FD Compute Parameters

Read by the C++ code via `getenv()` — passed to executors via `spark.executorEnv.*`.

| Variable | Default | Description |
|---|---|---|
| `FD_MAPREDUCE_MAPRULES` | `mm1` | Traffic classification ruleset (`mm1`, `w143`, `h`, …) |
| `FD_MAPREDUCE_NETWORK` | `FF` | 2-letter network filter (`FF` = all networks) |
| `FD_MAPREDUCE_OUTPUT_DIR` | `/tmp/fd_output` | Local dir where C++ reducer writes `stdtime.*` before S3 upload |
| `FD_MAPREDUCE_SERIAL_BUCKET` | `8` | Serial bucketing factor for reducer assignment |
| `FD_MAPREDUCE_START_TIME` | `0` | Unix timestamp lower bound (`0` = no limit) |
| `FD_MAPREDUCE_END_TIME` | `2147483647` | Unix timestamp upper bound (`2147483647` = no limit) |
| `FD_MAPREDUCE_NODELEN` | `2000000` | Node length for stack distance algorithm (bytes) |

### Encryption Keys File

`conf/encryption_keys.json` maps each key index (the `.keyN` suffix on input files)
to its base64-encoded 32-byte AES-256 SSE-C key:

```json
{
  "1": "<base64-32-byte-key>",
  "2": "<base64-32-byte-key>"
}
```

---

## Output

After a successful run, `stdtime.*` files appear at:

```
s3://<S3_BUCKET>/<S3_OUTPUT_PREFIX>/stdtime.<partition>
```

Example with `NUM_REDUCERS=1` and `S3_OUTPUT_PREFIX=fd_output`:
```
s3://fds-e3testing-bucket-husingh/fd_output/stdtime.0
```

Each file is a plain-text footprint descriptor with one line per stack distance
bucket. These are consumed downstream by **fd-tools-spark** (`mix_maprule`).

---

## Testing / Debugging

The pipeline has two built-in debug modes controlled by environment variables —
no code changes or rebuilds required.

### Inspect mapper output

Runs the S3 read + C++ mapper, prints a summary, writes all output to local disk,
then **stops before the reducer**. Use this to verify the mapper is emitting the
right serial keys, timestamps, and line format.

```bash
FD_DEBUG_DUMP=true \
FD_DEBUG_DIR=/tmp/fd_debug \
S3_PREFIX=raw-logs/BLR/f/gi3/O \
FD_MAPREDUCE_MAPRULES=h \
NUM_MAPPERS=4 NUM_REDUCERS=1 \
./run_pipeline_job.sh 2>&1 | grep DEBUG
```

Example output (verified against real data):
```
[DEBUG] Total output lines : 9089689
[DEBUG] Unique serial keys : 2003
[DEBUG] Format: serial:timestamp:md5:size:bytes:cpcode:trueSerial:max_age
[DEBUG] First 20 lines:
[DEBUG]   1796:1772941620.000:148952475A61EA6C:2110676:2097152:476172:1796:-1
[DEBUG]   1371:1772941620.000:F88A52475A61EA6C:2110680:2097152:476172:5467:-1
...
[DEBUG] Full output in: /tmp/fd_debug/map_output/
```

Inspect the full output:
```bash
cat /tmp/fd_debug/map_output/part-00000 | head -50
```

### Inspect shuffle routing + sort order

Runs mapper + Spark shuffle, then **stops before the reducer**. Prints which serial
keys land on which partition and shows that timestamps are ascending within each
partition. Use this to verify that routing and sort order are correct.

```bash
FD_DEBUG_SORT=true \
FD_DEBUG_DIR=/tmp/fd_debug \
S3_PREFIX=raw-logs/BLR/f/gi3/O \
FD_MAPREDUCE_MAPRULES=h \
NUM_MAPPERS=4 NUM_REDUCERS=4 \
./run_pipeline_job.sh 2>&1 | grep DEBUG
```

Example output (verified against real data):
```
[DEBUG] partition=0  serial=1001  ts=1.772928E9
[DEBUG] partition=0  serial=1001  ts=1.77292806E9   ← same partition, time ascending ✓
[DEBUG] partition=1  serial=1     ts=1.77292812E9   ← different serial → different partition ✓
[DEBUG] partition=2  serial=1003  ts=1.77292818E9
[DEBUG] partition=3  serial=10    ts=1.772928E9
```

Inspect sorted output per partition:
```bash
# All serial keys in partition 0 — should be a consistent set
cut -d: -f1 /tmp/fd_debug/sort_output/part-00000 | sort -u

# Timestamps for one serial — should be strictly ascending
grep "^1001:" /tmp/fd_debug/sort_output/part-00000 | cut -d: -f2 | head -20
```

---

## Known Limitations / Future Work

| Item | Notes |
|---|---|
| Reducer is single-threaded | C++ reducer uses global state; run with `--master local[1]` |
| `unshard` not yet in Spark | Needed to scale per-reducer shards before mix phase; exists as a standalone binary in `fd_tools/` |
| `concat` not yet in Spark | Needed to combine all shards into one FD per site; exists as a standalone binary in `fd_tools/` |
| `t2c` / `t2s` not yet in Spark | Convert `stdtime.*` → `stdcount.*` / `stdspace.*` sizing curves; exist as standalone binaries |

Adding these tools to Spark requires JNI wrappers following the same pattern as
`FD_JNI.cpp` (`runMix`). The Scala wiring pattern is established in `FDPipelineJob`
in **fd-tools-spark**.
