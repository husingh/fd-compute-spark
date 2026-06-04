# FD Compute Spark — KPP Cloud Deployment Reference

> **Who this is for:** Anyone running a Spark job (fd-compute-spark, fd-tools-spark, or similar) on the KPP Kubernetes cluster against Linode S3 buckets with SSE-C encryption.  
> The filter/customer-pattern sections are omitted — those are job-specific. Everything here applies to the infrastructure layer.

---

## Table of Contents

1. [Infrastructure Overview](#1-infrastructure-overview)
2. [Critical Gotcha: env: Array Does NOT Reach JVM](#2-critical-gotcha-env-array-does-not-reach-jvm)
3. [Critical Gotcha: Non-spark.* SparkConf Keys in Cluster Mode](#3-critical-gotcha-non-spark-sparkconf-keys-in-cluster-mode)
4. [SIGSEGV Root Cause and Fix](#4-sigsegv-root-cause-and-fix)
5. [S3 Credentials Pattern](#5-s3-credentials-pattern)
6. [SSE-C Encryption Key Pattern](#6-sse-c-encryption-key-pattern)
7. [S3A Hadoop Configuration](#7-s3a-hadoop-configuration)
8. [Per-Run Isolation (FD_JOB_RUN_ID)](#8-per-run-isolation-fd_job_run_id)
9. [JNI Native Library](#9-jni-native-library)
10. [Full SparkApplication YAML Structure](#10-full-sparkapplication-yaml-structure)
11. [Deploy Workflow (run.sh Pattern)](#11-deploy-workflow-runsh-pattern)
12. [Watching a Running Job](#12-watching-a-running-job)
13. [Quick Troubleshooting Checklist](#13-quick-troubleshooting-checklist)

---

## 1. Infrastructure Overview

| Item | Value |
|---|---|
| Kubernetes | Linode LKE, spark-operator v1beta2, namespace `spark-operator` |
| Spark image | `apache/spark:3.5.8-scala2.12-java11-ubuntu` |
| Spark version | 3.5.8 (sparkConf declares `3.5.0` — that is fine) |
| S3 endpoint (all buckets) | `us-ord-10.linodeobjects.com` |
| S3 path style | `true` (required for Linode) |
| KUBECONFIG | `/Users/husingh/Desktop/fd-compute-spark-June/kubeconfig.yaml` |
| Service account | `spark` (must exist in namespace) |
| Pod security | `runAsNonRoot: true`, `runAsUser: 185`, `capabilities.drop: [ALL]` |

---

## 2. Critical Gotcha: env: Array Does NOT Reach JVM

**This is the #1 source of silent failures.**

The `env:` array under `driver:` / `executor:` in a SparkApplication spec sets **pod-level environment variables**, but these do **not** propagate into the JVM's `sys.env` / `System.getenv()`.

### What does NOT work
```yaml
executor:
  env:
    - name: FD_MAPREDUCE_NODELEN
      value: "2000000"          # ← executor JVM never sees this
```

### What DOES work
```yaml
sparkConf:
  "spark.executorEnv.FD_MAPREDUCE_NODELEN":  "2000000"   # ✅ reaches JVM sys.env
  "spark.kubernetes.driverEnv.FD_JOB_RUN_ID": "run-001"  # ✅ reaches driver JVM
  "spark.executorEnv.FD_JOB_RUN_ID":          "run-001"  # ✅ reaches executor JVM
```

### Rule of thumb
| Variable target | Use |
|---|---|
| Any env var the JVM code reads via `sys.env` or `System.getenv` | `spark.executorEnv.*` / `spark.kubernetes.driverEnv.*` in sparkConf |
| Secrets injected as env vars | `spark.kubernetes.executor.secretKeyRef.*` / `spark.kubernetes.driver.secretKeyRef.*` in sparkConf |
| Pod annotations / non-JVM config | `env:` array is fine |

---

## 3. Critical Gotcha: Non-spark.* SparkConf Keys in Cluster Mode

In **cluster mode**, keys in sparkConf that do not start with `spark.` are silently dropped and never appear in `SparkConf` inside the JVM.

### What does NOT work
```yaml
sparkConf:
  "fd.computation.enableFiltering": "true"   # ← dropped in cluster mode
```

### What DOES work
Inject as `-D` system properties via `spark.driver.extraJavaOptions`:
```yaml
sparkConf:
  "spark.driver.extraJavaOptions": "... -Dfd.computation.enableFiltering=true"
```

In Scala code, read with `sys.props` first, then `SparkConf` as fallback:
```scala
val enabled = sys.props.getOrElse("fd.computation.enableFiltering",
                conf.get("fd.computation.enableFiltering", "false"))
```

---

## 4. SIGSEGV Root Cause and Fix

**Symptom:** Executor pod crashes with `SIGSEGV` inside native JNI code (e.g., `libFDCompute.so`).

**Root cause:** `FD_MAPREDUCE_NODELEN` was set in the `env:` array (→ never reached JVM). The native code fell back to its hardcoded default of **105,000,000** nodes. Allocating data structures for 105M nodes overflows the stack/heap in `print_stats`, causing SIGSEGV.

**Fix:**
```yaml
sparkConf:
  "spark.executorEnv.FD_MAPREDUCE_NODELEN": "2000000"   # was: 105,000,000 default
```

**Verify the fix** — look for this in executor logs:
```
[envOverrides] FD_MAPREDUCE_NODELEN=2000000
```
If you see `105000000` anywhere in the logs, the env var is not reaching the JVM.

---

## 5. S3 Credentials Pattern

Two separate K8s Secrets — one per bucket — injected as env vars via `secretKeyRef`:

### Secrets (one per bucket)
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: fds-compute-input-creds
  namespace: spark-operator
stringData:
  access-key: "<INPUT_S3_ACCESS_KEY>"
  secret-key: "<INPUT_S3_SECRET_KEY>"
---
apiVersion: v1
kind: Secret
metadata:
  name: fds-compute-output-creds
  namespace: spark-operator
stringData:
  access-key: "<OUTPUT_S3_ACCESS_KEY>"
  secret-key: "<OUTPUT_S3_SECRET_KEY>"
```

### sparkConf wiring
```yaml
sparkConf:
  # Driver
  "spark.kubernetes.driver.secretKeyRef.S3_ACCESS_KEY":        "fds-compute-input-creds:access-key"
  "spark.kubernetes.driver.secretKeyRef.S3_SECRET_KEY":        "fds-compute-input-creds:secret-key"
  "spark.kubernetes.driver.secretKeyRef.S3_OUTPUT_ACCESS_KEY": "fds-compute-output-creds:access-key"
  "spark.kubernetes.driver.secretKeyRef.S3_OUTPUT_SECRET_KEY": "fds-compute-output-creds:secret-key"
  # Executor (same pattern)
  "spark.kubernetes.executor.secretKeyRef.S3_ACCESS_KEY":        "fds-compute-input-creds:access-key"
  "spark.kubernetes.executor.secretKeyRef.S3_SECRET_KEY":        "fds-compute-input-creds:secret-key"
  "spark.kubernetes.executor.secretKeyRef.S3_OUTPUT_ACCESS_KEY": "fds-compute-output-creds:access-key"
  "spark.kubernetes.executor.secretKeyRef.S3_OUTPUT_SECRET_KEY": "fds-compute-output-creds:secret-key"
```

### Scala code reads them as
```scala
val accessKey    = sys.env("S3_ACCESS_KEY")
val secretKey    = sys.env("S3_SECRET_KEY")
val outputAK     = sys.env.getOrElse("S3_OUTPUT_ACCESS_KEY", accessKey)
val outputSK     = sys.env.getOrElse("S3_OUTPUT_SECRET_KEY", secretKey)
```

---

## 6. SSE-C Encryption Key Pattern

Two separate K8s Secrets holding JSON files — one for input (may have multiple key versions), one for output.

### Secrets
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: fds-compute-encryption-keys          # input keys
  namespace: spark-operator
stringData:
  encryption-keys.json: '{"1":"<KEY1_BASE64>","2":"<KEY2_BASE64>"}'
---
apiVersion: v1
kind: Secret
metadata:
  name: fds-compute-encryption-keys-output   # output keys
  namespace: spark-operator
stringData:
  encryption-keys.json: '{"1":"<OUTPUT_KEY_BASE64>"}'
```

### Volume mounts (driver and executor both need both)
```yaml
driver:
  secrets:
    - name: fds-compute-encryption-keys
      path: /mnt/encryption-keys/input
      secretType: Generic
    - name: fds-compute-encryption-keys-output
      path: /mnt/encryption-keys/output
      secretType: Generic
```

### Passing paths to the job (positional args)
```yaml
arguments:
  - ...
  - "/mnt/encryption-keys/input/encryption-keys.json"    # input SSE-C key map
  - ...
  - "/mnt/encryption-keys/output/encryption-keys.json"   # output SSE-C key map
```

### Scala usage pattern
```scala
// Input: each file path contains its key index (e.g. ".key2")
// Look up from input JSON, select key matching the index in the path
val inputKeys  = loadEncryptionKeys(inputKeysFile)   // Map[String, String]
val sseKey     = inputKeys(keyIndexOf(s3Path))        // per-file

// Output: use the highest-indexed key from the output JSON for all uploads
val outputKeys = loadEncryptionKeys(outputKeysFile)
val outKeyIdx  = outputKeys.keys.map(_.toInt).max.toString
val uploadKey  = outputKeys(outKeyIdx)
```

> **Note:** This mirrors exactly how `FdPreProcessingSparkDF` handles keys in the preprocessing job — see args 8 and 9 in that job's SparkApplication YAML.

---

## 7. S3A Hadoop Configuration

Required for Linode S3 compatibility:

```yaml
hadoopConf:
  "fs.s3a.impl":                   "org.apache.hadoop.fs.s3a.S3AFileSystem"
  "fs.s3a.path.style.access":      "true"        # mandatory for Linode
  "fs.s3a.signing-algorithm":      "S3SignerType" # mandatory for Linode
  "fs.s3a.checksum.validation":    "false"
  "fs.s3a.connection.ssl.enabled": "true"
  "fs.s3a.endpoint":               "us-ord-10.linodeobjects.com"
```

Also set at pod level (avoids AWS SDK checksum errors):
```yaml
env:
  - name: AWS_REQUEST_CHECKSUM_CALCULATION
    value: "when_required"
```

S3A `Configuration` object built per-connection in Scala:
```scala
val conf = new Configuration()
conf.set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.endpoint",          sys.env.getOrElse("S3_ENDPOINT", "us-ord-10.linodeobjects.com"))
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.access.key",        accessKey)
conf.set("fs.s3a.secret.key",        secretKey)
// SSE-C
conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-C")
conf.set("fs.s3a.server-side-encryption.key",       sseKeyBase64)
```

---

## 8. Per-Run Isolation (FD_JOB_RUN_ID)

Each executor writes intermediate files to `/tmp/fd_compute/<FD_JOB_RUN_ID>/`. Without a unique run ID, concurrent or restarted jobs overwrite each other's working directories.

**Set in sparkConf** (must be identical on driver and executor):
```yaml
sparkConf:
  "spark.kubernetes.driverEnv.FD_JOB_RUN_ID": "run-20260603-003"
  "spark.executorEnv.FD_JOB_RUN_ID":           "run-20260603-003"
```

Also set in `env:` array (for non-JVM pod-level visibility — harmless duplication):
```yaml
env:
  - name: FD_JOB_RUN_ID
    value: "run-20260603-003"
```

**Bump this value for every new test run.** Convention: `run-YYYYMMDD-NNN`.

---

## 9. JNI Native Library

`libFDCompute.so` is compiled for **Ubuntu 22.04 (GLIBC 2.35)**, which matches the `apache/spark:3.5.8-scala2.12-java11-ubuntu` image.

### Distribution
```yaml
deps:
  files:
    - "https://fds-jar.in-maa-1.linodeobjects.com/fd_compute_jar/libFDCompute.so"
```
The file is downloaded to the working directory of each executor pod.

### Loading
```yaml
sparkConf:
  "spark.driver.extraJavaOptions":   "-Djava.library.path=. ..."
  "spark.executor.extraJavaOptions": "-Djava.library.path=. ..."
```

### JVM open-module flags (required for Java 11+)
```
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
```

---

## 10. Full SparkApplication YAML Structure

The deploy file contains **five YAML documents** separated by `---`:

```
Secret: fds-compute-input-creds          (input S3 credentials)
Secret: fds-compute-output-creds         (output S3 credentials)
Secret: fds-compute-encryption-keys      (input SSE-C key JSON)
Secret: fds-compute-encryption-keys-output (output SSE-C key JSON)
SparkApplication: fds-compute-pipeline
```

All five must be `kubectl apply`-ed together:
```bash
kubectl apply -f deploy/fds-compute-sparkapplication.yaml --kubeconfig kubeconfig.yaml
```

### Key sparkConf settings summary
```yaml
sparkConf:
  # Serializer / stability
  "spark.serializer":                 "org.apache.spark.serializer.KryoSerializer"
  "spark.network.timeout":            "800s"
  "spark.executor.heartbeatInterval": "60s"
  "spark.kryoserializer.buffer.max":  "512m"

  # JVM flags
  "spark.driver.extraJavaOptions":   "-Duser.home=/tmp -Djava.library.path=. --add-opens=..."
  "spark.executor.extraJavaOptions": "-Duser.home=/tmp -Djava.library.path=. --add-opens=..."

  # Credentials (via secretKeyRef — see §5)
  # FD_MAPREDUCE_* (via executorEnv — see §2 and §4)
  # SSE-C keys (via volume mounts — see §6)

  # Keep executor pods after completion for log retrieval
  "spark.kubernetes.executor.deleteOnTermination": "false"
```

---

## 11. Deploy Workflow (run.sh Pattern)

Credentials are never committed. The pattern:

```
deploy/run.sh          ← committed, placeholder values only
deploy/run.local.sh    ← gitignored, real credentials filled in
```

Setup once:
```bash
cp deploy/run.sh deploy/run.local.sh
# Edit run.local.sh — fill in INPUT_AK, INPUT_SK, OUTPUT_AK, OUTPUT_SK,
# INPUT_SSE_C_KEYS_JSON, OUTPUT_SSE_C_KEYS_JSON
chmod +x deploy/run.local.sh
```

Run:
```bash
./deploy/run.local.sh
```

The script does:
1. `sed` substitution of all placeholders → `/tmp/fds-compute-filled.yaml`
2. `kubectl delete sparkapplication` (clears previous run)
3. `kubectl apply -f /tmp/fds-compute-filled.yaml`

---

## 12. Watching a Running Job

```bash
# Watch SparkApplication state (RUNNING → COMPLETED / FAILED)
kubectl --kubeconfig kubeconfig.yaml \
  get sparkapplication fds-compute-pipeline -n spark-operator -w

# Stream driver logs (permanent — survives executor pod deletion)
kubectl --kubeconfig kubeconfig.yaml \
  logs -n spark-operator -l spark-role=driver -f

# List executor pods
kubectl --kubeconfig kubeconfig.yaml \
  get pods -n spark-operator -l spark-role=executor

# Logs from a specific executor (replace pod name)
kubectl --kubeconfig kubeconfig.yaml \
  logs -n spark-operator fds-compute-pipeline-exec-1 --tail=200
```

Driver logs contain all `[P<N>]`-prefixed executor log lines (they are returned to the driver via `collect()`).

---

## 13. Quick Troubleshooting Checklist

| Symptom | Likely cause | Fix |
|---|---|---|
| `S3_ACCESS_KEY env var not set` | secretKeyRef not wired in sparkConf | Add `spark.kubernetes.executor.secretKeyRef.S3_ACCESS_KEY` |
| SIGSEGV in executor | `FD_MAPREDUCE_NODELEN` not reaching JVM (defaulting to 105M) | Use `spark.executorEnv.FD_MAPREDUCE_NODELEN` |
| Empty S3 output | `FD_JOB_RUN_ID` mismatch or not set | Set consistently via `spark.kubernetes.driverEnv.*` + `spark.executorEnv.*` |
| `No encryption key for index` | Wrong path to keys JSON, or key index in filename doesn't match JSON | Verify mount path, check `keyIndexOf()` against actual filenames |
| `403 Forbidden` on S3 read | Wrong SSE-C key for that file | Check key index embedded in filename vs input JSON |
| `403 Forbidden` on S3 write | Wrong output SSE-C key | Verify `fds-compute-encryption-keys-output` Secret value |
| Config not read (`enableFiltering` etc.) | Non-`spark.*` key dropped in cluster mode | Inject via `-D` in `spark.driver.extraJavaOptions` |
| Executor pods disappear before you can read logs | `deleteOnTermination` default | Set `spark.kubernetes.executor.deleteOnTermination: "false"` |
| `UnsatisfiedLinkError` for native lib | Wrong GLIBC version or path | Verify image is Ubuntu-based; check `-Djava.library.path=.` |
