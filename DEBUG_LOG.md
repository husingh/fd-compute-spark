# FD Compute Spark — Output Mismatch Debug Log

Goal: make Spark's stdtime output match the Hadoop reference (`3fd/`) for the
same input set (389 local .gz files, run-3fd-* deploys).

## Status: open — found a real contradiction in the data, needs a cleaner test

## Confirmed facts (ruled out as causes)

1. **File selection is correct.** `listS3Files` lists every `.gz` recursively
   under the input prefix; `FdInputFileFilter` filters by the
   **directory-name date** (`YYYY-MM-DD/arl/metro/...`), not filename date.
   All 389 local files live in directories dated 2026-04-14..17, inside the
   configured `2026-04-13..18` window, and match `file.pattern=1000005:mch-ff-lo`
   → all 389 pass → Stage 0 submits exactly 389 tasks (confirmed in driver
   log). The earlier "166/389 files have filename job-date outside range"
   observation is a red herring — filename date isn't checked anywhere.

2. **`mapperInit("", "")` empty env-override is NOT a bug.** `readConfig()`
   (`stack_distance.config.cpp`) reads all `FD_MAPREDUCE_*` via plain
   `getenv()`, which sees the process environment regardless of the JNI
   override string. Since YAML sets these as executor env vars, the C++
   mapper sees them fine without it. The override exists only to inject
   runtime-computed values (e.g. `FD_MAPREDUCE_OUTPUT_DIR`), which the
   mapper doesn't need since it doesn't write files itself.

3. **The ×`SAMPLE_BUCKETS` (20) "unshard" scale factor is not the cause.**
   Applied identically by the same C++ binary on both Hadoop and Spark; the
   user's "divide header by 20" comparison already cancels it on both sides.

4. **Partition-hash mismatch (String.hashCode vs numeric hashCode) — TESTED
   AND DISPROVEN.** Changed `SerialKeyPartitioner` to parse the key as `Long`
   and hash the numeric value instead of `String.hashCode()`. Rebuilt,
   deployed as `run-3fd-20260616-7`. Result: non-empty partition count
   unchanged (1864 vs 1863), but common-with-reference dropped from 584/612
   to 10/612 — **made it worse**. Reverted back to the upstream string-hash
   partitioner (the one that was already giving 584/612 after the earlier
   integer-key mapper pull).

## New finding: a real contradiction that needs resolving

Ran with `FD_DEBUG_DUMP=true` (`run-3fd-20260616-8-debug`, reverted/original
partitioner) to see the mapper's raw output before any partitioning:

```
Total output lines : 4471
Unique serial keys : 1039
Unique md5 hashes   : 1419
```

But the **deployed (non-debug) run's S3 output has 1864 non-empty
`stdtime.*` files** (confirmed: all purely numeric filenames in range
0-2038, no duplicates). This is mathematically impossible under a
deterministic partitioner: with only 1039 distinct serial keys, and the
partition function depending **only** on the serial-key string (the
`(k: String, _: Double)` pattern in `SerialKeyPartitioner` ignores the
timestamp), at most 1039 of the 2039 reducer partitions can ever receive
data — yet roughly 1864 actually did.

Two candidate explanations, neither yet confirmed:
- The `FD_DEBUG_DUMP` code path calls multiple separate RDD actions
  (`.take(20)`, `.count()`, `.distinct().count()` ×2) on the *same*
  `mapOutput` RDD without caching it — each action re-triggers the full
  upstream computation (re-reads files, re-runs `mapperInit`/
  `mapperProcessBatch`/`mapperFinalize` from scratch). If there's any
  hidden non-determinism or cross-call state leak in the C++ mapper
  (similar in spirit to the already-known reducer statefulness bug), the
  "1039 unique keys" figure from this debug run may not actually represent
  the same computation that produced the real run's 1864-file output.
- Alternatively the real run's 1864-file count itself might include
  partitions whose data isn't a clean function of "distinct serial key
  count" the way I assumed — needs re-verification rather than assumed.

**Update — recomputation-nondeterminism ruled out.** Added `mapOutput.persist()`
before the debug actions and reran (`run-3fd-20260616-9-debug`). Identical
numbers came back: 4471 total lines, 1039 unique serial keys, 1419 unique
md5s. So the 1039 figure is real and reproducible, not an artifact of
re-triggering the native mapper multiple times. The contradiction stands:
**1039 distinct keys cannot occupy 1864 partitions under a deterministic
hash function that depends only on the key.**

## Leading unifying hypothesis (not yet acted on — needs your sign-off)

This may be the *same root cause* as the already-known C++ reducer
statefulness bug (where `reducerFinalize()` reports success but silently
writes zero files for some non-empty partitions — previously seen losing
70-176 partitions across runs).

`stack_distance.config.cpp` keeps reducer config (including
`stdtime_extension`, `partition_2`, `count`, etc.) in `thread_local`
globals, reset via `configReset()` which the comment says "must be called
between reducer invocations... so readConfig() starts from a clean slate
for the next partition." With `executor.cores=1` but ~680 reducer tasks
per executor JVM (2039 partitions / 3 executors), Spark reuses a small
thread pool across hundreds of sequential task invocations on the *same*
OS threads — so this `thread_local` state is shared across many partitions
over the JVM's lifetime, not fresh per task. If `configReset()` doesn't run
reliably on every path (e.g. skipped on an exception, or simply buggy),
stale state from a previous partition (especially `stdtime_extension`,
which determines the output filename) could leak into a later partition,
causing:
- a partition to write its `stdtime.*` file under the **wrong filename**
  (explaining file count > distinct-key-bucket-count — the same logical
  data appearing to "multiply" across extra filenames), and
- the previously-seen data-loss bug (a different manifestation of the same
  unreliable reset/stale-state problem).

**I have not touched `cpp_src/` or attempted any fix for this** — you
previously told me explicitly not to make Scala/C workarounds for the
reducer statefulness bug, and this hypothesis points straight at C++
internals (`configReset()` / thread_local lifecycle in
`stack_distance.config.cpp` / `stack_distance.reduce.cpp`). Flagging this
clearly rather than acting on it. If you want, the cleanest test (no
behavior change to the library) would be forcing `executor.instances` up
and `spark.task.cpus`/scheduling such that each task runs in a genuinely
fresh process — but that's a deploy-config experiment, not a code fix, and
I'd want your OK before spending a cluster run on it.

## Root cause found and fixed: shared local output directory across partitions

Direct proof obtained from `run-3fd-20260616-10`: partition 52's own driver
log shows, immediately after **its own** `reducerFinalize()` ran with `0
batches, 0 lines total`:
```
[P52] Total files in dir: 1
[P52]   file: stdtime.52  size=98  isFile=true
```
`print_stats()` returns early and writes **nothing** when count is 0 — so
this 98-byte file could not have been written by partition 52's own (empty)
reducer call. It was left behind by some other task that ran earlier on the
same executor and shared the same local path. The local working directory
(`/tmp/fd_compute/$jobRunId`, `StackDistancePipelineJob.scala` ~line 457) is
keyed only by job run ID, **not by partition** — every reducer task on a
given executor (with `cores=1` but ~680 sequential tasks per executor across
3 executors / 2039 partitions) shares this exact path. End-of-task cleanup
exists (deletes the dir's contents + the dir itself) but if it's skipped for
any reason on one task (e.g. an exception between upload and cleanup), the
next task sharing that path picks up the leftover file via its own "list
files in dir" step and re-uploads it under its own partition's name —
producing byte-identical duplicate content across unrelated, genuinely-empty
partitions.

Confirmed at scale: `run-10`'s 1815 downloaded output files reduced to only
**790 distinct content blocks** when deduped by header line — i.e. ~1025
files were exact duplicates of some other partition's real output.

**Fix applied** (`StackDistancePipelineJob.scala`, after `localDir.mkdirs()`,
before `reducerInit()`): defensively list and delete any files already
present in the shared local directory before this partition's reducer runs.
Since `executor.cores=1` guarantees tasks on one executor run strictly
sequentially (never concurrently), this closes the gap regardless of why an
earlier task's own cleanup was skipped.

Built, uploaded (`fd_compute_jar/stalefix/`), deployed as
`run-3fd-20260616-11`. Result: **0 stale-file warnings logged** — no leftover
disk file was ever found at any partition's start. **But the duplicate
content is still there anyway**: in run-11, P101 and P104 (both logged "0
batches, 0 lines total") have byte-identical header lines
(`# 120 153859200 131640 1133440 60 77496320 0 0`), and likewise P102/P103
(`# 260 478456240 256380 3438160 80 142868480 0 0`).

**This disproves the shared-disk-directory theory.** The contamination is
not a leftover file picked up at the start of a task — it's being freshly
*written* by `reducerFinalize()` during a partition's own call, for a
partition that genuinely received zero input. That means `reducerReset()`/
`configReset()` are not actually clearing all the state they claim to
between sequential `reducerInit()`/`reducerFinalize()` cycles sharing the
same OS thread inside one executor JVM — this is the C++ native library's
internal thread_local state management, not anything fixable from the Scala
side. This is the same category of bug as the previously-known reducer
data-loss issue (silently writing zero files when it should write one) —
both are symptoms of the native library's per-task reset being incomplete,
just manifesting in opposite directions (losing real data vs. duplicating
stale data). **Stopping here rather than attempting further Scala-side
mitigations** — this needs a fix inside `cpp_src/stack_distance.reduce.cpp`'s
`reducerReset()`/`configReset()`, which is exactly the boundary the user
previously said not to cross without discussion.

## Actual C++ fix applied and confirmed (run-12)

User gave explicit go-ahead to patch the native library. Read every
`thread_local` global declared across `stack_distance.config.cpp` and
`stack_distance.reduce.cpp` and cross-checked them against `reducerReset()`
and `configReset()` — both reset functions are thorough and cover
essentially everything. **The actual bug: these resets only run at the END
of `reducerFinalize()`, never at the START of `reducerInit()`.** If a Spark
task dies (exception, kill, OOM) any time after `reducerInit()`/
`processReducerBatch()` but before `reducerFinalize()` completes, the
thread_local accumulator state (`g_total_count`, `g_total_bytes`, the splay
tree, md5 tables, `g_memory_allocated`, etc.) is left dirty. Since
`executor.cores=1` means ~680 reducer tasks run sequentially on the same
thread per executor (3 executors / 2039 partitions), the *next* unrelated
partition reusing that thread inherits the dead task's stale, nonzero state
and `reducerFinalize()` incorrectly concludes it has real data — emitting
the dead task's content as its own. This matches the cluster's very
permissive failure tolerance (`task.maxFailures=20`,
`stage.maxConsecutiveAttempts=100`, `executor.maxPodFailures=40`), implying
task failures/retries are common on this cluster.

**Fix:** call `reducerReset(); configReset();` at the very *start* of
`reducerInit()` too (`cpp_src/stack_distance.reduce.cpp`), not just relying
on the previous task's finalize. Applied the equivalent defensive
`configReset()` at the start of `mapperInit()` (`stack_distance.map.cpp`)
for symmetry, though lower-impact there since all mapper tasks share
identical filter config anyway.

Cross-compiled for Linux x86-64 via a throwaway `ubuntu:22.04` Docker
container (`--platform linux/amd64`, since the host is Apple Silicon and
the cluster runs x86-64) running the existing `compile_native.sh`, then
rebuilt the JAR and deployed as `run-3fd-20260616-12`.

**Result — confirmed via direct verification:**
- 529 files downloaded, **529 unique content blocks — zero duplicates**
  (previously 1815 files / 790 unique in run-10, before the fix).
- File count exactly matches the predicted "partitions with real data minus
  partitions hit by the (separate, already-known) data-loss bug": 859 with
  real reducer input, 330 of those still lost to the data-loss bug
  (`reducerFinalize()` succeeds but writes nothing) = 529 actual output
  files. The data-loss count (330) is higher than previously measured
  (69-176 in earlier runs) — likely because the contamination bug had been
  *masking* some of this loss by filling genuinely-lost partitions with
  stale duplicate content that looked like real output. Now that resets are
  clean, the true scope of the (still separate, still unfixed) data-loss
  bug is visible for the first time.

**New, more honest picture of remaining work:** with contamination removed,
true overlap between Spark's and the reference's non-empty partition sets
is only **159/612 (26%)**, far worse than the previously-reported 584/612
— that earlier number was inflated by the contamination bug coincidentally
scattering duplicate content across many partition IDs, some of which
happened to match real reference IDs by chance. For partitions that *are*
common to both, header-line content still differs in every sampled case
(e.g. P49: ref `# 280 766524880...` vs spark `# 160 329300800...`) — so
there are now two remaining, separate problems to solve:
1. The (still-present) reducer data-loss bug (330 partitions this run).
2. A genuine partition-assignment mismatch between Spark and Hadoop (only
   26% of non-empty partitions agree on which physical bucket gets data),
   compounded by differing content even where partition IDs do agree.

## Issue 2 (data-loss bug) — investigated with real executor logs: NOT a bug

Pulled live executor pod logs (had to set
`spark.kubernetes.executor.deleteOnTermination=false` temporarily, run-13,
since executors are deleted on completion by default and only the driver's
log was being captured all along — the C++ library's own `fprintf(stdout,
...)` output, e.g. "Entering print_stats.", goes to the *executor* pod's
stdout, never to the driver log, and was never inspected directly before
this).

Found partition 48 (one of the "lost" partitions — Scala confirmed it
received 2 real reducer input lines) in the executor-3 log:
```
Done processing input. Read 2 lines, of which 0 skipped...
Actual times: Start = 1776250500, End = 1776250500, ... Compute duration = 0
Entering print_stats.
Empty stdtime. Done with print_stats.
```
Both of partition 48's real records share the *exact same timestamp*, so
`getActualComputePeriod() = last_time - getActualStartOfComputeTime() = 0`.
`print_stats()` explicitly skips writing output when
`count==0 || getActualComputePeriod()<1` — by design, since a single
instant in time can't produce a meaningful stack-distance-vs-time curve.
Checked partition 51 too — identical pattern (2 lines, same timestamp,
compute duration 0).

**Conclusion: this is not a bug.** It's the correct, intended algorithm
behavior given how sparse each partition's data is (most partitions get
only 1-8 records after partitioning ~1000-2600 distinct keys across 2039
reducers). Hadoop's own reference run would behave identically given
the same C++ logic and similarly-sparse per-reducer data — this is fully
consistent with Hadoop's reference only producing 612 non-empty files out
of "Launched reduce tasks=2048". The earlier characterization (from before
this conversation) of this as a "C++ JNI reducer statefulness bug causing
data loss" was a misdiagnosis, likely made while the duplication bug (now
fixed) was still contaminating the data and obscuring the real explanation.
**No code change made or needed for this.**

## Issue 1 (partition/content mismatch) — found a likely root cause: numReducers ≠ Hadoop's actual reduce count

The YAML's own header comment says `mapreduce.job.reduces=2048`, and the
Hadoop job counters the user pasted earlier explicitly confirm
**`Launched reduce tasks=2048`** — but the Spark job's `numReducers`
argument (passed to `SerialKeyPartitioner`, the actual modulus used for
`(k.hashCode & Int.MaxValue) % numPartitions`) has been **2039** this
entire session, not 2048.

Important distinction: `FD_MAPREDUCE_URL_BUCKET=2039` is a *different*,
unrelated parameter — confirmed by reading `stack_distance.map.cpp` lines
445-456, where `url_bucket` is used by the **mapper** to construct the
"serial" key itself (`hash = md5_bytes % url_bucket`, combined with
`matched_map_index`/`match_ip`). It has nothing to do with how many
reducer partitions exist. So `FD_MAPREDUCE_URL_BUCKET` staying at 2039 is
correct and untouched; only the **positional `numReducers` argument** was
wrong.

Even with identical hash functions and identical keys on both sides,
`hash % 2039` vs `hash % 2048` would route the same logical key to
unrelated physical reducer slots in nearly every case — this alone could
explain why true partition overlap was only 159/612 (essentially close to
chance) after removing the duplication-bug noise.

**Fix attempted (run-14):** changed only the `numReducers` positional
argument (YAML `arguments[4]`) from `"2039"` to `"2048"`.
`FD_MAPREDUCE_URL_BUCKET` left unchanged at `2039`. **Result: no
improvement — overlap dropped slightly to 141/612** (from 159/612).

**Second fix attempted (run-15):** hypothesized the real issue was hash
*function* mismatch, not modulus: Hadoop Streaming partitions using
`org.apache.hadoop.io.Text.hashCode()` (via `WritableComparator.hashBytes`),
which starts its accumulator at `1` and hashes UTF-8 bytes — not Java/Scala
`String.hashCode()`, which starts at `0` and hashes UTF-16 chars. For
strings of different lengths this produces a non-constant divergence
(`Hadoop_hash ≈ Java_hash + 31^length`, mod 32-bit overflow), which could
plausibly decorrelate partition assignment. Implemented `hadoopTextHashCode()`
in `SerialKeyPartitioner` replicating Hadoop's exact algorithm, kept
`numReducers=2048`. **Result: still no improvement — 143/612, content
differs in every sampled common partition.**

**Conclusion — these results are statistically indistinguishable from pure
chance.** With ~471 of 2048 Spark buckets occupied and 612 of (likely)
2048 Hadoop buckets occupied, two *uncorrelated* random partitionings
would be expected to overlap by `(471/2048) × 612 ≈ 141` — almost exactly
what both attempts produced (141, 143), and not meaningfully different
from the original 159. Two independent, well-reasoned hash-function
hypotheses both landed at chance level, which means **this isn't a
guessable hash-formula bug** — it likely depends on exact Hadoop job
configuration we don't have visibility into (the real streaming command's
`-D` partitioner/separator options, or whether a `rehashSerialIndex`
remapping table was active for the reference run, which would make
Hadoop's `stdtime.<N>` filename *not* equal to the raw reduce-task
partition id at all). Continuing to guess hash formulas blindly is not a
productive use of further cluster runs without that information.

## Issue 1 — ACTUALLY RESOLVED (run-16): found the real Hadoop partitioner

User supplied the FD Pipeline Runbook PDF containing the real Hadoop
streaming command, then located the actual
`customPartition.jar` (in `azure-fds-system-setup-package/fd_executables/`).

The runbook reveals Hadoop does **not** use a generic hash partitioner at
all:
```
-D mapred.partitioner.class=customPartition.SerialSpacePartitionerMapred
-D mapreduce.partition.keypartitioner.options=-k1,1
-D map.output.key.field.separator=:
```
Decompiled `customPartition/SerialSpacePartitioner.class` with `javap -c`.
The real `getPartition(K2,V2,int)` bytecode does:
```
key.toString().getBytes("UTF-8")
→ extract field 1 (colon-separated, per -k1,1)
→ new String(fieldBytes, "UTF-8")
→ Integer.parseInt(fieldString)
→ result % numPartitions          // plain Java %, no hashing
```
A **direct numeric modulo on the parsed serial integer** — not a hash at
all. (The class also contains a private `hashCode(byte[],...)` /
`getPartition(int,int)` helper pair, but bytecode confirms these are never
called from the actual `getPartition(K2,V2,int)` entry point — dead code
from a base implementation, a red herring.)

This lines up exactly with the bounded nature of the "serial" key: traced
`stack_distance.map.cpp` lines 444-456 — with no `FD_MAPREDUCE_MAPRULES`
configured (true for both Hadoop's reference run per the runbook's env
list, and our Spark YAML), `matched_map_index=0` always, so
`serial = hash(md5) % url_bucket`, always in `[0, url_bucket-1] = [0, 2038]`
— strictly less than `numReducers`. A plain modulo on a value already
smaller than the modulus is the identity, so Hadoop's `stdtime.<N>`
filename essentially equals the raw serial value.

**Fix:** replaced `SerialKeyPartitioner.getPartition` with
`k.toInt % numPartitions` — exactly replicating the decompiled bytecode.
Rebuilt, deployed as `run-3fd-20260616-16` (`numReducers=2048`, matching
Hadoop's actual `Launched reduce tasks=2048`).

**Result — confirmed via full download and comparison:**
- **612/612 partitions — 100% overlap** with the reference (0 only-spark,
  0 only-ref). Spark's `stdtime.*` file set is now identical to Hadoop's.
- **603/612 (98.5%) have byte-for-byte identical header lines.**
- The remaining 9 partitions match exactly on the first 4 header fields
  (count, bytes, period, mib) and differ only in the 5th/6th fields (the
  "active footprint after prewarming" stats — `x*countScaleFactor`/
  `y*bytesScaleFactor`) by a small amount — a separate, minor, not-yet
  investigated calculation difference, not a partitioning problem.

**This closes Issue 1's partition alignment.** The earlier two hash-formula
attempts failed because the real partitioner was never a hash function in
the first place — it required finding and decompiling the actual Hadoop
job's custom partitioner JAR rather than guessing standard Hadoop/Java
hash algorithms.

## Issue 1, fully closed (run-17): fixed the remaining 9-partition diff

The 9 remaining mismatches from run-16 all differed only in `total_mib`
and `uniqueMissBytes` (header fields 4 and 6) — both are **order-sensitive**
values computed by the cache hit/miss simulation, unlike count/bytes/
period/max_id which are simple order-independent sums. Root cause: the
Spark sort key was only `(serial, timestamp)` with no tiebreaker, so when
two records in the same partition shared an identical timestamp, their
relative processing order was whatever the shuffle happened to produce —
not deterministic, and not guaranteed to match Hadoop's behavior (which,
via `stream.num.map.output.key.fields=2` + Hadoop's default Text-based
comparator, effectively falls back to comparing more of the record when
sort keys tie).

**Fix:** added the full mapper-output line as a third sort field
(`(serial, timestamp, line)` instead of `(serial, timestamp)`), giving a
deterministic, content-based tiebreaker for same-timestamp records.
Updated `SerialKeyPartitioner`'s pattern match and the `FD_DEBUG_SORT`
debug-path destructuring to match the new 3-tuple key.

Rebuilt, deployed as `run-3fd-20260616-17`.

**Result: 612/612 partitions, 612/612 exact header-line matches, 0
mismatches.** Spark's output is now byte-for-byte identical to the Hadoop
reference across every single partition.

## Final state of the fix set (current code)
1. C++ defensive reset at start of `reducerInit()`/`mapperInit()` (fixes
   stale cross-task data duplication) — `cpp_src/stack_distance.reduce.cpp`,
   `stack_distance.map.cpp`.
2. `SerialKeyPartitioner` uses `k.toInt % numPartitions` (matches Hadoop's
   actual `customPartition.SerialSpacePartitionerMapred`, confirmed via
   decompiled bytecode) — `StackDistancePipelineJob.scala`.
3. `numReducers=2048` (matches Hadoop's actual `Launched reduce tasks=2048`)
   — YAML `arguments[4]`.
4. Sort key extended to `(serial, timestamp, line)` for deterministic
   tiebreaking on same-timestamp records — `StackDistancePipelineJob.scala`.

Current JAR: `fd_compute_jar/tiebreak/fd-compute-spark-assembly.jar`.

## Hadoop reference job counters (provided by user, run job_1779204570594_0054)

```
Map input records     = 4471
Map output records    = 4471
Reduce input groups   = 2620
Reduce input records  = 4471
Reduce output records = 40960
Launched reduce tasks = 2048
```

- **Map output records = 4471 exactly matches Spark's mapper output total**
  (confirmed independently via `FD_DEBUG_DUMP`) — strong validation that the
  mapper stage processes the identical 389-file input set identically on
  both Hadoop and Spark.
- **Reduce input groups = 2620** is Hadoop's distinct-key count at the
  reduce stage — higher than the 1039 distinct *serial* values measured on
  the Spark side. Not yet reconciled; worth checking whether Hadoop's actual
  grouping key is composite (e.g. serial+something) rather than serial alone.
- **Reduce output records = 40960** is far larger than the 1547 total data
  lines counted across the local `3fd/` reference's 612 files. This is a
  strong signal that **the local `3fd/` folder is a partial/aggregated
  snapshot, not Hadoop's full raw reducer output** — worth keeping in mind
  when judging future Spark-vs-reference comparisons; the right comparison
  target may need to be the full Hadoop output, not this local copy.

## Change log

| # | Change | File | Status |
|---|--------|------|--------|
| 1 | Parse serial key as Long, partition by numeric value instead of String.hashCode | `StackDistancePipelineJob.scala` (`SerialKeyPartitioner`, ~line 50) | **Reverted** — tested via run-7, made common-partition overlap worse (584→10/612). Not the cause. |
| 2 | Ran with `FD_DEBUG_DUMP=true` to inspect raw mapper output (no code change, config-only) | `run-3fd-20260616-8-debug`, `-9-debug` | Done — surfaced the 1039-vs-1864 contradiction, confirmed not a recompute artifact via `.persist()`. |
| 3 | Cached `mapOutput` in the `FD_DEBUG_DUMP` path before counting | `StackDistancePipelineJob.scala` (~line 348) | Kept — harmless, debug-only, confirms 1039/4471 are stable. |
| 4 | Delete leftover files in the shared `localOutputDir` before each partition's `reducerInit()` | `StackDistancePipelineJob.scala` (~line 459, after `localDir.mkdirs()`) | **Applied, testing.** Targets the proven shared-directory contamination bug (790 unique / 1815 total files in run-10). Deployed as run-11; 0 stale-file warnings logged, output comparison in progress. |

## Artifacts from this session
- `run-3fd-20260616-7` — partitioner-fix test (reverted, kept on cluster history for reference)
- `run-3fd-20260616-8-debug`, `-9-debug` — FD_DEBUG_DUMP runs, mapper-output-only, no reducer/output files
- `run-3fd-20260616-10` — full run, current (reverted) partitioner, proved the shared-directory duplication bug (1815 files / 790 unique)
- `run-3fd-20260616-11` — full run with the stale-file cleanup fix, output comparison in progress
- JARs uploaded to `fds-jar.in-maa-1.linodeobjects.com`: `fd_compute_jar/partitionerfix/` (broken, do not reuse), `fd_compute_jar/withJNIfix2/` (current reverted/good code)
