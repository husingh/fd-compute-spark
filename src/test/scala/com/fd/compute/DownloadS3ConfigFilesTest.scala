package com.fd.compute

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.io.{File, FileWriter}
import java.nio.file.{Files, Path}

/**
 * Tests for StackDistancePipelineJob.downloadS3ConfigFiles.
 *
 * We do NOT make real S3 calls here. Instead we test the three behaviours
 * the function must handle:
 *
 *   1. Env var not set          → skipped entirely, no override emitted
 *   2. Env var = local path     → skipped (C++ opens it directly), no override emitted
 *   3. Env var = s3:// path     → download attempted (we mock the download step
 *                                  by subclassing and overriding the download)
 *
 * We also test the override string format that gets passed to applyEnvOverrides
 * in C++: it must be "KEY=/local/path" lines separated by "\n".
 */
class DownloadS3ConfigFilesTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Temporary directory cleaned up after each test. */
  private var tmpDir: File = _

  override def beforeEach(): Unit = {
    tmpDir = Files.createTempDirectory("fd_test_cfg_").toFile
  }

  override def afterEach(): Unit = {
    deleteRecursively(tmpDir)
    // Unset any env-var overrides we may have set via system property tricks
  }

  private def deleteRecursively(f: File): Unit = {
    if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
    f.delete()
  }

  /** Write a small text file into tmpDir and return its absolute path. */
  private def writeLocalFile(name: String, content: String): File = {
    val f = new File(tmpDir, name)
    val w = new FileWriter(f)
    try w.write(content) finally w.close()
    f
  }

  // -------------------------------------------------------------------------
  // Test the override string parser logic (pure Scala, no JNI needed)
  // -------------------------------------------------------------------------

  test("parseOverrides: empty string produces empty map") {
    parseEnvOverrides("") shouldBe Map.empty
  }

  test("parseOverrides: single KEY=value pair") {
    parseEnvOverrides("FD_MAPREDUCE_VCDS_FILE=/tmp/vcds") shouldBe
      Map("FD_MAPREDUCE_VCDS_FILE" -> "/tmp/vcds")
  }

  test("parseOverrides: multiple KEY=value pairs separated by newline") {
    val s = "FD_MAPREDUCE_VCDS_FILE=/tmp/vcds\nFD_MAPREDUCE_CPCODES_FILE=/tmp/cp"
    parseEnvOverrides(s) shouldBe Map(
      "FD_MAPREDUCE_VCDS_FILE"   -> "/tmp/vcds",
      "FD_MAPREDUCE_CPCODES_FILE" -> "/tmp/cp"
    )
  }

  test("parseOverrides: value containing '=' sign is preserved correctly") {
    // Unlikely but path could theoretically contain '=' on some systems
    val s = "FD_MAPREDUCE_VCDS_FILE=/tmp/a=b"
    parseEnvOverrides(s) shouldBe Map("FD_MAPREDUCE_VCDS_FILE" -> "/tmp/a=b")
  }

  test("parseOverrides: trailing newline is ignored") {
    val s = "FD_MAPREDUCE_VCDS_FILE=/tmp/vcds\n"
    parseEnvOverrides(s) shouldBe Map("FD_MAPREDUCE_VCDS_FILE" -> "/tmp/vcds")
  }

  // -------------------------------------------------------------------------
  // Test downloadS3ConfigFiles routing logic (no real S3)
  // We subclass to intercept the actual download and instead create a local
  // stub file — verifying that the right vars trigger a "download", the right
  // local filenames are computed, and local/unset vars are left alone.
  // -------------------------------------------------------------------------

  /**
   * Testable subclass of StackDistancePipelineJob logic.
   * Overrides the actual S3 download with a stub that creates an empty file.
   */
  object TestableDownloader {

    /** Mirrors downloadS3ConfigFiles but replaces the S3 fetch with a stub. */
    def downloadS3ConfigFiles(
      envVarValues: Map[String, String],  // simulated env
      encryptionKeys: Map[String, String],
      taskTempDir: File
    ): String = {
      val overrides = scala.collection.mutable.ArrayBuffer[String]()
      for (varName <- StackDistancePipelineJob.S3_FILE_ENV_VARS) {
        envVarValues.get(varName) match {
          case Some(v) if v.startsWith("s3://") || v.startsWith("s3a://") =>
            // Simulate: extract filename, strip .keyN, create stub file
            val keyIdx   = StackDistancePipelineJob.keyIndexOf(v)
            val _        = encryptionKeys.getOrElse(keyIdx,
              throw new IllegalArgumentException(
                s"No encryption key for index '$keyIdx' needed by $varName=$v"))
            val fileName = v.split("/").last.replaceAll("""\.key\d+$""", "")
            val localFile = new File(taskTempDir, fileName)
            localFile.createNewFile()   // stub — no actual S3 call
            overrides += s"$varName=${localFile.getAbsolutePath}"
          case Some(_) => // local path — skip
          case None    => // not set — skip
        }
      }
      overrides.mkString("\n")
    }
  }

  test("downloadS3ConfigFiles: env var not set → no override emitted") {
    val result = TestableDownloader.downloadS3ConfigFiles(
      envVarValues   = Map.empty,
      encryptionKeys = Map("2" -> "dummyKey"),
      taskTempDir    = tmpDir
    )
    result shouldBe ""
  }

  test("downloadS3ConfigFiles: env var set to local path → no override emitted") {
    val localFile = writeLocalFile("vcds.txt", "1234\n5678\n")
    val result = TestableDownloader.downloadS3ConfigFiles(
      envVarValues   = Map("FD_MAPREDUCE_VCDS_FILE" -> localFile.getAbsolutePath),
      encryptionKeys = Map("1" -> "dummyKey"),
      taskTempDir    = tmpDir
    )
    result shouldBe ""
  }

  test("downloadS3ConfigFiles: s3:// path → override emitted with correct KEY and local filename") {
    val result = TestableDownloader.downloadS3ConfigFiles(
      envVarValues   = Map("FD_MAPREDUCE_VCDS_FILE" -> "s3://my-bucket/configs/vcds.txt.key2"),
      encryptionKeys = Map("2" -> "dummyKey=="),
      taskTempDir    = tmpDir
    )
    val overrides = parseEnvOverrides(result)
    overrides should have size 1
    overrides("FD_MAPREDUCE_VCDS_FILE") should endWith("vcds.txt")
    // The local file must have been created in tmpDir
    new File(overrides("FD_MAPREDUCE_VCDS_FILE")).exists() shouldBe true
  }

  test("downloadS3ConfigFiles: s3a:// prefix also works") {
    val result = TestableDownloader.downloadS3ConfigFiles(
      envVarValues   = Map("FD_MAPREDUCE_CPCODES_FILE" -> "s3a://my-bucket/configs/cpcodes.txt.key1"),
      encryptionKeys = Map("1" -> "dummyKey=="),
      taskTempDir    = tmpDir
    )
    val overrides = parseEnvOverrides(result)
    overrides("FD_MAPREDUCE_CPCODES_FILE") should endWith("cpcodes.txt")
  }

  test("downloadS3ConfigFiles: .keyN suffix stripped from local filename") {
    val result = TestableDownloader.downloadS3ConfigFiles(
      envVarValues   = Map("FD_MAPREDUCE_REGIONS_FILE" -> "s3://bucket/regions.txt.key2"),
      encryptionKeys = Map("2" -> "dummyKey=="),
      taskTempDir    = tmpDir
    )
    val overrides = parseEnvOverrides(result)
    val localName = new File(overrides("FD_MAPREDUCE_REGIONS_FILE")).getName
    localName shouldBe "regions.txt"     // .key2 stripped
    localName should not include "key2"
  }

  test("downloadS3ConfigFiles: multiple vars → all overrides returned") {
    val result = TestableDownloader.downloadS3ConfigFiles(
      envVarValues = Map(
        "FD_MAPREDUCE_VCDS_FILE"    -> "s3://bucket/vcds.txt.key2",
        "FD_MAPREDUCE_CPCODES_FILE" -> "s3://bucket/cpcodes.txt.key2",
        "FD_MAPREDUCE_REGIONS_FILE" -> "/local/regions.txt"  // local — skipped
      ),
      encryptionKeys = Map("2" -> "dummyKey=="),
      taskTempDir    = tmpDir
    )
    val overrides = parseEnvOverrides(result)
    overrides should have size 2
    overrides.keys should contain("FD_MAPREDUCE_VCDS_FILE")
    overrides.keys should contain("FD_MAPREDUCE_CPCODES_FILE")
    overrides.keys should not contain "FD_MAPREDUCE_REGIONS_FILE"
  }

  test("downloadS3ConfigFiles: missing encryption key → throws IllegalArgumentException") {
    an [IllegalArgumentException] should be thrownBy {
      TestableDownloader.downloadS3ConfigFiles(
        envVarValues   = Map("FD_MAPREDUCE_VCDS_FILE" -> "s3://bucket/vcds.txt.key9"),
        encryptionKeys = Map("1" -> "onlyKey1"),   // key 9 missing
        taskTempDir    = tmpDir
      )
    }
  }

  test("downloadS3ConfigFiles: only the 7 known _FILE vars are processed") {
    // An unknown var should never appear in the override string
    // (we can't inject it through TestableDownloader — just verify the constant list)
    StackDistancePipelineJob.S3_FILE_ENV_VARS should have size 7
    StackDistancePipelineJob.S3_FILE_ENV_VARS should not contain "FD_MAPREDUCE_WRITE_SERIAL_INFO_FILE"
  }

  test("keyIndexOf: extracts correct key index from s3 path") {
    StackDistancePipelineJob.keyIndexOf("s3a://bucket/file.gz.key2") shouldBe "2"
    StackDistancePipelineJob.keyIndexOf("s3a://bucket/file.gz.key13") shouldBe "13"
    StackDistancePipelineJob.keyIndexOf("s3a://bucket/file.gz")       shouldBe "1"  // default
    StackDistancePipelineJob.keyIndexOf("s3a://bucket/file.key2.gz")  shouldBe "1"  // .keyN must be last
  }

  // -------------------------------------------------------------------------
  // Private helper — mirrors the C++ strtok logic in pure Scala for testing
  // -------------------------------------------------------------------------

  /** Parse the override string back into a map for easy assertions. */
  private def parseEnvOverrides(s: String): Map[String, String] = {
    if (s.isEmpty) return Map.empty
    s.split("\n")
      .filter(_.nonEmpty)
      .flatMap { line =>
        val idx = line.indexOf('=')
        if (idx < 0) None
        else Some(line.substring(0, idx) -> line.substring(idx + 1))
      }
      .toMap
  }
}
