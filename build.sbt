name         := "fd-compute-spark"
version      := "0.1"
scalaVersion := "2.12.20"

// Spark is "provided" on a real cluster.
// Change to "compile" temporarily to run via `sbt run` without spark-submit.
val sparkScope = "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % sparkScope,
  "org.apache.spark" %% "spark-sql"  % "3.5.0" % sparkScope,

  // S3A connector — must match Spark 3.5.0's bundled hadoop-common (3.3.4)
  "org.apache.hadoop" % "hadoop-aws"          % "3.3.4",
  "com.amazonaws"     % "aws-java-sdk-bundle" % "1.12.262",

  // Lightweight JSON parsing for encryption_keys.json
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",

  // Test framework
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

// Make "provided" deps visible on the classpath when using `sbt run`
// (they are excluded from the assembly jar — only needed for local testing)
Compile / run / unmanagedClasspath ++= (Compile / dependencyClasspath).value
  .filter(_.data.name.contains("spark"))

// lib/ directory contains libFDCompute.dylib (macOS) and libFDCompute.so (Linux)
// Drop them here before running: cp fd_compute/libFDCompute.dylib lib/
run / fork        := true
run / javaOptions ++= Seq(
  s"-Djava.library.path=${baseDirectory.value}/lib",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

Test / fork        := true
Test / javaOptions ++= Seq(
  s"-Djava.library.path=${baseDirectory.value}/lib",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

// ---------------------------------------------------------------------------
// Bundle native libs into the jar as resources under "native/".
// Picks up whichever of libFDCompute.so / libFDCompute.dylib exist in lib/.
// FDComputeNative.ensureLoaded() extracts them at runtime.
// ---------------------------------------------------------------------------
Compile / resourceGenerators += Def.task {
  val libDir   = baseDirectory.value / "lib"
  val outDir   = (Compile / resourceManaged).value / "native"
  IO.createDirectory(outDir)
  val candidates = Seq("libFDCompute.so", "libFDCompute.dylib")
  candidates.flatMap { name =>
    val src = libDir / name
    if (src.exists()) {
      val dst = outDir / name
      IO.copyFile(src, dst)
      streams.value.log.info(s"[native] bundling $src → resource native/$name")
      Some(dst)
    } else None
  }
}.taskValue

// Fat JAR: bundle Scala stdlib + all deps; exclude Spark (provided by cluster)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => xs match {
    case "MANIFEST.MF" :: Nil         => MergeStrategy.discard
    case "services" :: _              => MergeStrategy.concat
    case _                            => MergeStrategy.discard
  }
  case "reference.conf"              => MergeStrategy.concat
  case _                             => MergeStrategy.first
}

assembly / assemblyJarName := "fd-compute-spark-assembly.jar"
