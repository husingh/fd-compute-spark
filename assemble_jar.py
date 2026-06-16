"""
Fat-JAR assembler using Python's zipfile (bypasses Java ZipFileSystem NPE).
Mirrors the assemblyMergeStrategy in build.sbt:
  META-INF/MANIFEST.MF       → discard
  META-INF/services/*        → concat
  META-INF/**                → discard
  reference.conf             → concat
  *                          → first
"""
import os
import sys
import zipfile
import io

PROJECT_DIR = "/Users/husingh/Desktop/fd-compute-spark-June"
OUTPUT_JAR  = os.path.join(PROJECT_DIR, "target/scala-2.12/fd-compute-spark-assembly.jar")

# Non-provided dependency JARs (from `sbt "show Runtime/dependencyClasspath"`)
DEPENDENCY_JARS = [
    os.path.expanduser("~/.cache/coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala-library/2.12.20/scala-library-2.12.20.jar"),
    os.path.expanduser("~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/scala-lang/scala-library/2.12.20/scala-library-2.12.20.jar"),
    os.path.expanduser("~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"),
    os.path.expanduser("~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"),
    os.path.expanduser("~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar"),
    os.path.expanduser("~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar"),
    os.path.expanduser("~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar"),
    os.path.expanduser("~/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar"),
]

# Directories containing our compiled classes and generated resources
SOURCE_DIRS = [
    os.path.join(PROJECT_DIR, "target/scala-2.12/classes"),
    os.path.join(PROJECT_DIR, "target/scala-2.12/resource_managed/main"),
]

def merge_strategy(name):
    parts = name.split("/")
    if parts[0] == "META-INF":
        if len(parts) == 2 and parts[1] == "MANIFEST.MF":
            return "discard"
        if len(parts) >= 2 and parts[1] == "services":
            return "concat"
        return "discard"
    if name == "reference.conf":
        return "concat"
    return "first"


def main():
    # collected[name] = bytes (for "first") or [bytes, ...] (for "concat")
    collected = {}

    def add_entry(name, data):
        strategy = merge_strategy(name)
        if strategy == "discard":
            return
        if strategy == "concat":
            if name not in collected:
                collected[name] = []
            collected[name].append(data)
        else:  # first
            if name not in collected:
                collected[name] = data

    # 1. Process compiled classes + resources (highest priority — goes first)
    for src_dir in SOURCE_DIRS:
        if not os.path.isdir(src_dir):
            print(f"  [skip] {src_dir} (not found)")
            continue
        print(f"  [dir] {src_dir}")
        for root, _, files in os.walk(src_dir):
            for f in files:
                full = os.path.join(root, f)
                rel  = os.path.relpath(full, src_dir).replace("\\", "/")
                with open(full, "rb") as fh:
                    add_entry(rel, fh.read())

    # 2. Process dependency JARs
    for jar_path in DEPENDENCY_JARS:
        if not os.path.isfile(jar_path):
            print(f"  [skip] {jar_path} (not found)")
            continue
        print(f"  [jar] {os.path.basename(jar_path)}")
        try:
            with zipfile.ZipFile(jar_path, "r") as zf:
                for info in zf.infolist():
                    name = info.filename
                    if name.endswith("/"):   # directory entry
                        continue
                    try:
                        data = zf.read(name)
                    except Exception as e:
                        print(f"    [warn] cannot read {name}: {e}")
                        continue
                    add_entry(name, data)
        except zipfile.BadZipFile as e:
            print(f"  [warn] bad zip {jar_path}: {e}")

    # 3. Write output JAR
    os.makedirs(os.path.dirname(OUTPUT_JAR), exist_ok=True)
    print(f"\nWriting {OUTPUT_JAR} ...")
    with zipfile.ZipFile(OUTPUT_JAR, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as out:
        # Write a minimal MANIFEST
        manifest = (
            "Manifest-Version: 1.0\n"
            "Main-Class: com.fd.compute.StackDistancePipelineJob\n"
            "\n"
        )
        out.writestr("META-INF/MANIFEST.MF", manifest)

        for name, value in collected.items():
            if isinstance(value, list):
                # concat strategy
                out.writestr(name, b"\n".join(value))
            else:
                out.writestr(name, value)

    size_mb = os.path.getsize(OUTPUT_JAR) / (1024 * 1024)
    print(f"Done. {OUTPUT_JAR} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
