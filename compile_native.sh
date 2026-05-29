#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# compile_native.sh — Build libFDCompute.so (Linux) or libFDCompute.dylib (macOS)
# from the C++ sources in ./cpp_src/ and copy the result into ./lib/
#
# Run this on every target platform before running the Spark job.
#
# Requirements (Linux):
#   sudo apt-get install -y g++ openjdk-11-jdk libssl-dev zlib1g-dev
#
# Requirements (macOS):
#   brew install llvm openssl openjdk@11
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$SCRIPT_DIR/cpp_src"
LIB_DIR="$SCRIPT_DIR/lib"

mkdir -p "$LIB_DIR"

# ---------------------------------------------------------------------------
# Detect platform
# ---------------------------------------------------------------------------
OS="$(uname -s)"
ARCH="$(uname -m)"
echo "Platform: $OS / $ARCH"

# ---------------------------------------------------------------------------
# Locate JDK for JNI headers
# ---------------------------------------------------------------------------
if [ -n "${JAVA_HOME:-}" ] && [ -d "$JAVA_HOME" ]; then
  JDK_HOME="$JAVA_HOME"
elif [ -d "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" ]; then
  JDK_HOME="/opt/homebrew/opt/openjdk.jdk/Contents/Home"
  JDK_HOME="/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
elif command -v java &>/dev/null; then
  JDK_HOME="$(java -XshowSettings:all -version 2>&1 | grep 'java.home' | awk '{print $3}')"
  # strip /jre suffix if present
  JDK_HOME="${JDK_HOME%/jre}"
else
  echo "ERROR: JDK not found. Set JAVA_HOME or install openjdk-11."
  exit 1
fi
echo "JDK_HOME: $JDK_HOME"

# JNI platform-specific include dir
if [ "$OS" = "Darwin" ]; then
  JNI_PLATFORM_INC="$JDK_HOME/include/darwin"
else
  JNI_PLATFORM_INC="$JDK_HOME/include/linux"
fi
JNI_CFLAGS="-I$JDK_HOME/include -I$JNI_PLATFORM_INC"

# ---------------------------------------------------------------------------
# Compiler and flags
# ---------------------------------------------------------------------------
if [ "$OS" = "Darwin" ]; then
  CXX="${CXX:-clang++}"
  # Locate OpenSSL: prefer env override, then well-known Homebrew paths
  if [ -z "${OPENSSL_PREFIX:-}" ]; then
    for candidate in \
        /opt/homebrew/opt/openssl \
        /opt/homebrew/opt/openssl@3 \
        /usr/local/opt/openssl \
        /usr/local/opt/openssl@3; do
      if [ -f "$candidate/lib/libcrypto.dylib" ]; then
        OPENSSL_PREFIX="$candidate"
        break
      fi
    done
  fi
  if [ -z "${OPENSSL_PREFIX:-}" ]; then
    echo "ERROR: OpenSSL not found. Install with: brew install openssl"
    exit 1
  fi
  echo "OpenSSL prefix: $OPENSSL_PREFIX"
  CXXFLAGS="-std=c++14 -O2 -Wall -fPIC -DBUILD_SHARED_LIB -I${OPENSSL_PREFIX}/include"
  LDFLAGS="-L${OPENSSL_PREFIX}/lib"
else
  CXX="${CXX:-g++}"
  CXXFLAGS="-std=c++14 -O2 -Wall -fPIC -DBUILD_SHARED_LIB"
  LDFLAGS=""
fi
LIBS="-lpthread -lm -lcrypto -lz"

echo "Compiler: $CXX"
echo "Sources:  $SRC_DIR"

# ---------------------------------------------------------------------------
# Compile each source to a PIC object
# ---------------------------------------------------------------------------
cd "$SRC_DIR"

SRCS=(
  splay.cpp
  stack_distance.config.cpp
  stack_distance.map.cpp
  stack_distance_map_jni.cpp
  stack_distance.reduce.cpp
  stack_distance_reduce_jni.cpp
  replication.map.cpp
  replication.reduce.cpp
  replication.merge.cpp
)

OBJS=()
for src in "${SRCS[@]}"; do
  obj="${src%.cpp}_jni_build.o"
  echo "  Compiling $src ..."
  $CXX $CXXFLAGS $JNI_CFLAGS -c -o "$obj" "$src"
  OBJS+=("$obj")
done

# ---------------------------------------------------------------------------
# Link shared library
# ---------------------------------------------------------------------------
if [ "$OS" = "Darwin" ]; then
  TARGET="$LIB_DIR/libFDCompute.dylib"
  echo "Linking $TARGET ..."
  $CXX -dynamiclib $LDFLAGS -o "$TARGET" "${OBJS[@]}" $LIBS
  # Also produce a .so symlink for compatibility
  ln -sf libFDCompute.dylib "$LIB_DIR/libFDCompute.so"
else
  TARGET="$LIB_DIR/libFDCompute.so"
  echo "Linking $TARGET ..."
  $CXX -shared -fPIC $LDFLAGS -o "$TARGET" "${OBJS[@]}" $LIBS
fi

# ---------------------------------------------------------------------------
# Clean up intermediate objects
# ---------------------------------------------------------------------------
rm -f "${OBJS[@]}"

echo ""
echo "✓ Built: $TARGET"
echo "✓ Library is ready in: $LIB_DIR"
