// ---------------------------------------------------------------------------
// stack_distance_map_jni.cpp
//
// JNI shim between Scala (FDNative) and the C++ mapper logic in
// stack_distance.map.cpp.
//
// Native method signatures match:
//   package com.fd.compute
//   class FDComputeNative {
//     @native def mapperInit(inputFilename: String, envOverrides: String): Long
//     @native def mapperProcessBatch(handle: Long, batch: String): String
//     @native def mapperFinalize(handle: Long): Unit
//   }
//
// envOverrides is a newline-separated list of KEY=value pairs.
// Any key whose value is an S3 path will have been pre-downloaded by Scala
// to a local temp path, and that local path is what arrives here.
// We call setenv() for each pair before readConfig() runs inside mapperInit().
// ---------------------------------------------------------------------------

#include <jni.h>
#include <string>
#include <cstring>
#include <cstdlib>
#include <mutex>
#include "stack_distance.map.h"

// ---------------------------------------------------------------------------
// readConfig() writes into global std::map structures and is NOT thread-safe.
// This mutex serializes concurrent mapperInit() calls from Spark executor
// threads sharing the same JVM process (e.g. local[4] mode).
// After mapperInit() returns, all globals are read-only and concurrent
// processLogLinesBatch() calls are safe.
// ---------------------------------------------------------------------------
static std::mutex g_init_mutex ;

// ---------------------------------------------------------------------------
// Helper: convert jstring → std::string (UTF-8)
// ---------------------------------------------------------------------------
static std::string jstr(JNIEnv *env, jstring js) {
    if (js == NULL) return "" ;
    const char *c = env->GetStringUTFChars(js, NULL) ;
    std::string s(c) ;
    env->ReleaseStringUTFChars(js, c) ;
    return s ;
}

// ---------------------------------------------------------------------------
// Helper: parse "KEY=value\nKEY2=value2\n..." and call setenv() for each pair.
// Called inside the init mutex, before readConfig().
// ---------------------------------------------------------------------------
static void applyEnvOverrides(const std::string &overrides) {
    if (overrides.empty()) return ;
    char *buf = strdup(overrides.c_str()) ;
    char *line = strtok(buf, "\n") ;
    while (line) {
        char *eq = strchr(line, '=') ;
        if (eq) {
            *eq = '\0' ;
            setenv(line, eq + 1, /*overwrite=*/1) ;
            fprintf(stderr, "[JNI] setenv %s=%s\n", line, eq + 1) ;
            *eq = '=' ;   // restore for safety (not strictly needed)
        }
        line = strtok(NULL, "\n") ;
    }
    free(buf) ;
}

// ---------------------------------------------------------------------------
// mapperInit(inputFilename: String, envOverrides: String): Long
// ---------------------------------------------------------------------------
extern "C"
JNIEXPORT jlong JNICALL
Java_com_fd_compute_FDComputeNative_mapperInit(JNIEnv *env, jobject /*obj*/,
                                               jstring jInputFilename,
                                               jstring jEnvOverrides) {
    std::string filename  = jstr(env, jInputFilename) ;
    std::string overrides = jstr(env, jEnvOverrides) ;
    std::lock_guard<std::mutex> lock(g_init_mutex) ;  // serialize readConfig() calls
    applyEnvOverrides(overrides) ;                     // setenv before readConfig()
    MapperCtx *ctx = mapperInit(filename.c_str()) ;
    return reinterpret_cast<jlong>(ctx) ;
}

// ---------------------------------------------------------------------------
// mapperProcessBatch(handle: Long, batch: String): String
//
// batch is a newline-separated block of raw log lines.
// Returns a newline-separated block of mapper output lines.
// ---------------------------------------------------------------------------
extern "C"
JNIEXPORT jstring JNICALL
Java_com_fd_compute_FDComputeNative_mapperProcessBatch(JNIEnv *env, jobject /*obj*/,
                                         jlong handle, jstring jBatch) {
    MapperCtx *ctx = reinterpret_cast<MapperCtx *>(handle) ;
    std::string batch = jstr(env, jBatch) ;
    std::string outbuf ;
    outbuf.reserve(batch.size()) ;   // output is usually smaller than input
    processLogLinesBatch(ctx, batch.c_str(), outbuf) ;
    return env->NewStringUTF(outbuf.c_str()) ;
}

// ---------------------------------------------------------------------------
// mapperFinalize(handle: Long): Unit
// ---------------------------------------------------------------------------
extern "C"
JNIEXPORT void JNICALL
Java_com_fd_compute_FDComputeNative_mapperFinalize(JNIEnv *env, jobject /*obj*/, jlong handle) {
    MapperCtx *ctx = reinterpret_cast<MapperCtx *>(handle) ;
    mapperFinalize(ctx) ;   // logs stats and deletes ctx
}
