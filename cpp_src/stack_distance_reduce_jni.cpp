// ---------------------------------------------------------------------------
// stack_distance_reduce_jni.cpp
//
// JNI shim between Scala (FDComputeNative) and the C++ reducer logic.
//
// reducerInit now accepts an envOverrides string (newline-separated KEY=value
// pairs) so Spark can redirect any FD_MAPREDUCE_*_FILE env vars that were
// pointing at S3 paths to locally-downloaded temp files before readConfig()
// runs inside reducerInit().
// ---------------------------------------------------------------------------

#include <jni.h>
#include <string>
#include <cstring>
#include <cstdlib>
#include <mutex>
#include "stack_distance_reduce_api.h"

static std::mutex g_reduce_init_mutex ;

static std::string jstr(JNIEnv *env, jstring js) {
    if (js == NULL) return "" ;
    const char *c = env->GetStringUTFChars(js, NULL) ;
    std::string s(c) ;
    env->ReleaseStringUTFChars(js, c) ;
    return s ;
}

// ---------------------------------------------------------------------------
// Helper: parse "KEY=value\nKEY2=value2\n..." and call setenv() for each pair.
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
            *eq = '=' ;
        }
        line = strtok(NULL, "\n") ;
    }
    free(buf) ;
}

// reducerInit(partitionId: Int, numReducers: Int, envOverrides: String): Unit
extern "C"
JNIEXPORT void JNICALL
Java_com_fd_compute_FDComputeNative_reducerInit(JNIEnv *env, jobject /*obj*/,
                                                 jint partitionId, jint numReducers,
                                                 jstring jEnvOverrides) {
    std::lock_guard<std::mutex> lock(g_reduce_init_mutex) ;
    char buf[32] ;
    snprintf(buf, sizeof(buf), "%d", (int)partitionId) ;
    setenv("mapreduce_task_partition", buf, 1) ;
    snprintf(buf, sizeof(buf), "%d", (int)numReducers) ;
    setenv("mapreduce_job_reduces", buf, 1) ;
    std::string overrides = jstr(env, jEnvOverrides) ;
    applyEnvOverrides(overrides) ;                     // setenv before readConfig()
    reducerInit() ;
}

// reducerProcessBatch(batch: String): Unit
extern "C"
JNIEXPORT void JNICALL
Java_com_fd_compute_FDComputeNative_reducerProcessBatch(JNIEnv *env, jobject /*obj*/,
                                                         jstring jBatch) {
    std::string batch = jstr(env, jBatch) ;
    processReducerBatch(batch.c_str()) ;
}

// reducerFinalize(): Unit
extern "C"
JNIEXPORT void JNICALL
Java_com_fd_compute_FDComputeNative_reducerFinalize(JNIEnv *env, jobject /*obj*/) {
    reducerFinalize() ;
}
