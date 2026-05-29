// ---------------------------------------------------------------------------
// stack_distance_reduce_jni.cpp
//
// JNI shim between Scala (FDComputeNative) and the C++ reducer logic.
// ---------------------------------------------------------------------------

#include <jni.h>
#include <string>
#include <cstring>
#include <mutex>
#include <cstdlib>
#include "stack_distance_reduce_api.h"

static std::mutex g_reduce_init_mutex ;

static std::string jstr(JNIEnv *env, jstring js) {
    if (js == NULL) return "" ;
    const char *c = env->GetStringUTFChars(js, NULL) ;
    std::string s(c) ;
    env->ReleaseStringUTFChars(js, c) ;
    return s ;
}

// reducerInit(partitionId: Int, numReducers: Int): Unit
extern "C"
JNIEXPORT void JNICALL
Java_com_fd_compute_FDComputeNative_reducerInit(JNIEnv *env, jobject /*obj*/,
                                                 jint partitionId, jint numReducers) {
    std::lock_guard<std::mutex> lock(g_reduce_init_mutex) ;
    char buf[32] ;
    snprintf(buf, sizeof(buf), "%d", (int)partitionId) ;
    setenv("mapreduce_task_partition", buf, 1) ;
    snprintf(buf, sizeof(buf), "%d", (int)numReducers) ;
    setenv("mapreduce_job_reduces", buf, 1) ;
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
