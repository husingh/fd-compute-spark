#ifndef STACK_DISTANCE_REDUCE_API_H
#define STACK_DISTANCE_REDUCE_API_H

// ---------------------------------------------------------------------------
// Public API for stack_distance reducer — callable from JNI or standalone.
// ---------------------------------------------------------------------------

// Call once per Spark partition before any batch processing.
// Reads all configuration from env vars (FD_MAPREDUCE_*).
void reducerInit() ;

// Call with each batch of newline-separated mapper output lines.
// May be called many times; state accumulates across calls.
void processReducerBatch(const char *inputBuf) ;

// Call after all batches.  Writes output files to FD_MAPREDUCE_OUTPUT_DIR
// and logs final stats to stderr.
void reducerFinalize() ;

#endif // STACK_DISTANCE_REDUCE_API_H
