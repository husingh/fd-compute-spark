#ifndef STACK_DISTANCE_MAP_H
#define STACK_DISTANCE_MAP_H

#include <string>

// ---------------------------------------------------------------------------
// Per-partition context.  Lives on the heap; created by mapperInit(),
// passed to every processLogLinesBatch() call, freed by mapperFinalize().
// ---------------------------------------------------------------------------
struct MapperCtx {
    int  match_ip ;          // ghost-IP index from ghosts[] map (-1 = no filter)
    bool skipAllLines ;      // true when this file's ghost is not in the filter list

    long long allLinesCount ;
    long long invalidLinesCount ;
    long long ignoredLinesCount ;
};

// ---------------------------------------------------------------------------
// Public API — implemented in stack_distance.map.cpp
// ---------------------------------------------------------------------------
#ifdef __cplusplus
extern "C" {
#endif

// Call once per Spark partition.  Reads config from env vars.
// inputFilename: the S3/local path being processed (for ghost-IP lookup).
//               Pass "" or NULL when no ghost filter is configured.
// Returns a heap-allocated MapperCtx* cast to void* for C callers.
MapperCtx *mapperInit(const char *inputFilename) ;

// Call for each batch of log lines (newline-separated).
// Output lines are appended to outbuf.
void processLogLinesBatch(MapperCtx *ctx, const char *inputBuf, std::string &outbuf) ;

// Call once at the end of the partition.  Logs stats to stderr, frees ctx.
void mapperFinalize(MapperCtx *ctx) ;

#ifdef __cplusplus
}
#endif

#endif // STACK_DISTANCE_MAP_H
