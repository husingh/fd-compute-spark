// ---------------------------------------------------------------------------
// stack_distance_map_main.cpp
//
// Standalone entry point for the stack_distance mapper.
// Reads log lines from stdin, writes mapper output to stdout —
// identical to the original Hadoop streaming behaviour.
//
// Build: linked only into the `stack_distance.map` binary (not the .dylib).
// ---------------------------------------------------------------------------

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "stack_distance.map.h"

int main(int argc, char *argv[]) {
    // inputFilename for ghost-IP resolution: original code used
    // getenv("mapreduce_map_input_file").  Keep that behaviour here.
    const char *inputFilename = getenv("mapreduce_map_input_file") ;
    if (!inputFilename) inputFilename = "" ;

    MapperCtx *ctx = mapperInit(inputFilename) ;

    if (ctx->skipAllLines) {
        // Ghost not in filter — drain stdin silently, same as original code
        while (fgetc(stdin) != EOF) ;
        mapperFinalize(ctx) ;
        return 0 ;
    }

    // Read all of stdin in one shot and hand it to processLogLinesBatch.
    // For very large files we could chunk, but a single batch is fine here
    // because this is the standalone (non-Spark) path.
    std::string input ;
    char buf[65536] ;
    while (fgets(buf, sizeof(buf), stdin) != NULL)
        input += buf ;

    std::string output ;
    output.reserve(input.size()) ;
    processLogLinesBatch(ctx, input.c_str(), output) ;

    // Write output to stdout
    fwrite(output.c_str(), 1, output.size(), stdout) ;

    mapperFinalize(ctx) ;
    return 0 ;
}
