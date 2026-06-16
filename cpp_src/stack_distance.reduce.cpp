#include <stdio.h>
#include <climits>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <libgen.h>
#include <sys/stat.h>
#include <map>
#include <functional>
#include "splay.h"
#include "stack_distance.config.h"
#include "stack_distance.reduce.h"

//---------------------------------------------------------------------------
// Variables — all thread_local so each Spark executor thread (task) gets
// its own isolated copy, matching Hadoop's per-process isolation.
// No logic changes anywhere: function bodies are untouched.
//---------------------------------------------------------------------------
thread_local iat_t *iat = nullptr;
thread_local stdtime_t *stack_distance_time_ph1 = nullptr;
thread_local md5_t *md5_data = nullptr;

thread_local long long *seen = nullptr;
thread_local long long *objsize = nullptr;
thread_local double *reftime = nullptr;
thread_local long long uniqueMissBytes = 0 ;
thread_local long long entryBarrierMissBytes = 0 ;
thread_local long long entryBarrierMissRequests = 0 ;
thread_local int max_id = 0;
thread_local unsigned int *md5_hash = nullptr;
thread_local unsigned int *md5_hash_overflow = nullptr;
thread_local unsigned int md5_hash_overflow_depth = 0;
thread_local unsigned int md5_count = 1;

thread_local double start_time = -1, last_time = -1;
thread_local int timelen = TIMELEN ;
thread_local traffic_t *traffic_buckets = nullptr ;

thread_local std::map<int,serial_info_t> perSerialInfo ;
thread_local std::map<int,int> lastValidated ;

thread_local FILE *g_outfile = nullptr ;

// Session state — persists across processReducerBatch() calls for one task.
// thread_local gives each concurrent task its own copy; reducerReset() clears
// it between sequential tasks reusing the same thread.
static thread_local long long g_readcount           = 0 ;
static thread_local long long g_count               = 0 ;
static thread_local long long g_prewarmSkippedCount = 0 ;
static thread_local long long g_total_bytes         = 0 ;
static thread_local long long g_total_count         = 0 ;
static thread_local long long g_total_mib           = 0 ;
static thread_local long long g_sampleSkippedCount  = 0 ;
static thread_local long long g_sampleSkippedBytes  = 0 ;
static thread_local Tree     *g_root                = nullptr ;
static thread_local int       g_md5_count_exceeded  = 0 ;
static thread_local bool      g_memory_allocated    = false ;
static thread_local bool      g_doSpatialSampling   = false ;
static thread_local int       g_non_empty           = 0 ;


//---------------------------------------------------------------------------
// reducerReset — free all heap memory and zero every global so the next
// reducerInit() starts from a clean slate.  Called at the end of
// reducerFinalize() so memory is released promptly between partitions.
//---------------------------------------------------------------------------
void reducerReset() {
  // Splay tree: freetree() recursively frees every malloc'd node
  freetree(g_root) ;
  g_root = NULL ;

  // calloc/malloc'd arrays from allocate_memory()
  free(iat) ;                     iat                    = NULL ;
  free(stack_distance_time_ph1) ; stack_distance_time_ph1 = NULL ;
  free(md5_data) ;                md5_data               = NULL ;
  free(seen) ;                    seen                   = NULL ;
  free(objsize) ;                 objsize                = NULL ;
  free(reftime) ;                 reftime                = NULL ;
  free(md5_hash) ;                md5_hash               = NULL ;
  free(md5_hash_overflow) ;       md5_hash_overflow      = NULL ;
  free(traffic_buckets) ;         traffic_buckets        = NULL ;

  // Static session counters
  g_readcount           = 0 ;
  g_count               = 0 ;
  g_prewarmSkippedCount = 0 ;
  g_total_bytes         = 0 ;
  g_total_count         = 0 ;
  g_total_mib           = 0 ;
  g_sampleSkippedCount  = 0 ;
  g_sampleSkippedBytes  = 0 ;
  g_md5_count_exceeded  = 0 ;
  g_memory_allocated    = false ;
  g_doSpatialSampling   = false ;
  g_non_empty           = 0 ;

  // Non-static globals
  uniqueMissBytes          = 0 ;
  entryBarrierMissBytes    = 0 ;
  entryBarrierMissRequests = 0 ;
  max_id                   = 0 ;
  md5_hash_overflow_depth  = 0 ;
  md5_count                = 1 ;   // index starts at 1, not 0
  start_time               = -1 ;
  last_time                = -1 ;
  timelen                  = TIMELEN ;

  // STL containers
  perSerialInfo.clear() ;
  lastValidated.clear() ;

  // Output file handle (should already be closed by reducerFinalize)
  g_outfile = NULL ;
}

//---------------------------------------------------------------------------
// A bunch of misc functions first
//---------------------------------------------------------------------------
// Memory status related
void read_oom_score(int *score) {
  const char* oom_score_path = "/proc/self/oom_score";
  FILE *f = fopen(oom_score_path,"r");
  if(!f){
    perror(oom_score_path);
    return;
  }
  if(1 != fscanf(f,"%d",score))
  {
    perror(oom_score_path);
    return;
  }
  fclose(f);
}

void read_off_memory_status(statm_t *result) {
  const char* statm_path = "/proc/self/statm";

  FILE *f = fopen(statm_path,"r");
  if(!f){
    perror(statm_path);
    return;
  }
  if(7 != fscanf(f,"%ld %ld %ld %ld %ld %ld %ld",
    &result->size,&result->resident,&result->share,&result->text,&result->lib,&result->data,&result->dt)) {
    perror(statm_path);
    return;
  }
  fclose(f);
}

// Peak traffic related
traffic_t getPeaktraffic() {
  traffic_t retval ;
  int i ;

  retval = traffic_buckets[0];
  for (i=1; i<timelen/TIMEBUCKET;i++) {
    if (traffic_buckets[i].requests>retval.requests) retval.requests = traffic_buckets[i].requests ;
    if (traffic_buckets[i].bytes>retval.bytes) retval.bytes = traffic_buckets[i].bytes ;
  }
  return retval ;
}

//---------------------------------------------------------------------------
// Some md5 related functionality
//---------------------------------------------------------------------------
int cmpf(const void *p1, const void *p2) {
    return memcmp(p1, p2, MD5_DIGEST_LENGTH);
}

// Searches for an md5.
// If found, returns the index in md5_data. 
// If not found, adds the md5 to the end of md5_data, returns the index.
int search_md5(unsigned char *md5) {
    int found = 0;
    unsigned int hash_index, hash_value;

    hash_index = *((unsigned int *) md5);
    hash_index %= nodelen;
    hash_value = md5_hash[hash_index];

    if (hash_value == 0) {
        md5_hash[hash_index] = md5_count;
    }
    else {
        unsigned int i, prev_i=0;
        unsigned int overflow_deep = 0;

        for (i = hash_value; i != 0; i = md5_hash_overflow[i], overflow_deep++) {
            if (cmpf(md5, md5_data[i].md5) == 0) {
                found = i;
                break;
            }
            prev_i = i;
        }

        if (overflow_deep > md5_hash_overflow_depth) {
            md5_hash_overflow_depth = overflow_deep;
        }

        if (found == 0) {
            md5_hash_overflow[prev_i] = md5_count;
        }
    }   
    if (found == 0) {
        memcpy(md5_data[md5_count].md5, md5, MD5_DIGEST_LENGTH);
        found = md5_count;

        md5_count++;
        if (md5_count >= nodelen) {
            fprintf(stdout, "md5_count %d exceeds maximum %d. Terminating. Rerun this serial with serial splitting\n", md5_count, nodelen);
            exit(1);
        }
    }
    return found;
}

//---------------------------------------------------------------------------
// Malloc various arrays
//---------------------------------------------------------------------------
void allocate_memory() {

  long long allocated = 0 ;

  if ((seen = (long long *) calloc(nodelen, sizeof(long long))) == NULL ) {
    fprintf(stdout, "fail to allocate seen\n");
    exit(1);
  } else {
    allocated += (nodelen*sizeof(long long)) ;
  }

  if ((objsize = (long long *) calloc(nodelen, sizeof(long long))) == NULL) {
    fprintf(stdout, "fail to allocate objsize\n");
    exit(1);
  } else {
    allocated += (nodelen*sizeof(long long)) ;
  }

  if ((reftime = (double *) calloc(nodelen, sizeof(double))) == NULL) {
    fprintf(stdout, "fail to allocate reftime\n");
    exit(1);
  } else {
    allocated += (nodelen*sizeof(double)) ;
  } 

  if ((iat = (iat_t *) calloc(nodelen, sizeof(iat_t))) == NULL) {
    fprintf(stdout, "fail to allocate iat\n");
    exit(1);
  } else {
    allocated += (nodelen*sizeof(iat_t)) ;
  }

  if ((stack_distance_time_ph1 = (stdtime_t *) calloc(timelen, sizeof(stdtime_t))) == NULL) {
    fprintf(stdout, "fail to allocate stack distance_time_ph1. timelen = %d\n", timelen);
    exit(1);
  } else {
    allocated += (timelen*sizeof(stdtime_t)) ;
  }

  if ((md5_data = (md5_t *) malloc(sizeof(md5_t) * nodelen)) == NULL) {
    fprintf(stdout, "fail to malloc md5_data\n");
    exit(1);
  } else {
    allocated += (sizeof(md5_t) * nodelen) ;
  }

  if ((md5_hash = (unsigned int *) calloc(nodelen, sizeof(unsigned int))) == NULL) {
    fprintf(stdout, "fail to malloc md5_hash\n");
    exit(1);
  } else {
    allocated += (nodelen*sizeof(unsigned int)) ;
  }

  if ((md5_hash_overflow = (unsigned int *) calloc(nodelen, sizeof(unsigned int))) == NULL) {
    fprintf(stdout, "fail to malloc md5_hash_overflow\n");
    exit(1);
  } else {
    allocated += (nodelen*sizeof(unsigned int)) ;
  }

  if ((traffic_buckets = (traffic_t*) calloc(timelen/TIMEBUCKET, sizeof(traffic_t))) == NULL) {
    fprintf(stdout, "failed to malloc traffic_buckets\n") ;
    exit(1);
  } else {
    allocated += ( (timelen/TIMEBUCKET)*sizeof(traffic_t) ) ;
  }
  fprintf ( stdout, "Allocated memory = %lld Bytes\n", allocated ) ;

}

//---------------------------------------------------------------------------
// Local file I/O — replaces old HDFS helpers
//---------------------------------------------------------------------------
void prepareOutputDir(char *directory_name) {
  struct stat st;
  if (stat(directory_name, &st) != 0) {
    fprintf(stdout, "Output directory %s does not exist, creating...\n", directory_name);
    if (mkdir(directory_name, 0755) != 0) {
      fprintf(stdout, "fail to create output directory %s\n", directory_name);
      exit(1);
    }
  }
}

void openOutputFile(char *directory_name, char *fname) {
  char buf[LINESIZE];
  sprintf(buf, "%s/%s", directory_name, fname);
  fprintf(stdout, "Opening file %s to write\n", buf);
  g_outfile = fopen(buf, "w");
  if (!g_outfile) {
    fprintf(stdout, "fail to open output file %s\n", buf);
    exit(1);
  }
  fprintf(stdout, "Opened.\n");
}

void closeOutputFile() {
  if (g_outfile) { fclose(g_outfile); g_outfile = NULL; }
}

//---------------------------------------------------------------------------
// Configuration related: read and print
//---------------------------------------------------------------------------
static void print_config() {
  fprintf ( stdout, "Reducer Configuration\n" ) ;
  fprintf ( stdout, "--------------------\n" ) ;
  fprintf ( stdout, "Reducer attempt = %d\n", attemptId ) ;
  fprintf ( stdout, "NODELEN = %d\n", nodelen ) ;
  fprintf ( stdout, "Output directory = %s\n", output_dir ) ;
  fprintf ( stdout, "Thinning Percentage jump = %lf\n", pctJump ) ;
  fprintf ( stdout, "Partition = %d, stdtime extension = %s\n", partition_2, stdtime_extension ) ;
  fprintf ( stdout, "zfmd MD5_DATA_LINE_LENGTH ^%d\n", MD5_DIGEST_LENGTH);
  if (spatialSampleAttempt>0) {
    fprintf ( stdout, "Spatial sample attempt = %d\n", spatialSampleAttempt ) ;
    fprintf ( stdout, "Spatial sample buckets = %d\n", spatialSampleBuckets ) ;
  }
  if (spatialSampleAttempt==0) {
    fprintf ( stdout, "Spatial sampling in mapper.\n") ; 
    fprintf ( stdout, "Spatial sample buckets = %d\n", spatialSampleBuckets ) ;
  }
  else {
    fprintf ( stdout, "No spatial sampling in reducer\n") ;
  }
  if (spatialSampleBucketsOverride.size() > 0) {
    fprintf ( stdout, "Spatial sample override found\n") ;
    for ( std::map<string, std::pair<int,int> >::iterator it = spatialSampleBucketsOverride.begin() ;
          it != spatialSampleBucketsOverride.end(); it++ ) {
       const string &k = it->first ;
       int b1 = it->second.first ;
       int b2 = it->second.second ;
       fprintf ( stdout, "%s <%d,%d>\n" , k.c_str(), b1, b2) ;
    }
  }
  if (prewarmPeriod>0) {
    fprintf (stdout, "Prewarming period = %d sec\n", prewarmPeriod) ;
  }
  else {
    fprintf (stdout, "No prewarming period configured\n") ;
  }
   fprintf ( stdout, "\nSplitting:\n") ;
   fprintf ( stdout, "splitByCpcodes = %d, by url_bucket = %d, by mapid=%d, by arlid = %d, by account = %d, by vcd-map-network = %d\n",
                     splitByCpcodes, url_bucket, splitByMapid, splitByArlid, splitByAccount, splitByVcdMapNetwork) ;
   fprintf ( stdout, "Considering TTL: %d\n", considerTTL) ;

}

//---------------------------------------------------------------------------
// Durations and prewarming related complexity
//---------------------------------------------------------------------------

// What has been configured
int getConfiguredStartTime() { return start_time_config;}
int getConfiguredEndTime() { return end_time_config;}
int getConfiguredTotalTime() { return end_time_config-start_time_config;}
int getConfiguredPrewarmingPeriod() { return prewarmPeriod;}
int getConfiguredComputePeriod() { return end_time_config - start_time_config - prewarmPeriod;}
int getConfiguredStartOfComputeTime() { return start_time_config + prewarmPeriod; }

// What is the reality
int getActualStartTime() { return start_time;}
int getActualEndTime() { return last_time;}
int getActualStartOfComputeTime() {
  if (prewarmConfigRelative) {
    int t1 = getConfiguredStartOfComputeTime(); 
    int t2 = getActualStartTime() ;
    if (t1 < t2) {
      // Configured prewarming ends before logs start.
      // So no prewarming occurs. Return the start of log
      return t2; 
    } else {
      // Configured prewarming ends after logs start
      // So at least some prewarming occurs in reality, and 
      // it does indeed end where the configured prewarming ends. 
      // Return getConfiguredStartOfComputeTime
      return t1 ;
    }
  } else {
    return getActualStartTime() + prewarmPeriod ;
  }
}
int getActualTotalTime() { return getActualEndTime() - getActualStartTime(); }
int getActualPrewarmingPeriod() { return getActualStartOfComputeTime() - start_time; }
int getActualComputePeriod() { return last_time - getActualStartOfComputeTime() ; }

void printTimeDetails() {
  // This needs to be called after the last input line is processed.
  printf ("Configured times:\n") ; 
  printf ("Start = %d, End = %d, End of Prewarm = %d, Total duration = %d, Prewarm duration = %d, Compute duration = %d\n", 
          getConfiguredStartTime(), getConfiguredEndTime(), getConfiguredStartOfComputeTime(), 
          getConfiguredTotalTime(), getConfiguredPrewarmingPeriod(), getConfiguredComputePeriod() ) ; 
  printf ("Actual times:\n") ; 
  printf ("Start = %d, End = %d, End of Prewarm = %d, Total duration = %d, Prewarm duration = %d, Compute duration = %d\n", 
	  getActualStartTime(), getActualEndTime(), getActualStartOfComputeTime(), 
          getActualTotalTime(), getActualPrewarmingPeriod(), getActualComputePeriod() ) ;
}

bool inPrewarmingPhase(int t) {
  if (t < getActualStartOfComputeTime()) {
    return true;
  } else {
    return false ;
  }
}



void getActiveFootprintAfterPrewarming ( int &objcount, long long &bytes ) {

  objcount = 0 ; 
  bytes = 0 ;
  double prewarmEndTime = getActualStartOfComputeTime();
  for ( int i = 1 ; i <= max_id ; i++ ) {
    if (reftime[i] > prewarmEndTime) {
      objcount++ ;
      bytes += objsize[i] ;
    }
  }
}
  
//---------------------------------------------------------------------------
// Print the stdtime file
// If serial info files is enabled, then print that one too
//---------------------------------------------------------------------------
void print_stats (long long count, long long total_bytes, long long total_mib, 
                  char *extension, char *directory_name, 
                  long long sampleSkippedCount, long long sampleSkippedBytes ) {
  char buf[LINESIZE];
  int i;

  // If directory_name isn't "." then write to a local file, else stdout
  bool printToFile = strcmp(directory_name, ".") ;

  fprintf(stdout, "Entering print_stats.\n");
  if ( extension == NULL ) { 
    // Happens when splitting on a cpcode and this is a redundant reducer not corresponding to any cpcode
    fprintf(stdout, "Redundant reducer, skipping print_stats.\n");
    return ;
  }

  if ( count==0 || getActualComputePeriod() < 1 ) {
    // Empty stdtime
    fprintf(stdout, "Empty stdtime. Done with print_stats.\n");
    return ;
  }

  if (printToFile) { 
    prepareOutputDir (directory_name) ;
    char outFname[1024] ;
    sprintf (outFname, "stdtime.%s", extension) ;
    openOutputFile (directory_name, outFname) ;
  }


  // Figure out 1-copy to 2-copy transition bytes
  for (i = 0; i < NODELEN; i++) {
    if (iat[i].ghost0 && iat[i].ghost1 && iat[i].iatCount>0) {
      int avgiat = iat[i].iatSum / iat[i].iatCount;
      stack_distance_time_ph1[avgiat].Transition1to2_bytes += objsize[i];
      stack_distance_time_ph1[avgiat].Transition1to2_requests++;
    }
  }

  // Calculate the scale factors for unsharding
  double bytesScaleFactor = (sampleSkippedBytes+total_bytes)/total_bytes ;
  double countScaleFactor = (sampleSkippedCount+count)/count ;
  //double countScaleFactor = bytesScaleFactor ;
  if (spatialSampleAttempt==0) {
    // Sampling in mapper, but reducer should still unshard
    std::pair<string, string> mn = getMapNetworkFromExtension (extension) ;
    bytesScaleFactor = getSpatialSampleBuckets (mn.first.c_str(), mn.second.c_str(), OVERRIDE_UNSHARD) ;
    countScaleFactor = bytesScaleFactor ;
  }
  fprintf(stdout, "Unsharding scale factors are: bytesScaleFactor = %f, countScaleFactor = %f\n", bytesScaleFactor, countScaleFactor);

  int x = max_id; 
  long long y = uniqueMissBytes ;
  if (prewarmPeriod>0) {
    getActiveFootprintAfterPrewarming(x,y);
  }

  fprintf(stdout, "Writing header.\n");
  traffic_t peakTraffic = getPeaktraffic() ;
  sprintf(buf, "# %lld %lld %lld %lld %lld %lld %lld %lld\n", 
               (long long) (count*countScaleFactor), 
               (long long) (total_bytes*bytesScaleFactor), 
               (long long) getActualComputePeriod(), 
               (long long) (total_mib*bytesScaleFactor), 
               (long long) (x*countScaleFactor), 
               (long long) (y*bytesScaleFactor), 
               (long long) (peakTraffic.requests*countScaleFactor), 
               (long long) (peakTraffic.bytes*bytesScaleFactor));

  if (printToFile) {
    fprintf(g_outfile, "%s", buf);
  } else {
    printf ("%s", buf) ;
  }

  // Fields in stdtime
  int t;
  long long requests, ttl_expirations, bytes, mib, expected_stack_distance, expected_stack_distance_count, transition_bytes, transition_requests ;
  requests = ttl_expirations = bytes = mib = expected_stack_distance = expected_stack_distance_count = transition_bytes = transition_requests = 0 ;

  // Thinning related stuff
  // Requests, bytes, MIB, 1-2Transition Requests, 1-2Transition Bytes, cumulative sum since t=0
  long long R, B, MIB, TR, TB ;
  R = B = MIB = TR = TB = 0 ;

  // For the rows that were not printed due to thinning, how much was skipped
  long long skippedR, skippedB, skippedMIB, skipped_transition_bytes, skipped_transition_requests ;
  skippedR = skippedB = skippedMIB = skipped_transition_bytes = skipped_transition_requests = 0 ;

  // Pct increment since last written line, for each of delta-t, stack-distance-bytes, 
  // stack-distance-objects, request hit rate, byte hit rate
  double dtinc, stdinc, stdoinc, rhrinc, bhrinc ;
  dtinc = stdinc = stdoinc = rhrinc = bhrinc = 0 ;

  // The values written on the previous printed line for delta-t, stack-distance-bytes, stack-distance-objects, Requests, Bytes, 
  // 1-2Transition Requests, 1-2Transition Bytes, Request hit rate, byte hit rate
  long long pdt, pstd, pstdo, pR, pB, pTR, pTB; 
  double prhr, pbhr ;
  pdt = pstd = pstdo = pR = pB = pTR = pTB = prhr = pbhr = 0 ;

  // Must print the first line and the last line. 
  int firstTime = 1 ;

  // Last known timestamp with valid dataA
  int lkt ;

  // Iterate over the ph1 structure
  for (t = 0; t < timelen-1; t++) {
      if (stack_distance_time_ph1[t].requests) {
        // nonzero requests means that t is a valid iat observed
        lkt = t ; 
        requests = stack_distance_time_ph1[t].requests;
        ttl_expirations = stack_distance_time_ph1[t].ttl_expirations;
        bytes = stack_distance_time_ph1[t].bytes;
        mib = stack_distance_time_ph1[t].mib;
        expected_stack_distance = stack_distance_time_ph1[t].stdsum / (requests * SIZEUNIT);
        expected_stack_distance_count = stack_distance_time_ph1[t].stdosum / requests;
        transition_bytes = stack_distance_time_ph1[t].Transition1to2_bytes;
        transition_requests = stack_distance_time_ph1[t].Transition1to2_requests;

        // Compute hit rates based on cumulative info since the beginning
        R += requests ; 
	if (considerTTL) {
	  //fprintf (stdout, "Reducing requests by ttl expirations %lld - %lld  = %lld\n", R, ttl_expirations, (R-ttl_expirations)) ;
          R -= ttl_expirations ;
          requests -= ttl_expirations ;
        }
        B += bytes ;  
        MIB += mib ;
        TB += transition_bytes ;
        TR += transition_requests; 
        double rhr = R * 100.0 /  count ;
        double bhr = (B+MIB) * 100.0 /  total_bytes ;	// Wrong when considering mib
	// Complex calculation involving mib
        //long long served_bytes_to_right = total_bytes - B ;
        //long long mib_to_right = total_mib - MIB ;
        //double bhr = 100.0 - (served_bytes_to_right + mib_to_right) * 100.0 / total_bytes ;

        // Compute the % differences from the previously written line (if any)
        if (firstTime) {
          // Nothing written before. Initialize to indicate 100% change.
          dtinc = 100 ;
          stdinc = 100 ;     
          stdoinc = 100 ; 
          rhrinc = 100 ;
          bhrinc = 100 ;
        } else {
          dtinc = (t-pdt)*100.0/pdt ;
          stdinc = (expected_stack_distance - pstd)*100.0/pstd ;
          stdoinc = (expected_stack_distance_count-pstdo)*100.0/pstdo ;  
          rhrinc = (rhr-prhr)*100.0/prhr ;
          bhrinc = fabs((bhr-pbhr)*100.0/pbhr) ;
        }

        //fprintf(stdout, "Thin check: dtinc=%f stdinc=%f stdoinc=%f rhrinc=%f bhrinc=%f. pctJump=%f\n", dtinc, stdinc, stdoinc, rhrinc, bhrinc, pctJump) ;
        sprintf(buf, "%d %lld %lld %lld %lld %lld %lld\n", 
                t, 
                (long long) (expected_stack_distance*bytesScaleFactor), 
                (long long) (expected_stack_distance_count*countScaleFactor), 
                (long long) ((requests+skippedR)*countScaleFactor), 
                (long long) ((bytes+skippedB+mib+skippedMIB)*bytesScaleFactor), 
                (long long) ((transition_bytes+skipped_transition_bytes)*bytesScaleFactor), 
                (long long) ((transition_requests+skipped_transition_requests)*countScaleFactor));
        //if (dtinc>=pctJump||stdinc>=pctJump||stdoinc>=pctJump||rhrinc>=pctJump||bhrinc>=pctJump) {
        if (fabs(dtinc)>pctJump||fabs(stdinc)>=pctJump) {
          if (printToFile) {
            fprintf(g_outfile, "%s", buf);
          } else {
            printf ("%s", buf) ;
          }

          firstTime = 0 ;
          skippedR = skippedB = skippedMIB = skipped_transition_bytes = skipped_transition_requests = 0 ;
          pdt = t ; pstd = expected_stack_distance; pstdo = expected_stack_distance_count; pR = R ; pB = B ; prhr = rhr ; pbhr = bhr ;
        } else {
          // Skip it
          //fprintf(stdout, "%s skipped\n", buf) ;
          skippedR += requests;
          skippedB += bytes;
          skippedMIB += mib ;
          skipped_transition_requests += transition_requests;
          skipped_transition_bytes += transition_bytes;
        }
      }
  }
  // Check if the last line should be written
  if (t!=pdt && skippedR >0) {
    sprintf(buf, "%d %lld %lld %lld %lld %lld %lld\n", 
            lkt, 
            (long long) (expected_stack_distance*bytesScaleFactor), 
            (long long) (expected_stack_distance_count*countScaleFactor), 
            (long long) (skippedR*countScaleFactor), 
            (long long) ((skippedB+skippedMIB)*bytesScaleFactor), 
            (long long) (skipped_transition_bytes*bytesScaleFactor), 
            (long long) (skipped_transition_requests*countScaleFactor));
    //h->fputs(buf) ;
    if (printToFile) {
      fprintf(g_outfile, "%s", buf);
    } else {
      printf ("%s", buf) ;
    }
  }

  if (printToFile) {
    closeOutputFile();
  }
  fprintf(stdout, "Closed file. Done with print_stats.\n");

  if (printToFile && writeSerialInfoFile && perSerialInfo.size()>0) {
    char outFname[1024] ;
    char buf[LINESIZE];

    sprintf (outFname, "serialInfo.%s", extension) ;
    openOutputFile (directory_name, outFname) ;
    for (std::map<int,serial_info_t>::iterator it = perSerialInfo.begin(); it != perSerialInfo.end(); it++) {
      sprintf (buf, "%d %lld %lld %lld %lld %lld\n", it->first,
		    (long long) (it->second.served_requests*countScaleFactor), (long long) (it->second.served_bytes*bytesScaleFactor),
		    (long long) getActualComputePeriod(),
		    (long long) (it->second.unique_objectcount*countScaleFactor), (long long) (it->second.unique_objectsize*bytesScaleFactor) ) ;
      fprintf(g_outfile, "%s", buf);
    }
    closeOutputFile();
  }
}

//---------------------------------------------------------------------------
// reducerInit — call once per Spark partition before any batch processing.
// Reads config from env vars, sets up timelen, seeds the RNG.
//---------------------------------------------------------------------------
void reducerInit() {
  setIsMapper(false) ;
  readConfig() ;
  print_config() ;

  if (prewarmConfigRelative && start_time_config == 0) {
    fprintf(stdout, "Prewarm is relative to configured start time, but start time configuration is missing (or configured to be zero)\n") ;
    exit(1) ;
  }

  srandom(123456789) ;
  fprintf(stdout, "Ready to read lines from stdin\n") ;

  // Establish timelen if start_time and end_time are defined
  if (start_time_config != 0 && end_time_config != INT_MAX) {
    timelen = end_time_config - start_time_config + 2 ;
    fprintf(stdout, "end_time = %d, start_time = %d, timelen = %d\n",
            end_time_config, start_time_config, timelen) ;
  }

  if (spatialSampleAttempt > 0 && spatialSampleAttempt == attemptId) {
    g_doSpatialSampling = true ;
  }
}

//---------------------------------------------------------------------------
// processReducerBatch — call with each batch of newline-separated mapper
// output lines.  May be called many times; state accumulates across calls.
//---------------------------------------------------------------------------
void processReducerBatch(const char *inputBuf) {
  // Per-line local variables
  char s[LINESIZE] ;
  char md5_ascii[MD5_DIGEST_LENGTH*2+1] ;
  char md5_ascii_copy[MD5_DIGEST_LENGTH*2+1] ;
  unsigned char md5[MD5_DIGEST_LENGTH] ;
  int serial ;
  int trueSerial ;
  double timestamp ;
  int id ;
  long long size, bytes ;
  int cpc ;
  int max_age = -1 ;

  // Iterate over lines in the input buffer
  const char *cur = inputBuf ;
  while (*cur != '\0') {
    const char *nl = strchr(cur, '\n') ;
    size_t len = nl ? (size_t)(nl - cur) : strlen(cur) ;
    if (len == 0) { cur = nl ? nl + 1 : cur + len ; continue ; }
    if (len >= LINESIZE) len = LINESIZE - 1 ;
    memcpy(s, cur, len) ;
    s[len] = '\0' ;
    cur = nl ? nl + 1 : cur + len ;

    int rval = 0 ;
    double interval = 0 ;
    int i ;
    bool prewarming = false ;
    trueSerial = -1 ;

    g_readcount++ ;
    if ((rval = sscanf(s, "%d:%lf:%16s:%lld:%lld:%d:%d:%d", &serial,
                       &timestamp, md5_ascii, &size, &bytes, &cpc, &trueSerial, &max_age)) != 8) {
      if ((rval = sscanf(s, "%d:%lf:%16s:%lld:%lld:%d", &serial, &timestamp, md5_ascii, &size, &bytes, &cpc)) != 6) {
        fprintf(stdout, "Mapper sent a bad line to the reducer. Failed to parse line ^%s. Ignoring\n", s) ;
        continue ;
      }
    }

    if (!g_memory_allocated) {
      allocate_memory() ;
      g_memory_allocated = true ;
    }

    if (start_time == -1) start_time = timestamp ;
    last_time = timestamp ;

    strcpy(md5_ascii_copy, md5_ascii) ;

    // Is this md5 known before?
    for (i = 0; i < MD5_DIGEST_LENGTH; i++)
      sscanf((const char *)md5_ascii + i*2, "%02x", (unsigned int *)(md5 + i)) ;
    id = search_md5(md5) ;

    if (id == nodelen+1) {
      g_md5_count_exceeded = 1 ;
      fprintf(stdout, "zfmd - count exceed ... cont... ^%d\n", id) ;
      continue ;
    }

    if (id >= nodelen) {
      fprintf(stdout, "reach node limit %d %f\n", id, timestamp) ;
      exit(1) ;
    }

    if (id > max_id) max_id = id ;

    if (size <= 0) continue ;

    // Are we prewarming?
    if (inPrewarmingPhase(timestamp)) {
      prewarming = true ;
      g_prewarmSkippedCount++ ;
    }

    //---------------------------------------------------------------------------
    // If lines should be skipped, do it here
    //---------------------------------------------------------------------------

    // If spatial sampling is enabled to be done in the reducer
    if (g_doSpatialSampling) {
      size_t hash2 = std::hash<std::string>{}(md5_ascii_copy) ;
      std::pair<string, string> mn = getMapNetworkFromExtension(stdtime_extension) ;
      int ss = getSpatialSampleBuckets(mn.first.c_str(), mn.second.c_str(), OVERRIDE_FILTER) ;
      if (hash2 % ss > 0) {
        if (!prewarming) {
          g_sampleSkippedCount++ ;
          g_sampleSkippedBytes += bytes ;
        }
        continue ;
      }
    }

    if (g_md5_count_exceeded == 1) {
      //increment count to report the MAX count
      g_count++ ;
      if ((g_count % 1000000) == 0) {
        int oom_score = -1 ;
        fprintf(stdout, "skipped %lld :: md5_count %d ===", g_count, md5_count) ;
        read_oom_score(&oom_score) ;
        fprintf(stdout, "oom_score %d", oom_score) ;
        fprintf(stdout, "\n") ;
      }
      continue ;
    }

    //---------------------------------------------------------------------------
    // If lines should be skipped, do it before here
    //---------------------------------------------------------------------------

    g_count++ ;
    if (!prewarming) {
      g_total_bytes += bytes ;
      g_total_count++ ;
    }

    // Update per-serial info, if writeSerialInfoFile is true
    if (writeSerialInfoFile) {
      if (!prewarming) {
        perSerialInfo[trueSerial].served_requests++ ;
        perSerialInfo[trueSerial].served_bytes += bytes ;
      }
      if ((!prewarming && !seen[id]) ||
          (!prewarming && seen[id] && inPrewarmingPhase(reftime[id]) && !inPrewarmingPhase(timestamp))) {
        perSerialInfo[trueSerial].unique_objectcount++ ;
        perSerialInfo[trueSerial].unique_objectsize += size ;
      }
    }

    // Check after each million (i.e. 2^20) lines
    if ((g_count % 1048576) == 0) {
      statm_t mem_stat ;
      int oom_score = -1 ;
      fprintf(stdout, "processed %lld :: md5_count %d ===", g_count, md5_count) ;
      //read_off_memory_status(&mem_stat) ;
      fprintf(stdout, " size %ldkB rss %ldkB text %ldkB data %ldkB ",
              mem_stat.size*4096/1000, mem_stat.resident*4096/1000,
              mem_stat.text*4096/1000, mem_stat.data*4096/1000) ;
      fprintf(stdout, "\n") ;
    }

    bool skippingStackOnEntryBarrier = false ;
    if (!seen[id]) {
      // Not in stack
      if (entryProbabilityPct < 100.0 && random()%100 > entryProbabilityPct) {
        // Not entering this object into the stack.
        skippingStackOnEntryBarrier = true ;
        entryBarrierMissBytes += size ;
        entryBarrierMissRequests++ ;
        if (size - bytes > 0) {
          g_total_mib += (size - bytes) ;
        }
      } else {
        // This is the only place where we insert an object into the stack upon a cache miss.
        // If we're considering TTLs, make an entry in lastValidated
        if (considerTTL) lastValidated[id] = timestamp ;
        g_root = insert(g_count, g_root, size) ;
        objsize[id] = size ;
        if (!prewarming) {
          uniqueMissBytes += size ;
          if (size - bytes > 0) {
            g_total_mib += (size - bytes) ;
          }
        }
      }
    } else {
      // In stack
      if (size != objsize[id]) size = objsize[id] ;
      g_root = insert(seen[id], g_root, size) ;
      interval = (timestamp - reftime[id]) ;
      if ((int)interval < 0 || (int)interval >= timelen) {
        fprintf(stdout, "Longer than expected log, ignoring a line. previous reftime = %f, now = %f\n",
                reftime[id], timestamp) ;
        reftime[id] = timestamp ;
        continue ;
      }

      // Was the object stale in cache?
      int wasStale = 0 ;
      if (considerTTL && max_age > -1 && timestamp > lastValidated[id] + max_age) {
        wasStale = 1 ;
      }

      // Access to stale object — cache validates, lastValidated becomes current timestamp
      lastValidated[id] = timestamp ;

      // Compute distance and update TTL expiration count only if not prewarming
      if (!prewarming) {
        struct sizeObjsize s1 = {0, 0} ;
        long long distance_size = 0 ;
        long distance_count = 0 ;

        if (g_root->right != NULL) {
          s1 = getsizeObjsize(*(g_root->right)) ;
          distance_size = s1.objsize + size ;
          distance_count = s1.size + 1 ;
        }

        stack_distance_time_ph1[(int)interval].stdosum += distance_count ;
        stack_distance_time_ph1[(int)interval].stdsum  += distance_size ;
        stack_distance_time_ph1[(int)interval].requests++ ;
        stack_distance_time_ph1[(int)interval].ttl_expirations += wasStale ;
        stack_distance_time_ph1[(int)interval].bytes += bytes ;
        if (size - bytes > 0) {
          stack_distance_time_ph1[(int)interval].mib += (size - bytes) ;
          g_total_mib += (size - bytes) ;
        }
        g_non_empty = 1 ;
      }

      // Put object on top of the stack — prewarming or not
      g_root = treedelete(seen[id], g_root, size) ;
      g_root = insert(g_count, g_root, size) ;
    }

    if (!skippingStackOnEntryBarrier) {
      seen[id] = g_count ;
      reftime[id] = timestamp ;

      if (!prewarming) {
        iat[id].iatSum += interval ;
        iat[id].iatCount++ ;
        if (random() % 2 == 0) iat[id].ghost0 |= 1 ;
        else                   iat[id].ghost1 |= 1 ;
      }
    }
  } // end while over lines in inputBuf
}

//---------------------------------------------------------------------------
// reducerFinalize — call after all batches.  Writes output files, logs stats.
//---------------------------------------------------------------------------
void reducerFinalize() {
  fprintf(stdout, "Done processing input. Read %lld lines, of which %lld skipped due to spatial sampling, %lld skipped in prewarming\n",
          g_readcount, g_sampleSkippedCount, g_prewarmSkippedCount) ;
  fprintf(stdout, "%lld requests, %lld bytes bypassed cache due to entry barrier.\n",
          entryBarrierMissRequests, entryBarrierMissBytes) ;
  printTimeDetails() ;

  if (g_total_bytes == 0) g_non_empty = 0 ;

  if (g_non_empty) {
    if (g_md5_count_exceeded) {
      fprintf(stdout, "md5 count exceeded, skipping everything\n") ;
    } else {
      bool writeStdtime = true ;
      double frac1 = g_total_bytes * 1.0 / (g_total_bytes + g_sampleSkippedBytes) ;
      double frac2 = g_total_count * 1.0 / (g_total_count + g_sampleSkippedCount) ;
      double frac3 = 0.7 ;
      std::pair<string, string> mn = getMapNetworkFromExtension(stdtime_extension) ;
      int ssb = getSpatialSampleBuckets(mn.first.c_str(), mn.second.c_str(), OVERRIDE_UNSHARD) ;
      if (ssb > 0) frac3 = 0.7 / ssb ;

      fprintf(stdout, "total_bytes = %lld, sampleSkippedBytes = %lld, total count = %lld, sampleSkippedCount = %lld\n",
              g_total_bytes, g_sampleSkippedBytes, g_total_count, g_sampleSkippedCount) ;
      fprintf(stdout, "bytes frac = %f, count frac = %f\n", frac1, frac2) ;
      if (g_sampleSkippedCount > 0 && frac1 <= frac3) {
        writeStdtime = false ;
        fprintf(stdout, "Decided to skip writing stdtime, because spatial sampling fractions appear out of whack\n") ;
      }
      if (g_sampleSkippedCount == 0 || writeStdtime) {
        print_stats(g_total_count, g_total_bytes, g_total_mib, stdtime_extension, output_dir,
                    g_sampleSkippedCount, g_sampleSkippedBytes) ;
      }
    }
  } else {
    // Empty
    print_stats(0, 0, 0, stdtime_extension, output_dir, 0, 0) ;
  }

  // Reset all state so the next reducerInit() starts from a clean slate.
  // Essential when multiple reducer partitions run sequentially in the same
  // JVM (Spark local mode or multi-task executors on Kubernetes).
  reducerReset() ;
  configReset() ;
}

//---------------------------------------------------------------------------
// Main loop — standalone entry point (not compiled into shared lib)
//---------------------------------------------------------------------------
#ifndef BUILD_SHARED_LIB
int main(int argc, char *argv[]) {
  reducerInit() ;

  // Feed stdin line by line into processReducerBatch
  char linebuf[LINESIZE] ;
  while (fgets(linebuf, LINESIZE, stdin))
    processReducerBatch(linebuf) ;

  reducerFinalize() ;
  return 0 ;
}
#endif // BUILD_SHARED_LIB
