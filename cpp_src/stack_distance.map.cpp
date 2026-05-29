#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <sys/stat.h>
#include <map>
#include <vector>
#include <climits>
#include <functional>
#include <string>
#include "stack_distance.config.h"
#include "stack_distance.map.h"

using namespace std ;

//---------------------------------------------------------------------------
// Print configuration
//---------------------------------------------------------------------------
void ip2str ( int ip, char *buf ) {
  int i1, i2, i3, i4;
  i1 = ip & 0xff000000 >> 24 ;
  i2 = ip & 0x00ff0000 >> 16 ;
  i3 = ip & 0x0000ff00 >> 8 ;
  i4 = ip & 0x000000ff ;
  sprintf (buf, "%d.%d.%d.%d", i1, i2, i3, i4) ; 
}

static void print_config() {

   fprintf ( stderr, "Mapper Configuration\n" ) ;
   fprintf ( stderr, "--------------------\n" ) ;
   fprintf ( stderr, "Network = %d. (0 = FF 1 = ESSL 2 = CW)\n", network ) ;
   fprintf ( stderr, "Filters:\n") ;
   fprintf ( stderr, "start timestamp = %d, end timestamp = %d.\n", start_time_config, end_time_config) ;
   fprintf ( stderr, "Maps: ") ;
   if (num_maps > 50) {
     fprintf ( stderr, "%d maps, too many to list\n", num_maps) ;
   } else {
     for (int i = 0 ; i < num_maps; i++) {
        fprintf ( stderr, "%s ", map_names[i]) ;
     }
     fprintf ( stderr, "\n") ;
   }
   fprintf ( stderr, "Mapids: ") ;
   if (mapids.size()>50) {
     fprintf ( stderr, "%ld mapids, too many to list\n", mapids.size()) ;
   } else {
     for (std::map<int,int>::iterator it = mapids.begin(); it != mapids.end(); it++) {
       fprintf ( stderr, "%d ", it->first ) ;
     }
   }
   fprintf ( stderr, "\n") ;
   fprintf ( stderr, "Cpcodes: ") ;
   if ( numcpcodes>50 ) {
     fprintf ( stderr, "%d cpcodes, too many to list\n", numcpcodes) ;
   } else {
     for (std::map<int,int>::iterator it = cpcodes.begin(); it != cpcodes.end(); it++ ) {
       fprintf ( stderr, "%d ", it->first) ;
     }
     fprintf ( stderr, "\n") ;
   }
   fprintf ( stderr, "VCDs: ") ;
   if ( numvcds>50 ) {
     fprintf ( stderr, "%d vcds, too many to list\n", numcpcodes) ;
   } else {
     for (std::map<int,int>::iterator it = vcds.begin(); it != vcds.end(); it++ ) {
       fprintf ( stderr, "%d ", it->first) ;
     }
     fprintf ( stderr, "\n") ;
   }
   fprintf ( stderr, "Regions: ") ;
   if ( numregions>50 ) {
     fprintf ( stderr, "%d regions, too many to list\n", numregions) ;
   } else {
     for (std::map<int,int>::iterator it = regions.begin(); it != regions.end(); it++ ) {
       fprintf ( stderr, "%d ", it->first) ;
     }
     fprintf ( stderr, "\n") ;
   }
   fprintf ( stderr, "Ghosts:\n" ) ;
   if (ghosts.size() > 0) {
     for (std::map<int,int>::iterator it = ghosts.begin(); it != ghosts.end(); it++ ) {
       char ip[32] ;
       ip2str(it->first,ip) ;
       fprintf ( stderr, "%s ", ip) ; 
     }
     fprintf ( stderr, "\n") ;
   }
   fprintf ( stderr, "filter_combine = %d\n", filter_combine ) ;
   fprintf ( stderr, "Process misses only = %d\n", processMissesOnly ) ;
   fprintf ( stderr, "Compute both served and miss FDs = %d\n", computeBothServedAndMiss ) ;
   fprintf ( stderr, "Ignore ICP log lines = %d\n", ignoreICP ) ;
   fprintf ( stderr, "Ignore Prefetch log lines = %d\n", ignorePrefetch ) ;
   if (ignorePrefetchOverride.size() > 0) {
     fprintf ( stderr, "Ignore_Prefetch override found\n") ;
     for ( std::map<string,bool>::iterator it = ignorePrefetchOverride.begin() ; 
           it != ignorePrefetchOverride.end(); it++ ) {
        const string &k = it->first ;
        bool b = it->second ;
        fprintf ( stderr, "%s %s\n" , k.c_str(), b==true?"true":"false") ;
     }
   }
   fprintf ( stderr, "\nSplitting:\n") ;
   fprintf ( stderr, "splitByCpcodes = %d, by url_bucket = %d, by mapid=%d, by arlid = %d, by account = %d, by vcd-map-network = %d\n", 
                     splitByCpcodes, url_bucket, splitByMapid, splitByArlid, splitByAccount, splitByVcdMapNetwork) ;
   if ( serial_start.size() == 0 ) {
     fprintf ( stderr, "No serials for rehashing\n" ) ;
   } else {
     fprintf ( stderr, "Serials for rehashing:\n" ) ;
     if (serial_start.size() > 50 ){
       fprintf ( stderr, "%ld rehashing serials, too many to print\n", serial_start.size() ) ;
     }
     else {
       for ( std::map<int,int>::iterator it = serial_start.begin() ; it != serial_start.end(); it++ ) {
         int s = it->first ;
         fprintf ( stderr, "%d: start at %d, range size %d\n", s, serial_start[s], serial_bucket[s] ) ;
       }
     }
     fprintf ( stderr, "rehash_only = %d\n", rehash_only ) ;
   }
   if (spatialSampleAttempt==0) { 
     fprintf ( stderr, "Spatial Sample Attempt = %d\n", spatialSampleAttempt ) ;
     fprintf ( stderr, "Spatial Sample Buckets = %d\n", spatialSampleBuckets ) ;
     if (spatialSampleBucketsOverride.size() > 0) {
       fprintf ( stderr, "Spatial sample override found\n") ;
       for ( std::map<string, std::pair<int,int> >::iterator it = spatialSampleBucketsOverride.begin() ; 
             it != spatialSampleBucketsOverride.end(); it++ ) {
          const string &k = it->first ;
          int b1 = it->second.first ;
          int b2 = it->second.second ;
          fprintf ( stderr, "%s <%d,%d>\n" , k.c_str(), b1, b2) ;
       }
     }
   } else {
    fprintf ( stderr, "No spatial sampling in mapper\n") ;
   }
   if (splitByVcdMapNetwork) {
       fprintf ( stderr, "Number of vcd-map-network entries in config = %d\n", numVcdMapNetwork) ;
   }
}


//--------------------------------------------------------------------------- 
// Is this a map or a mapid?
//--------------------------------------------------------------------------- 
static bool isMapid (char *map, int &mapid) {
  char *endptr ;
  int b = strtol(map, &endptr, 10) ;
  // The whole string must be valid.
  if (b>0 && *map != '\0' && *endptr == '\0') {
    mapid = b ;
    return true ;
  }
  return false ;
}

//---------------------------------------------------------------------------
// Do filtering
//---------------------------------------------------------------------------
bool doFilter ( double timestamp, char *map, int cpcode, int vcd, int region, int &matched_map_index) {
  int match = 0 ;
  int match_map = 0;
  int match_cpcode = 0;
  int match_vcd = 0;
  int match_region = 0;
  int mapid = 0 ;

  // Initialize to no map match
  matched_map_index = -1 ;

  // Check if timestamp in range
  if (timestamp < start_time_config || timestamp > end_time_config) {
    return false ;
  }
  
  // Check if map matches
  bool itsAMapid = isMapid(map, mapid) ;
  if (itsAMapid) {
    if (mapids.find(mapid)!=mapids.end()) {
      match_map = 1 ; 
      matched_map_index = mapid2m_index[mapid] ;
    }
  } 
  else {
    for (int i = 0 ; i < num_maps; i++) {
      if (strcmp(map, map_names[i]) == 0) {
        match_map = 1 ;
        matched_map_index = map_index[i] ;
        break ;
      }
    }
  }
  // Check if cpcode matches
  if (numcpcodes>0) {
    bool inMap = (cpcodes.find(cpcode) != cpcodes.end()) ;
    bool mc = (exclude_cpcodes ? !inMap : inMap) ;
    match_cpcode = mc ? 1 : 0 ;
  }

  // check if vcd matches
  if (numvcds>0) {
    if (vcd == -1) {
      fprintf (stderr, "Filtering by VCD specified, but no vcd found in log line\n") ;
			exit(1);
	  }
    bool inMap = (vcds.find(vcd) != vcds.end()) ;
    bool mv = (exclude_vcds ? !inMap : inMap) ;
    match_vcd = mv ? 1 : 0;
  }

  // Check if regions matches
  
  if (numregions>0) {
    if (region == -1) { 
      match_region = 1 ;
    } else {
      bool inMap = (regions.find(region) != regions.end()) ;
      bool mv = (exclude_regions ? !inMap : inMap) ;
      match_region = mv ? 1 : 0;
    }
  }
  
  if (filter_combine==0) {
    // OR the filters
    match = match_map + match_cpcode + match_vcd + match_region ;
  } else { 
    match = 0 ;
    // AND the match flags, but ignore a filter if it hasn't been specified
    if (num_maps == 0 && mapids.size()==0) match_map = 1 ;
    if (numcpcodes == 0) match_cpcode = 1 ;
    if (numvcds == 0) match_vcd = 1 ;
    if (numregions == 0 ) match_region = 1 ;

    match = match_map * match_cpcode * match_vcd * match_region ;
  }
  return match ;
} 


//---------------------------------------------------------------------------
// Resolve ghost IP from a log filename.
// Returns the match_ip index from the ghosts map, or -1 if no ghost filter
// is configured, or -2 if the file belongs to a ghost NOT in the list
// (caller should skip all lines in this case).
//---------------------------------------------------------------------------
static int resolveGhostIp(const char *inputFilename) {
  int numghosts = (int)ghosts.size() ;
  if (numghosts == 0) return -1 ;           // no ghost filter configured

  char filenameCopy[4096] ;
  strncpy(filenameCopy, inputFilename, sizeof(filenameCopy)-1) ;
  filenameCopy[sizeof(filenameCopy)-1] = '\0' ;
  char *base = basename(filenameCopy) ;

  int i1, i2, i3, i4, ip ;
  if (sscanf(base, "fd_storelog_%d.%d.%d.%d_", &i1, &i2, &i3, &i4) != 4) {
    int reg ;
    if (sscanf(base, "fd_storelog_%d_%d.%d.%d.%d_", &reg, &i1, &i2, &i3, &i4) != 5) {
      fprintf(stderr, "resolveGhostIp: cannot parse IP from filename '%s'\n", base) ;
      return -2 ;
    }
  }
  ip = (i1 << 24) | (i2 << 16) | (i3 << 8) | i4 ;
  if (ghosts.find(ip) == ghosts.end()) {
    fprintf(stderr, "Skipping log from ghost %d.%d.%d.%d, not in specified list of ghostips.\n",
            i1, i2, i3, i4) ;
    return -2 ;                             // ghost not in list — skip file
  }
  return ghosts[ip] ;
}

//---------------------------------------------------------------------------
// mapperInit — call once per Spark partition (or once for the whole file in
// standalone tests).  Reads config from env vars, resolves ghost IP.
// Returns a heap-allocated MapperCtx that the caller owns.
// inputFilename: path/name of the log file being processed (used for ghost
//               IP resolution only).  Pass "" or NULL if no ghost filter.
//---------------------------------------------------------------------------
MapperCtx *mapperInit(const char *inputFilename) {
  setIsMapper(true) ;
  readConfig() ;
  print_config() ;
  fprintf(stderr, "Starting mapper\n") ;

  MapperCtx *ctx = new MapperCtx() ;
  ctx->allLinesCount     = 0 ;
  ctx->invalidLinesCount = 0 ;
  ctx->ignoredLinesCount = 0 ;
  ctx->skipAllLines      = false ;

  if (inputFilename && inputFilename[0] != '\0') {
    int r = resolveGhostIp(inputFilename) ;
    if (r == -2) {
      ctx->skipAllLines = true ;   // wrong ghost — Spark should still drain input
      ctx->match_ip = -1 ;
    } else {
      ctx->match_ip = r ;          // -1 = no ghost filter, >=0 = ghost index
    }
  } else {
    ctx->match_ip = -1 ;
  }
  return ctx ;
}

//---------------------------------------------------------------------------
// processLogLinesBatch — call repeatedly with batches of newline-separated
// log lines.  Appends mapper output lines to outbuf.  Updates stats in ctx.
//---------------------------------------------------------------------------
void processLogLinesBatch(MapperCtx *ctx, const char *inputBuf, string &outbuf) {
  if (ctx->skipAllLines) return ;

  int match_ip  = ctx->match_ip ;
  int numghosts = (int)ghosts.size() ;

  char line[LOG_LINE_LENGTH];
  char map[LOG_LINE_LENGTH];
  char objstatus[LOG_LINE_LENGTH] ;
  short serial;
  double timestamp;
  char md5[LOG_LINE_LENGTH];
  long long size;
  long long bytes;
  int cpcode;
  int hash;
  int max_age = -1 ;

  int fds_arlid = -1 ;
  char fds_network[LOG_LINE_LENGTH] ;
  char fds_mapname[LOG_LINE_LENGTH] ;
  int fds_region = -1 ;
  int fds_vcd = -1 ;
  char fds_product[LOG_LINE_LENGTH] ;

  // Walk inputBuf line by line without modifying it
  const char *cur = inputBuf ;
  while (*cur != '\0') {
    // Copy one line into 'line'
    const char *nl = strchr(cur, '\n') ;
    size_t len = nl ? (size_t)(nl - cur) : strlen(cur) ;
    if (len >= LOG_LINE_LENGTH) len = LOG_LINE_LENGTH - 1 ;
    memcpy(line, cur, len) ;
    line[len] = '\0' ;
    cur = nl ? nl + 1 : cur + len ;
    if (line[0] == '\0') continue ;        // skip blank lines between batches

    int arlid ;
    char sro_mapname[256] ;
    char network = ' ' ;
    int trueSerial ;

    ctx->allLinesCount++ ;
    objstatus[0] = '\0' ;
    max_age = -1 ;

    // Lines that start with a # are comments
    if (line[0] == '#') { ctx->invalidLinesCount++ ; continue ; }

    if (randomSamplingPct < 100) {
      if ((random() % 100) >= randomSamplingPct) { ctx->ignoredLinesCount++ ; continue ; }
    }

    fds_network[0] = '\0' ;
    fds_mapname[0] = '\0' ;
    if (sscanf(line, "%[^:]:%hd:%lf:%[^:]:%lld:%lld:%d:%[^:]:%d:%[^:]:%[^:]:%d:%d:%s",
                     map, &serial, &timestamp, md5, &size, &bytes, &cpcode, objstatus,
                     &fds_arlid, fds_network, fds_mapname, &fds_region, &fds_vcd, fds_product) != 14) {
      if (sscanf(line, "%[^:]:%hd:%lf:%[^:]:%lld:%lld:%d:%[^:]:%d:%[^:]:%[^:]:%d:%d",
                       map, &serial, &timestamp, md5, &size, &bytes, &cpcode, objstatus,
                       &fds_arlid, fds_network, fds_mapname, &fds_region, &max_age) != 13) {
        if (sscanf(line, "%[^:]:%hd:%lf:%[^:]:%lld:%lld:%d:%s",
                   map, &serial, &timestamp, md5, &size, &bytes, &cpcode, objstatus) != 8) {
          if (sscanf(line, "%[^:]:%hd:%lf:%[^:]:%lld:%lld:%d",
                     map, &serial, &timestamp, md5, &size, &bytes, &cpcode) != 7) {
            ctx->invalidLinesCount++ ; continue ;
          }
        }
      }
    }
    char *junkptr = strchr(objstatus, ':') ;
    if (junkptr) *junkptr = '\0' ;

    if (serial <= 0) { ctx->invalidLinesCount++ ; continue ; }

    if (spatialSampleAttempt == 0) {
      size_t hash2 = std::hash<std::string>{}(md5) ;
      if (hash2 % getSpatialSampleBuckets(fds_mapname, fds_network, OVERRIDE_FILTER)) {
        ctx->ignoredLinesCount++ ; continue ;
      }
    }

    trueSerial = serial ;
    serial = translateSerial(serial) ;

    if (rehash_only && serial_start.find(serial) == serial_start.end()) {
      ctx->ignoredLinesCount++ ; continue ;
    }

    bool isCacheMiss = false ;
    long long bytesForServedFD = 0 ;
    long long bytesForMissFD  = 0 ;
    bytesForServedFD = bytesForMissFD = bytes ;
    if (strchr(objstatus, 'o') != NULL || strchr(objstatus, 'p') != NULL)
      isCacheMiss = true ;

    if (processMissesOnly) {
      if (!isCacheMiss) { ctx->ignoredLinesCount++ ; continue ; }
      else { if (bytes < size) { bytes = size ; bytesForMissFD = size ; } }
    }
    if (computeBothServedAndMiss && isCacheMiss && bytes < size)
      bytesForMissFD = size ;

    if (ignoreICP && strchr(objstatus, 'g') != NULL) {
      ctx->ignoredLinesCount++ ; continue ;
    }

    bool isPrefetch = (strchr(objstatus, 'K') != NULL) ;
    bool ignoreForServedFD = false ;
    bool ignoreForMissFD   = false ;
    if (processMissesOnly || computeBothServedAndMiss) {
      ignoreForServedFD = (getIgnorePrefetch(fds_mapname, fds_network) && isPrefetch) ;
      ignoreForMissFD   = !isCacheMiss ;
    } else if (getIgnorePrefetch(fds_mapname, fds_network) && isPrefetch) {
      ctx->ignoredLinesCount++ ; continue ;
    }

    if (fds_vcd == 0 && arlid2vcd.find(fds_arlid) != arlid2vcd.end())
      fds_vcd = arlid2vcd[fds_arlid] ;

    int matched_map_index ;
    bool match = doFilter(timestamp, map, cpcode, fds_vcd, fds_region, matched_map_index) ;
    if (match) {
      char emitbuf[2048] ;
#define EMIT(fmt, ...) do { \
        snprintf(emitbuf, sizeof(emitbuf), fmt, ##__VA_ARGS__) ; \
        outbuf += emitbuf ; \
      } while(0)

      if (url_bucket) {
        hash = 0 ;
        if (url_bucket > 1) { sscanf(md5, "%04x", &hash) ; hash %= url_bucket ; }
        if (map_index.size() == 0) matched_map_index = 0 ;
        if (matched_map_index != -1) {
          if (numghosts > 0)
            EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n",
                 (int)(matched_map_index*url_bucket*numghosts+match_ip*url_bucket+hash),
                 timestamp, md5, size, bytes, cpcode, trueSerial, max_age) ;
          else
            EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n",
                 (int)(matched_map_index*url_bucket+hash),
                 timestamp, md5, size, bytes, cpcode, trueSerial, max_age) ;
          continue ;
        }
      }
      if (splitByCpcodes) {
        int cp2id = cpcodes[cpcode] ;
        int id1 = cp2id, id2 = numcpcodes + cp2id ;
        if (!processMissesOnly && !ignoreForServedFD)
          EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", id1, timestamp, md5, size, bytesForServedFD, cpcode, trueSerial, max_age) ;
        if (processMissesOnly && !ignoreForMissFD)
          EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", id1, timestamp, md5, size, bytesForMissFD,  cpcode, trueSerial, max_age) ;
        if (computeBothServedAndMiss && !ignoreForMissFD)
          EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", id2, timestamp, md5, size, bytesForMissFD,  cpcode, trueSerial, max_age) ;
        continue ;
      }
      if (splitByMapid) {
        EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", matched_map_index, timestamp, md5, size, bytes, cpcode, trueSerial, max_age) ;
        continue ;
      }
      if (serial_start.find(serial) != serial_start.end()) {
        sscanf(md5, "%02x", &hash) ;
        hash %= serial_bucket[serial] ;
        EMIT("%hd:%.3lf:%s:%lld:%lld:%d:%d:%d\n", serial_start[serial]+hash, timestamp, md5, size, bytes, cpcode, trueSerial, max_age) ;
        continue ;
      }
      if (splitByArlid || splitByAccount) {
        if (cpcode2accountid.find(cpcode) != cpcode2accountid.end()) {
          int cp2id = cpcode2accountid[cpcode] ;
          int id1 = cp2id, id2 = numAccounts + id1 ;
          if (!processMissesOnly && !ignoreForServedFD)
            EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", id1, timestamp, md5, size, bytesForServedFD, cpcode, trueSerial, max_age) ;
          if (processMissesOnly && !ignoreForMissFD)
            EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", id1, timestamp, md5, size, bytesForMissFD,  cpcode, trueSerial, max_age) ;
          if (computeBothServedAndMiss && !ignoreForMissFD)
            EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", id2, timestamp, md5, size, bytesForMissFD,  cpcode, trueSerial, max_age) ;
          continue ;
        } else { ctx->ignoredLinesCount++ ; continue ; }
      }
      if (splitByVcdMapNetwork) {
        char key[1024] ;
        snprintf(key, sizeof(key), "%d^%s^%s", fds_vcd, fds_mapname, fds_network) ;
        std::map<string,int>::iterator it = vcdMapNetwork2id.find(key) ;
        if (it != vcdMapNetwork2id.end()) {
          int id1 = it->second, id2 = numVcdMapNetwork + id1 ;
          if (!processMissesOnly && !ignoreForServedFD)
            EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", id1, timestamp, md5, size, bytesForServedFD, cpcode, trueSerial, max_age) ;
          if (processMissesOnly && !ignoreForMissFD)
            EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", id1, timestamp, md5, size, bytesForMissFD,  cpcode, trueSerial, max_age) ;
          if (computeBothServedAndMiss && !ignoreForMissFD)
            EMIT("%d:%.3lf:%s:%lld:%lld:%d:%d:%d\n", id2, timestamp, md5, size, bytesForMissFD,  cpcode, trueSerial, max_age) ;
          continue ;
        } else { ctx->ignoredLinesCount++ ; continue ; }
      }
      // None of the special splitting conditions apply
      EMIT("%hd:%.3lf:%s:%lld:%lld:%d:%d:%d\n", serial, timestamp, md5, size, bytes, cpcode, trueSerial, max_age) ;
#undef EMIT
    } else {
      ctx->ignoredLinesCount++ ;
    }
  }
}

//---------------------------------------------------------------------------
// mapperFinalize — call once after all batches.  Logs stats and frees ctx.
//---------------------------------------------------------------------------
void mapperFinalize(MapperCtx *ctx) {
  fprintf(stderr, "Lines read %lld, invalid lines %lld, ignored lines %lld\n",
          ctx->allLinesCount, ctx->invalidLinesCount, ctx->ignoredLinesCount) ;
  delete ctx ;
}
