#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <string>
#include <sstream>
#include <set>
#include <utility>
#include <cctype>
#include "stack_distance.config.h"

//---------------------------------------------------------------------------
// All the configuration parameters.
// thread_local: each executor thread (Spark task) gets its own isolated copy.
//---------------------------------------------------------------------------
thread_local int network = 0 ; // 0 = FF, 1 ESSL, 2 Cloud Wrapper
thread_local int MAX_SERIAL = MAX_FF_SERIAL ;

//---------------------------------------------------------------------------
// How to filter logs
//---------------------------------------------------------------------------
thread_local int start_time_config = 0;
thread_local int end_time_config = INT_MAX;
thread_local int prewarmPeriod = 0 ;
thread_local bool prewarmConfigRelative = false ;
thread_local char maprules[LOG_LINE_LENGTH];
thread_local std::map<int,char *> map_names ;
thread_local std::map<int,int> map_index ;
thread_local std::map<int,int> mapid2m_index ;
thread_local int num_maps = 0 ;
thread_local std::map<int,int> mapids ;
thread_local std::map<int,int> cpcodes;
thread_local int numcpcodes = 0 ;
thread_local bool exclude_cpcodes = false ;
thread_local std::map<int,int> vcds ;
thread_local int numvcds = 0 ;
thread_local bool exclude_vcds = false ;
thread_local std::map<int,int> regions ;
thread_local int numregions = 0 ;
thread_local bool exclude_regions = false ;
thread_local std::map<int,int> ghosts;
thread_local int filter_combine = 1 ;
thread_local bool processMissesOnly = false ;
thread_local bool computeBothServedAndMiss = false ;
thread_local bool ignoreICP = true ;
thread_local bool ignorePrefetch = false ;
thread_local std::map<string, bool> ignorePrefetchOverride ;
thread_local bool writeSerialInfoFile = false ;
thread_local double randomSamplingPct = 100.0 ;

thread_local bool splitByVcdMapNetwork = false ;
thread_local std::map<string,int> vcdMapNetwork2id ;
thread_local std::map<int, string> vcdMapNetworkId2Extension ;
thread_local int numVcdMapNetwork = 0 ;

thread_local bool considerTTL = false ;
thread_local double entryProbabilityPct = 100.0;
thread_local bool region_ip_in_log = false ;
thread_local bool isMapper = false ;

//---------------------------------------------------------------------------
// How mapper should split logs
//---------------------------------------------------------------------------
thread_local int url_bucket = 0 ;
thread_local int splitByCpcodes = 0 ;
thread_local int splitByMapid = 0 ;
thread_local int splitByArlid = 0 ;
thread_local int splitByAccount = 0 ;
thread_local std::map<int,int> serial_start ;
thread_local std::map<int,int> serial_bucket ;
thread_local std::map<int,int> ESSLModulusExceptions ;
thread_local int default_serial_bucket = 8;
thread_local int rehash_only = 0 ;
thread_local std::map<int,int> rehashSerialIndex ;
thread_local int rehash_from_0 = 0 ;
thread_local int rehash_range_start = MAX_FF_SERIAL ;
thread_local int deduce_serial = 0 ;

// Thinning related
thread_local double pctJump = 0.0 ;

// Spatial sampling related
thread_local int attemptId = 0 ;
thread_local int spatialSampleAttempt = -1 ;
thread_local int spatialSampleBuckets = 1 ;
thread_local std::map<string, std::pair<int,int> > spatialSampleBucketsOverride ;

thread_local std::map<string,int> account2id ;
thread_local std::map<int,int> cpcode2accountid ;
thread_local int numAccounts = 0 ;
thread_local std::map<int,int> arlid2vcd ;

//---------------------------------------------------------------------------
// How reducer should allocate memory and write stdtime files
//---------------------------------------------------------------------------
thread_local int nodelen ;
thread_local char *output_dir = nullptr ;
thread_local int partition_2 = 0 ;
thread_local char stdtime_extension[1024] ;
thread_local int numReducers = -1;

void splitPair (string a, char sep, int &rangeStart, int &rangeEnd) {

  // Integer range with 2 integers
  vector<string> y = split(a, sep) ;
  if (y.size() != 2) {
    // Bad range in config, exit
    fprintf(stderr, "Invalid mapid integer range in FD_MAPREDUCE_MAPRULES: %s.\n", a.c_str());
    exit(1);
  }
  
  const char *nptr1 = y.at(0).c_str(); 
  const char *nptr2 = y.at(1).c_str(); 
  char *endptr1, *endptr2 ;
  
  int d = strtol(nptr1, &endptr1, 10); 
  int e = strtol(nptr2, &endptr2, 10); 

  if (d > 0 && *nptr1!='\0' && *endptr1 == '\0' && 
      e > 0 && *nptr2!='\0' && *endptr2 == '\0' && 
      d != e )  {
      rangeStart = (d<e)?d:e ;
      rangeEnd   = (d<e)?e:d ;
   }
   else { 
    // Bad range in config, exit
    fprintf(stderr, "Invalid mapid integer range in FD_MAPREDUCE_MAPRULES: %s.\n", a.c_str());
    exit(1);
  }
}


//---------------------------------------------------------------------------
// Config related functions
//---------------------------------------------------------------------------
int str2ip ( char *p ) {
  int i1, i2, i3, i4, ip;
  if (sscanf(p, "%d.%d.%d.%d", &i1, &i2, &i3, &i4) != 4) {
    fprintf(stderr, "Invalid ghost ip provided: %s\n", p);
    exit(1);
  }
  ip = (i1 << 24) | (i2 << 16) | (i3 << 8) | i4;
  return ip ;
}

vector<string> split(string s, char delim) {
  stringstream ss(s);
  string item ;
  vector<string> ret ;
  while (getline(ss, item, delim)) {
    ret.push_back(item);
  }
  return ret ;
}


void readConfig () { 

  char *value ;

  value = getenv("FD_MAPREDUCE_REGION_IP_IN_LOG") ;
  if (value) {
    if (!strcasecmp(value,"true") || !strcmp(value, "1")) {
      region_ip_in_log = true ;
    }
    else {
      region_ip_in_log = false ;
    }
  }

  value = getenv("FD_MAPREDUCE_CONSIDER_TTL") ;
  if (value) {
    if (!strcasecmp(value,"true") || !strcmp(value, "1")) {
      considerTTL = true ;
    }
    else {
      considerTTL = false ;
    }
  }

  value = getenv("FD_MAPREDUCE_ENTRY_PROBABILITY_PCT") ;
  if (value) {
    if (sscanf (value, "%lf", &entryProbabilityPct) != 1 ){
      fprintf(stderr, "Cannot parse double value in FD_MAPREDUCE_ENTRY_PROBABILITY_PCT: %s\n", value ) ;
      exit(1);
    }
  }

  value = getenv("FD_MAPREDUCE_RANDOM_SAMPLING_PCT") ;
  if (value) {
    if (sscanf (value, "%lf", &randomSamplingPct) != 1 ){
      fprintf(stderr, "Cannot parse double value in FD_MAPREDUCE_RANDOM_SAMPLING_PCT: %s\n", value ) ;
      exit(1);
    }
  }

  value = getenv("FD_MAPREDUCE_WRITE_SERIAL_INFO_FILE") ;
  if (value) {
    if (!strcasecmp(value,"true") || !strcmp(value, "1")) {
      writeSerialInfoFile = true ;
    }
    else {
      writeSerialInfoFile = false ;
    }
  }

  value = getenv("FD_MAPREDUCE_IGNORE_ICP_LINES") ;
  if (value) {
    if (!strcasecmp(value,"true") || !strcmp(value, "1")) {
      ignoreICP = true ;
    }
    else {
      ignoreICP = false ;
    }
  }

  value = getenv("FD_MAPREDUCE_IGNORE_PREFETCH_LINES") ;
  if (value) {
    if (!strcasecmp(value,"true") || !strcmp(value, "1")) {
      ignorePrefetch = true ;
    }
    else {
      ignorePrefetch = false ;
    }
  }

  value = getenv("FD_MAPREDUCE_URL_BUCKET") ;
  if (value && sscanf(value, "%d", &url_bucket) != 1) {
    fprintf(stderr, "url_bucket need follow by 0 or bucket number\n");
    exit(1);
  }


  value = getenv("FD_MAPREDUCE_FILTER_COMBINE") ;
  if (value && !strcasecmp(value, "OR") ) {
    filter_combine = 0 ;
  }

  value = getenv("FD_MAPREDUCE_PROCESS_MISSES_ONLY") ;
  if ( value && (!strcasecmp(value,"true") || !strcmp(value, "1")) ) {
    processMissesOnly = true ;
    ignorePrefetch = false ; 	
  }

  value = getenv("FD_MAPREDUCE_COMPUTE_BOTH_SERVED_AND_MISS_FDS") ;
  if ( value && (!strcasecmp(value,"true") || !strcmp(value, "1")) ) {
    computeBothServedAndMiss = true ;
  }
  if (processMissesOnly && computeBothServedAndMiss) {
    fprintf(stderr, "FD_MAPREDUCE_PROCESS_MISSES_ONLY and FD_MAPREDUCE_COMPUTE_BOTH_SERVED_AND_MISS_FDS cannot both be true.\n") ;
    exit(1);
  }

  value = getenv("FD_MAPREDUCE_PREWARM_PERIOD") ;
  if ( value && sscanf(value, "%d", &prewarmPeriod) != 1) {
    fprintf(stderr, "FD_MAPREDUCE_PREWARM_PERIOD is not an integer: %s\n", value) ; 
    exit(1);
  }

  prewarmConfigRelative = false ;
  value = getenv("FD_MAPREDUCE_PREWARM_CONFIG_RELATIVE") ;
  if (value) {
    prewarmConfigRelative = true ;
  }


  value = getenv("FD_MAPREDUCE_REHASH_ONLY") ;
  if ( value && (!strcasecmp(value,"true") || !strcmp(value, "1")) ) {
    rehash_only = 1 ; 
  } 

  value = getenv("FD_MAPREDUCE_DEDUCE_REHASHED_SERIAL") ;
  if ( value && (!strcasecmp(value,"true") || !strcmp(value, "1")) ) {
    deduce_serial = 1 ; 
  } 

  value = getenv("FD_MAPREDUCE_SAMPLE_ON_ATTEMPT") ;
  if ( value && sscanf(value, "%d", &spatialSampleAttempt) != 1) {
    fprintf(stderr, "Invalid FD_MAPREDUCE_SAMPLE_ON_ATTEMPT value: %s. It needs to be an integer\n", value);
    exit(1);
  } 

  value = getenv("FD_MAPREDUCE_SAMPLE_BUCKETS") ;
  if ( value && sscanf(value, "%d", &spatialSampleBuckets) != 1) {
    fprintf(stderr, "Invalid FD_MAPREDUCE_SAMPLE_BUCKETS value: %s. It needs to be an integer\n", value);
    exit(1);
  } 

  value = getenv("FD_MAPREDUCE_SPLIT_BY_CPCODE") ;
  if (value) {
    if (rehash_only == 1 ) { 
      fprintf (stderr, "Cannot turn on both split_by_cpcode as well as rehash_only together\n") ;
      exit (1) ;
    }
    splitByCpcodes = 1 ;
    splitByMapid = 0 ;
    splitByArlid = 0 ;
    splitByAccount = 0 ;
    splitByVcdMapNetwork = 0 ;
  }

  value = getenv("FD_MAPREDUCE_SPLIT_BY_MAPID") ;
  if (value) {
    splitByMapid = 1 ;
    splitByCpcodes = 0 ;
    splitByArlid = 0 ;
    splitByAccount = 0 ;
    splitByVcdMapNetwork = 0 ;
  }

  value = getenv("FD_MAPREDUCE_SPLIT_BY_ARLID") ;
  if (value) {
    splitByMapid = 0 ;
    splitByCpcodes = 0 ;
    splitByArlid = 1 ;
    splitByAccount = 0 ;
    splitByVcdMapNetwork = 0 ;
  }

  value = getenv("FD_MAPREDUCE_SPLIT_BY_ACCOUNT") ;
  if (value) {
    splitByMapid = 0 ;
    splitByCpcodes = 0 ;
    splitByArlid = 0 ;
    splitByAccount = 1 ;
    splitByVcdMapNetwork = 0 ;
  }
  
  value = getenv("FD_MAPREDUCE_SPLIT_BY_VCD_MAP_NETWORK") ;
  if (value) {
    if (!strcasecmp(value,"true") || !strcmp(value, "1")) {
      splitByMapid = 0 ;
      splitByCpcodes = 0 ;
      splitByArlid = 0 ;
      splitByAccount = 0 ;
      splitByVcdMapNetwork = 1 ;
    }
  }

  if (splitByVcdMapNetwork) {
    // Must have config specified in a file given in FD_MAPREDUCE_VCD_MAP_NETWORK_FILE
    value = getenv("FD_MAPREDUCE_VCD_MAP_NETWORK_FILE") ;
    if (!value) {
      fprintf(stderr, "Splitting by vcd/map/network, but no config file provided in FD_MAPREDUCE_VCD_MAP_NETWORK_FILE\n" ) ;
      exit(1);
    }

    char *cfile = value ;
    struct stat bufc ;
    FILE *fpc ;
    char cline[LOG_LINE_LENGTH];

    fprintf(stderr, "Reading vmn config from %s\n", cfile);
    if (stat(cfile, &bufc) == 0 ) {
      if ((fpc = fopen(cfile, "r")) == NULL ) {
        fprintf(stderr, "Failed to open config file %s\n", cfile) ;
        exit(1) ;
      }
    } else {
      fprintf(stderr, "Config file %s does not exist\n", cfile) ;
      exit(1) ;
    }
    while (fgets(cline, LOG_LINE_LENGTH, fpc)) {
      // Ignore comments, lines beginning with a space and empty lines
      if ((cline[0] == '#') || (cline[0] == ' ') || (cline[0] == '\n')) {
        continue;
      }

      int vcdid ; 
      char map[1024] ;
      char network[1024] ;
      char extension[1024] ;
      char key[1024] ;

      int n = sscanf (cline, "%d %s %s %s", &vcdid, map, network, extension) ;
      if (n != 4) {
	fprintf (stdout, "Cannot parse line %s in %s\n", cline, cfile) ;
	exit (1) ;
      }
      snprintf (key, 1024, "%d^%s^%s", vcdid, map, network) ;
      vcdMapNetwork2id[key] = numVcdMapNetwork ;
      vcdMapNetworkId2Extension[numVcdMapNetwork] = extension ;
      numVcdMapNetwork ++ ;
    }
    fprintf(stderr, "Read %d numVcdMapNetwork entries from file %s\n", numVcdMapNetwork, cfile) ;
    fclose(fpc) ;
  }

  value = getenv("FD_MAPREDUCE_MAPRULES") ;
  if (value) {
    int m_index = 0 ;
    // Is there parenthesis in the string?
    // If yes, then the format is (mr1,mr2,mr3,...)(mr4,mg5,...)...
    // Otherwise, it's just mr1,mr2,mr3,...
    string valuestring = string(value) ;
    if (valuestring.find(')') != string::npos) {
      // It has parens
      vector<string> x = split(value, ')') ;
      for (vector<string>::iterator it = x.begin(); it != x.end(); it++ ) {
        const char *y = (*it).c_str();
        if (y[0] == ',') y++ ;
        if (y[0] == '(') y++ ;
        vector<string> z = split (y, ',') ;
        for (vector<string>::iterator it1 = z.begin(); it1 != z.end(); it1++ ) {
          map_names[num_maps] = strdup((*it1).c_str()) ;
          map_index[num_maps] = m_index ;
          num_maps ++ ;
        }
        m_index++ ;
      }
    } else {
      // It's just a comma-separated list
      vector<string> x = split(value, ',') ;
      for (vector<string>::iterator it = x.begin(); it != x.end(); it++ ) {
        string &a = *it ;
        // If the string starts with a character, then it's a map. Otherwise, it can be a mapid
        bool alpabetical = isalpha(a.c_str()[0]) ;
        bool hasDash = (a.find('-') != string::npos) ;
       
        int mapid = 0 ;
        bool notANumber = true ;
        if (!hasDash) {
          char *endptr ;
          const char *nptr = a.c_str() ;
          int b = strtol(nptr, &endptr, 10) ;
          // The whole string must be valid.
          if (b>0 && *nptr != '\0' && *endptr == '\0') {
            mapid = b ;
            notANumber = false ;
          }
        }

        // if the first letter is alphabetical or 
        // if it is a non-dash string that atoi fails on, 
        // Then this is not a mapid. Add to maps.
        if (alpabetical || (!hasDash&&notANumber) ) {
          map_names[num_maps] = strdup(a.c_str()) ;
          map_index[num_maps] = m_index ;
          num_maps ++ ;
          m_index++ ;
          continue ;
        } 

        // if the first letter is not alphabetical and 
        // if it is a non-dash string that atoi works on 
        // Then this is a mapid. Add to mapids.
        if (!alpabetical && !hasDash && !notANumber) {
          mapids[mapid] = 1 ;
          mapid2m_index[mapid] = m_index ;
          // Just keep these consistent
          map_names[num_maps] = strdup(a.c_str());
          map_index[num_maps] = m_index ;
          num_maps++ ;
          m_index++ ;
          continue ;
        }
        // Range of mapids
        if (hasDash) {
          int mapidStart, mapidEnd ;
          splitPair(a, '-', mapidStart, mapidEnd) ;
          for (int j = mapidStart; j <= mapidEnd ; j++) {
            mapids[j] = 1 ;
            mapid2m_index[j] = m_index ;
            // Just keep these consistent
            stringstream ss ; 
            ss << j ;
            map_names[num_maps] = strdup(ss.str().c_str()) ;
            map_index[num_maps] = m_index ;
            num_maps ++ ;
            // If we're going to split by mapid, then give each a different index.
            if (splitByMapid) m_index++ ;
          }
          // If we're not splitting by mapid, then they all mapids in the range have 
          // one index. Up it now
          if (!splitByMapid) m_index ++ ;
          continue ;
        }
        // Format not supported
        fprintf(stderr, "Unsupported string format in FD_MAPREDUCE_MAPRULES: %s\n", a.c_str());
        exit(1);

      }    
    }
  }

  // RAM size
  nodelen = NODELEN ;
  value = getenv("FD_MAPREDUCE_RAM_SIZE") ;
  if (value) {
    if ( !strcasecmp(value, "32GB") ) {
      nodelen = 150000000 ;
    } else if ( !strcasecmp(value, "64GB") ) {
      nodelen = 200000000 ;
    }
  }

  // Output directory — always strdup so configReset() can safely free it
  value = getenv("FD_MAPREDUCE_OUTPUT_DIR") ;
  output_dir = strdup(value ? value : ".") ;

  // Thinning jump percentage
  value = getenv("FD_MAPREDUCE_THINNING_PCT_JUMP") ;
  if (value && sscanf(value, "%lf", &pctJump) != 1) {
    fprintf (stderr, "Invalid FD_MAPREDUCE_THINNING_PCT_JUMP: %s\n", value ) ;
    exit(1) ;
  }

  value = getenv("FD_MAPREDUCE_NETWORK") ;
  network = 0 ;
  MAX_SERIAL = MAX_FF_SERIAL ;
  if (value) {
    if (!strcasecmp(value, "ESSL")) {
      network = 1 ;
      createESSLModulusExceptions() ;
    }
    else {
      if (!strcasecmp(value, "CW")){
        MAX_SERIAL = MAX_CW_SERIAL ;
        network = 2 ;
      }
    }
  }

  value = getenv("FD_MAPREDUCE_START_TIME") ;
  if (value) {
    if (sscanf(value, "%d", &start_time_config) != 1) {
      fprintf (stderr, "Invalid FD_MAPREDUCE_START_TIME\n") ;
      exit (1) ;
    }
  }

  value = getenv("FD_MAPREDUCE_END_TIME") ;
  if (value) {
    if (sscanf(value, "%d", &end_time_config) != 1) {
      fprintf (stderr, "Invalid FD_MAPREDUCE_END_TIME\n") ;
      exit (1) ;
    }
  }

  if ( end_time_config <= start_time_config ) {
    fprintf (stderr, "End time must be after start time: %d and %d\n", start_time_config, end_time_config ) ;
    exit(1);
  }

  exclude_cpcodes = false ;
  value = getenv("FD_MAPREDUCE_EXCLUDE_SPECIFIED_CPCODES") ;
  if (value) {
    exclude_cpcodes = true ;
  }

  value = getenv("FD_MAPREDUCE_CPCODES") ;
  if (value) {
    int i, flag;
    char cpcodeLine[1024];
    char sep = ',' ;

    // If there's a colon in the cpcodes list, then use a colon as the separator
    if (strchr(value,':') != NULL ) {
      sep = ':' ;
    }
    vector<string> x = split(value, sep) ;
    for (vector<string>::iterator it = x.begin(); it != x.end(); it++ ) {
      const char *x = (*it).c_str(); 
      int xx = 0 ;
      xx = atoi(x) ;
      if (xx<=0) {
        fprintf(stderr, "Bad cpcode %s, ignoring.\n", x);
        exit(1) ;
      } 
      cpcodes[xx] = numcpcodes++ ;
    }
  }

  value = getenv("FD_MAPREDUCE_CPCODES_FILE") ;
  if (value) {
    char *cpfile = value ;
    struct stat bufcp ;
    FILE *fpcp ;
    char cpline[16];

    // Open the cpcodes file
    if (stat(cpfile, &bufcp) == 0 ) {
      if ((fpcp = fopen(cpfile, "r")) == NULL ) {
        fprintf(stderr, "Failed to open cpcode file %s\n", cpfile) ;
        exit(1) ;
      }
    } else {
      fprintf(stderr, "cpcode file %s does not exist\n", cpfile) ;
      exit(1) ;
    }
    while (fgets(cpline, 16, fpcp)) {
      // Ignore comments, lines beginning with a space and empty lines
      if ((cpline[0] == '#') || (cpline[0] == ' ') || (cpline[0] == '\n')) {
        continue;
      }
      int xx = atoi(cpline) ;
      if ( xx <= 0 ) { 
        fprintf(stderr, "Bad cpcode %s, ignoring.\n", cpline) ;
        continue ;
      }
      cpcodes[xx] = numcpcodes++ ;
    } 
    fprintf(stderr, "Read %d cpcodes from file %s\n", numcpcodes, cpfile) ;
    fclose(fpcp) ;
  }

  // filtering based on VCDs
  exclude_vcds = false ;
  value = getenv("FD_MAPREDUCE_EXCLUDE_SPECIFIED_VCDS") ;
  if (value) {
    exclude_vcds = true ;
  }

  value = getenv("FD_MAPREDUCE_VCDS") ;
  if (value) {
    int i, flag;
    char vcdLine[1024];
    char sep = ',' ;

    // If there's a colon in the cpcodes list, then use a colon as the separator
    if (strchr(value,':') != NULL ) {
      sep = ':' ;
    }
    vector<string> x = split(value, sep) ;
    for (vector<string>::iterator it = x.begin(); it != x.end(); it++ ) {
      const char *x = (*it).c_str(); 
      int xx = 0 ;
      xx = atoi(x) ;
      if (xx<=0) {
        fprintf(stderr, "Bad vcd %s, ignoring.\n", x);
        exit(1) ;
      } 
      vcds[xx] = numvcds++ ;
    }
  }

  value = getenv("FD_MAPREDUCE_VCDS_FILE") ;
  if (value) {
    char *vcdfile = value ;
    struct stat bufvcd ;
    FILE *fpvcd ;
    char vcdline[16];

    // Open the vcds file
    if (stat(vcdfile, &bufvcd) == 0 ) {
      if ((fpvcd = fopen(vcdfile, "r")) == NULL ) {
        fprintf(stderr, "Failed to open vcd file %s\n", vcdfile) ;
        exit(1) ;
      }
    } else {
      fprintf(stderr, "vcd file %s does not exist\n", vcdfile) ;
      exit(1) ;
    }
    while (fgets(vcdline, 16, fpvcd)) {
      // Ignore comments, lines beginning with a space and empty lines
      if ((vcdline[0] == '#') || (vcdline[0] == ' ') || (vcdline[0] == '\n')) {
        continue;
      }
      int xx = atoi(vcdline) ;
      if ( xx <= 0 ) { 
        fprintf(stderr, "Bad vcd %s, ignoring.\n", vcdline) ;
        continue ;
      }
      vcds[xx] = numvcds++ ;
    } 
    fprintf(stderr, "Read %d vcds from file %s\n", numvcds, vcdfile) ;
    fclose(fpvcd) ;
  }

  // filtering based on regions
  exclude_regions = false ;
  value = getenv("FD_MAPREDUCE_EXCLUDE_SPECIFIED_REGIONS") ;
  if (value) {
    exclude_regions = true ;
  }

  value = getenv("FD_MAPREDUCE_REGIONS") ;
  if (value) {
    int i, flag;
    char regionLine[1024];
    char sep = ',' ;

    // If there's a colon in the regios list, then use a colon as the separator
    if (strchr(value,':') != NULL ) {
      sep = ':' ;
    }

    vector<string> x = split(value, sep) ;
    for (vector<string>::iterator it = x.begin(); it != x.end(); it++ ) {
      const char *x = (*it).c_str(); 
      int xx = 0 ;
      xx = atoi(x) ;
      if (xx<=0) {
        fprintf(stderr, "Bad region %s, ignoring.\n", x);
        exit(1) ;
      } 
      regions[xx] = numregions++ ;
    }
  }

  value = getenv("FD_MAPREDUCE_REGIONS_FILE") ;
  if (value) {
    char *regfile = value ;
    struct stat bufreg ;
    FILE *fpreg ;
    char regline[16];

    // Open the regions file
    if (stat(regfile, &bufreg) == 0 ) {
      if ((fpreg = fopen(regfile, "r")) == NULL ) {
        fprintf(stderr, "Failed to open reg file %s\n", regfile) ;
        exit(1) ;
      }
    } else {
      fprintf(stderr, "reg file %s does not exist\n", regfile) ;
      exit(1) ;
    }
    while (fgets(regline, 16, fpreg)) {
      // Ignore comments, lines beginning with a space and empty lines
      if ((regline[0] == '#') || (regline[0] == ' ') || (regline[0] == '\n')) {
        continue;
      }
      int xx = atoi(regline) ;
      if ( xx <= 0 ) { 
        fprintf(stderr, "Bad region %s, ignoring.\n", regline) ;
        continue ;
      }
      regions[xx] = numregions++ ;
    } 
    fprintf(stderr, "Read %d regions from file %s\n", numregions, regfile) ;
    fclose(fpreg) ;
  }


  set<string> accountsOnly ;
  value = getenv("FD_MAPREDUCE_ACCOUNTS") ;
  if (value) {
    char sep = ',' ;

    // If there's a colon in the cpcodes list, then use a colon as the separator
    if (strchr(value,':') != NULL ) {
      sep = ':' ;
    }
    vector<string> v = split(value, sep) ;
    accountsOnly = set<string>(v.begin(), v.end()) ;
  }

  // If there's a file mapping cpcode to accountname or arlid, read it
  value = getenv("FD_MAPREDUCE_CPCODE_MAP_FILE") ;
  if (accountsOnly.size()>0 && value == NULL) {
    // If accounts only has been specified, we must have this map file
    fprintf (stderr, "When FD_MAPREDUCE_ACCOUNTS is specified, FD_MAPREDUCE_CPCODE_MAP_FILE is mandatory.\n") ;
    exit(1) ;
  }
  if (value) {
    FILE *fpcp ;
    char *cpfile = value ;
    struct stat bufcp ;
    if (stat(cpfile, &bufcp) == 0 ) {
      if ((fpcp = fopen(cpfile, "r")) == NULL ) {
        fprintf(stderr, "Failed to open cpcode file %s\n", cpfile) ;
        exit(1) ;
      }
    } else {
      fprintf(stderr, "cpcode file %s does not exist\n", cpfile) ;
      exit(1) ;
    }
    //int numAccounts = 0 ;
    char cpline[64*1024] ;
    while (fgets(cpline, 64*1024, fpcp)) {
      int cpcode ;
      char acname[1024] ;
      int acid ;

      // Ignore comments, lines beginning with a space and empty lines
      if ((cpline[0] == '#') || (cpline[0] == ' ') || (cpline[0] == '\n')) {
        continue;
      }
      if (sscanf (cpline, "%d %s", &cpcode, acname) != 2) {
        fprintf (stderr, "Ignoring bad line in cpcode map: %s\n", cpline) ;
        continue ;
      }
      string s(acname); 
      // If accountsOnly has been defined, skip all the accounts that are not in it 
      if (accountsOnly.size() > 0 && accountsOnly.find(s) == accountsOnly.end()) {
        continue ;
      }

      // If this account wasn't seen before, enter it into account2id and give it an id
      if (account2id.find(s) == account2id.end()) {
        account2id[s] = numAccounts++ ;
      }
      acid = account2id[s] ;

      // A cpcode may appear under different account names (sadly this is true)
      // Was this cpcode seen before?
      if ( cpcode2accountid.find(cpcode) == cpcode2accountid.end() ) {
        // not seen before
        cpcode2accountid[cpcode] = acid ;  
        if (accountsOnly.size() > 0) {
          cpcodes[cpcode] = numcpcodes ++;
        }
      } else {
        // This cpcode was seen before.
	// If the account on the present line has a lower account id, change the cpcode's id to this one
	int prev_acid = cpcode2accountid[cpcode] ;
	if ( acid < prev_acid ) {
	  cpcode2accountid[cpcode] = acid ;
        }
      }
    }
    // Done reading the file     
    fprintf(stderr, "Read info of %d account/arlids from file %s. Number of cpcodes = %d\n", numAccounts, cpfile, numcpcodes) ;
    fclose(fpcp) ;
  }

  int numghosts = 0 ;
  value = getenv("FD_MAPREDUCE_GHOSTIP") ;
  if (value) {
    int i, ip;
    int len = strlen(value);
    char *p = value ;
    for (i = 0; i <= len; i++) {
      if (value[i] == ',' || value[i] == '\0') {
        ip = str2ip(p) ; 
        ghosts[ip] =  numghosts++;
        p = value+i+1;
      }
    }
  }

  value = getenv("FD_MAPREDUCE_GHOSTIP_FILE") ;
  if (value) {
    char *gipfile = value ;
    struct stat bufgip ; 
    FILE *fpgip ;        
    char gipline[32];    
    int i, i1, i2, i3, i4, ip;

    if (stat(gipfile, &bufgip) == 0 ) {
      if ((fpgip = fopen(gipfile, "r")) == NULL ) {
        fprintf(stderr, "Failed to open ghostip file %s\n", gipfile) ;
        exit(1) ;
      }
    } else {
      fprintf(stderr, "Ghostip file %s does not exist\n", gipfile) ;
      exit(1) ;
    }

    while (fgets(gipline, 16, fpgip)) {
      // Ignore comments, lines beginning with a space and empty lines
      if ((gipline[0] == '#') || (gipline[0] == ' ') || (gipline[0] == '\n')) {
        continue;
      }
      if (sscanf(gipline, "%d.%d.%d.%d", &i1, &i2, &i3, &i4) != 4) {
        fprintf(stderr, "Invalid ip found in %s: %s\n", gipfile, gipline);
        exit(1);
      }
      ip = (i1 << 24) | (i2 << 16) | (i3 << 8) | i4;
      ghosts[ip] = numghosts++ ;
    }
    fclose(fpgip) ;
  }


  // If serials are to be rehashed, how many shards per serial?
  // Although, rehashing is only done if split by cpcode is false
  if ( splitByCpcodes == 0 ) {
    value = getenv("FD_MAPREDUCE_SERIAL_BUCKET") ;
    if (value) {
      if (sscanf(value, "%d", &default_serial_bucket) != 1) {
        fprintf(stderr, "serial_bucket provided is not a number\n");
        exit(1);
      }
    }
  
    value = getenv("FD_MAPREDUCE_REHASH_RANGE_FROM_0") ;
    if ( value && (!strcasecmp(value,"true") || !strcmp(value, "1")) ) {
      // This is acceptable only when rehash_only is enabled
      if (rehash_only == 1 ) {
        rehash_range_start = -1 ;
        rehash_from_0 = 1 ;
      }
    }
  
    // Which serials should be rehashed?
    value = getenv("FD_MAPREDUCE_SERIALS") ;
    if (value) {
      int start_serial_expansion = rehash_range_start + 1;
      int xx = 0 ;
      vector<int> givenSerials ;
  
      vector<string> x = split(value, ',') ;
      for (vector<string>::iterator it = x.begin(); it != x.end(); it++ ) {
        string &a = *it ;
        // See if it's a range of serials
        if (a.find('-') == string::npos) {
          // Expect a single number
          int b = atoi(a.c_str()) ;
          if (b >=0) {
            givenSerials.push_back(b) ;
          }
          else {
            // Bad integer in config, exit
            fprintf(stderr, "Invalid serial in FD_MAPREDUCE_SERIALS: %s.\n", a.c_str());
            exit(1);
          }
        } 
        else {
          // integer range with 2 integers
          int serialStart, serialEnd ;
          splitPair (a, '-', serialStart, serialEnd) ;
          for (int j = serialStart ; j < serialEnd ; j++) {
            givenSerials.push_back(j) ;
          }          
        }
      }
      for (vector<int>::iterator sit = givenSerials.begin() ; sit != givenSerials.end(); sit++) {
        int serial = *sit ;
        serial = translateSerial(serial) ;
        rehashSerialIndex[xx] = serial ;
        xx++ ; 
        serial_start[serial] = start_serial_expansion;
        serial_bucket[serial] = default_serial_bucket; 
        start_serial_expansion += serial_bucket[serial]; 
      }
    }
  }

  // If there is config specified in a file given in FD_MAPREDUCE_ARL_VCD_TRANSLATION_FILE, read that file
  value = getenv("FD_MAPREDUCE_ARL_VCD_TRANSLATION_FILE") ;
  if (value) {
    char *tfile = value ;
    struct stat bufc ;
    FILE *tpc ;
    char cline[LOG_LINE_LENGTH];

    fprintf(stderr, "Reading arl vcd translation file from %s\n", tfile) ;
    if (stat(tfile, &bufc) == 0 ) {
      if ((tpc = fopen(tfile, "r")) == NULL ) {
        fprintf(stderr, "Failed to open config file %s\n", tfile) ;
        exit(1) ;
      }
    } else {
      fprintf(stderr, "Config file %s does not exist\n", tfile) ;
      exit(1) ;
    }
    // Read it now
    while (fgets(cline, LOG_LINE_LENGTH, tpc)) {
      // Ignore comments, lines beginning with a space and empty lines
      if ((cline[0] == '#') || (cline[0] == ' ') || (cline[0] == '\n')) {
        continue;
      }

      int vcdid ;
      int arlid ;
      int n = sscanf (cline, "%d %d", &arlid, &vcdid) ;
      if (n !=2) {
        fprintf (stdout, "Cannot parse line %s in %s\n", cline, tfile) ;
        exit (1) ;
      }
      arlid2vcd[arlid] = vcdid ;
    }
    fprintf(stderr, "Read %ld entries from file %s\n", arlid2vcd.size(), tfile) ;
    fclose(tpc) ;
  }

  // Figure out the attempt number from task attempt id
  value = getenv("mapreduce_task_attempt_id");
  if (value) {
    char *c = value+(strlen(value)-1) ;
    attemptId = atoi(c) ;
  }
  // Figure out extension for stdtime file written from this partition
  value = getenv("mapreduce_task_partition");
  if (value && sscanf(value, "%d", &partition_2) != 1) {
    fprintf (stderr, "Invalid mapreduce_task_partition") ;
    exit(1) ;
  }
  // Figure out the total number of reducers configured in this job
  value = getenv("mapreduce_job_reduces");
  if (value && sscanf(value, "%d", &numReducers) != 1) {
    fprintf (stderr, "Invalid mapreduce_job_reduces") ;
    exit(1) ;
  }

  // If reducer, figure out the extension for stdtime
  if (!isMapper) {
    sprintf (stdtime_extension, "%d", -1 * partition_2) ; // Default, just to make sure it's unique
    if (splitByCpcodes) {
      char suffix[16] ;
      suffix[0] = '\0' ;
  
      int expectedReducerCount = numcpcodes ;
      if (computeBothServedAndMiss) {expectedReducerCount*=2; } 
      if ( numReducers != -1 && numReducers < expectedReducerCount ) {
        fprintf (stderr, "This job needs %d reducers, but only %d configured\n", expectedReducerCount, numReducers) ;
        exit(1) ;
      }
      // Infer extension from the partition id
      if ( partition_2>=2*numcpcodes || (!computeBothServedAndMiss && partition_2 >= numcpcodes) ) {
        // This is a redundant reducer, it will get no data. 
        fprintf (stderr, "Reducer with mapreduce_task_partition %d is redundant. Only %d reducers are needed\n", 
                 partition_2, expectedReducerCount) ;
        exit(0) ;
      }
      if (computeBothServedAndMiss && partition_2 >= numcpcodes ) {
        // We have 2x as many partitions as cpcodes due to computeBothServedAndMiss
        partition_2 -= numcpcodes ;
        strcpy ( suffix, "^miss" ) ;
      }
      for ( std::map<int,int>::iterator it = cpcodes.begin() ; it != cpcodes.end(); it++ ) {
        if ( it->second == partition_2 ) {
          sprintf (stdtime_extension, "%d%s", it->first, suffix) ;
          break;
        }
      }
    }
    else if (splitByMapid) {
      for (std::map<int,int>::iterator it = mapid2m_index.begin(); it != mapid2m_index.end(); it++) {
        if (it->second == partition_2 ) {
          sprintf (stdtime_extension, "%d", it->first) ;
          break ;
        }
      }
    }
    else if (splitByArlid||splitByAccount) {
      char suffix[16] ;
      suffix[0] = '\0' ;
      int expectedReducerCount = numAccounts ;
      if (computeBothServedAndMiss) {expectedReducerCount*=2; }
      if ( numReducers != -1 && numReducers < expectedReducerCount ) {
        fprintf (stderr, "This job needs %d reducers, but only %d configured\n", expectedReducerCount, numReducers) ;
        exit(1) ;
      }
  
      // Infer extension from the partition id
      if ( partition_2>=2*numAccounts || (!computeBothServedAndMiss && partition_2 >= numAccounts)) {
        // This is a redundant reducer, it will get no data. 
        fprintf (stderr, "Reducer with mapreduce_task_partition %d is redundant. Only %d reducers expected\n",
                 partition_2, expectedReducerCount) ;
        exit(0) ;
      }
      if (computeBothServedAndMiss && partition_2 >= numAccounts) {
        // We have 2x as many partitions as Accounts due to computeBothServedAndMiss
        partition_2 -= numAccounts ;
        strcpy ( suffix, "^miss" ) ;
      }
      for (std::map<string,int>::iterator it = account2id.begin() ; it != account2id.end(); it++) {
        if (it->second == partition_2 ) {
          sprintf (stdtime_extension, "%s%s", it->first.c_str(), suffix) ;
          break ;
        }
      }
    } 
    else if (splitByVcdMapNetwork) {
      char suffix[16] ;
      suffix[0] = '\0' ;  
      int expectedReducerCount = numVcdMapNetwork ;
      if (computeBothServedAndMiss) {expectedReducerCount*=2;}
      if ( numReducers != -1 && numReducers < expectedReducerCount ) {
        fprintf (stderr, "This job needs %d reducers, but only %d configured\n", expectedReducerCount, numReducers) ;
        exit(1) ;
      }
      // Infer extension from the partition id
      if ( partition_2>=2*numVcdMapNetwork || (!computeBothServedAndMiss && partition_2 >= numVcdMapNetwork) ) {
        // This is a redundant reducer, it will get no data. 
        fprintf (stderr, "Reducer with mapreduce_task_partition %d is redundant. Only %d reducers expected\n",
                 partition_2, expectedReducerCount) ;
        exit(0) ;
      }   
      if (computeBothServedAndMiss && partition_2 >= numVcdMapNetwork) {
        // We have 2x as many partitions as Accounts due to computeBothServedAndMiss
        partition_2 -= numVcdMapNetwork ;
        strcpy ( suffix, "^miss" ) ;
      }
      std::map<int, string>::iterator it = vcdMapNetworkId2Extension.find(partition_2) ;
      if (it != vcdMapNetworkId2Extension.end()) {
        sprintf (stdtime_extension, "%s%s", it->second.c_str(), suffix) ;
      }
      else {
        fprintf (stdout, "Reducer id %d, not found in VcdMapNetwork config. Exiting reducer normally.\n", partition_2) ;
        exit(0) ;
      }
    }
    else if ( rehash_only && default_serial_bucket==1 && rehash_from_0 && deduce_serial == 1 && 
         rehashSerialIndex.find(partition_2) != rehashSerialIndex.end() ) {
      // Figure out the original serial from rehashSerialIndex
      sprintf (stdtime_extension, "%d", rehashSerialIndex[partition_2]) ;
    }
    else {
      sprintf (stdtime_extension, "%d", partition_2) ;
    }
  }

  //---------------------------------------------------------------------------
  // Functions related to overriding parameters
  // FD_MAPREDUCE_SAMPLE_BUCKETS_OVERRIDE and FD_MAPREDUCE_IGNORE_PREFETCH_OVERRIDE
  //---------------------------------------------------------------------------
  value = getenv("FD_MAPREDUCE_SAMPLE_BUCKETS_OVERRIDE") ;
  if (value) {
    // Value is a csv of strings of the form map:network:bucket1:bucket2
    std::istringstream ss(value);
    std::string token;
    while(std::getline(ss, token, '^')) {
      char m[128], n[128] ;
      int b1, b2; 
      const char *v = token.c_str() ;
      int ct = sscanf (v, "%[^:]:%[^:]:%d:%d", m, n, &b1, &b2) ;
      if (ct !=4){
        // Ignore this badly configured line
        fprintf(stderr, "Bad Sample buckets override %s, ignoring.\n", value);
      } else {
        char k[256] ;
        sprintf( k, "%s:%s", m, n) ;
        spatialSampleBucketsOverride[k] = std::pair<int,int>(b1, b2) ;
      }  
    }
  }

  value = getenv("FD_MAPREDUCE_IGNORE_PREFETCH_OVERRIDE") ;
  if (value) {
    // Value is a csv of strings of the form map:network:bool
    std::istringstream ss(value);
    std::string token;
    while(std::getline(ss, token, '^')) {
      char m[128], n[128], flag[128] ;
      const char *v = token.c_str() ;
      int ct = sscanf (v, "%[^:]:%[^:]:%s", m, n, flag) ;
      if (ct !=3){
        // Ignore this badly configured line
        fprintf(stderr, "Bad Sample buckets override %s, ignoring.\n", value);
      } else {
        char k[256] ;
        sprintf( k, "%s:%s", m, n) ;
        bool f = false ;
        if (!strcasecmp(flag,"true") || !strcmp(flag, "1")) {
        f = true ;
        }
        ignorePrefetchOverride[k] = f ;
      }  
    }
  }
}  
  
//---------------------------------------------------------------------------
// Serial translation functions
//---------------------------------------------------------------------------
void createESSLModulusExceptions() {
// Only for ESSL network
// Translated from the following part of DNSP dynamic config
// Snapshotted on Sept 26, 2018
/*
# WARNING! WARNING! WARNING!
# Serials S.4094 and S.4095 are special isolation serials.
# Do not use serial 4094 or 4095 unless you really know what you are doing!
e Add a bunch of Amazon slots to special map them to serial 2102. These are slots that are expected to show up on the Amazon CAC map
LoadBalancer.slotToSerial.exceptions.S 5153:2007 3353:2008 566:2009 8132:2010 8091:2011 10204:2012 7121:2013 3091:2014 7876:2015 10499:2017 10617:2018 10853:2019 2051:2020 10737:2021 2022:2022 10711:2023 11294:2024 3951:2025 8451:2100 8496:2101 15312:2102 15313:2102 15314:2102 15315:2102 15316:2102 15317:2102 15318:2102 15319:2102 15320:2102 15321:2102 15322:2102 15323:2102 15324:2102 15325:2102 15326:2102 15327:2102 6796:2026 6737:2026 3207:2047
# WARNING! WARNING! WARNING!
*/
  ESSLModulusExceptions[5153] = 2007;
  ESSLModulusExceptions[3353] = 2008;
  ESSLModulusExceptions[566] = 2009;
  ESSLModulusExceptions[8132] = 2010;
  ESSLModulusExceptions[8091] = 2011;
  ESSLModulusExceptions[10204] = 2012;
  ESSLModulusExceptions[7121] = 2013;
  ESSLModulusExceptions[3091] = 2014;
  ESSLModulusExceptions[7876] = 2015;
  ESSLModulusExceptions[10499] = 2017;
  ESSLModulusExceptions[10617] = 2018;
  ESSLModulusExceptions[10853] = 2019;
  ESSLModulusExceptions[2051] = 2020;
  ESSLModulusExceptions[10737] = 2021;
  ESSLModulusExceptions[2022] = 2022;
  ESSLModulusExceptions[10711] = 2023;
  ESSLModulusExceptions[11294] = 2024;
  ESSLModulusExceptions[3951] = 2025;
  ESSLModulusExceptions[8451] = 2100;
  ESSLModulusExceptions[8496] = 2101;
  ESSLModulusExceptions[15312] = 2102;
  ESSLModulusExceptions[15313] = 2102;
  ESSLModulusExceptions[15314] = 2102;
  ESSLModulusExceptions[15315] = 2102;
  ESSLModulusExceptions[15316] = 2102;
  ESSLModulusExceptions[15317] = 2102;
  ESSLModulusExceptions[15318] = 2102;
  ESSLModulusExceptions[15319] = 2102;
  ESSLModulusExceptions[15320] = 2102;
  ESSLModulusExceptions[15321] = 2102;
  ESSLModulusExceptions[15322] = 2102;
  ESSLModulusExceptions[15323] = 2102;
  ESSLModulusExceptions[15324] = 2102;
  ESSLModulusExceptions[15325] = 2102;
  ESSLModulusExceptions[15326] = 2102;
  ESSLModulusExceptions[15327] = 2102;
  ESSLModulusExceptions[6796] = 2026;
  ESSLModulusExceptions[6737] = 2026;
  ESSLModulusExceptions[3207] = 2047;
}


int translateSerialFreeflow(int serial) {
  return serial % (MAX_SERIAL+1) ;
}

int translateSerialCW(int serial) {
  return serial % (MAX_SERIAL+1) ;
}

int translateSerialESSL(int slot) {

  if (ESSLModulusExceptions.find(slot) != ESSLModulusExceptions.end()) {
    return ESSLModulusExceptions[slot] ;
  }
  else {
    return slot % (ESSL_SLOTMODULUS) ;
  }
}

int translateSerial(int serial) {
  switch (network) {
    case 0: return translateSerialFreeflow(serial) ;
    case 1: return translateSerialESSL(serial) ;
    case 2: return translateSerialCW(serial) ;
  }
  return serial ;
}

//---------------------------------------------------------------------------
// Override functions
//---------------------------------------------------------------------------
int getSpatialSampleBuckets (const char *m, const char *n, override_type t) {
  char k[256] ;
  sprintf( k, "%s:%s", m, n) ;
  if ( spatialSampleBucketsOverride.find(k) != spatialSampleBucketsOverride.end() ) {
     // Found an override
     std::pair<int, int> &x = spatialSampleBucketsOverride[k] ;
     if (t == OVERRIDE_FILTER) return x.first ;
     else return x.second ;
  }
  return spatialSampleBuckets ;
}    

bool getIgnorePrefetch (char *m, char *n) {
  char k[256] ;
  sprintf( k, "%s:%s", m, n) ;
  if (ignorePrefetchOverride.find(k) != ignorePrefetchOverride.end() ) {
    // Found an override
    return ignorePrefetchOverride[k] ;
  }
  return ignorePrefetch ;
}

// ---------------------------------------------------------------------------
// configReset — restore every config global to its initial value.
// Must be called between reducer invocations (end of reducerFinalize) so
// that readConfig() starts from a clean slate for the next partition.
// ---------------------------------------------------------------------------
void configReset() {
  // Free strdup'd char* values in map_names before clearing the map
  for (auto &kv : map_names) free(kv.second) ;
  map_names.clear() ;
  mapids.clear() ;
  map_index.clear() ;
  mapid2m_index.clear() ;
  cpcodes.clear() ;
  vcds.clear() ;
  regions.clear() ;
  ghosts.clear() ;
  serial_start.clear() ;
  serial_bucket.clear() ;
  rehashSerialIndex.clear() ;
  account2id.clear() ;
  cpcode2accountid.clear() ;
  arlid2vcd.clear() ;
  vcdMapNetwork2id.clear() ;
  vcdMapNetworkId2Extension.clear() ;
  spatialSampleBucketsOverride.clear() ;
  ignorePrefetchOverride.clear() ;

  // Reset insertion-index counters
  num_maps          = 0 ;
  numcpcodes        = 0 ;
  numvcds           = 0 ;
  numregions        = 0 ;
  numVcdMapNetwork  = 0 ;
  numAccounts       = 0 ;
  numReducers       = -1 ;

  // Reset scalar config to initial defaults (must mirror initial values above)
  network               = 0 ;
  MAX_SERIAL            = MAX_FF_SERIAL ;
  start_time_config     = 0 ;
  end_time_config       = INT_MAX ;
  prewarmPeriod         = 0 ;
  prewarmConfigRelative = false ;
  url_bucket            = 0 ;
  splitByCpcodes        = 0 ;
  splitByMapid          = 0 ;
  splitByArlid          = 0 ;
  splitByAccount        = 0 ;
  splitByVcdMapNetwork  = false ;
  filter_combine        = 1 ;
  processMissesOnly     = false ;
  computeBothServedAndMiss = false ;
  ignoreICP             = true ;
  ignorePrefetch        = false ;
  writeSerialInfoFile   = false ;
  randomSamplingPct     = 100.0 ;
  rehash_only           = 0 ;
  deduce_serial         = 0 ;
  default_serial_bucket = 8 ;
  rehash_from_0         = 0 ;
  rehash_range_start    = MAX_SERIAL ;
  pctJump               = 0.0 ;
  attemptId             = 0 ;
  spatialSampleAttempt  = -1 ;
  spatialSampleBuckets  = 1 ;
  partition_2           = 0 ;
  nodelen               = NODELEN ;
  entryProbabilityPct   = 100.0 ;
  considerTTL           = false ;
  region_ip_in_log      = false ;
  exclude_cpcodes       = false ;
  exclude_vcds          = false ;
  exclude_regions       = false ;
  isMapper              = false ;

  // Free the always-strdup'd output_dir and reset
  free(output_dir) ;
  output_dir = NULL ;

  stdtime_extension[0] = '\0' ;
  maprules[0]          = '\0' ;
}

std::pair<string, string> getMapNetworkFromExtension (char *ext) {
  char m[128] ;
  char n[128] ; 
  char c[16*1024] ; 
  if (sscanf(ext, "%[^^]^%[^^]^%s", m, n, c) == 3) {
    return std::pair<string, string>(m,n) ;
  }
  else {
    return std::pair<string, string>("unknown","unknown") ;
  }
}
