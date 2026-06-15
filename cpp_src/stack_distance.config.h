#ifndef STACK_DISTANCE_CONFIG_H
#define STACK_DISTANCE_CONFIG_H

#include <climits>
#include <map>
#include <string>
#include <vector>

using namespace std; 

//--------------------------------------------------------------------------- 
// Misc ENUM
//--------------------------------------------------------------------------- 
typedef enum {OVERRIDE_FILTER=0, OVERRIDE_UNSHARD=1} override_type ;

//---------------------------------------------------------------------------
// Defaults
//---------------------------------------------------------------------------
#define LOG_LINE_LENGTH 1024
#define LINESIZE 256
#define MAX_FF_SERIAL 2047
#define MAX_CW_SERIAL 4095
#define ESSL_SLOTMODULUS 2000
#define MAX_MAPS 100
#define NODELEN 105000000
#define TIMELEN (86400*31+1)
#define TIMEBUCKET 300
#define SIZEUNIT (1024*1024)
#define MD5_DIGEST_LENGTH 8

// Extern declarations for all the config variables.
// thread_local: each Spark executor thread (task) gets its own isolated copy,
// matching Hadoop's per-process isolation without any logic changes.
extern thread_local int network ;
extern thread_local int MAX_SERIAL ;
extern thread_local int start_time_config ;
extern thread_local int end_time_config ;
extern thread_local int prewarmPeriod ;
extern thread_local bool prewarmConfigRelative ;
extern thread_local char maprules[LOG_LINE_LENGTH];
extern thread_local std::map<int,char *> map_names ;
extern thread_local std::map<int,int> mapids ;
extern thread_local std::map<int,int> map_index ;
extern thread_local std::map<int,int> mapid2m_index ;
extern thread_local int num_maps ;
extern thread_local std::map<int,int> cpcodes ;
extern thread_local int numcpcodes ;
extern thread_local bool exclude_cpcodes ;
extern thread_local std::map<int,int> vcds ;
extern thread_local int numvcds ;
extern thread_local bool exclude_vcds ;
extern thread_local std::map<int,int> regions ;
extern thread_local int numregions ;
extern thread_local bool exclude_regions ;
extern thread_local int filter_combine ;
extern thread_local bool processMissesOnly ;
extern thread_local bool computeBothServedAndMiss ;
extern thread_local std::map<int,int> ghosts ;
extern thread_local int url_bucket ;
extern thread_local int splitByCpcodes ;
extern thread_local int splitByMapid ;
extern thread_local int splitByArlid ;
extern thread_local int splitByAccount ;
extern thread_local int default_serial_bucket ;
extern thread_local std::map<int,int> serial_start ;
extern thread_local std::map<int,int> serial_bucket ;
extern thread_local int rehash_only ;
extern thread_local int rehash_start_range ;
extern thread_local std::map<int,int> rehashSerialIndex ;
extern thread_local double pctJump ;
extern thread_local int attemptId ;
extern thread_local int spatialSampleAttempt ;
extern thread_local int spatialSampleBuckets ;
extern thread_local int nodelen ;
extern thread_local char *output_dir ;
extern thread_local int partition_2 ;
extern thread_local char stdtime_extension[1024] ;
extern thread_local bool ignoreICP ;
extern thread_local bool ignorePrefetch ;
extern thread_local std::map<string,int> account2id ;
extern thread_local int numAccounts ;
extern thread_local std::map<int,int> cpcode2accountid ;
extern thread_local bool writeSerialInfoFile ;
extern thread_local double randomSamplingPct ;

// FDS-G's per vcd-map-network splitting
extern thread_local bool splitByVcdMapNetwork ;
extern thread_local int numVcdMapNetwork ;
extern thread_local std::map<string,int> vcdMapNetwork2id ;
extern thread_local std::map<int, string> vcdMapNetworkId2Extension ;
extern thread_local std::map<int,int> arlid2vcd ;

// Probabilistic entry barrier
extern thread_local double entryProbabilityPct ;

// Does the log have region and ip in a comment line in the logs?
extern thread_local bool region_ip_in_log ;

// TTL handling for web CW
extern thread_local bool considerTTL ;

// Am I a mapper or a reducer?
extern thread_local bool isMapper ;

// extern declarations for all the config functions
inline void setIsMapper( bool flag) { isMapper = flag ;}
extern void readConfig () ;
extern void configReset () ;
extern vector<string> split(string s, char delim) ;
extern int translateSerial(int serial) ;
extern void createESSLModulusExceptions() ;

// Override hashes
extern thread_local std::map<string, bool> ignorePrefetchOverride ;
extern thread_local std::map<string, std::pair<int,int> > spatialSampleBucketsOverride ;
extern bool getIgnorePrefetch (char *m, char *n)  ;
extern int getSpatialSampleBuckets (const char *m, const char *n, override_type t) ;
extern std::pair<string, string> getMapNetworkFromExtension (char *ext) ;

#endif

