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

// Extern declarations for all the config variables
extern int network ;
extern int MAX_SERIAL ;
extern int start_time_config ;
extern int end_time_config ;
extern int prewarmPeriod ;
extern bool prewarmConfigRelative ;
extern char maprules[LOG_LINE_LENGTH];
extern std::map<int,char *> map_names ;
extern std::map<int,int> mapids ;
extern std::map<int,int> map_index ;
extern std::map<int,int> mapid2m_index ;
extern int num_maps ;
extern std::map<int,int> cpcodes ;
extern int numcpcodes ;
extern bool exclude_cpcodes ;
extern std::map<int,int> vcds ;
extern int numvcds ;
extern bool exclude_vcds ;
extern std::map<int,int> regions ;
extern int numregions ;
extern bool exclude_regions ;
extern int filter_combine ;
extern bool processMissesOnly ;
extern bool computeBothServedAndMiss ;
extern std::map<int,int> ghosts ;
extern int url_bucket ;
extern int splitByCpcodes ;
extern int splitByMapid ;
extern int splitByArlid ;
extern int splitByAccount ;
extern int default_serial_bucket ; 
extern std::map<int,int> serial_start ;
extern std::map<int,int> serial_bucket ;
extern int rehash_only ; 
extern int rehash_start_range ; 
extern std::map<int,int> rehashSerialIndex ;
extern double pctJump ;	
extern int attemptId ;
extern int spatialSampleAttempt ;
extern int spatialSampleBuckets ;
extern int nodelen ;
extern char *output_dir ;
extern int partition_2 ;
extern char stdtime_extension[1024] ;
extern bool ignoreICP ;
extern bool ignorePrefetch ;
extern std::map<string,int> account2id ;
extern int numAccounts ;
extern std::map<int,int> cpcode2accountid ;
extern bool writeSerialInfoFile ;
extern double randomSamplingPct ;

// FDS-G's per vcd-map-network splitting
extern bool splitByVcdMapNetwork ;
extern int numVcdMapNetwork ;
extern std::map<string,int> vcdMapNetwork2id ;
extern std::map<int, string> vcdMapNetworkId2Extension ;
extern std::map<int,int> arlid2vcd ;

// Probabilistic entry barrier 
extern double entryProbabilityPct ;

// Does the log have region and ip in a comment line in the logs?
extern bool region_ip_in_log ;

// TTL handling for web CW
extern bool considerTTL ;

// Am I a mapper or a reducer?
extern bool isMapper ;

// extern declarations for all the config functions
inline void setIsMapper( bool flag) { isMapper = flag ;} 
extern void readConfig () ;
extern vector<string> split(string s, char delim) ;
extern int translateSerial(int serial) ;
extern void createESSLModulusExceptions() ;

// Override hashes
extern std::map<string, bool> ignorePrefetchOverride ;
extern std::map<string, std::pair<int,int> > spatialSampleBucketsOverride ;
extern bool getIgnorePrefetch (char *m, char *n)  ;
extern int getSpatialSampleBuckets (const char *m, const char *n, override_type t) ;
extern std::pair<string, string> getMapNetworkFromExtension (char *ext) ;

#endif

