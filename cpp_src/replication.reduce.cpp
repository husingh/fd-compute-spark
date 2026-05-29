#include <unordered_set>
#include <stdio.h>
#include <climits>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <libgen.h>
#include <sys/stat.h>
#include <map>
#include "stack_distance.config.h"

//---------------------------------------------------------------------------
// data structs
//---------------------------------------------------------------------------
typedef struct {
  long long size;
  std::unordered_set <int> ghosts ;
} objectInfo ;
std::map<string, objectInfo> allobjects ;

//---------------------------------------------------------------------------
// Configuration related: read and print
//---------------------------------------------------------------------------
static void print_config() {
  fprintf ( stdout, "Reducer Configuration\n" ) ;
  fprintf ( stdout, "--------------------\n" ) ;
  fprintf ( stdout, "Reducer attempt = %d\n", attemptId ) ;
  fprintf ( stdout, "Output directory = %s\n", output_dir ) ;
  fprintf ( stdout, "Partition = %d, partition extension = %s\n", partition_2, stdtime_extension ) ;
}

//---------------------------------------------------------------------------
// Main loop
//---------------------------------------------------------------------------
#ifndef BUILD_SHARED_LIB
int main(int argc, char *argv[]) {
  char s[LINESIZE] ;
  char md5_ascii[64] ;
  int serial;
  int readcount = 0 ;
  int rval ;
  int ip; 
  int md5_count_exceeded=0;
  bool memory_allocated=false ;
  bool doSpatialSampling = false ;

  readConfig();
  print_config();

  fprintf(stdout, "Ready to read lines from stdin\n" ) ;

  // Read the request file
  while (fgets(s, LINESIZE, stdin)) {

    long long size = 0 ;

    readcount ++; 
    if ( (rval=sscanf(s, "%d:%16s:%d:%lld", &serial, md5_ascii, &ip, &size)) != 4) {
      fprintf(stdout, "Mapper sent a bad line to the reducer. Failed to parse line ^%s. Ignoring\n", s);
      continue;
    }

    string obj(md5_ascii) ;
    if (allobjects.find(obj) == allobjects.end() ) {
      // Seeing for the first time. Set size.
      allobjects[obj].size = size ;
    }
    allobjects[obj].ghosts.insert(ip) ;
  }

  fprintf(stdout, "Done processing input. Read %d lines.\n", readcount ) ;

  // Loop over all objects and compute replication
  //long long uniqueObjCount=0, allObjCount=0, uniqueBytesize=0, allObjectSize=0;
  map<int, long long> replCountDistr ;
  map<int, long long> replBytesizeDistr ;
 
  for (std::map<string, objectInfo>::iterator it = allobjects.begin(); 
       it != allobjects.end(); it++ ) {
    long long s = it->second.size ;
    int c = it->second.ghosts.size(); 
    
    //uniqueObjCount ++ ;
    //allObjCount += c ;
    
    //uniqueBytesize += s ;
    //allObjectSize += (s*c) ;
    
    replCountDistr[c] ++ ;
    replBytesizeDistr[c] += s ;
  }

  // Write to local file or stdout
  char buf[128*1024];
  buf[0] = '\0';
  for ( map<int, long long>::iterator it = replCountDistr.begin();  it != replCountDistr.end(); it++ ) {
    int replCount = it->first ;
    sprintf (buf + strlen(buf), "%d %lld %lld\n", replCount, replCountDistr[replCount], replBytesizeDistr[replCount] ) ; 
  }

  if ( !strcmp(output_dir, ".") ) {
    printf ("%s", buf) ;
  } else {
    struct stat st;
    if (stat(output_dir, &st) != 0) mkdir(output_dir, 0755);
    char fnameBuf[4096];
    sprintf(fnameBuf, "%s/replication.%s", output_dir, stdtime_extension);
    FILE *fp = fopen(fnameBuf, "w");
    if (!fp) { fprintf(stderr, "Failed to open %s to write\n", fnameBuf); exit(1); }
    fprintf(fp, "%s", buf);
    fclose(fp);
  }
  return 0;
}

#endif // BUILD_SHARED_LIB
