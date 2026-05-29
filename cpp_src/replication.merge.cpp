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
  long long count ;
  long long size ;
} footprintSize ;


//---------------------------------------------------------------------------
// Configuration related: read and print
//---------------------------------------------------------------------------
static void print_config() {
  fprintf ( stdout, "Merger Configuration\n" ) ;
  fprintf ( stdout, "--------------------\n" ) ;
  fprintf ( stdout, "Merger attempt = %d\n", attemptId ) ;
  fprintf ( stdout, "Input/Output directory = %s\n", output_dir ) ;
}

//---------------------------------------------------------------------------
// Main loop
//---------------------------------------------------------------------------
#ifndef BUILD_SHARED_LIB
int main(int argc, char *argv[]) {
  long long uniqueCount = 0 ;
  long long uniqueSize = 0 ;
  long long totalCount = 0 ;
  long long totalSize = 0 ;
  char fname[LINESIZE] ;
  unordered_set<string> replFnames ;
  std::map<int, footprintSize> footprintByRepl ;

  readConfig();
  print_config();
  fprintf(stdout, "Ready to read lines from stdin\n" ) ;

  // Read the config file from stdin
  // It provides a list of filenames that are in output_dir
  while (fgets(fname, LINESIZE, stdin)) {
    if (fname[strlen(fname)-1] == '\n') { fname[strlen(fname)-1] = '\0'; }
    replFnames.insert(string(output_dir) + "/" + string(fname) );
  }

  if (replFnames.size()==0) { exit(0) ; }

  // Verify output_dir exists
  struct stat dirStat;
  if (stat(output_dir, &dirStat) != 0) {
    fprintf(stderr, "Input/Output directory %s does not exist, exiting...\n", output_dir);
    exit(1);
  }

  int filesRead = 0 ;
  for (unordered_set<string>::iterator it = replFnames.begin() ; it != replFnames.end(); it++ ) {
    char buf[LINESIZE];
    const string &s = *it ;
    FILE *fp = fopen(s.c_str(), "r");
    if (!fp) {
      fprintf(stderr, "Fail to open file %s, ignoring it\n", s.c_str());
      continue ;
    }
    while (fgets(buf, LINESIZE, fp) != NULL) {
      int rval ;
      int replCount = 0 ;
      long long cc, ss ;
      if ( (rval=sscanf(buf, "%d %lld %lld", &replCount, &cc, &ss)) != 3) {
        fprintf(stdout, "Failed to parse line ^%s in file %s. Ignoring\n", buf, s.c_str());
        fclose(fp);
        exit(1) ;
      }

      printf ( "Read a line: %d %lld %lld\n", replCount, cc, ss) ;
      footprintByRepl[replCount].count += cc ;
      footprintByRepl[replCount].size += ss ;
      uniqueCount += cc ;
      uniqueSize += ss ;
      totalCount += cc*replCount ;
      totalSize += ss*replCount ;
    }
    filesRead ++ ;
    fclose(fp);
  }

  fprintf(stdout, "Done processing input. Read %d files out of %ld in config.\n", filesRead, replFnames.size() ) ;
  
  if (filesRead>0) {
    char buf[128*1024];
    sprintf (buf, "uniqueCount, totalCount, countReplFactor, uniqueSize, totalSize, sizeReplFactor\n %lld %lld %.2f %lld %lld %.2f\n", 
                   uniqueCount, totalCount,
                   totalCount*1.0/uniqueCount, 
                   uniqueSize, totalSize, 
                   totalSize*1.0/uniqueSize) ;
    sprintf (buf + strlen(buf), "%s", "CopyCount objectCount countFraction uniqueFootprintSize sizeFraction\n") ;
    for ( map<int, footprintSize>::iterator it = footprintByRepl.begin();  it != footprintByRepl.end(); it++ ) {
      int replCount = it->first ;
      sprintf (buf + strlen(buf), "%d %lld %.2f %lld %.2f\n", 
                                  replCount, 
                                  footprintByRepl[replCount].count, footprintByRepl[replCount].count*100.0/uniqueCount,
                                  footprintByRepl[replCount].size, footprintByRepl[replCount].size*100.0/uniqueSize ) ;
    }
  
    printf ("%s\n", buf) ;
  
    char fnameBuf[16*1024] ;
    sprintf(fnameBuf, "%s/replication", output_dir);
    FILE *outfp = fopen(fnameBuf, "w");
    if (!outfp) {
      fprintf(stdout, "Failed to open file %s to write\n", fnameBuf) ;
      exit(1);
    }
    fputs(buf, outfp);
    fclose(outfp);
  } else {
    fprintf (stderr, "%s", "No input could be read. Empty output\n") ;
  }

  return 0;
}



#endif // BUILD_SHARED_LIB
