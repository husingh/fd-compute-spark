#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <sys/stat.h>
#include <map>
#include <vector>
#include <climits>
#include "stack_distance.config.h"

using namespace std ;

//---------------------------------------------------------------------------
// Print configuration
//---------------------------------------------------------------------------
static void print_config() {

   fprintf ( stderr, "Mapper Configuration\n" ) ;
   fprintf ( stderr, "--------------------\n" ) ;
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

   fprintf ( stderr, "Regions: ") ;
   if ( numregions>50 ) {
     fprintf ( stderr, "%d regions, too many to list\n", numregions) ;
   } else {
     for (std::map<int,int>::iterator it = regions.begin(); it != regions.end(); it++ ) {
       fprintf ( stderr, "%d ", it->first) ;
     }
     fprintf ( stderr, "\n") ;
   }

   fprintf ( stderr, "filter_combine = %d\n", filter_combine ) ;
   fprintf ( stderr, "URL buckets = %d\n", url_bucket ) ;

   if (region_ip_in_log) {
     fprintf ( stderr, "Reading region and ghostip from comment lines in log\n") ;
   }
   else {
     fprintf ( stderr, "Reading ghostip from log filename\n") ;
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
bool doFilter ( double timestamp, char *map, int cpcode, int region, int &matched_map_index) {
  int match = 0 ;
  int match_map = 0;
  int match_cpcode = 0;
  int match_region = 0;
  int mapid = 0 ;

  // Initialize to no map match
  matched_map_index = -1 ;

  // Check if timestamp in range
  if (timestamp < start_time_config || timestamp > end_time_config) {
    return false ;
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

  if (filter_combine==0) {
    // OR the two filters
    match = match_map + match_cpcode + match_region;
  } else { 
    match = 0 ;
    // AND the two match flags, but ignore a filter if it hasn't been specified
    if (num_maps == 0 && mapids.size()==0) match_map = 1 ;
    if (numcpcodes==0) match_cpcode = 1 ;
    if (numregions == 0 ) match_region = 1 ;

    match = match_map * match_cpcode * match_region ;
  }
  return match ;
} 

//---------------------------------------------------------------------------
// Two ways to get ghostip 
// one from filename
// second from comment lines in a file
//---------------------------------------------------------------------------
int getGhostipFromFilename() {
  int i, i1, i2, i3, i4, ip ;
  char *input_filename = basename(getenv("mapreduce_map_input_file"));
  if (sscanf(input_filename, "fd_storelog_%d.%d.%d.%d_", &i1, &i2, &i3, &i4) != 4) {
    int reg ;
    if (sscanf(input_filename, "fd_storelog_%d_%d.%d.%d.%d_", &reg, &i1, &i2, &i3, &i4) != 5) {
      fprintf(stderr, "fail to get ip from mapper input file name from %s\n", input_filename);
      exit(1);
    }
  }
  ip = (i1 << 24) | (i2 << 16) | (i3 << 8) | i4;
  fprintf(stderr, "Input filename is %s. Extracted ip addeess is %d.%d.%d.%d. In integer form %d\n", 
                  input_filename, i1, i2, i3, i4, ip) ;
  return ip ;
}
 
int  getIpFromLine(char *line) {
  int r, i1, i2, i3, i4, ip ;
  if (sscanf(line, "# %d %d.%d.%d.%d", &r, &i1, &i2, &i3, &i4) != 5) {
    fprintf(stderr, "fail to get ip from comment line %s\n", line);
    return 0 ;
  }
  ip = (i1 << 24) | (i2 << 16) | (i3 << 8) | i4;
  return ip ;
}
 
//---------------------------------------------------------------------------
// Main loop
//---------------------------------------------------------------------------
#ifndef BUILD_SHARED_LIB
int main(int argc, char *argv[]) {
  char line[LOG_LINE_LENGTH];
  char map[LOG_LINE_LENGTH];
  char objstatus[LOG_LINE_LENGTH] ;
  short serial;
  double timestamp;
  char md5[LOG_LINE_LENGTH];
  long long bytes;
  long long size;
  int cpcode;
  int hash;
  long long allLinesCount = 0 ;
  long long invalidLinesCount = 0 ;
  long long ignoredLinesCount = 0 ;
  int ip ;
  int match_ip = -1 ;

  // Fields that FDS preprocessing outputs (not seen on GSP cluster)
  int fds_arlid = -1 ;
  char fds_network[LOG_LINE_LENGTH] ;
  char fds_mapname[LOG_LINE_LENGTH] ;
  int fds_region = -1 ;
  int fds_vcd = -1 ;
  char fds_product[LOG_LINE_LENGTH] ;

  readConfig(); 
  print_config(); 
  fprintf(stderr, "Starting mapper\n");

  if (region_ip_in_log) {
    ip = getGhostipFromFilename() ;
  }

  while (fgets(line, LOG_LINE_LENGTH, stdin) != NULL) {

    // If a comment line, get ip from it
    if (line[0] == '#') {
      int x = getIpFromLine(line) ;
      if (x) ip = x ;
      continue ;
    }

    allLinesCount ++ ;
    objstatus[0] = '\0' ;
    if (sscanf(line, "%[^:]:%hd:%lf:%[^:]:%lld:%lld:%d:%[^:]:%d:%[^:]:%[^:]:%d:%d:%s",
                     map, &serial, &timestamp, md5, &size, &bytes, &cpcode, objstatus,
                     &fds_arlid, fds_network, fds_mapname, &fds_region, &fds_vcd, fds_product) != 14) {
      if (sscanf(line, "%[^:]:%hd:%lf:%[^:]:%lld:%lld:%d:%s",
                 map, &serial, &timestamp, md5, &size, &bytes, &cpcode, objstatus) != 8) {
        if (sscanf(line, "%[^:]:%hd:%lf:%[^:]:%lld:%lld:%d",
            map, &serial, &timestamp, md5, &size, &bytes, &cpcode) != 7) {
           //fprintf(stderr, "Ignoring a bad line: %s\n", line);
          invalidLinesCount ++ ;
          continue;
        }
      }
    }

/* 
    if (sscanf(line, "%[^:]:%hd:%lf:%[^:]:%lld:%lld:%d:%s", 
	       map, &serial, &timestamp, md5, &size, &bytes, &cpcode, objstatus) != 8) {
      if (sscanf(line, "%[^:]:%hd:%lf:%[^:]:%lld:%lld:%d", 
	         map, &serial, &timestamp, md5, &size, &bytes, &cpcode) != 7) {
        // fprintf(stderr, "Ignoring line with not enough fields: %s\n", line);
        // This typically happens with ghostmon service check lines, that have empty map
        invalidLinesCount ++ ;
        continue;
      }
    }
*/
    // Check serial sanity
    if ((serial <= 0)) {
      //fprintf(stderr, "Ignoring line with a bad serial serial %d: %s\n", serial, line);
      invalidLinesCount ++ ;
      continue ; 
    }

    int matched_map_index ;
    bool match = doFilter(timestamp, map, cpcode, fds_region, matched_map_index) ;
    if (match) {
      if (url_bucket) {
        // Mapper splits by URL hash
        sscanf(md5, "%04x", &hash);
        hash %= url_bucket;
        //printf( "%d:%s:%lld:%d\n", hash, md5, size, ip) ;
        printf( "%d:%s:%d:%lld\n", hash, md5, ip, size) ;
      }
      else {
        // mapper splits by serial number
        printf( "%hd:%s:%d:%lld\n", serial%2039, md5, ip, size) ;
      }
    }
    else {
      //printf( "Ignoring line %s\n", line) ;
      ignoredLinesCount ++ ;
    }
  }
  fprintf ( stderr, "Lines read %lld, invalid lines %lld, ignored lines %lld\n", allLinesCount, invalidLinesCount, ignoredLinesCount ) ;

  return 0 ;
}




#endif // BUILD_SHARED_LIB
