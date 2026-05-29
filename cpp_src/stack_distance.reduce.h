#ifndef STACK_DISTANCE_REDUCE_H
#define STACK_DISTANCE_REDUCE_H

typedef struct {
  unsigned int iatSum;
  unsigned int ghost0   :1;
  unsigned int ghost1   :1;
  unsigned int iatCount :30;
} iat_t;

typedef struct {
  long long stdsum;
  long long stdosum;
  long long requests;
  long long bytes;
  long long mib ;       // Miss Inflation Bytes
  long long ttl_expirations ;
  long long Transition1to2_bytes;
  long long Transition1to2_requests;
} stdtime_t;

typedef struct {
  unsigned char md5[MD5_DIGEST_LENGTH];
} md5_t;

typedef struct {
    unsigned long size,resident,share,text,lib,data,dt;
} statm_t;

typedef struct {
  long long bytes ;
  int requests ;
} traffic_t ;

typedef struct {
  long long served_requests ;
  long long served_bytes ;
  long long unique_objectcount ;
  long long unique_objectsize ;
} serial_info_t ;

#endif

