#ifndef FILE_HELPER_H
#define FILE_HELPER_H

class FileHelper {

  public:
    virtual void connect () = 0 ;
    virtual void disconnect () = 0 ;
    virtual bool fopen ( const char *fname, const char *flags) = 0 ;
    virtual bool fclose () = 0 ;
    virtual char* fgets ( char *buf, int len) = 0 ;
    virtual void fputs ( char *buf ) = 0 ;
    virtual bool exists ( const char *fname ) = 0 ;
    virtual bool createDirectory ( const char *dirname ) = 0 ;
    
    bool inUse ;

} ;

#endif

