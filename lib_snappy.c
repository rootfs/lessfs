#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#ifdef SNAPPY
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <snappy-c.h>
#include "lib_safe.h"
#include "lib_log.h"
#include "retcodes.h"

extern int BLKSIZE;
#define die_snappyerr(f...) { LFATAL(f); exit(EXIT_SNAPPYERR); }

// Return the decompressed block without the 
// leading byte that informs about the used compression
// First byte = 'Q' Indicates that the block is QLZ5 1.4.1 compressed
// First byte = 'R' Indicates that the block is QLZ5 1.5.1 compressed
// First byte = 0 The block is not compressed
// First byte = 'L' Indicates that the block is LZO compressed
// First byte = 'S' Indicates that the block is SNAPPY compressed
compr *lfssnappy_decompress(unsigned char *buf, int buflen)
{
    compr *retdata;
    int ret;
    size_t *decompsize;

    if ( buflen <= 0 ) return NULL;
    decompsize=s_zmalloc(sizeof(size_t)); 
    *decompsize=BLKSIZE;
    retdata=s_malloc(sizeof(compr));
    retdata->data=s_zmalloc(BLKSIZE+1);
    if (buf[0] == 'S') {
        ret=snappy_uncompress((const char *)&buf[1], (size_t)buflen-1, (char *)retdata->data, decompsize);
        retdata->size = *decompsize;
    } else {
        retdata->size = buflen - 1;
        memcpy(retdata->data, &buf[1], buflen - 1);
    }
    s_free(decompsize);
    return retdata;
}

compr *lfssnappy_compress(unsigned char *buf, int buflen)
{
    compr *retdata;
    size_t *compsize;
    int ret;

    compsize=s_zmalloc(sizeof(size_t)); 
    retdata = s_zmalloc(sizeof(compr));
    *compsize = snappy_max_compressed_length(buflen);
    retdata->data = s_zmalloc(*compsize+1);
    ret=snappy_compress((const char*) buf,(size_t)buflen,(char *)&retdata->data[1],compsize);
    if ( ret != SNAPPY_OK ) die_snappyerr("lfssnappy_compress : SNAPPY compress failed");
    retdata->size=*compsize;
    s_free(compsize);
    if (retdata->size < buflen) {
        retdata->size++;
        retdata->data[0] = 'S';
    } else {
        retdata->size = buflen + 1;
        memcpy(&retdata->data[1], buf, buflen);
        retdata->data[0] = 0;
    }
    return retdata;
}
#endif
