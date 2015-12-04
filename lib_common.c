/*
 *   Lessfs: A data deduplicating filesystem.
 *   Copyright (C) 2008 Mark Ruijter <mruijter@lessfs.com>
 *
 *   This program is s_free software.
 *   You can redistribute lessfs and/or modify it under the terms of either
 *   (1) the GNU General Public License; either version 3 of the License,
 *   or (at your option) any later version as published by
 *   the Free Software Foundation; or (2) obtain a commercial license
 *   by contacting the Author.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY;  without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See
 *   the GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program;  if not, write to the Free Software
 *   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#ifndef LFATAL
#include "lib_log.h"
#endif

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <malloc.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/un.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <libgen.h>
#include <sys/utsname.h>
#include <sys/vfs.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <mhash.h>
#include <tcutil.h>
#include <tchdb.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include "lib_cfg.h"
#include "lib_safe.h"
#include "lib_common.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#ifdef SNAPPY
#include "lib_snappy.h"
#endif
#include "lib_qlz.h"
#include "lib_qlz15.h"
#ifdef BERKELEYDB
#include <db.h>
#include "lib_bdb.h"
#else
#ifndef HAMSTERDB
#include "lib_tc.h"
#else
#include "lib_hamster.h"
#endif
#endif
#include "lib_net.h"
#include "file_io.h"
#include "lib_repl.h"
#include "retcodes.h"
#ifdef ENABLE_CRYPTO
#include "lib_crypto.h"
#endif

extern struct configdata *config;
unsigned long long nextoffset;
extern int fdbdta;
extern int frepl;
extern int freplbl;             /* Backlog processing */
extern int BLKSIZE;
extern long working;

#ifndef HAMSTERDB
TCHDB *dbb = NULL;
TCHDB *dbu = NULL;
TCHDB *dbp = NULL;
TCBDB *dbl = NULL;              // Hardlink
TCHDB *dbs = NULL;              // Symlink
TCHDB *dbdta = NULL;
TCBDB *dbdirent = NULL;
TCBDB *freelist = NULL;         // Free list for file_io
#endif

TCTREE *workqtree;              // Used to buffer incoming data (writes)
TCTREE *readcachetree;          // Used to cache chunks of data that are likely to be read
TCTREE *path2inotree;           // Used to cache path to inode translations
TCTREE *hashtree;               // Used to lock hashes
TCTREE *inodetree;              // Used to lock inodes
TCTREE *metatree;               // Used to cache metadata of open files (e.g. inode -> struct stat).

static pthread_mutex_t global_lock_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *global_lockedby;
static pthread_mutex_t write_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *write_lockedby;
static pthread_mutex_t hash_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *hash_lockedby;
static pthread_mutex_t inode_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *inode_lockedby;
static pthread_mutex_t offset_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *offset_lockedby;
static pthread_mutex_t worker_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *worker_lockedby;
static pthread_mutex_t meta_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *meta_lockedby;
static pthread_mutex_t repl_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *repl_lockedby;
static pthread_mutex_t replbl_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *replbl_lockedby;
static pthread_mutex_t relax_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *cachep2i_lockedby;
static pthread_mutex_t cachep2i_mutex = PTHREAD_MUTEX_INITIALIZER;

void init_chunk_io()
{
    char *high;
    char *medium;
    char *low;
    struct stat stbuf;

    if ( -1 == stat(config->blockdata,&stbuf)) die_dataerr("init_chunk_io : path %s does not exist",config->blockdata);
    if (!S_ISDIR(stbuf.st_mode)) die_dataerr("init_chunk_io : path %s is not a directory",config->blockdata);
    high=as_sprintf(__FILE__, __LINE__,"%s/high",config->blockdata);
    medium=as_sprintf(__FILE__, __LINE__,"%s/medium",config->blockdata);
    low=as_sprintf(__FILE__, __LINE__,"%s/low",config->blockdata);
    //mkchunk_dir(high);
    mkchunk_dir(medium);
    //mkchunk_dir(low);
    s_free(low);
    s_free(medium);
    s_free(high);
}

void mkchunk_dir(char *path)
{
    int n,e,f,g,h;
    char *a="0123456789ABCDEF";
    char p[2];
    char q[2];
    char r[2];
    char s[2];
    char t[2];

    p[1]=0;
    q[1]=0;
    r[1]=0;
    s[1]=0;
    t[1]=0;

    if ( -1 == chdir(config->blockdata)) die_syserr();
    mkdir(path,0755);
    if ( -1 == chdir(path)) die_syserr();
    
    for (n =0; n<16 ; n++)
    {
      p[0]=a[n];        
      mkdir(p,0755);
      if ( -1 == chdir(p)) die_syserr();
      for (e=0; e<16; e++)
      {
         q[0]=a[e];
         mkdir(q,0755);
         if ( config->chunk_depth > 2 ) {
             if ( -1 == chdir(q)) die_syserr();
             for (f=0; f<16; f++)
             {
                r[0]=a[f];
                mkdir(r,0755);
                if ( config->chunk_depth > 3 ) {
                   if ( -1 == chdir(r)) die_syserr();
                   for (g=0; g<16; g++){
                      s[0]=a[g];
                      mkdir(s,0755);
                      if ( config->chunk_depth > 4 ) {
                         if ( -1 == chdir(s)) die_syserr();
                         for (h=0; h<16; h++){
                             t[0]=a[h];
                             mkdir(t,0755);
                         }
                         if ( -1 == chdir("..")) die_syserr();
                      }
                   }
                   if ( -1 == chdir("..")) die_syserr();
                }
             } 
             if ( -1 == chdir("..")) die_syserr();
          }
       }
       if ( -1 == chdir("..")) die_syserr();
    }
}

unsigned char *thash(unsigned char *buf, int size)
{
    MHASH td;
    unsigned char *hash;

    td = mhash_init(config->selected_hash);
    if (td == MHASH_FAILED)
        exit(1);

    mhash(td, buf, size);
    hash = mhash_end(td);
    return hash;
}

void check_datafile_sanity()
{
    struct stat stbuf;
    unsigned long long rsize;

    FUNC;
    if (-1 == stat(config->blockdata, &stbuf))
        die_dataerr("Failed to stat %s\n", config->blockdata);
    if (stbuf.st_size > nextoffset) {
        LDEBUG("nextoffset = %llu, real size = %llu", nextoffset,
               stbuf.st_size);
        rsize = round_512(nextoffset);
        if (-1 == ftruncate(fdbdta, rsize))
            die_dataerr("Failed to truncate %s to %llu bytes\n",
                        config->blockdata, rsize);
    }
    EFUNC;
}

INUSE *get_offset(unsigned long long size)
{
    unsigned long long mbytes;
    unsigned long long offset;
    unsigned long long notfound = (0 - 1);
    time_t thetime;
    INUSE *inuse = NULL;

    LDEBUG("get_offset %llu", size);
    thetime = time(NULL);
    get_offset_lock((char *) __PRETTY_FUNCTION__);
    mbytes = round_512(size);
    mbytes = mbytes / 512;
    if ( config->reclaim) {
       offset = get_offset_fast(mbytes);
       if (offset == notfound) {
           if (config->nospace != -ENOSPC && config->nospace != ENOSPC
               && config->nospace != RECLAIM_AGRESSIVE) {
               offset = nextoffset;
               set_new_offset(size);
           } else {
               inuse = get_offset_reclaim(mbytes, offset);
               if (NULL == inuse) {
                   // We failed to reclaim space. 
                   // Write this last block and disable new writes
                   offset = nextoffset;
                   set_new_offset(size);
                   config->nospace = ENOSPC;
               } else
                   inuse->size = size;
           }
       }
    } else {
       offset = nextoffset;
       set_new_offset(size);
    }
    if (NULL == inuse) {
        inuse = s_zmalloc(sizeof(INUSE));
        inuse->offset = offset;
        inuse->size = size;
        inuse->allocated_size = round_512(size);
    } else
        LDEBUG("get_offset : needed %llu bytes, got %llu", size,
               inuse->allocated_size);
    release_offset_lock();
    LDEBUG("/get_offset %llu", size);
    return inuse;
}

int fs_symlink(char *from, char *to)
{
    int res = 0;
    char *todir;

    todir = s_dirname(to);
    dbmknod(to, 0777 | S_IFLNK, from, 0);
    res = update_parent_time(todir, 0);
    s_free(todir);
    return (res);
}

int fs_readlink(const char *path, char *buf, size_t size)
{
    int res = 0;
    DAT *data;
    unsigned long long inode;

    FUNC;
    inode = get_inode(path);
    if (0 == inode)
        return (-ENOENT);

    data = search_dbdata(DBS, &inode, sizeof(unsigned long long), LOCK);
    if (NULL == data) {
        res = -ENOENT;
    } else {
        if (size - 1 > data->size) {
            memcpy(buf, data->data, data->size + 1);
        } else {
            memcpy(buf, data->data, size - 1);
        }
        DATfree(data);
    }
    return (res);
}

void invalidate_p2i(char *filename)
{
    cachep2i_lock((char *) __PRETTY_FUNCTION__);
    tctreeout(path2inotree, filename, strlen(filename));
    release_cachep2i_lock();
    return;
}

void erase_p2i()
{
    cachep2i_lock((char *) __PRETTY_FUNCTION__);
    tctreeclear(path2inotree);
    release_cachep2i_lock();
    return;
}


/* Compare two list elements in INOBNO order.
   `a' specifies the pointer to one element.
   `b' specifies the pointer to the other element.
   The return value is positive if the former is big, negative if the latter is big, 0 if both
   are equivalent. */
static int inobnolistelemcmp(const void *a, const void *b){
  INOBNO *left;
  INOBNO *right;
  TCLISTDATUM *c;
  TCLISTDATUM *d;
  int ret=1;

  if ( a == NULL && b == NULL ) return(0);
  if ( a == NULL ) return(-1);
  if ( b == NULL ) return(-1);
  c=(TCLISTDATUM *)a;
  d=(TCLISTDATUM *)b;
  if ( c->size != sizeof(INOBNO)) return(-1);
  if ( d->size != sizeof(INOBNO)) return(-1);
  left=(INOBNO *)c->ptr;
  right=(INOBNO *)d->ptr;
  if ( left->inode < right->inode ) ret=-1;
  if ( left->inode == right->inode ) {
      if ( left->blocknr < right->blocknr ) ret=-1;
      if ( left->blocknr == right->blocknr ) ret=0;
  }
  return(ret);
}

void inobnolistsort(TCLIST *list){
   qsort(list->array + list->start, list->num, sizeof(list->array[0]), inobnolistelemcmp);
}

void cache_p2i(char *filename, struct stat *stbuf)
{
    cachep2i_lock((char *) __PRETTY_FUNCTION__);
    LDEBUG("Cache |%s|", filename);
    if (tctreernum(path2inotree) > MAX_META_CACHE) {
        LINFO
            ("Clearing path2inode cache, when this message is logged with high frequency consider raising MAX_META_CACHE");
        tctreeclear(path2inotree);
    }
    tctreeput(path2inotree, filename, strlen(filename), (void *) stbuf,
              sizeof(struct stat));
    release_cachep2i_lock();
    return;
}

#ifdef DEBUG
void logiv(char *msg, unsigned char *bhash)
{
    char *ascii_hash = NULL;
    int n;
    char *p1, *p2;

    for (n = 0; n < 8; n++) {
        p1 = as_sprintf(__FILE__, __LINE__, "%02X", bhash[n]);
        if (n == 0) {
            ascii_hash = s_strdup(p1);
        } else {
            p2 = s_strdup(ascii_hash);
            s_free(ascii_hash);
            ascii_hash = as_sprintf(__FILE__, __LINE__, "%s%s", p2, p1);
            s_free(p2);
        }
        s_free(p1);
    }
    LDEBUG("%s : %s", msg, ascii_hash);
    s_free(ascii_hash);
}
#else
void logiv(char *msg, unsigned char *bhash)
{
}
#endif

#ifdef DEBUG
void loghash(char *msg, unsigned char *bhash)
{
    char *ascii_hash = NULL;
    int n;
    char *p1, *p2;

    for (n = 0; n < config->hashlen; n++) {
        p1 = as_sprintf(__FILE__, __LINE__, "%02X", bhash[n]);
        if (n == 0) {
            ascii_hash = s_strdup(p1);
        } else {
            p2 = s_strdup(ascii_hash);
            s_free(ascii_hash);
            ascii_hash = as_sprintf(__FILE__, __LINE__, "%s%s", p2, p1);
            s_free(p2);
        }
        s_free(p1);
    }
    LDEBUG("%s : %s", msg, ascii_hash);
    s_free(ascii_hash);
}
# else
void loghash(char *msg, unsigned char *bhash)
{
}
# endif

void log_fatal_hash(char *msg, unsigned char *bhash)
{
    char *ascii_hash = NULL;
    int n;
    char *p1, *p2;

    for (n = 0; n < config->hashlen; n++) {
        p1 = as_sprintf(__FILE__, __LINE__, "%02X", bhash[n]);
        if (n == 0) {
            ascii_hash = s_strdup(p1);
        } else {
            p2 = s_strdup(ascii_hash);
            s_free(ascii_hash);
            ascii_hash = as_sprintf(__FILE__, __LINE__, "%s%s", p2, p1);
            s_free(p2);
        }
        s_free(p1);
    }
    LFATAL("%s : %s", msg, ascii_hash);
    s_free(ascii_hash);
}

unsigned char *hash_to_ascii(unsigned char *bhash)
{
    char *ascii_hash = NULL;
    int n;
    char *p1, *p2;

    for (n = 0; n < config->hashlen; n++) {
        p1 = as_sprintf(__FILE__, __LINE__, "%02X", bhash[n]);
        if (n == 0) {
            ascii_hash = s_strdup(p1);
        } else {
            p2 = s_strdup(ascii_hash);
            s_free(ascii_hash);
            ascii_hash = as_sprintf(__FILE__, __LINE__, "%s%s", p2, p1);
            s_free(p2);
        }
        s_free(p1);
    }
    return (unsigned char *)ascii_hash;
}

void create_hash_note(unsigned char *hash)
{
    unsigned long long inuse = 0;
    wait_hash_pending(hash);
    tctreeput(hashtree, (void *) hash, config->hashlen, &inuse,
              sizeof(unsigned long long));
    release_hash_lock();
}

void wait_hash_pending(unsigned char *hash)
{
    const char *data = NULL;
    int vsize;
    while (1) {
        get_hash_lock((char *) __PRETTY_FUNCTION__);
        data = tctreeget(hashtree, hash, config->hashlen, &vsize);
        if (NULL == data)
            break;
        release_hash_lock();
        usleep(10);
    }
}

void delete_hash_note(unsigned char *hash)
{
    get_hash_lock((char *) __PRETTY_FUNCTION__);
    tctreeout(hashtree, (void *) hash, config->hashlen);
    release_hash_lock();
}


void create_inode_note(unsigned long long inode)
{
    wait_inode_pending(inode);
    LDEBUG("create_inode_note : %llu locked", inode);
    tctreeput(inodetree, (void *) &inode, sizeof(unsigned long long),
              &inode, sizeof(unsigned long long));
    release_inode_lock();
}

void wait_inode_pending(unsigned long long inode)
{
    const char *data = NULL;
    int vsize;
    while (1) {
        get_inode_lock((char *) __PRETTY_FUNCTION__);
        data = tctreeget(inodetree, &inode,
                         sizeof(unsigned long long), &vsize);
        if (NULL == data)
            break;
        release_inode_lock();
        usleep(10);
    }
}

int inode_isnot_locked(unsigned long long inode)
{
    const char *data = NULL;
    int vsize;
    int ret = 0;

    get_inode_lock((char *) __PRETTY_FUNCTION__);
    data = tctreeget(inodetree, &inode,
                     sizeof(unsigned long long), &vsize);
    if (NULL == data)
        ret = 1;
    release_inode_lock();
    return (ret);
}

void wait_inode_pending2(unsigned long long inode)
{
    const char *data = NULL;
    int vsize;
    while (1) {
        get_inode_lock((char *) __PRETTY_FUNCTION__);
        data = tctreeget(inodetree, &inode,
                         sizeof(unsigned long long), &vsize);
        if (NULL == data)
            break;
        LDEBUG("wait_inode_pending2 : waiting on locked inode %llu",
               inode);
        release_inode_lock();
        usleep(10);
    }
    release_inode_lock();
}

void delete_inode_note(unsigned long long inode)
{
    get_inode_lock((char *) __PRETTY_FUNCTION__);
    tctreeout(inodetree, (void *) &inode, sizeof(unsigned long long));
    LDEBUG("delete_inode_note : %llu unlocked", inode);
    release_inode_lock();
}

void die_lock_report(const char *msg, const char *msg2)
{
    LFATAL("die_lock_report : timeout on %s, called by %s", msg2, msg);
    if (0 == try_global_lock()) {
        LFATAL("global_lock : 0 (unset)");
    } else {
        LFATAL("global_lock : 1 (set)");
    }
    if (0 == try_write_lock()) {
        LFATAL("write_lock : 0 (unset)");
    } else {
        LFATAL("write_lock : 1 (set)");
    }
    if (0 == try_meta_lock()) {
        LFATAL("meta_lock : 0 (unset)");
    } else {
        LFATAL("meta_lock : 1 (set)");
    }
    die_dataerr("Abort after deadlock");
}

void get_global_lock(const char *msg)
{
    FUNC;
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + GLOBAL_LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;

    err_code = pthread_mutex_timedlock(&global_lock_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&global_lock_mutex);
#endif
    global_lockedby = msg;
    EFUNC;
    return;
}

void release_global_lock()
{
    FUNC;
    pthread_mutex_unlock(&global_lock_mutex);
    EFUNC;
    return;
}

int try_global_lock()
{
    int res;
    FUNC;
    res = pthread_mutex_trylock(&global_lock_mutex);
    EFUNC;
    return (res);
}


int try_repl_lock()
{
    int res;
    FUNC;
    res = pthread_mutex_trylock(&repl_mutex);
    EFUNC;
    return (res);
}

int try_replbl_lock()
{
    int res;
    FUNC;
    res = pthread_mutex_trylock(&replbl_mutex);
    EFUNC;
    return (res);
}


void write_lock(const char *msg)
{
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;


    err_code = pthread_mutex_timedlock(&write_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&write_mutex);
#endif
    write_lockedby = msg;
    return;
}

void release_write_lock()
{
    pthread_mutex_unlock(&write_mutex);
    return;
}


void cachep2i_lock(const char *msg)
{
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;


    err_code = pthread_mutex_timedlock(&cachep2i_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&cachep2i_mutex);
#endif
    cachep2i_lockedby = msg;
    return;
}

void release_cachep2i_lock()
{
    pthread_mutex_unlock(&cachep2i_mutex);
    return;
}

int try_cachep2i_lock()
{
    int res;
    res = pthread_mutex_trylock(&cachep2i_mutex);
    return (res);
}


int try_write_lock()
{
    int res;
    res = pthread_mutex_trylock(&write_mutex);
    return (res);
}

int try_offset_lock()
{
    FUNC;
    int res;
    res = pthread_mutex_trylock(&offset_mutex);
    EFUNC;
    return (res);
}

void get_hash_lock(const char *msg)
{
    FUNC;
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;

    FUNC;
    err_code = pthread_mutex_timedlock(&hash_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&hash_mutex);
#endif
    hash_lockedby = msg;
    EFUNC;
    return;
}

int try_hash_lock()
{
    FUNC;
    int res;
    res = pthread_mutex_trylock(&hash_mutex);
    EFUNC;
    return (res);
}

void release_hash_lock()
{
    FUNC;
    pthread_mutex_unlock(&hash_mutex);
    EFUNC;
    return;
}

void get_inode_lock(const char *msg)
{
    FUNC;
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;

    FUNC;
    err_code = pthread_mutex_timedlock(&inode_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&inode_mutex);
#endif
    inode_lockedby = msg;
    EFUNC;
    return;
}

void release_inode_lock()
{
    FUNC;
    pthread_mutex_unlock(&inode_mutex);
    EFUNC;
    return;
}

void get_offset_lock(const char *msg)
{
    FUNC;
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + GLOBAL_LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;

    err_code = pthread_mutex_timedlock(&offset_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&offset_mutex);

#endif
    offset_lockedby = msg;
    EFUNC;
    return;
}

void release_offset_lock()
{
    FUNC;
    pthread_mutex_unlock(&offset_mutex);
    EFUNC;
    return;
}

void worker_lock(const char *msg)
{
    FUNC;
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + GLOBAL_LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;

    err_code = pthread_mutex_timedlock(&worker_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&worker_mutex);

#endif
    worker_lockedby = msg;
    EFUNC;
    return;
}

void release_worker_lock()
{
    FUNC;
    pthread_mutex_unlock(&worker_mutex);
    EFUNC;
    return;
}

void meta_lock(const char *msg)
{
    FUNC;
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;

    FUNC;
    err_code = pthread_mutex_timedlock(&meta_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&meta_mutex);
#endif
    meta_lockedby = msg;
    EFUNC;
    return;
}

void repl_lock(const char *msg)
{
    //FUNC;
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;

    err_code = pthread_mutex_timedlock(&repl_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&repl_mutex);
#endif
    repl_lockedby = msg;
    //EFUNC;
    return;
}

void release_repl_lock()
{
    pthread_mutex_unlock(&repl_mutex);
    return;
}

void release_replbl_lock()
{
    pthread_mutex_unlock(&replbl_mutex);
    return;
}

void release_meta_lock()
{
    FUNC;
    pthread_mutex_unlock(&meta_mutex);
    EFUNC;
    return;
}

int try_meta_lock()
{
    int res;
    res = pthread_mutex_trylock(&meta_mutex);
    return (res);
}

void trunc_lock()
{
    if (config->background_delete != 0) {
        pthread_mutex_lock(&relax_mutex);
    }
    return;
}

void release_trunc_lock()
{
    if (config->background_delete != 0) {
        pthread_mutex_unlock(&relax_mutex);
    }
    return;
}

void truncation_wait()
{
    if (config->background_delete != 0) {
        pthread_mutex_lock(&relax_mutex);
        pthread_mutex_unlock(&relax_mutex);
    }
    return;
}

DAT *create_mem_ddbuf(MEMDDSTAT * ddstat)
{
    DAT *ddbuf;

    ddbuf = s_malloc(sizeof(DAT));
    ddbuf->data = s_malloc(sizeof(MEMDDSTAT));
    ddbuf->size = sizeof(MEMDDSTAT);
    memcpy(ddbuf->data, ddstat, sizeof(MEMDDSTAT));
    return ddbuf;
}

DDSTAT *value_to_ddstat(DAT * vddstat)
{
    DDSTAT *ddbuf;
    int filelen;
    DAT *decrypted;

    FUNC;
    decrypted = vddstat;
#ifdef ENABLE_CRYPTO
    if (config->encryptmeta && config->encryptdata) {
        decrypted = lfsdecrypt(vddstat);
    }
#endif
    filelen =
        decrypted->size - (sizeof(struct stat) +
                           sizeof(unsigned long long));
    ddbuf = s_zmalloc(sizeof(DDSTAT));
    memcpy(&ddbuf->stbuf, decrypted->data, sizeof(struct stat));
    memcpy(&ddbuf->real_size, decrypted->data + sizeof(struct stat),
           sizeof(unsigned long long));
    if (1 == filelen) {
        ddbuf->filename[0] = 0;
    } else {
        memcpy(ddbuf->filename,
               &decrypted->data[(sizeof(struct stat) +
                                 sizeof(unsigned long long))], filelen);
    }
    LDEBUG("value_to_ddstat : return %llu", ddbuf->stbuf.st_ino);
#ifdef ENABLE_CRYPTO
    if (config->encryptmeta && config->encryptdata) {
        DATfree(decrypted);
    }
#endif
    EFUNC;
    return ddbuf;
}

DAT *create_ddbuf(struct stat stbuf, char *filename,
                  unsigned long long real_size)
{
    DAT *ddbuf;
    int len;
#ifdef ENABLE_CRYPTO
    DAT *lencrypted;
#endif

    FUNC;
    if (filename) {
        len =
            sizeof(struct stat) + sizeof(unsigned long long) +
            strlen((char *) filename) + 1;
    } else
        len = sizeof(struct stat) + sizeof(unsigned long long) + 1;

    ddbuf = s_malloc(sizeof(DAT));
    ddbuf->size = len;
    ddbuf->data = s_malloc(ddbuf->size);
    memcpy(ddbuf->data, &stbuf, sizeof(struct stat));
    memcpy(ddbuf->data + sizeof(struct stat), &real_size,
           sizeof(unsigned long long));
    if (filename) {
        memcpy(ddbuf->data + sizeof(struct stat) +
               sizeof(unsigned long long), (char *) filename,
               strlen((char *) filename) + 1);
    } else
        memset(ddbuf->data + sizeof(struct stat) +
               sizeof(unsigned long long), 0, 1);

#ifdef ENABLE_CRYPTO
    if (config->encryptmeta && config->encryptdata) {
        lencrypted = lfsencrypt(ddbuf->data, ddbuf->size);
        DATfree(ddbuf);
        EFUNC;
        return lencrypted;
    }
#endif
    EFUNC;
    return ddbuf;
}

void dbmknod(const char *path, mode_t mode, char *linkdest, dev_t rdev)
{
    unsigned long long inode;

    FUNC;
    LDEBUG("dbmknod : %s", path);
    inode = get_next_inode();
    write_file_ent(path, inode, mode, linkdest, rdev);
    EFUNC;
    return;
}

/* Fill struct stat from cache if present in the cache
   return 1 when found or 0 when not found in cache. */
int get_realsize_fromcache(unsigned long long inode, struct stat *stbuf)
{
    int result = 0;
    const char *data;
    MEMDDSTAT *mddstat;
    int vsize;
    meta_lock((char *) __PRETTY_FUNCTION__);
    data = tctreeget(metatree, &inode, sizeof(unsigned long long), &vsize);
    if (data == NULL) {
        LDEBUG("inode %llu not found use size from database.", inode);
        release_meta_lock();
        return (result);
    }
    result++;
    mddstat = (MEMDDSTAT *) data;
    memcpy(stbuf, &mddstat->stbuf, sizeof(struct stat));
    LDEBUG
        ("get_realsize_fromcache : return stbuf from cache : size %llu time %lu",
         stbuf->st_size, mddstat->stbuf.st_atim.tv_sec);
    release_meta_lock();
    return (result);
}

int get_dir_inode(char *dname, struct stat *stbuf, bool cache_request)
{
    char *p;
    int depth = 0;
    DDSTAT *filestat = NULL;
    int res = 0, vsize;
    unsigned long long inode = 1;
    char *basedname;
    struct stat *st;

    FUNC;

    basedname = s_dirname(dname);
    cachep2i_lock((char *) __PRETTY_FUNCTION__);
    st = (struct stat *) tctreeget(path2inotree, (void *) basedname,
                                   strlen(basedname), &vsize);
    if (st) {
        memcpy(stbuf, st, vsize);
        s_free(basedname);
        release_cachep2i_lock();
        return (res);
    }
    release_cachep2i_lock();
    while (1) {
        p = strchr(dname, '/');
        if (NULL == p)
            break;
        p[0] = 0;
        LDEBUG("p=%s", p);
        if (depth == 0) {
            LDEBUG("Lookup inode 1");
            filestat =
                dnode_bname_to_inode(&inode, sizeof(unsigned long long),
                                     "/");
        } else {
            ddstatfree(filestat);
            inode = stbuf->st_ino;
            filestat =
                dnode_bname_to_inode(&inode, sizeof(unsigned long long),
                                     dname);
            if (NULL == filestat) {
                res = -ENOENT;
                break;
            }
        }
        memcpy(stbuf, &filestat->stbuf, sizeof(struct stat));
        LDEBUG("After memcpy %llu", stbuf->st_ino);
        p++;
        depth++;
        dname = p;
        if (NULL == p)
            break;
    }
    if (res == 0) {
        ddstatfree(filestat);
        if (cache_request)
            cache_p2i(basedname, stbuf);
        LDEBUG("return %s stbuf.st_ino=%llu", basedname, stbuf->st_ino);
    }
    s_free(basedname);
    EFUNC;
    return (res);
}

int path_from_cache(char *path, struct stat *stbuf)
{
    int res = 0, vsize;
    struct stat *st;
    DAT *statdata;
    DDSTAT *ddstat;

    cachep2i_lock((char *) __PRETTY_FUNCTION__);
    st = (struct stat *) tctreeget(path2inotree, (void *) path,
                                   strlen(path), &vsize);
    if (st) {
        res = get_realsize_fromcache(st->st_ino, stbuf);
        if (!res) {
            memcpy(stbuf, st, vsize);
            // We always have to fetch hardlinks from disk.
            if (stbuf->st_nlink > 1) {
                statdata =
                    search_dbdata(DBP, &stbuf->st_ino,
                                  sizeof(unsigned long long), LOCK);
                if (NULL == statdata) {
                    release_cachep2i_lock();
                    return (res);
                }
                ddstat = value_to_ddstat(statdata);
                DATfree(statdata);
                memcpy(stbuf, &ddstat->stbuf, sizeof(struct stat));
                ddstatfree(ddstat);
            }
            res = 1;
        }
    }
    release_cachep2i_lock();
    LDEBUG("path_from_cache : return  %s %i", path, res);
    return (res);
}


int dbstat(const char *filename, struct stat *stbuf, bool cache_request)
{
    int retcode = 0;
    char *dname = NULL;
    char *bname = NULL;
    char *dupdname = NULL;
    char *mdupdname = NULL;
    DDSTAT *filestat = NULL;

    FUNC;
    dname = s_dirname((char *) filename);
    bname = s_basename((char *) filename);
    dupdname = s_strdup((char *) filename);
    mdupdname = dupdname;


    if (!path_from_cache((char *) filename, stbuf)) {
        // Walk the directory
        retcode = get_dir_inode(dupdname, stbuf, cache_request);
        if (0 == retcode) {
            if (0 != strcmp(bname, dname)) {    /* This is the rootdir */
                // Now find the file within the directory
                filestat =
                    dnode_bname_to_inode(&stbuf->st_ino,
                                         sizeof(unsigned long long),
                                         bname);
                if (filestat) {
                    if (0 == strcmp(bname, filestat->filename)) {
                        if (0 ==
                            get_realsize_fromcache(filestat->stbuf.st_ino,
                                                   stbuf)) {
                            memcpy(stbuf, &filestat->stbuf,
                                   sizeof(struct stat));
                            if (cache_request)
                                cache_p2i((char *) filename, stbuf);
                        }
                    } else {
                        retcode = -ENOENT;
                    }
                    ddstatfree(filestat);
                } else {
                    retcode = -ENOENT;
                }
            }
        } else
            retcode = -ENOENT;
    }
    s_free(mdupdname);
    s_free(bname);
    s_free(dname);
    if (retcode == -ENOENT)
        LDEBUG("dbstat : File %s not found.", filename);
    EFUNC;
    return (retcode);
}

/* Free the ddstat stucture when not NULL */
void ddstatfree(DDSTAT * ddstat)
{
    if (ddstat) {
        LDEBUG("ddstatfree really s_free");
        s_free(ddstat);
        ddstat = NULL;
    }
}

MEMDDSTAT *value_tomem_ddstat(char *value, int size)
{
    MEMDDSTAT *memddstat;
    memddstat = s_malloc(size);
    memcpy(memddstat, value, size);
    return memddstat;
}

void memddstatfree(MEMDDSTAT * ddstat)
{
    if (ddstat) {
        LDEBUG("memddstatfree really s_free");
        s_free(ddstat);
    }
    ddstat = NULL;
    return;
}

void comprfree(compr * compdata)
{
    s_free(compdata->data);
    s_free(compdata);
}

void write_file_ent(const char *filename, unsigned long long inode,
                    mode_t mode, char *linkdest, dev_t rdev)
{
    struct stat stbuf;
    struct stat dirstat;
    time_t thetime;
    char *bname;
    char *parentdir;
    int res = 0;
    DAT *ddbuf;
    bool isdot = 0;
    bool isrootdir = 0;

    FUNC;
    LDEBUG("write_file_ent : filename %s, inodenumber %llu", filename,
           inode);
    bname = s_basename((char *) filename);
    parentdir = s_dirname((char *) filename);
    if (0 == strcmp(filename, "/"))
        isrootdir = 1;
    LDEBUG("write_file_ent: parentdir = %s", parentdir);
    write_nfi(inode + 1);

//Write stat structure to create an empty file.
    stbuf.st_ino = inode;
    stbuf.st_dev = 999988;
    stbuf.st_mode = mode;
    if (S_ISDIR(mode)) {
        stbuf.st_nlink = 2;
    } else
        stbuf.st_nlink = 1;
    if (!isrootdir) {
        if (0 == strcmp(filename, "/lost+found") ||
            0 == strcmp(filename, "/.lessfs") ||
            0 == strcmp(filename, "/.lessfs/locks") ||
            0 == strcmp(filename, "/.lessfs/replication") ||
            0 == strcmp(filename, "/.lessfs/replication/enabled") ||
            0 == strcmp(filename, "/.lessfs/replication/backlog") ||
            0 == strcmp(filename, "/.lessfs/replication/sequence") ||
            0 == strcmp(filename, "/.lessfs/replication/rotate_replog") ||
            0 == strcmp(filename, "/lost+found/.") ||
            0 == strcmp(filename, "/lost+found/..") ||
            0 == strcmp(filename, "/.lessfs/lessfs_stats")) {
            stbuf.st_uid = 0;
            stbuf.st_gid = 0;
        } else {
            stbuf.st_uid = fuse_get_context()->uid;
            stbuf.st_gid = fuse_get_context()->gid;
        }
    } else {
        stbuf.st_uid = 0;
        stbuf.st_gid = 0;
    }
    stbuf.st_rdev = rdev;
    if (S_ISDIR(mode)) {
        stbuf.st_size = 4096;
        stbuf.st_blocks = 1;
    } else {
        stbuf.st_size = 0;
        stbuf.st_blocks = 0;
    }
    if (S_ISLNK(mode)) {
        stbuf.st_size = 3;
        stbuf.st_blocks = 1;
    }
    if (config->sticky_on_locked && S_ISREG(stbuf.st_mode)) {
        if (S_ISVTX == (S_ISVTX & stbuf.st_mode)) {
            LINFO("reset stickybit on : %llu",
                  (unsigned long long) stbuf.st_ino);
            stbuf.st_mode = stbuf.st_mode ^ S_ISVTX;
        }
    }
    stbuf.st_blksize = BLKSIZE;
    thetime = time(NULL);
    stbuf.st_atim.tv_sec = thetime;
    stbuf.st_atim.tv_nsec = 0;
    stbuf.st_mtim.tv_sec = thetime;
    stbuf.st_mtim.tv_nsec = 0;
    stbuf.st_ctim.tv_sec = thetime;
    stbuf.st_ctim.tv_nsec = 0;

    ddbuf = create_ddbuf(stbuf, bname, 0);
    LDEBUG("write_file_ent : write dbp inode %llu", inode);
    bin_write_dbdata(DBP, &inode, sizeof(inode), ddbuf->data, ddbuf->size);
    cache_p2i((char *) filename, &stbuf);
    DATfree(ddbuf);
    if (linkdest) {
        bin_write_dbdata(DBS, &inode, sizeof(unsigned long long), linkdest,
                         strlen(linkdest) + 1);
    }
    if (0 == strcmp(bname, "."))
        isdot = 1;
    if (0 == strcmp(bname, ".."))
        isdot = 1;
  recurse:
    if (S_ISDIR(mode) && isdot != 1) {
        btbin_write_dup(DBDIRENT, &stbuf.st_ino, sizeof(stbuf.st_ino),
                        &inode, sizeof(inode), LOCK);
    } else {
        res = dbstat(parentdir, &dirstat, 1);
        btbin_write_dup(DBDIRENT, &dirstat.st_ino, sizeof(dirstat.st_ino),
                        &inode, sizeof(inode), LOCK);
    }
    if (S_ISDIR(mode) && !isdot && !isrootdir) {
        // Create the link inode to the previous directory
        LDEBUG("Create the link inode to the previous directory");
        isdot = 1;
        // Only if !isrootdir. Nothing lower the root.
        goto recurse;
    }
    s_free(parentdir);
    s_free(bname);
    EFUNC;
}

void write_nfi(unsigned long long nextinode)
{
    bin_write_dbdata(DBP, (unsigned char *) "NFI", strlen("NFI"),
                     (unsigned char *) &nextinode, sizeof(nextinode));
    return;
}

void write_seq(unsigned long sequence)
{
    bin_write(DBP, (unsigned char *) "SEQ", strlen("SEQ"),
              (unsigned char *) &sequence, sizeof(sequence));
    if (config->replication == 1 && config->replication_role == 0) {
        write_replication_data(DBP, REPLWRITE, (char *) "SEQ",
                               strlen("SEQ"), (char *) &sequence,
                               sizeof(sequence), MAX_ALLOWED_THREADS - 2);
    }
    return;
}

unsigned long get_sequence()
{
    unsigned long sequence;
    DAT *data;
    FUNC;
    data =
        search_dbdata(DBP, (unsigned char *) "SEQ", strlen("SEQ"), LOCK);
    if (NULL == data)
        die_dataerr
            ("Unable to retrieve current replication sequence number");
    memcpy(&sequence, data->data, data->size);
    DATfree(data);
    EFUNC;
    return (sequence);
}

void next_sequence()
{
    unsigned long sequence;
    FUNC;
    sequence = get_sequence();
    sequence++;
    write_seq(sequence);
    EFUNC;
    return;
}

void formatfs()
{
    struct stat stbuf;
    unsigned long long nextinode = 0;
    unsigned char *stiger;
    char *blockdatadir;
    unsigned long sequence = 0;
#ifdef ENABLE_CRYPTO
    CRYPTO crypto;
#endif
    char *hashstr;
    INUSE inuse;

    FUNC;

    hashstr =
        as_sprintf(__FILE__, __LINE__, "%s%i", config->hash,
                   config->hashlen);
    stiger = thash((unsigned char *) hashstr, strlen(hashstr));
    s_free(hashstr);
    if ( config->blockdata_io_type == TOKYOCABINET ) {
        update_inuse(stiger, 1);
    } else {
        inuse.inuse = 1;
        inuse.size = 0;
        inuse.offset = 0;
        file_update_inuse(stiger, &inuse);
    }
    lessfs_trans_stamp();
    s_free(stiger);

#ifdef ENABLE_CRYPTO
    if (config->encryptdata) {
        stiger = thash(config->passwd, strlen((char *) config->passwd));
        loghash("store passwd as hash", stiger);
        memcpy(&crypto.passwd, stiger, config->hashlen);
        memcpy(&crypto.iv, config->iv, 8);
        bin_write_dbdata(DBP, &nextinode, sizeof(unsigned long long),
                         &crypto, sizeof(CRYPTO));
        s_free(stiger);
    }
#endif
    nextinode = 1;
    if (config->blockdata_io_type !=  TOKYOCABINET ) {
        blockdatadir = s_dirname(config->blockdata);
        stat(blockdatadir, &stbuf);
        s_free(blockdatadir);
    } else {
        stat(config->blockdata, &stbuf);
    }
    write_nfi(nextinode);
    fs_mkdir("/", stbuf.st_mode);
    fs_mkdir("/.lessfs", stbuf.st_mode);
    fs_mkdir("/.lessfs/locks", stbuf.st_mode);
    dbmknod("/.lessfs/lessfs_stats", 0755 | S_IFREG, NULL, 0);
    fs_mkdir("/.lessfs/replication", stbuf.st_mode);
    dbmknod("/.lessfs/replication/enabled", 0755 | S_IFREG, NULL, 0);
    dbmknod("/.lessfs/replication/backlog", 0755 | S_IFREG, NULL, 0);
    dbmknod("/.lessfs/replication/sequence", 0755 | S_IFREG, NULL, 0);
    dbmknod("/.lessfs/replication/rotate_replog", 0755 | S_IFREG, NULL, 0);
    /* All database transactions are written to the replog file
       In the case of formatting the filesystem the information
       should not be replicated, so we truncate replog */
    if (-1 == ftruncate(frepl, 0))
        die_syserr();
    write_seq(sequence);
    nextinode = 100;
    write_nfi(nextinode);
    db_close(0);
    return;
}

DAT *lfsdecompress(DAT * cdata)
{
    DAT *data = NULL;
    int rsize;
    DAT *decrypted;

    decrypted = cdata;
#ifdef ENABLE_CRYPTO
    if (config->encryptdata) {
        decrypted = lfsdecrypt(cdata);
    }
#endif

    if (decrypted->data[0] == 0 || decrypted->data[0] == 'Q') {
        data = (DAT *) clz_decompress(decrypted->data, decrypted->size);
        goto end;
    }
    if (decrypted->data[0] == 0 || decrypted->data[0] == 'P') {
        data = (DAT *) clz15_decompress(decrypted->data, decrypted->size);
        goto end;
    }
    if (decrypted->data[0] == 'S') {
#ifdef SNAPPY 
        data = (DAT *) lfssnappy_decompress(decrypted->data, decrypted->size);
        goto end;
#else
        LFATAL("lessfs is compiled without support for SNAPPY");
        db_close(0);
        exit(EXIT_DATAERR);
#endif
    }
    if (decrypted->data[0] == 'L') {
#ifdef LZO
        data = (DAT *) lzo_decompress(decrypted->data, decrypted->size);
        goto end;
#else
        LFATAL("lessfs is compiled without LZO support");
        db_close(0);
        exit(EXIT_DATAERR);
#endif
    }
    if (decrypted->data[0] == 'G') {
        data = s_malloc(sizeof(DAT));
        data->data = (unsigned char *) tcgzipdecode((const char *)
                                                    &decrypted->data[1],
                                                    decrypted->size - 1,
                                                    &rsize);
        data->size = rsize;
        goto end;
    }
    if (decrypted->data[0] == 'B') {
        data = s_malloc(sizeof(DAT));
        data->data = (unsigned char *) tcbzipdecode((const char *)
                                                    &decrypted->data[1],
                                                    decrypted->size - 1,
                                                    &rsize);
        data->size = rsize;
        goto end;
    }
    if (decrypted->data[0] == 'D') {
        data = s_malloc(sizeof(DAT));
        data->data =
            (unsigned char *) tcinflate((const char *) &decrypted->data[1],
                                        decrypted->size - 1, &rsize);
        data->size = rsize;
        goto end;
    }
    LFATAL("Data found with unsupported compression type %c",
                decrypted->data[0]);
  end:
#ifdef ENABLE_CRYPTO
    if (config->encryptdata) {
        DATfree(decrypted);
    }
#endif
    return data;
}

void auto_repair(INOBNO * inobno)
{
    LINFO("Inode %llu is damaged at offset %llu", inobno->inode,
          inobno->blocknr * BLKSIZE);
    delete_dbb(inobno);
    return;
}

unsigned long long readBlock(unsigned long long blocknr,
                             char *blockdata, size_t rsize,
                             size_t block_offset, unsigned long long inode)
{
    char *cachedata;
    DAT *cdata;
    DAT *data;
    INOBNO inobno;
    CCACHEDTA *ccachedta = NULL;
    DAT *tdata;
    int vsize;
    uintptr_t p;
    int locked;
    int ret = 0;

    inobno.inode = inode;
    inobno.blocknr = blocknr;

    locked = inode_isnot_locked(inode);
    write_lock((char *) __PRETTY_FUNCTION__);
    cachedata =
        (char *) tctreeget(readcachetree, (void *) &inobno, sizeof(INOBNO),
                           &vsize);
    if (NULL == cachedata) {
        tdata = check_block_exists(&inobno);
        if (NULL == tdata) {
            release_write_lock();
            return (0);
        }
        cdata = search_dbdata(DBDTA, tdata->data, tdata->size, LOCK);
        if (NULL == cdata) {
            auto_repair(&inobno);
            DATfree(tdata);
            release_write_lock();
            return (0);
        }
        DATfree(tdata);
        data = lfsdecompress(cdata);
        if ( NULL == data ) die_dataerr("inode %llu - %llu failed to decompress block", inobno.inode,inobno.blocknr); 
        LDEBUG("readBlock blocknr %llu comes from db", blocknr);
        if (block_offset < data->size) {
            if (rsize > data->size - block_offset) {
                memcpy(blockdata, data->data + block_offset,
                       data->size - block_offset);
            } else {
                memcpy(blockdata, data->data + block_offset, rsize);
            }
        }
        DATfree(cdata);
//---
// Do not cache blocks as long as the inode is being truncated
        if (locked) {
// When we read a block < BLKSIZE there it is likely that we need
// to read it again so it makes sense to put it in a cache.
            if (rsize < BLKSIZE) {
// Make sure that we don't overflow the cache.
                if (tctreernum(workqtree) * 2 > config->cachesize ||
                    tctreernum(readcachetree) * 2 > config->cachesize) {
                    flush_wait(inobno.inode);
                    purge_read_cache(0, 1, (char *) __PRETTY_FUNCTION__);
                }
                ccachedta = s_zmalloc(sizeof(CCACHEDTA));
                p = (uintptr_t) ccachedta;
                ccachedta->dirty = 0;
                ccachedta->pending = 0;
                ccachedta->creationtime = time(NULL);
                memcpy(&ccachedta->data, data->data, data->size);
                ccachedta->datasize = data->size;
                tctreeput(readcachetree, (void *) &inobno, sizeof(INOBNO),
                          (void *) &p, sizeof(unsigned long long));
            }
        }
//---
        DATfree(data);
        release_write_lock();
        ret = BLKSIZE;
        return (ret);
// Fetch the block from disk and put it in the cache.
    }
    memcpy(&p, cachedata, vsize);
    ccachedta = get_ccachedta(p);
    ccachedta->creationtime = time(NULL);
    if (rsize > BLKSIZE - block_offset) {
        memcpy(blockdata, &ccachedta->data[block_offset],
               BLKSIZE - block_offset);
    } else {
        memcpy(blockdata, &ccachedta->data[block_offset], rsize);
    }
    ret = BLKSIZE;
    release_write_lock();
    return (ret);
}

void delete_inuse(unsigned char *stiger)
{
    loghash("delete_inuse", stiger);
    delete_key(DBU, stiger, config->hashlen, (char *) __PRETTY_FUNCTION__);
    return;
}

void delete_dbb(INOBNO * inobno)
{
    LDEBUG("delete_dbb:  %llu-%llu", inobno->inode, inobno->blocknr);
    delete_key(DBB, inobno, sizeof(INOBNO), (char *) __PRETTY_FUNCTION__);
    return;
}

/* Return the number of times this block is linked to files */
unsigned long long getInUse(unsigned char *tigerstr)
{
    unsigned long long counter;
    DAT *data;

    loghash("getInuse search", tigerstr);
    if (NULL == tigerstr) {
        LDEBUG("getInuse : return 0");
        return (0);
    }

    data = search_dbdata(DBU, tigerstr, config->hashlen, LOCK);
    if (NULL == data) {
        loghash("getInuse nothing found return 0 ", tigerstr);
        LDEBUG("getInuse nothing found return 0.");
        return (0);
    }
    memcpy(&counter, data->data, sizeof(counter));
    DATfree(data);
    LDEBUG("getInuse : return %llu", counter);
    return counter;
}

void DATfree(DAT * data)
{
    s_free(data->data);
    s_free(data);
    data = NULL;
}

void update_inuse(unsigned char *hashdata, unsigned long long inuse)
{
    loghash("update_inuse ", hashdata);
    if (inuse > 0) {
        bin_write_dbdata(DBU, hashdata, config->hashlen,
                         (unsigned char *) &inuse,
                         sizeof(unsigned long long));
    } else {
        LDEBUG("update_inuse : skip %llu", inuse);
        loghash("update_inuse : skip", hashdata);
    }
    EFUNC;
    return;
}

unsigned long long get_next_inode()
{
    DAT *data;
    unsigned long long nextinode = 0;
    FUNC;

    data =
        search_dbdata(DBP, (unsigned char *) "NFI", strlen("NFI"), LOCK);
    if (data) {
        memcpy(&nextinode, data->data, sizeof(nextinode));
        DATfree(data);
    }
    LDEBUG("Found next inode number: %llu", nextinode);
    EFUNC;
    return (nextinode);
}

MEMDDSTAT *inode_meta_from_cache(unsigned long long inode)
{
    const char *dataptr;
    int vsize;

    MEMDDSTAT *memddstat = NULL;
    dataptr =
        tctreeget(metatree, &inode, sizeof(unsigned long long), &vsize);
    if (dataptr == NULL) {
        LDEBUG("inode %llu not found to update.", inode);
        release_meta_lock();
        return NULL;
    }
    memddstat = value_tomem_ddstat((char *) dataptr, vsize);
    return memddstat;
}

void update_filesize_onclose(unsigned long long inode)
{
    MEMDDSTAT *memddstat;

    LDEBUG("update_filesize_onclose : inode %llu", inode);
    memddstat = inode_meta_from_cache(inode);
    if (NULL == memddstat) {
        LDEBUG("inode %llu not found to update.", inode);
        return;
    }
    hash_update_filesize(memddstat, inode);
    memddstatfree(memddstat);
    return;
}

int update_filesize_cache(struct stat *stbuf, off_t size)
{
    const char *data;
    DAT *dskdata;
    int vsize;
    MEMDDSTAT *memddstat;
    DDSTAT *ddstat = NULL;
    DAT *ddbuf;
    time_t thetime;

    thetime = time(NULL);
    LDEBUG("update_filesize_cache : %llu", stbuf->st_ino);
    meta_lock((char *) __PRETTY_FUNCTION__);
    data = tctreeget(metatree, &stbuf->st_ino,
                     sizeof(unsigned long long), &vsize);
    if (data) {
        memddstat = (MEMDDSTAT *) data;
        memcpy(&memddstat->stbuf, stbuf, sizeof(struct stat));
        memddstat->stbuf.st_size = size;
        memddstat->stbuf.st_ctim.tv_sec = thetime;
        memddstat->stbuf.st_ctim.tv_nsec = 0;
        memddstat->stbuf.st_mtim.tv_sec = thetime;
        memddstat->stbuf.st_mtim.tv_nsec = 0;
        memddstat->stbuf.st_blocks = size / 512;
        if (512 * memddstat->stbuf.st_blocks < memddstat->stbuf.st_size)
            memddstat->stbuf.st_blocks++;
        memddstat->stbuf.st_mode = stbuf->st_mode;
        memddstat->updated = 1;
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &stbuf->st_ino,
                  sizeof(unsigned long long), (void *) ddbuf->data,
                  ddbuf->size);
        DATfree(ddbuf);
    } else {
        ddstatfree(ddstat);
        dskdata =
            search_dbdata(DBP, &stbuf->st_ino, sizeof(unsigned long long),
                          LOCK);
        if (NULL == dskdata) {
            release_meta_lock();
            return (-ENOENT);
        }
        ddstat = value_to_ddstat(dskdata);
        ddstat->stbuf.st_mtim.tv_sec = thetime;
        ddstat->stbuf.st_mtim.tv_nsec = 0;
        ddstat->stbuf.st_ctim.tv_sec = thetime;
        ddstat->stbuf.st_ctim.tv_nsec = 0;
        ddstat->stbuf.st_size = size;
        ddstat->stbuf.st_mode = stbuf->st_mode;
        ddstat->stbuf.st_blocks = stbuf->st_size / 512;
        if (512 * ddstat->stbuf.st_blocks < stbuf->st_size)
            ddstat->stbuf.st_blocks++;
        DATfree(dskdata);
        dskdata =
            create_ddbuf(ddstat->stbuf, ddstat->filename,
                         ddstat->real_size);
        bin_write_dbdata(DBP, &stbuf->st_ino, sizeof(unsigned long long),
                         (void *) dskdata->data, dskdata->size);
        DATfree(dskdata);
    }
    ddstatfree(ddstat);
    release_meta_lock();
    return (0);
}

void update_filesize(unsigned long long inode, unsigned long long fsize,
                     unsigned int offsetblock, unsigned long long blocknr)
{
    const char *dataptr;
    MEMDDSTAT *memddstat;
    DAT *ddbuf;
    int addblocks;
    INOBNO inobno;
    int vsize;

    meta_lock((char *) __PRETTY_FUNCTION__);
    LDEBUG
        ("update_filesize : inode %llu, filesize %llu, offsetblock %u, blocknr %llu",
         inode, fsize, offsetblock, blocknr);
    dataptr =
        tctreeget(metatree, &inode, sizeof(unsigned long long), &vsize);
    if (dataptr == NULL)
        goto endupdate;
    memddstat = (MEMDDSTAT *) dataptr;
    memddstat->updated++;
    memddstat->blocknr = blocknr;
    memddstat->stbuf.st_mtim.tv_sec = time(NULL);
    memddstat->stbuf.st_mtim.tv_nsec = 0;

    addblocks = fsize / 512;
    if ((memddstat->stbuf.st_blocks + addblocks) * 512 <
        memddstat->stbuf.st_size + fsize)
        addblocks++;
    // The file has not grown in size. This is an updated block.
    if (((blocknr * BLKSIZE) + offsetblock + fsize) <=
        memddstat->stbuf.st_size) {
        inobno.inode = inode;
        inobno.blocknr = blocknr;
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &inode, sizeof(unsigned long long),
                  (void *) ddbuf->data, ddbuf->size);
        DATfree(ddbuf);
        goto endupdate;
    }
    if (blocknr < 1) {
        if (memddstat->stbuf.st_size < fsize + offsetblock)
            memddstat->stbuf.st_size = fsize + offsetblock;
    } else {
        if (memddstat->stbuf.st_size <
            (blocknr * BLKSIZE) + fsize + offsetblock)
            memddstat->stbuf.st_size =
                fsize + offsetblock + (blocknr * BLKSIZE);
    }
    if (memddstat->stbuf.st_size > (512 * memddstat->stbuf.st_blocks)) {
        memddstat->stbuf.st_blocks = memddstat->stbuf.st_size / 512;
        if (512 * memddstat->stbuf.st_blocks < memddstat->stbuf.st_size)
            memddstat->stbuf.st_blocks++;
    }
    ddbuf = create_mem_ddbuf(memddstat);
    tctreeput(metatree, &inode, sizeof(unsigned long long),
              (void *) ddbuf->data, ddbuf->size);
    DATfree(ddbuf);
    cache_p2i(memddstat->filename, &memddstat->stbuf);
// Do not flush data until cachesize is reached
    if (memddstat->updated > config->cachesize) {
        hash_update_filesize(memddstat, inode);
        memddstat->updated = 0;
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &inode, sizeof(unsigned long long),
                  (void *) ddbuf->data, ddbuf->size);
        DATfree(ddbuf);
    }
  endupdate:
    release_meta_lock();
    return;
}

void hash_update_filesize(MEMDDSTAT * memddstat, unsigned long long inode)
{
    DAT *ddbuf;
// Wait until the data queue is written before we update the filesize
    if (memddstat->stbuf.st_nlink > 1) {
        ddbuf = create_ddbuf(memddstat->stbuf, NULL, memddstat->real_size);
    } else {
        ddbuf =
            create_ddbuf(memddstat->stbuf, memddstat->filename,
                         memddstat->real_size);
    }
    bin_write_dbdata(DBP, &inode,
                     sizeof(unsigned long long), (void *) ddbuf->data,
                     ddbuf->size);
    cache_p2i(memddstat->filename, &memddstat->stbuf);
    DATfree(ddbuf);
    return;
}

DAT *check_block_exists(INOBNO * inobno)
{
    DAT *data = NULL;
    FUNC;
    data = search_dbdata(DBB, inobno, sizeof(INOBNO), LOCK);
    EFUNC;
    return data;
}

int db_unlink_file(const char *path)
{
    int res = 0;
    int haslinks = 0;
    int dir_links = 0;
    struct stat st;
    struct stat dirst;
    char *dname;
    char *bname;
    unsigned long long inode;
    time_t thetime;
    DAT *vdirnode;
    DAT *ddbuf;
    DAT *dataptr;
    DDSTAT *ddstat;
    DINOINO dinoino;
    INOBNO inobno;
    DAT *fname;

    FUNC;

    LDEBUG("unlink_file %s", path);
    res = dbstat(path, &st, 0);
    if (res == -ENOENT)
        return (res);
    inode = st.st_ino;
    haslinks = st.st_nlink;
    thetime = time(NULL);
    dname = s_dirname((char *) path);
    /* Change ctime and mtime of the parentdir Posix std posix behavior */
    res = update_parent_time(dname, 0);
    bname = s_basename((char *) path);
    res = dbstat(dname, &dirst, 1);
    if (S_ISLNK(st.st_mode) && haslinks == 1) {
        LDEBUG("unlink symlink %s inode %llu", path, inode);
        delete_key(DBS, &inode, sizeof(unsigned long long),
                   (char *) __PRETTY_FUNCTION__);
        LDEBUG("unlink symlink done %s", path);
    }
    inobno.inode = inode;
    inobno.blocknr = st.st_size / BLKSIZE;
    if (inobno.blocknr * BLKSIZE < st.st_size)
        inobno.blocknr = 1 + st.st_size / BLKSIZE;
// Start deleting the actual data blocks.
    db_fs_truncate(&st, 0, bname, 1);
    if (haslinks == 1) {
        if (0 !=
            (res =
             btdelete_curkey(DBDIRENT, &dirst.st_ino,
                             sizeof(unsigned long long), &inode,
                             sizeof(unsigned long long),
                             (char *) __PRETTY_FUNCTION__))) {
            s_free(bname);
            s_free(dname);
            return (res);
        }
        delete_key(DBP, (unsigned char *) &inode,
                   sizeof(unsigned long long),
                   (char *) __PRETTY_FUNCTION__);
    } else {
        dataptr =
            search_dbdata(DBP, (unsigned char *) &inode,
                          sizeof(unsigned long long), LOCK);
        if (dataptr == NULL) {
            die_dataerr("Failed to find file %llu", inode);
        }
        ddstat = value_to_ddstat(dataptr);
        ddstat->stbuf.st_nlink--;
        ddstat->stbuf.st_ctim.tv_sec = thetime;
        ddstat->stbuf.st_ctim.tv_nsec = 0;
        ddstat->stbuf.st_mtim.tv_sec = thetime;
        ddstat->stbuf.st_mtim.tv_nsec = 0;
        dinoino.dirnode = dirst.st_ino;
        dinoino.inode = ddstat->stbuf.st_ino;
        dir_links = count_dirlinks(&dinoino, sizeof(DINOINO));
        res =
            btdelete_curkey(DBL, &dinoino, sizeof(DINOINO), bname,
                            strlen(bname), (char *) __PRETTY_FUNCTION__);
        btdelete_curkey(DBL, &ddstat->stbuf.st_ino,
                        sizeof(unsigned long long), &dinoino,
                        sizeof(DINOINO), (char *) __PRETTY_FUNCTION__);
// Restore to regular file settings and clean up.
        if (ddstat->stbuf.st_nlink == 1) {
            vdirnode =
                btsearch_keyval(DBL, &ddstat->stbuf.st_ino,
                                sizeof(unsigned long long), NULL, 0, LOCK);
            memcpy(&dinoino, vdirnode->data, vdirnode->size);
            DATfree(vdirnode);
            fname =
                btsearch_keyval(DBL, &dinoino, sizeof(DINOINO),
                                NULL, 0, LOCK);
            memcpy(&ddstat->filename, fname->data, fname->size);
            DATfree(fname);
            btdelete_curkey(DBL, &dinoino, sizeof(DINOINO),
                            ddstat->filename, strlen(ddstat->filename),
                            (char *) __PRETTY_FUNCTION__);
            btdelete_curkey(DBL, &ddstat->stbuf.st_ino,
                            sizeof(unsigned long long), &dinoino,
                            sizeof(DINOINO), (char *) __PRETTY_FUNCTION__);
            btdelete_curkey(DBL, &inode, sizeof(unsigned long long),
                            &dinoino, sizeof(DINOINO),
                            (char *) __PRETTY_FUNCTION__);
            res = 0;
        }
        if (dir_links == 1) {
            if (0 !=
                (res =
                 btdelete_curkey(DBDIRENT, &dirst.st_ino,
                                 sizeof(unsigned long long), &inode,
                                 sizeof(unsigned long long),
                                 (char *) __PRETTY_FUNCTION__))) {
                die_dataerr("unlink_file : Failed to delete record.");
            }
        }
        ddbuf =
            create_ddbuf(ddstat->stbuf, ddstat->filename,
                         ddstat->real_size);
        bin_write_dbdata(DBP, &inode, sizeof(unsigned long long),
                         (void *) ddbuf->data, ddbuf->size);
        DATfree(dataptr);
        DATfree(ddbuf);
        ddstatfree(ddstat);
    }
    s_free(bname);
    s_free(dname);
    EFUNC;
    return (res);
}

int fs_mkdir(const char *path, mode_t mode)
{
    unsigned long long inode;
    char *rdir;
    char *pdir;
    int rootdir = 0;
    int res;

    FUNC;
    LDEBUG("mode =%i", mode);
    if (0 == strcmp("/", path))
        rootdir = 1;
    inode = get_next_inode();
    write_file_ent(path, inode, S_IFDIR | mode, NULL, 0);
    if (rootdir) {
        rdir = as_sprintf(__FILE__, __LINE__, "%s.", path);
    } else {
        rdir = as_sprintf(__FILE__, __LINE__, "%s/.", path);
    }
    inode = get_next_inode();
    write_file_ent(rdir, inode, S_IFDIR | 0755, NULL, 0);
    s_free(rdir);
    if (rootdir) {
        rdir = as_sprintf(__FILE__, __LINE__, "%s..", path);
    } else {
        rdir = as_sprintf(__FILE__, __LINE__, "%s/..", path);
    }
    inode = get_next_inode();
    write_file_ent(rdir, inode, S_IFDIR | 0755, NULL, 0);
    s_free(rdir);
    /* Change ctime and mtime of the parentdir Posix std posix behavior */
    pdir = s_dirname((char *) path);
    res = update_parent_time(pdir, 1);
    s_free(pdir);
    return (res);
}

DAT *tc_compress(unsigned char *dbdata, unsigned long dsize)
{
    DAT *compressed;
    int rsize;
    char *data = NULL;

    compressed = s_malloc(sizeof(DAT));
    switch (config->compression) {
    case 'G':
        data = tcgzipencode((const char *) dbdata, dsize, &rsize);
        if (rsize > dsize)
            goto def;
        compressed->data = s_malloc(rsize + 1);
        compressed->data[0] = 'G';
        break;
    case 'B':
        data = tcbzipencode((const char *) dbdata, dsize, &rsize);
        if (rsize > dsize)
            goto def;
        compressed->data = s_malloc(rsize + 1);
        compressed->data[0] = 'B';
        break;
    case 'D':
        data = tcdeflate((const char *) dbdata, dsize, &rsize);
        if (rsize > dsize)
            goto def;
        compressed->data = s_malloc(rsize + 1);
        compressed->data[0] = 'D';
        break;
    default:
      def:
        if (data)
            s_free(data);
        compressed->data = s_malloc(dsize + 1);
        memcpy(&compressed->data[1], dbdata, dsize);
        compressed->data[0] = 0;
        compressed->size = dsize + 1;
        return compressed;
    }
    memcpy(&compressed->data[1], data, rsize);
    compressed->size = rsize + 1;
    s_free(data);
    return compressed;
}

DAT *lfscompress(unsigned char *dbdata, unsigned long dsize)
{
    DAT *compressed = NULL;
#ifdef ENABLE_CRYPTO
    DAT *encrypted;
#endif

    switch (config->compression) {
    case 'L':
#ifdef LZO
        compressed = (DAT *) lzo_compress(dbdata, dsize);
#else
        LFATAL("lessfs is compiled without support for LZO");
        db_close(0);
        exit(EXIT_DATAERR);
#endif
        break;
    case 'Q':
        compressed = (DAT *) clz_compress(dbdata, dsize);
        break;
    case 'R':
        compressed = (DAT *) clz15_compress(dbdata, dsize);
        break;
    case 'S':
#ifdef SNAPPY 
        compressed = (DAT *) lfssnappy_compress(dbdata, dsize);
#else
        LFATAL("lessfs is compiled without support for SNAPPY");
        db_close(0);
        exit(EXIT_DATAERR);
#endif
        break;
    default:
        compressed = (DAT *) tc_compress(dbdata, dsize);
    }

#ifdef ENABLE_CRYPTO
    if (config->encryptdata) {
        encrypted = lfsencrypt(compressed->data, compressed->size);
        DATfree(compressed);
        return encrypted;
    }
#endif
    return compressed;
}

unsigned int db_commit_block(unsigned char *dbdata,
                             INOBNO inobno, unsigned long dsize)
{
    unsigned char *stiger = NULL;
    DAT *compressed;
    unsigned long long inuse;
    unsigned int ret = 0;

    FUNC;
    LDEBUG("db_commit_block");
    stiger = thash(dbdata, dsize);
    create_hash_note(stiger);
    inuse = getInUse(stiger);
    if (0 == inuse) {
        compressed = lfscompress((unsigned char *) dbdata, dsize);
        ret = compressed->size;
        bin_write_dbdata(DBDTA, stiger, config->hashlen, compressed->data,
                         compressed->size);
        DATfree(compressed);
    } else {
        loghash("commit_block : only updated inuse for hash ", stiger);
    }
    inuse++;
    update_inuse(stiger, inuse);
    LDEBUG("db_commit_block : dbb %llu-%llu", inobno.inode,
           inobno.blocknr);
    bin_write_dbdata(DBB, (char *) &inobno, sizeof(INOBNO), stiger,
                     config->hashlen);
    delete_hash_note(stiger);
    s_free(stiger);
    return (ret);
}

void partial_truncate_block(unsigned long long inode,
                            unsigned long long blocknr,
                            unsigned int offset)
{
    unsigned char *blockdata;
    DAT *uncompdata;
    INOBNO inobno;
    DAT *data;
#ifdef ENABLE_CRYPTO
    DAT *encrypted;
#endif
    unsigned char *stiger;
    unsigned long long inuse;

    FUNC;
    LDEBUG("partial_truncate_block : inode %llu, blocknr %llu, offset %u",
           inode, blocknr, offset);
    inobno.inode = inode;
    inobno.blocknr = blocknr;

    data = search_dbdata(DBB, &inobno, sizeof(INOBNO), LOCK);
    if (NULL == data) {
        LDEBUG("Deletion of non existent block?");
        return;
    }
    stiger = s_malloc(data->size);
    memcpy(stiger, data->data, data->size);
    DATfree(data);
    data = search_dbdata(DBDTA, stiger, config->hashlen, LOCK);
    if (NULL == data) {
        log_fatal_hash("Hmmm, did not expect this to happen.", stiger);
        die_dataerr("Hmmm, did not expect this to happen.");
    }
    create_hash_note(stiger);
    inuse = getInUse(stiger);
//---
    blockdata = s_zmalloc(BLKSIZE);
    uncompdata = lfsdecompress(data);
    if ( NULL == uncompdata ) die_dataerr("partial_truncate_block: inode %llu - %llu failed to decompress block", inobno.inode,inobno.blocknr); 
    if (uncompdata->size >= offset) {
        memcpy(blockdata, uncompdata->data, offset);
    } else {
        memcpy(blockdata, uncompdata->data, uncompdata->size);
    }
    DATfree(uncompdata);
    db_commit_block(blockdata, inobno, offset);
//---

//  Not needed, is overwritten by db_commit_block.
//  delete_dbb(&inobno);
    if (inuse == 1) {
        loghash("partial_truncate_block : delete hash", stiger);
        delete_inuse(stiger);
        delete_key(DBDTA, stiger, config->hashlen,
                   (char *) __PRETTY_FUNCTION__);
    } else {
        if (inuse > 1)
            inuse--;
        update_inuse(stiger, inuse);
    }
    DATfree(data);
    s_free(blockdata);
    delete_hash_note(stiger);
    s_free(stiger);
    return;
}

void *tc_truncate_worker(void *threadarg)
{
    unsigned long long blocknr;
    unsigned long long lastblocknr;
    struct truncate_thread_data *trunc_data;
    unsigned char *stiger;
    DAT *data;
    unsigned long long inuse;
    INOBNO inobno;
    unsigned int offsetblock;

    trunc_data = (struct truncate_thread_data *) threadarg;
    lastblocknr = trunc_data->lastblocknr;
    blocknr = trunc_data->blocknr;
    offsetblock = trunc_data->offsetblock;
    inobno.blocknr = trunc_data->blocknr;
    inobno.inode = trunc_data->inode;

    FUNC;
    while (lastblocknr >= blocknr) {
        if (config->background_delete != 0) {
            truncation_wait();
            write_lock((char *) __PRETTY_FUNCTION__);
        }
        if (offsetblock != 0 && lastblocknr == blocknr) {
            if (config->background_delete != 0)
                release_write_lock();
            break;
        }
        inobno.blocknr = lastblocknr;
        data = search_dbdata(DBB, &inobno, sizeof(INOBNO), LOCK);
        if (NULL == data) {
            LDEBUG
                ("Deletion of non existent block inode : %llu, blocknr %llu",
                 inobno.inode, inobno.blocknr);
            if (lastblocknr > 0)
                lastblocknr--;
            else {
                if (config->background_delete != 0)
                    release_write_lock();
                break;
            }
// Need to continue in case of a sparse file.
            if (config->background_delete != 0)
                release_write_lock();
            continue;
        }
        stiger = s_malloc(data->size);
        memcpy(stiger, data->data, data->size);
        LDEBUG("lessfs_truncate Search to delete blocknr %llu:",
               lastblocknr);
        loghash("lessfs_truncate tiger :", stiger);
        DATfree(data);
        create_hash_note(stiger);
        delete_dbb(&inobno);
        inuse = getInUse(stiger);
        if (inuse == 1) {
            loghash("truncate : delete hash", stiger);
            delete_inuse(stiger);
            delete_key(DBDTA, stiger, config->hashlen,
                       (char *) __PRETTY_FUNCTION__);
        } else {
            if (inuse > 1)
                inuse--;
            update_inuse(stiger, inuse);
        }
        delete_hash_note(stiger);
        s_free(stiger);
        if (lastblocknr > 0)
            lastblocknr--;
        if (config->background_delete) {
            release_write_lock();
            if (config->shutdown) {
                LINFO("Truncation thread aborts truncating inode %llu",
                      inobno.inode);
                pthread_exit(NULL);
            }
        }
    }

    if (0 != offsetblock) {
        if (config->background_delete != 0)
            write_lock((char *) __PRETTY_FUNCTION__);
        partial_truncate_block(inobno.inode, lastblocknr, offsetblock);
        if (config->background_delete != 0)
            release_write_lock();
    }
    delete_inode_note(inobno.inode);
    write_lock((char *) __PRETTY_FUNCTION__);
    btdelete_curkey(FREELIST, "TRNCTD", strlen("TRNCTD"), threadarg,
                    sizeof(struct truncate_thread_data),
                    (char *) __PRETTY_FUNCTION__);
    release_write_lock();
    s_free(threadarg);
    EFUNC;
    if (config->background_delete == 0) {
        return NULL;
    } else
        pthread_exit(NULL);
}

struct tm *init_transactions()
{
    INUSE *finuse;
    unsigned long long inuse;
    time_t tdate;
    struct tm *timeinfo = NULL;
    unsigned long replogsize;
    struct stat stbuf;

    if (config->transactions) {
        config->commithash =
            thash((unsigned char *) "COMMITSTAMP", strlen("COMMITSTAMP"));
        if ( config->blockdata_io_type == TOKYOCABINET ) {
            inuse = getInUse((unsigned char *) config->commithash);
            if (0 == inuse) {
                LFATAL("COMMITSTAMP not found");
                lessfs_trans_stamp();
                inuse = getInUse((unsigned char *) config->commithash);
            }
            tdate = inuse;
        } else {
            finuse = file_get_inuse((unsigned char *) config->commithash);
            if (NULL == finuse) {
                LFATAL
                    ("COMMITSTAMP not found, upgrading the filesystem to support transactions");
                lessfs_trans_stamp();
                finuse =
                    file_get_inuse((unsigned char *) config->commithash);
            }
            replogsize=finuse->size;
            if ( -1 == fstat(frepl, &stbuf)) die_syserr();
            if ( replogsize < stbuf.st_size ) {
               if ( -1 == ftruncate(frepl,replogsize) ) die_syserr();
            }
            tdate = finuse->inuse;
            if (finuse)
                s_free(finuse);
        }
        timeinfo = localtime(&tdate);
    }
    return timeinfo;
}

int update_parent_time(char *path, int linkcount)
{
    int res;
    struct stat stbuf;
    time_t thetime;

    FUNC;
    LDEBUG("update_parent_time : %s", path);
    thetime = time(NULL);
    /* Change ctime and mtime of the parentdir Posix std posix behavior */
    res = dbstat(path, &stbuf, 0);
    if (0 != res)
        return (res);
    stbuf.st_ctim.tv_sec = thetime;
    stbuf.st_ctim.tv_nsec = 0;
    stbuf.st_mtim.tv_sec = thetime;
    stbuf.st_mtim.tv_nsec = 0;
    stbuf.st_nlink = stbuf.st_nlink + linkcount;
    res = update_stat(path, &stbuf);
    EFUNC;
    return (res);
}

int update_stat(char *path, struct stat *stbuf)
{
    DDSTAT *ddstat;
    MEMDDSTAT *memddstat;
    DAT *ddbuf;
    DAT *dataptr;
    const char *cdata;
    int vsize;
    unsigned long long inode;
    int ret = 0;

    FUNC;
    inode = stbuf->st_ino;

    meta_lock((char *) __PRETTY_FUNCTION__);
    cdata = tctreeget(metatree, (unsigned char *) &inode,
                      sizeof(unsigned long long), &vsize);
    if (cdata) {
        memddstat = (MEMDDSTAT *) cdata;
        memcpy(&memddstat->stbuf, stbuf, sizeof(struct stat));
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &inode, sizeof(unsigned long long),
                  (void *) ddbuf->data, ddbuf->size);
        DATfree(ddbuf);
        goto unlock_return;
    }
    dataptr = search_dbdata(DBP, &inode, sizeof(unsigned long long), LOCK);
    if (dataptr == NULL) {
        ret = -ENOENT;
        goto unlock_return;
    }
    ddstat = value_to_ddstat(dataptr);
    memcpy(&ddstat->stbuf, stbuf, sizeof(struct stat));
    ddbuf =
        create_ddbuf(ddstat->stbuf, ddstat->filename, ddstat->real_size);
    bin_write_dbdata(DBP, &inode, sizeof(unsigned long long),
                     (void *) ddbuf->data, ddbuf->size);
    DATfree(dataptr);
    ddstatfree(ddstat);
    DATfree(ddbuf);
  unlock_return:
    cache_p2i(path, stbuf);
    release_meta_lock();
    EFUNC;
    return (ret);
}

void db_close(bool defrag)
{
#ifdef BERKELEYDB
    bdb_close();
#else
#ifndef HAMSTERDB
    tc_close(defrag);
#else
    hm_close(defrag);
#endif
#endif
}

void flush_wait(unsigned long long inode)
{
    char *key;
    int size;
    int vsize;
    char *val;
    uintptr_t p;
    INOBNO *inobno;
    CCACHEDTA *ccachedta;
    TCLIST *keylist;
    int i;

    release_write_lock();
    while (1) {
        LDEBUG("working=%lu - tctreernum = %llu", working,
               (unsigned long long) tctreernum(workqtree));
        if (working == 0 && 0 == tctreernum(workqtree))
            break;
        usleep(1);
    }
    write_lock((char *) __PRETTY_FUNCTION__);
    keylist = tctreekeys(readcachetree);
    inobnolistsort(keylist);

    for (i = 0; i < tclistnum(keylist); i++) {
        key = (char *) tclistval(keylist, i, &size);
        val =
            (char *) tctreeget(readcachetree, (void *) key, size, &vsize);
        if (val) {
            memcpy(&p, val, vsize);
            ccachedta = get_ccachedta(p);
            inobno = (INOBNO *) key;
            if (ccachedta->pending)
                continue;
            if (inode == inobno->inode || inode == 0) {
                if (ccachedta->dirty == 1 && ccachedta->pending != 1) {
                    ccachedta->pending = 1;
                    tctreeout(workqtree, key, size);
                    LDEBUG("flush_wait : cook_cache %llu-%llu",
                           inobno->inode, inobno->blocknr);
                    cook_cache(key, size, ccachedta, 0 );
                    ccachedta->dirty = 0;
                    ccachedta->pending = 0;
                }
            }
        }
    }
    tclistdel(keylist);
    return;
}

void cook_cache(char *key, int ksize, CCACHEDTA * ccachedta, unsigned long sequence)
{
    INOBNO *inobno;
    unsigned char *hash;
    inobno = (INOBNO *) key;
    LDEBUG("cook_cache : %llu-%llu", inobno->inode, inobno->blocknr);
    hash = thash((unsigned char *) &ccachedta->data, ccachedta->datasize);
    memcpy(&ccachedta->hash, hash, config->hashlen);
    free(hash);
    if ( config->blockdata_io_type == TOKYOCABINET ) {
        tc_write_cache(ccachedta, inobno);
    } else {
        fl_write_cache(ccachedta, inobno);
    }
    return;
}

unsigned long long get_inode(const char *path)
{
    struct stat stbuf;

    FUNC;
    if (0 != dbstat(path, &stbuf, 1)) {
        LDEBUG("get_inode : nothing found for %s", path);
        return (0);
    }
    EFUNC;
    return (stbuf.st_ino);
}

void purge_read_cache(unsigned long long inode, bool force, char *caller)
{
    char *key;
    int size;
    int vsize;
    char *val;
    uintptr_t p;
    INOBNO *inobno;
    CCACHEDTA *ccachedta;
    bool pending;

  restart:
    pending = 0;
    LDEBUG("purge_read_cache %llu", inode);
    tctreeiterinit(readcachetree);
    while (key = (char *) tctreeiternext(readcachetree, &size)) {
        val =
            (char *) tctreeget(readcachetree, (void *) key, size, &vsize);
        if (val) {
            memcpy(&p, val, vsize);
            ccachedta = get_ccachedta(p);
            inobno = (INOBNO *) key;
            if (ccachedta->pending) {
                pending = 1;
                continue;
            }
            if (ccachedta->dirty == 1) {
                if (inode == 0 || inode == inobno->inode) {
                    ccachedta->pending = 1;
                    LDEBUG("purge_read_cache : cook_cache %llu-%llu",
                           inobno->inode, inobno->blocknr);
                    cook_cache(key, size, ccachedta,0);
                    tctreeout(workqtree, key, size);
                    tctreeout(readcachetree, key, size);
                    s_free(ccachedta);
                    continue;
                }
            }
            if (inode == 0) {
                if (force) {
                    tctreeout(readcachetree, key, size);
                    s_free(ccachedta);
                } else {
                    if (ccachedta->creationtime + CACHE_MAX_AGE <
                        time(NULL)) {
                        tctreeout(readcachetree, key, size);
                        s_free(ccachedta);
                    }
                }
                continue;
            }
            if (inode == inobno->inode) {
                if (force) {
                    tctreeout(readcachetree, key, size);
                    s_free(ccachedta);
                } else {
                    if (ccachedta->creationtime + CACHE_MAX_AGE <
                        time(NULL)) {
                        tctreeout(readcachetree, key, size);
                        s_free(ccachedta);
                    }
                }
            }
        }
    }
    if (pending)
        goto restart;
    EFUNC;
    return;
}

void update_meta(unsigned long long inode, unsigned long size, int sign)
{
    const char *data;
    int vsize;
    MEMDDSTAT *mddstat;
    DAT *statdata;
    DDSTAT *ddstat;
    DAT *ddbuf;

    LDEBUG("update_meta : inode %llu database.", inode);
    meta_lock((char *) __PRETTY_FUNCTION__);
    data = tctreeget(metatree, &inode, sizeof(unsigned long long), &vsize);
    if (data == NULL) {
        LDEBUG("meta update on inode not in cache");
        statdata =
            search_dbdata(DBP, &inode, sizeof(unsigned long long), LOCK);
        if (NULL == statdata)
            goto bailout;
        ddstat = value_to_ddstat(statdata);
        if (sign == 1) {
            ddstat->real_size = ddstat->real_size + size;
        } else {
            if (ddstat->real_size < size) {
                ddstat->real_size = 0;
            } else
                ddstat->real_size = ddstat->real_size - size;
        }
        ddbuf =
            create_ddbuf(ddstat->stbuf, ddstat->filename,
                         ddstat->real_size);
        bin_write_dbdata(DBP, &inode, sizeof(unsigned long long),
                         ddbuf->data, ddbuf->size);
//       cache_p2i(ddstat->filename, &ddstat->stbuf, NOLOCK);
        DATfree(statdata);
        ddstatfree(ddstat);
        DATfree(ddbuf);
    } else {
        mddstat = (MEMDDSTAT *) data;
        if (sign == 1) {
            mddstat->real_size = mddstat->real_size + size;
        } else {
            if (mddstat->real_size < size) {
                mddstat->real_size = 0;
            } else
                mddstat->real_size = mddstat->real_size - size;
        }
        tctreeput(metatree, &inode, sizeof(unsigned long long),
                  (void *) mddstat, vsize);
    }
  bailout:
    EFUNC;
    release_meta_lock();
    return;
}

void lessfs_trans_stamp()
{
    unsigned long long ldate;
    time_t tdate;
    INUSE finuse;
    struct tm *timeinfo;
    struct stat stbuf;

    tdate = time(NULL);
    timeinfo = localtime(&tdate);
    ldate = tdate;
    LDEBUG("lessfs_trans_stamp : filesystem commit at %s",
           asctime(timeinfo));
    if ( -1 == fstat(frepl, &stbuf)) die_syserr();
    if ( config->blockdata_io_type != TOKYOCABINET ) {
        finuse.inuse = ldate;
        finuse.size = stbuf.st_size;
        finuse.offset = 0;
        finuse.allocated_size = 0;
        bin_write_dbdata(DBU, config->commithash, config->hashlen,
                         (unsigned char *) &finuse, sizeof(INUSE));
    } else {
        bin_write_dbdata(DBU, config->commithash, config->hashlen,
                         (unsigned char *) &ldate,
                         sizeof(unsigned long long));
    }
    return;
}

int db_fs_truncate(struct stat *stbuf, off_t size, char *bname,
                   bool unlink)
{
    unsigned int offsetblock;
    unsigned long long blocknr;
    unsigned long long lastblocknr;
    off_t oldsize;
    time_t thetime;
    pthread_t truncate_thread;
    struct truncate_thread_data *trunc_data;

    FUNC;

    trunc_data = s_zmalloc(sizeof(struct truncate_thread_data));
    LDEBUG("lessfs_truncate inode %llu - size %llu", stbuf->st_ino,
           (unsigned long long) size);
    thetime = time(NULL);
    blocknr = size / BLKSIZE;
    offsetblock = size - (blocknr * BLKSIZE);
    oldsize = stbuf->st_size;
    lastblocknr = oldsize / BLKSIZE;
    LDEBUG
        ("db_fs_truncate : (got inode lock) truncate new block %llu, oldblock %llu size=%llu",
         blocknr, lastblocknr, size);
    trunc_data->inode = stbuf->st_ino;
    trunc_data->blocknr = blocknr;
    trunc_data->lastblocknr = lastblocknr;
    trunc_data->offsetblock = offsetblock;
    memcpy(&trunc_data->stbuf, stbuf, sizeof(struct stat));
    trunc_data->stbuf.st_size = size;
    trunc_data->unlink = unlink;
    if (!unlink)
        update_filesize_cache(stbuf, size);
    if (config->background_delete == 0) {
        tc_truncate_worker((void *) trunc_data);
    } else {
        write_trunc_todolist(trunc_data);
        if (0 !=
            pthread_create(&truncate_thread, NULL, tc_truncate_worker,
                           (void *) trunc_data))
            die_syserr();
        if (0 != pthread_detach(truncate_thread))
            die_syserr();
    }
    return (0);
}

void write_trunc_todolist(struct truncate_thread_data *trunc_data)
{
    char *key = "TRNCTD";

    FUNC;
    LDEBUG
        ("write_trunc_todolist : inode %llu : start %llu -> end %llu size %llu",
         trunc_data->inode, trunc_data->blocknr, trunc_data->lastblocknr,
         (unsigned long long) trunc_data->stbuf.st_size);
    btbin_write_dup(FREELIST, key, strlen(key),
                    (unsigned char *) trunc_data,
                    sizeof(struct truncate_thread_data), LOCK);
    EFUNC;
    return;
}

void tc_write_cache(CCACHEDTA * ccachedta, INOBNO * inobno)
{
    unsigned long long inuse;
    DAT *compressed;

    create_hash_note((unsigned char *) &ccachedta->hash);
    db_delete_stored(inobno);
    inuse = getInUse((unsigned char *) &ccachedta->hash);
    if (inuse == 0) {
        compressed = lfscompress(ccachedta->data, ccachedta->datasize);
        bin_write_dbdata(DBDTA, &ccachedta->hash, config->hashlen,
                         compressed->data, compressed->size);
        if (ccachedta->newblock == 1)
            update_meta(inobno->inode, compressed->size, 1);
        if (ccachedta->updated != 0) {
            if (compressed->size > ccachedta->updated) {
                update_meta(inobno->inode,
                            compressed->size - ccachedta->updated, 1);
            } else {
                update_meta(inobno->inode,
                            ccachedta->updated - compressed->size, 0);
            }
        }
        DATfree(compressed);
    }
    inuse++;
    update_inuse((unsigned char *) &ccachedta->hash, inuse);
    bin_write_dbdata(DBB, (char *) inobno, sizeof(INOBNO), ccachedta->hash,
                     config->hashlen);
    delete_hash_note((unsigned char *) &ccachedta->hash);
    ccachedta->dirty = 0;
    ccachedta->pending = 0;
    ccachedta->newblock = 0;
    return;
}

void db_delete_stored(INOBNO * inobno)
{
    unsigned long long inuse;
    unsigned char *hash;
    DAT *data;

    data = search_dbdata(DBB, inobno, sizeof(INOBNO), LOCK);
    if (NULL == data)
        return;
    hash = data->data;
    delete_dbb(inobno);
    inuse = getInUse(hash);
    if (inuse <= 1) {
        delete_inuse(hash);
        delete_key(DBDTA, hash, config->hashlen, NULL);
        if (config->replication == 1 && config->replication_role == 0) {
            write_repl_data(DBDTA, REPLDELETE, (char *) hash,
                            config->hashlen, NULL, 0,
                            MAX_ALLOWED_THREADS - 2);
        }
    } else {
        inuse--;
        update_inuse(hash, inuse);
    }
    DATfree(data);
    return;
}

void fil_fuse_info(DDSTAT * ddstat, void *buf, fuse_fill_dir_t filler,
                   struct fuse_file_info *fi)
{
    struct stat st;
    char *bname;

    memcpy(&st, &ddstat->stbuf, sizeof(struct stat));
    bname = s_basename(ddstat->filename);
    if (bname) {
        // Don't show the directory
        if (0 != strcmp(bname, "/")) {
            filler(buf, bname, &st, 0);
        }
    }
    s_free(bname);
}

void locks_to_dir(void *buf, fuse_fill_dir_t filler,
                  struct fuse_file_info *fi)
{
    struct stat stbuf;
    char *bname;
    const char *key;
    unsigned long long inode;
    int size;

    FUNC;
    get_inode_lock((char *) __PRETTY_FUNCTION__);
    tctreeiterinit(inodetree);
    while (key = tctreeiternext(inodetree, &size)) {
        memcpy(&inode, key, sizeof(unsigned long long));
        bname = as_sprintf(__FILE__, __LINE__, "%llu", inode);
        lckname_to_stat(bname, &stbuf);
        filler(buf, bname, &stbuf, 0);
        s_free(bname);
    }
    EFUNC;
    release_inode_lock();
    return;
}

int lckname_to_stat(char *path, struct stat *stbuf)
{
    const char *data;
    time_t thetime;
    int vsize;
    unsigned long long inode;

    FUNC;
    sscanf(path, "%llu", &inode);
    data = tctreeget(inodetree, &inode,
                     sizeof(unsigned long long), &vsize);
    if (NULL == data)
        return (-ENOENT);
    stbuf->st_ino = inode;
    stbuf->st_dev = 999988;
    stbuf->st_mode = 33060;
    stbuf->st_nlink = 1;
    stbuf->st_uid = 0;
    stbuf->st_gid = 0;
    stbuf->st_size = 0;
    stbuf->st_blocks = 1;
    stbuf->st_blksize = BLKSIZE;
    thetime = time(NULL);
    stbuf->st_atim.tv_sec = thetime;
    stbuf->st_atim.tv_nsec = 0;
    stbuf->st_mtim.tv_sec = thetime;
    stbuf->st_mtim.tv_nsec = 0;
    stbuf->st_ctim.tv_sec = thetime;
    stbuf->st_ctim.tv_nsec = 0;
    EFUNC;
    return (0);
}

int fs_link(char *from, char *to)
{
    int res = 0;
    unsigned long long inode;
    unsigned long long todirnode;
    unsigned long long fromdirnode;
    struct stat stbuf;
    struct stat tobuf;
    struct stat frombuf;
    char *bfrom;
    char *bto;
    char *todir;
    char *fromdir;
    DAT *ddbuf;
    DAT *symdata;
    time_t thetime;
    DINOINO dinoino;
    const char *data;
    int vsize;
    MEMDDSTAT *memddstat;

    FUNC;
    LDEBUG("fs_link called with from=%s to=%s", from, to);
    res = dbstat(from, &stbuf, 0);

    if (res != 0)
        return (res);
    fromdir = s_dirname(from);
    res = dbstat(fromdir, &frombuf, 1);
    todir = s_dirname(to);
    res = dbstat(todir, &tobuf, 0);
    if (res != 0) {
        s_free(todir);
        s_free(fromdir);
        return -ENOENT;
    }
    bfrom = s_basename(from);
    bto = s_basename(to);
/* Update inode nrlinks */
    todirnode = tobuf.st_ino;
    fromdirnode = frombuf.st_ino;
    inode = stbuf.st_ino;
    // Update nr of links.
    if (stbuf.st_nlink == 1)
        invalidate_p2i(from);
    stbuf.st_nlink++;
    thetime = time(NULL);

    meta_lock((char *) __PRETTY_FUNCTION__);
    data = tctreeget(metatree, &stbuf.st_ino,
                     sizeof(unsigned long long), &vsize);
    stbuf.st_ctim.tv_sec = thetime;
    stbuf.st_ctim.tv_nsec = 0;
    if (data) {
        memddstat = (MEMDDSTAT *) data;
        memddstat->stbuf.st_ctim.tv_sec = thetime;
        memddstat->stbuf.st_ctim.tv_nsec = 0;
        memddstat->stbuf.st_mtim.tv_sec = thetime;
        memddstat->stbuf.st_mtim.tv_nsec = 0;
        memddstat->stbuf.st_nlink++;
        memddstat->updated = 1;
        //memset(&memddstat->filename,0,2);
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &stbuf.st_ino,
                  sizeof(unsigned long long), (void *) ddbuf->data,
                  ddbuf->size);
        DATfree(ddbuf);
    }
    release_meta_lock();
    ddbuf = create_ddbuf(stbuf, NULL, 0);
    LDEBUG("fs_link : update links on %llu to %i", inode, stbuf.st_nlink);
    bin_write_dbdata(DBP, &inode, sizeof(unsigned long long),
                     ddbuf->data, ddbuf->size);
    DATfree(ddbuf);
/* Link dest filename inode to dest directory if it does not exist*/
    if (0 ==
        bt_entry_exists(DBDIRENT, &todirnode, sizeof(unsigned long long),
                        &inode, sizeof(unsigned long long))) {
        LDEBUG("fs_link : write link %llu : %llu", todirnode, inode);
        btbin_write_dup(DBDIRENT, &todirnode, sizeof(unsigned long long),
                        &inode, sizeof(unsigned long long), LOCK);
    }

/* Link some more, hardlink a symlink. */
    if (S_ISLNK(stbuf.st_mode)) {
        LDEBUG("fs_link : hardlink a symlink");
        symdata =
            search_dbdata(DBS, &inode, sizeof(unsigned long long), LOCK);
        if (NULL == symdata)
            die_dataerr("Unable to read symlink");
        DATfree(symdata);
    }
/* Write L_destinodedir_inode : dest filename */
    dinoino.dirnode = tobuf.st_ino;
    dinoino.inode = stbuf.st_ino;
    LDEBUG("A. fs_link : write link %llu-%llu : %s", tobuf.st_ino,
           stbuf.st_ino, bto);
    btbin_write_dup(DBL, &dinoino, sizeof(DINOINO), bto, strlen(bto),
                    LOCK);
    btbin_write_dup(DBL, &stbuf.st_ino, sizeof(unsigned long long),
                    &dinoino, sizeof(DINOINO), LOCK);

/* Write Lfrominode_inode : from filename */
    dinoino.dirnode = frombuf.st_ino;
    dinoino.inode = stbuf.st_ino;
    if (stbuf.st_nlink == 2) {
        btbin_write_dup(DBL, &dinoino, sizeof(DINOINO), bfrom,
                        strlen(bfrom), LOCK);
        btbin_write_dup(DBL, &stbuf.st_ino, sizeof(unsigned long long),
                        &dinoino, sizeof(DINOINO), LOCK);
    }
    res = update_parent_time(todir, 0);
    s_free(todir);
    s_free(fromdir);
    s_free(bfrom);
    s_free(bto);
    EFUNC;
    return (res);
}

int fs_rename_link(const char *from, const char *to, struct stat stbuf)
{
    unsigned long long fromnode;
    unsigned long long tonode;
    unsigned long long inode;
    char *fromdir;
    char *todir;
    char *bfrom;
    char *bto;
    struct stat st;
    int res = 0;
    DINOINO dinoino;

    FUNC;

    LDEBUG("fs_rename_link from: %s : to %s", (char *) from, (char *) to);
    if (0 == strcmp(from, to))
        return (0);
    if (-ENOENT != dbstat(to, &st, 0)) {
        if ( config->blockdata_io_type == TOKYOCABINET ) {
            db_unlink_file(to);
        } else {
            file_unlink_file(to);
        }
    }
    fromdir = s_dirname((char *) from);
    todir = s_dirname((char *) to);
    bfrom = s_basename((char *) from);
    bto = s_basename((char *) to);
    inode = stbuf.st_ino;

    fromnode = get_inode(fromdir);
    tonode = get_inode(todir);
    if (0 == fromnode)
        die_dataerr("Unable to find directory %s for file %s", fromdir,
                    from);
    if (0 == tonode)
        die_dataerr("Unable to find directory %s for file %s", todir, to);

    dinoino.dirnode = fromnode;
    dinoino.inode = stbuf.st_ino;
    btdelete_curkey(DBL, &dinoino, sizeof(DINOINO), bfrom, strlen(bfrom),
                    (char *) __PRETTY_FUNCTION__);
    if (count_dirlinks(&dinoino, sizeof(DINOINO)) > 1) {
        btdelete_curkey(DBDIRENT, &fromnode, sizeof(unsigned long long),
                        &inode, sizeof(unsigned long long),
                        (char *) __PRETTY_FUNCTION__);
        btbin_write_dup(DBDIRENT, &tonode, sizeof(unsigned long long),
                        &inode, sizeof(unsigned long long), LOCK);
    }
    dinoino.dirnode = tonode;
    dinoino.inode = stbuf.st_ino;
    btbin_write_dup(DBL, &dinoino, sizeof(DINOINO), bto, strlen(bto),
                    LOCK);
    s_free(fromdir);
    s_free(bfrom);
    s_free(bto);
    s_free(todir);
    EFUNC;
    return (res);
}

int fs_rename(const char *from, const char *to, struct stat stbuf)
{
    DAT *dataptr;
    DDSTAT *ddstat;
    DAT *ddbuf;
    int res = 0;
    unsigned long long inode;
    time_t thetime;
    char *bto;
    char *bfrom;
    char *fromdir;
    char *todir;
    unsigned long long fromdirnode;
    unsigned long long todirnode;
    unsigned long long tonode;
    struct stat st;
    unsigned long long dirnodes;

    FUNC;

    LDEBUG("fs_rename from: %s : to %s", (char *) from, (char *) to);
    todir = s_dirname((char *) to);
    todirnode = get_inode(todir);
    tonode = get_inode(to);
    bto = s_basename((char *) to);
    bfrom = s_basename((char *) from);
    if (-ENOENT != dbstat(to, &st, 0)) {
        if (S_ISDIR(st.st_mode)) {
            LDEBUG("fs_rename bto %s bfrom %s", bto, bfrom);
            dirnodes = has_nodes(tonode);
            if (0 != dirnodes) {
                if (0 != strcmp(bto, bfrom)) {
                    LDEBUG
                        ("fs_rename : Cannot rename directory %llu with %llu files",
                         todirnode, dirnodes);
                    s_free(todir);
                    s_free(bfrom);
                    s_free(bto);
                    EFUNC;
                    return -ENOTEMPTY;
                }
            }
            fs_rmdir(to);
        } else {
            LDEBUG("fs_rename : destination file %s exists, unlink_file.",
                   to);
            if ( config->blockdata_io_type == TOKYOCABINET ) {
                db_unlink_file(to);
            } else {
                file_unlink_file(to);
            }
        }
    }
    s_free(bfrom);
    inode = stbuf.st_ino;
    fromdir = s_dirname((char *) from);
    fromdirnode = get_inode(fromdir);
    LDEBUG("fs_rename : bto = %s", bto);
    dataptr = search_dbdata(DBP, &inode, sizeof(unsigned long long), LOCK);
    if (dataptr == NULL) {
        die_dataerr("Failed to find file %llu", inode);
    }
    ddstat = value_to_ddstat(dataptr);
    thetime = time(NULL);
    ddstat->stbuf.st_ctim.tv_sec = thetime;
    ddstat->stbuf.st_ctim.tv_nsec = 0;
    ddbuf = create_ddbuf(ddstat->stbuf, (char *) bto, ddstat->real_size);
    bin_write_dbdata(DBP, &inode,
                     sizeof(unsigned long long), (void *) ddbuf->data,
                     ddbuf->size);
    if (fromdirnode != todirnode) {
        LDEBUG("fs_rename : rename inode %llu : %llu to another path %llu",
               inode, fromdirnode, todirnode);
        btdelete_curkey(DBDIRENT, &fromdirnode, sizeof(unsigned long long),
                        &inode, sizeof(unsigned long long),
                        (char *) __PRETTY_FUNCTION__);
        btbin_write_dup(DBDIRENT, &todirnode, sizeof(unsigned long long),
                        &inode, sizeof(unsigned long long), LOCK);
    }
    DATfree(dataptr);
    DATfree(ddbuf);
    ddstatfree(ddstat);
    s_free(bto);
    s_free(fromdir);
    s_free(todir);
    EFUNC;
    return (res);
}

int fs_rmdir(const char *path)
{
    int res;
    char *dotstr;
    char *dotdotstr;
    char *dname;
    unsigned long long dirnode;
    unsigned long long keynode;
    unsigned long long pathnode;
    unsigned long long dirnodes;

    FUNC;
    LDEBUG("rmdir called : %s", path);

    pathnode = get_inode(path);
    dirnodes = has_nodes(pathnode);
    if (0 != dirnodes) {
        LDEBUG("fs_rmdir : Cannot remove directory %s with %llu files",
               path, dirnodes);
        EFUNC;
        return -ENOTEMPTY;
    }

    dname = s_dirname((char *) path);
    dotstr = as_sprintf(__FILE__, __LINE__, "%s/.", path);
    dotdotstr = as_sprintf(__FILE__, __LINE__, "%s/..", path);
    invalidate_p2i((char *) dotstr);
    invalidate_p2i((char *) dotdotstr);

    keynode = get_inode(dotstr);
    LDEBUG("inode for %s is %llu", dotstr, keynode);
    delete_key(DBP, &keynode, sizeof(unsigned long long),
               (char *) __PRETTY_FUNCTION__);
    btdelete_curkey(DBDIRENT, &pathnode, sizeof(unsigned long long),
                    &keynode, sizeof(unsigned long long),
                    (char *) __PRETTY_FUNCTION__);
    keynode = get_inode(dotdotstr);
    delete_key(DBP, &keynode, sizeof(unsigned long long),
               (char *) __PRETTY_FUNCTION__);
    btdelete_curkey(DBDIRENT, &pathnode, sizeof(unsigned long long),
                    &keynode, sizeof(unsigned long long),
                    (char *) __PRETTY_FUNCTION__);

    dirnode = get_inode(dname);
    delete_key(DBP, &pathnode, sizeof(unsigned long long),
               (char *) __PRETTY_FUNCTION__);
    btdelete_curkey(DBDIRENT, &dirnode, sizeof(unsigned long long),
                    &pathnode, sizeof(unsigned long long),
                    (char *) __PRETTY_FUNCTION__);
    btdelete_curkey(DBDIRENT, &pathnode, sizeof(unsigned long long),
                    &pathnode, sizeof(unsigned long long),
                    (char *) __PRETTY_FUNCTION__);
    s_free(dotstr);
    s_free(dotdotstr);
    res = update_parent_time(dname, -1);
    s_free(dname);
    return (res);
}

void clear_dirty()
{
    unsigned char *stiger;
    char *brand;
    brand = as_sprintf(__FILE__, __LINE__, "LESSFS_DIRTY");
    stiger = thash((unsigned char *) brand, strlen(brand));
    delete_key(DBU, stiger, config->hashlen, NULL);
    free(stiger);
    s_free(brand);
    return;
}

void parseconfig(int mklessfs, bool force_optimize)
{
    char *cache, *flushtime;
    unsigned int cs = 0;
    unsigned long calc;
#ifdef ENABLE_CRYPTO
    unsigned long long pwl = 0;
    unsigned char *stiger;
    DAT *ivdb;
    CRYPTO *crypto;
#endif
    char *iv;
    char *dbpath;
    struct stat stbuf;

    FUNC;
    config = s_zmalloc(sizeof(struct configdata));
    config->shutdown = 0;
    config->chunk_depth=2;
    if (iv=getenv("CHUNK_DEPTH")) {
       config->chunk_depth=atoi(iv);
       if ( config->chunk_depth <= 1 ) config->chunk_depth=2;
       if ( config->chunk_depth > MAX_CHUNK_DEPTH ) {
          LFATAL("CHUNK_DEPTH > %i, if you really want this change MAX_CHUNK_DEPTH",MAX_CHUNK_DEPTH);  
          config->chunk_depth=MAX_CHUNK_DEPTH;
       }
    }
    config->reclaim = 0;
    config->bdb_private = 0;
    iv=getenv("BLKSIZE");
    if (iv) BLKSIZE=atoi(iv);
    if ( BLKSIZE < 4096 || BLKSIZE > 131072 ) {
        die_dataerr("Invalid BLKSIZE specified.\n");
    } else LINFO("Blocksize = %u bytes",BLKSIZE);
    config->tune_for_size=LFSSMALL;
    iv = getenv("TUNEFORSIZE");
    if (iv) {
      if ( 0 == strcasecmp("medium",iv)) config->tune_for_size=LFSMEDIUM;
      if ( 0 == strcasecmp("huge",iv)) config->tune_for_size=LFSHUGE;
    } 
    if (NULL == getenv("MIN_SPACE_CLEAN")) {
        config->nospace = 1;
        LINFO
            ("MIN_SPACE_CLEAN is not set, lessfs runs -ENOSPC when reaching MIN_SPACE_FREE");
    } else
        config->nospace = 0;
    iv=getenv("BDB_PRIVATE");
    if (iv) {
      if ( 0 == strcasecmp("on",iv)) config->bdb_private=1;
    } 
    config->frozen = 0;
    config->safe_down = 0;
    config->max_backlog_size = 0;
    config->tuneforspeed = 0;
    config->replication_watchdir = NULL;
    config->replication_last_rotated = time(NULL);
// BLOCKDATA_IO_TYPE tokyocabinet (default), file_io
    config->blockdata = read_val("BLOCKDATA_PATH");
    config->blockdata_io_type = TOKYOCABINET;
    iv = getenv("BLOCKDATA_IO_TYPE");
    if (NULL == iv) {
        config->blockdata_io_type = FILE_IO;
        LINFO("The selected data store is file_io.");
    } else {
        if (0 == strncasecmp(iv, "file_io", strlen("file_io"))) {
            config->blockdata_io_type = FILE_IO;
            config->blockdatabs = NULL;
#ifdef BERKELEYDB
            iv = getenv("TUNEFORSPEED");
            if (iv) {
                if (0 == strncasecmp(iv, "yes", strlen("yes"))) {
                    config->tuneforspeed = 1;
                    LINFO("Warning : tuneforspeed may cause loss of data");
                    LINFO
                        ("Read the Berkeleydb documentation about DB_TXN_WRITE_NOSYNC");
                }
            }
#endif
            LINFO("The selected data store is file_io.");
        } else {
            if (0 == strncasecmp(iv, "chunk_io", strlen("chunk_io"))) {
               config->blockdata_io_type = CHUNK_IO;
               config->blockdatabs = NULL;
               LINFO("The selected data store is chunk_io.");
            } else {
               if ( 0 != mklessfs ) fprintf(stderr,"Using tokyocabinet as datastore is deprecated!\n");
               LINFO("The selected data store is tokyocabinet.");
               config->blockdatabs = read_val("BLOCKDATA_BS");
            }
        }
    }
#ifndef BERKELEYDB
#ifndef HAMSTERDB
    if ( 0 != mklessfs ) { 
       fprintf(stderr,"Using tokyocabinet is DEPRECATED and no longer recommended. Please consider using (1) Berkeley DB or (2) Hamsterdb.\n");
    }
#endif
#endif
#ifdef BERKELEYDB
    if (config->blockdata_io_type == TOKYOCABINET) {
        if ( 0 != mklessfs ) fprintf(stderr,"Configuration error : berkeleydb only supports file_io or chunk_io\n");
        die_dataerr
            ("Configuration error : berkeleydb only supports file_io or chunk_io");
    }
#endif
#ifdef HAMSTERDB
    if (config->blockdata_io_type == TOKYOCABINET) {
        if ( 0 != mklessfs ) fprintf(stderr,"Configuration error : hamsterdb only supports file_io or chunk_io\n");
        die_dataerr
            ("Configuration error : hamsterdb only supports file_io or chunk_io");
    }
#endif
    iv = getenv("MAX_BACKLOG_SIZE");
    if (iv) {
        sscanf(iv, "%lu", &config->max_backlog_size);
        LINFO("The maximum backlog size is %lu bytes",
              config->max_backlog_size);
    }
    iv = getenv("ENABLE_TRANSACTIONS");
    if (NULL == iv) {
        config->transactions = 0;
        LINFO("Lessfs transaction support is disabled.");
    } else {
        if (0 == strncasecmp(iv, "on", strlen("on"))) {
            config->transactions = 1;
            LINFO("Lessfs transaction support is enabled.");
        } else {
            config->transactions = 0;
            LINFO("Lessfs transaction support is disabled.");
        }
    }
#ifdef BERKELEYDB
    config->meta = read_val("META_PATH");
#else
#ifdef HAMSTERDB
    config->meta = read_val("META_PATH");
#else
    config->freelist = read_val("FREELIST_PATH");
    config->freelistbs = read_val("FREELIST_BS");
    config->blockusage = read_val("BLOCKUSAGE_PATH");
    config->blockusagebs = read_val("BLOCKUSAGE_BS");
    config->dirent = read_val("DIRENT_PATH");
    config->direntbs = read_val("DIRENT_BS");
    config->fileblock = read_val("FILEBLOCK_PATH");
    config->fileblockbs = read_val("FILEBLOCK_BS");
    config->meta = read_val("META_PATH");
    config->metabs = read_val("META_BS");
    config->hardlink = read_val("HARDLINK_PATH");
    config->hardlinkbs = read_val("HARDLINK_BS");
    config->symlink = read_val("SYMLINK_PATH");
    config->symlinkbs = read_val("SYMLINK_BS");
#endif
#endif
    LINFO("config->blockdata = %s", config->blockdata);

    config->encryptdata = 0;
    config->encryptmeta = 1;
    config->hashlen = 24;
    config->hash = "MHASH_TIGER192";
    config->selected_hash = MHASH_TIGER192;
    config->compression = 'Q';
//  Background delete is now enabled by default
    config->background_delete = 1;
    config->sticky_on_locked = 0;
    iv = getenv("COMPRESSION");
    if (iv) {
        LINFO("compression = %s",iv);
#ifdef SNAPPY
        if (0 == strcasecmp("snappy", iv))
            config->compression = 'S';
#else
        if (0 == strcasecmp("snappy", iv))
            die_dataerr
                ("SNAPPY support is not available: please configure with --with-snappy");
#endif
        if (0 == strcasecmp("qlz151", iv))
            config->compression = 'P';
        if (0 == strcasecmp("qlz", iv))
            config->compression = 'Q';
        if (0 == strcasecmp("qlz141", iv))
            config->compression = 'Q';
#ifdef LZO
        if (0 == strcasecmp("lzo", iv))
            config->compression = 'L';
#else
        if (0 == strcasecmp("lzo", iv))
            die_dataerr
                ("LZO support is not available: please configure with --with-lzo");
#endif
        if (0 == strcasecmp("gzip", iv))
            config->compression = 'G';
        if (0 == strcasecmp("bzip", iv))
            config->compression = 'B';
        if (0 == strcasecmp("deflate", iv))
            config->compression = 'D';
        if (0 == strcasecmp("disabled", iv))
            config->compression = 0;
        if (0 == strcasecmp("none", iv))
            config->compression = 0;
    }
    iv = getenv("BACKGROUND_DELETE");
    if (iv) {
        if (0 == strcasecmp(iv, "off")) {
            config->background_delete = 0;
            LINFO("Threaded background delete is disabled");
        }
    }
    if (config->background_delete) {
        LINFO("Threaded background delete is enabled");
        iv = getenv("STICKY_ON_LOCKED");
        if (iv) {
            if (0 == strcasecmp(iv, "on")) {
                config->sticky_on_locked = 1;
                LINFO
                    ("Stickybit will be set during background delete and truncation.");
                LINFO("Indicating that the file is write locked.");
            }
        }
    }
    iv = getenv("HASHNAME");
    if (iv) {
        config->hash = iv;
        if (0 == strcmp("MHASH_SHA256", iv)) {
            config->selected_hash = MHASH_SHA256;
            LINFO("Hash SHA256 has been selected");
        }
        if (0 == strcmp("MHASH_SHA512", iv)) {
            config->selected_hash = MHASH_SHA512;
            LINFO("Hash SHA512 has been selected");
        }
        if (0 == strcmp("MHASH_WHIRLPOOL", iv)) {
            config->selected_hash = MHASH_WHIRLPOOL;
            LINFO("Hash WHIRLPOOL has been selected");
        }
        if (0 == strcmp("MHASH_HAVAL256", iv)) {
            config->selected_hash = MHASH_HAVAL256;
            LINFO("Hash HAVAL has been selected");
        }
        if (0 == strcmp("MHASH_SNEFRU256", iv)) {
            config->selected_hash = MHASH_SNEFRU256;
            LINFO("Hash SNEFRU has been selected");
        }
        if (0 == strcmp("MHASH_RIPEMD256", iv)) {
            config->selected_hash = MHASH_RIPEMD256;
            LINFO("Hash RIPEMD256 has been selected");
        }
        if (config->selected_hash == MHASH_TIGER192)
            LINFO("Hash MHASH_TIGER192 has been selected");
    } else
        LINFO("Hash MHASH_TIGER192 been selected");
    iv = getenv("HASHLEN");
    if (iv) {
        if (atoi(iv) >= 20 && atoi(iv) <= MAX_HASH_LEN) {
            if (atoi(iv) > 24 && config->selected_hash == MHASH_TIGER192) {
                die_dataerr
                    ("MHASH_TIGER192 can not be used with MAX_HASH_LEN > 24");
            }
            config->hashlen = atoi(iv);
        } else {
            LFATAL("The hash length is invalid.");
            exit(EXIT_USAGE);
        }
    }
    LINFO("Lessfs uses a %i bytes long hash.", config->hashlen);
    iv = getenv("SYNC_RELAX");
    if (NULL == iv) {
        config->relax = 0;
    } else {
        config->relax = atoi(iv);
        if (0 != config->relax) {
            LINFO
                ("Lessfs fsync does not sync the databases to the disk when fsync is called on an inode");
        }
    }
    iv = getenv("REPLICATION");
    config->replication = 0;
    config->replication_enabled = 0;
    config->replication_backlog = 0;
    config->replication_partner_ip = "0";
    config->rotate_replog_size = ROTATE_REPLOG_SIZE;
    if (iv) {
        if (0 == strcasecmp(iv, "masterslave"))
            config->replication = 1;
    }
    iv = getenv("ROTATE_REPLOG_SIZE");
    if (iv)
        sscanf(iv, "%lu", &config->rotate_replog_size);
    if (1 == config->replication) {
        config->replication_enabled = 1;
        iv = read_val("REPLICATION_ROLE");
        if (0 == strcasecmp(iv, "master")) {
            config->replication_role = 0;       // 0=master
            config->replication_partner_ip =
                read_val("REPLICATION_PARTNER_IP");
            config->replication_partner_port =
                read_val("REPLICATION_PARTNER_PORT");
        } else {
            config->replication_role = 1;       // 1=slave
            config->replication_listen_ip =
                getenv("REPLICATION_LISTEN_IP");
            if (config->replication_listen_ip) {
                if (0 == strcmp(config->replication_listen_ip, "0.0.0.0"))
                    config->replication_listen_ip = NULL;
            }
            iv = getenv("REPLICATION_WATCHDIR");
            if (iv)
                config->replication_watchdir = s_strdup(iv);
            config->replication_listen_port =
                read_val("REPLICATION_LISTEN_PORT");
        }
        iv = getenv("REPLICATION_ENABLED");
        if (iv) {
            if (0 != strcasecmp(iv, "on"))
                config->replication_enabled = 0;
        }
    }
    iv = getenv("INSPECT_DISK_INTERVAL");
    if (NULL == iv) {
        config->inspectdiskinterval = 1;
    } else {
        config->inspectdiskinterval = atoi(iv);
    }
    iv = getenv("DYNAMIC_DEFRAGMENTATION");
    config->defrag = 0;
    if (iv) {
        if (0 == strcasecmp("on", iv)) {
            config->defrag = 1;
        }
    }
    if (config->defrag)
        LINFO("Automatic defragmentation is enabled.");
    cache = read_val("CACHESIZE");
    if (cache)
        cs = atoi(cache);
    if (cs <= 0)
        cs = 1;
    calc = cs;
    config->cachesize = (calc * 1024 * 1024) / MAX_FUSE_BLKSIZE;
    flushtime = read_val("COMMIT_INTERVAL");
    cs = atoi(flushtime);
    if (cs <= 0)
        cs = 30;
    config->flushtime = cs;
    LINFO("cache %llu data blocks", config->cachesize);
#ifdef HAMSTERDB
    iv = getenv("HAMSTERDB_CACHESIZE");
    if (iv) {
        sscanf(iv, "%lu", &config->hamsterdb_cachesize);
    } else config->hamsterdb_cachesize=calc * 1024 * 1024/10;
    LINFO("The hamsterdb cache size is %lu bytes",
           config->hamsterdb_cachesize);
#endif
    if (mklessfs == 1) {
        dbpath =
            as_sprintf(__FILE__, __LINE__, "%s/fileblock.tch",
                       config->fileblock);
        if (-1 != stat(dbpath, &stbuf)) {
            fprintf(stderr,
                    "Data %s already exists, please remove it and try again\n",
                    dbpath);
            exit(EXIT_DATAERR);
        }
        if (config->blockdata_io_type == CHUNK_IO) init_chunk_io();
#ifdef HAMSTERDB
        hm_create(0);
#endif
    }
    if (mklessfs == 2) {
        if (config->blockdata_io_type == CHUNK_IO) {
            LFATAL("Please remove old datafiles manually and rerun mklessfs without -f");
            die_dataerr("Overwrite is not supported with chunk_io");
        }
        drop_databases();
#ifdef BERKELEYDB
        bdb_open();
#else
#ifdef HAMSTERDB
        hm_create(1);
#else
        if (NULL == dbp)
            tc_open(0, 1, force_optimize);
#endif
#endif
    } else {
#ifdef BERKELEYDB
        bdb_open();
#else
#ifdef HAMSTERDB
        if (mklessfs == 0 || mklessfs == 3)
            hm_open();
#else
        if (NULL == dbp)
            tc_open(0, 0, force_optimize);
#endif
#endif
    }

    if (mklessfs == 0 || mklessfs == 3) {
#ifdef ENABLE_CRYPTO
        iv = read_val("ENCRYPT_DATA");
        if (iv) {
            if (0 == strcasecmp(iv, "ON")) {
                config->encryptdata = 1;
                iv = getenv("ENCRYPT_META");
                LINFO("Data encryption is on");
                if (iv) {
                    if (0 != strcasecmp(iv, "ON")) {
                        LINFO("Metadata encryption is off");
                        config->encryptmeta = 0;
                    }
                }
            }
        }
        if (config->encryptdata) {
            if (NULL == getenv("PASSWORD")) {
                config->passwd =
                    (unsigned char *) s_strdup(getpass("Password: "));
            } else
                config->passwd =
                    (unsigned char *) s_strdup(getenv("PASSWORD"));
            unsetenv("PASSWORD");       /* Eat it after receiving.. */
            stiger =
                thash(config->passwd, strlen((char *) config->passwd));
            ivdb =
                search_dbdata(DBP, &pwl, sizeof(unsigned long long), LOCK);
            if (NULL == ivdb) {
                db_close(0);
                die_dataerr
                    ("The filesystem has not been formatted with encryption support.");
            }
            config->iv = s_malloc(8);
            crypto = (CRYPTO *) ivdb->data;
            memcpy(config->iv, crypto->iv, 8);
            //config->passwd is plain, crypto->passwd is hashed.
            checkpasswd(crypto->passwd);
            s_free(stiger);
            DATfree(ivdb);
        }
#endif
        if (0 == get_next_inode())
            die_dataerr
                ("Please format lessfs with mklessfs before mounting!");
    }
    return;
}

void sync_all_filesizes()
{
    unsigned long long inode;
    const char *key;
    FUNC;
    meta_lock((char *) __PRETTY_FUNCTION__);
    tctreeiterinit(metatree);
    while (key = tctreeiternext2(metatree)) {
        memcpy(&inode, key, sizeof(unsigned long long));
        update_filesize_onclose(inode);
    }
    release_meta_lock();
    EFUNC;
    return;
}

int get_blocksize()
{
    unsigned char *stiger;
    char *brand;
    INUSE *finuse;
    int blksize = 4096;
    unsigned long long inuse;

    brand = as_sprintf(__FILE__, __LINE__, "LESSFS_BLOCKSIZE");
    stiger = thash((unsigned char *) brand, strlen(brand));
    if (config->blockdatabs) {
        inuse = getInUse(stiger);
        if (0 == inuse) {
            brand_blocksize();
            blksize = BLKSIZE;
        } else {
            blksize = inuse;
        }
    } else {
        finuse = file_get_inuse(stiger);
        if (NULL == finuse) {
            brand_blocksize();
            blksize = BLKSIZE;
        } else {
            blksize = finuse->inuse;
        }
    }
    free(stiger);
    s_free(brand);
    return (blksize);
}

/* Add the hash for string LESSFS_BLOCKSIZE
   to lessfs so that we know the blocksize for
   lessfsck and when someone is foolish enough to
   remount with a different blocksize */
void brand_blocksize()
{
    unsigned char *stiger;
    char *brand;
    INUSE inuse;

    brand = as_sprintf(__FILE__, __LINE__, "LESSFS_BLOCKSIZE");
    stiger = thash((unsigned char *) brand, strlen(brand));
    if (config->blockdatabs) {
        update_inuse(stiger, BLKSIZE);
    } else {
        inuse.inuse = BLKSIZE;
        inuse.size = 0;
        inuse.offset = 0;
        file_update_inuse(stiger, &inuse);
    }
    free(stiger);
    s_free(brand);
    return;
}

#ifdef ENABLE_CRYPTO
void checkpasswd(char *cryptopasswd)
{
    unsigned char *stiger;

    FUNC;
    stiger = thash(config->passwd, strlen((char *) config->passwd));
    if (0 != memcmp(cryptopasswd, stiger, config->hashlen)) {
        sleep(5);
        fprintf(stderr, "Invalid password entered.\n");
        exit(EXIT_PASSWD);
    }
    s_free(stiger);
    EFUNC;
    return;
}
#endif

char *ascii_hash(unsigned char *bhash)
{
    char *ascii_hash = NULL;
    int n;
    char *p1 = NULL, *p2 = NULL;

    for (n = 0; n < config->hashlen; n++) {
        p1 = as_sprintf(__FILE__, __LINE__, "%02X", bhash[n]);
        if (n == 0) {
            ascii_hash = s_strdup(p1);
        } else {
            p2 = s_strdup(ascii_hash);
            s_free(ascii_hash);
            ascii_hash = as_sprintf(__FILE__, __LINE__, "%s%s", p2, p1);
            s_free(p2);
        }
        s_free(p1);
    }
    return (ascii_hash);
}


void open_trees()
{
    workqtree = tctreenew();
    readcachetree = tctreenew();
    hashtree = tctreenew();
    inodetree = tctreenew();
    metatree = tctreenew();
    path2inotree = tctreenew();
}

void close_trees()
{
    tctreeclear(workqtree);
    tctreeclear(readcachetree);
    tctreeclear(hashtree);
    tctreeclear(inodetree);
    tctreeclear(metatree);
    tctreeclear(path2inotree);

    tctreedel(workqtree);
    tctreedel(readcachetree);
    tctreedel(hashtree);
    tctreedel(inodetree);
    tctreedel(metatree);
    tctreedel(path2inotree);
}
