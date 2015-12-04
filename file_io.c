/*
 *   Lessfs: A data deduplicating filesystem.
 *   Copyright (C) 2008 Mark Ruijter <mruijter@lessfs.com>
 *
 *   This program is free software.
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
#define  _GNU_SOURCE
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif
#ifndef LFATAL
#include "lib_log.h"
#endif

#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/file.h>
#include <fuse.h>

#include <fcntl.h>
#include <pthread.h>
#include <libgen.h>

#include <tcutil.h>
#include <tcbdb.h>
#include <tchdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <aio.h>
#include "lib_safe.h"
#include "lib_cfg.h"
#include "retcodes.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#include "lib_qlz.h"
#include "lib_qlz15.h"
#include "lib_common.h"
#ifndef COMMERCIAL
#include "lib_tc.h"
#else
#include "lib_hamster.h"
#endif
#include "lib_crypto.h"
#include "file_io.h"
#include "lib_repl.h"
#include "lib_net.h"

extern char *logname;
extern char *function;
extern int debug;
extern int BLKSIZE;
extern int max_threads;
extern char *passwd;

extern TCHDB *dbb;
extern TCHDB *dbu;
extern TCHDB *dbp;
extern TCBDB *dbl;
extern TCHDB *dbs;
extern TCHDB *dbdta;
extern TCBDB *dbdirent;
extern TCBDB *freelist;
extern TCTREE *workqtree;
extern TCTREE *readcachetree;
extern int fdbdta;

extern unsigned long long nextoffset;
extern pthread_spinlock_t dbu_spinlock;
extern unsigned int dbu_qcount;

#define die_dataerr(f...) { LFATAL(f); exit(EXIT_DATAERR); }

INUSE *file_get_inuse(unsigned char *stiger)
{
    INUSE *inuse;
    OLDINUSE *oldinuse;

    DAT *data;

    FUNC;
    if (NULL == stiger)
        return NULL;
    data = search_dbdata(DBU, stiger, config->hashlen, LOCK);
    if (NULL == data) {
        LDEBUG("file_get_inuse: nothing found return NULL.");
        return NULL;
    }
    if ( data->size == sizeof(INUSE )) {
       inuse = (INUSE *) data->data;
    } else {
       oldinuse=(OLDINUSE *) data->data;
       inuse=s_zmalloc(sizeof(INUSE));
       inuse->inuse=oldinuse->inuse;
       inuse->offset=oldinuse->offset;
       inuse->size=oldinuse->size;
       s_free(data->data);
    }
    s_free(data);
    EFUNC;
    return inuse;
}

void file_update_inuse(unsigned char *stiger, INUSE * inuse)
{
    FUNC;
    LDEBUG
        ("file_update_inuse : update inuse->size = %lu, inuse->inuse = %llu",
         inuse->size, inuse->inuse);
    if (inuse) {
        bin_write_dbdata(DBU, stiger, config->hashlen,
                         (unsigned char *) inuse, sizeof(INUSE));
    }
    EFUNC;
    return;
}

unsigned long long round_512(unsigned long long size)
{
    unsigned long long so;
    unsigned long long bytes;

    FUNC;
    so = size / 512;
    if (so != 0) {
        so = so * 512;
    }
    if (so < size) {
        bytes = 512 + so;
    } else
        bytes = so;
    LDEBUG("round_512 : bytes is %llu : size %llu", bytes, size);
    return bytes;
}

void set_new_offset(unsigned long long size)
{
    unsigned long long offset = 0;

    FUNC;
    if (config->nospace == -ENOSPC) {
        LINFO("set_new_offset : out of disk space : ENOSPC");
        config->nospace = ENOSPC;
    }
    LDEBUG("oldoffset is %llu : add size %llu", nextoffset, size);
    offset = round_512(size);
    nextoffset = nextoffset + offset;
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(NEXTOFFSET, REPLSETNEXTOFFSET,
                        (char *) &nextoffset, sizeof(unsigned long long),
                        NULL, 0, MAX_ALLOWED_THREADS - 2);
    }
    LDEBUG("nextoffset is now %llu", nextoffset);
    EFUNC;
    return;
}

char *hash_to_path(unsigned char *thehash,unsigned long long chunk_store)
{
    unsigned char *aschash;
    char *path=NULL;
    char *fullpath;
    int len;
    int d;

    aschash=hash_to_ascii(thehash);
    switch(chunk_store)
    {
        case HIGH_PRIO:
        path=s_zmalloc(1+strlen("/high")+strlen(config->blockdata)+2*config->chunk_depth);
        sprintf(path,"%s/high",config->blockdata);
        break;
        case MEDIUM_PRIO:
        path=s_zmalloc(1+strlen("/medium")+strlen(config->blockdata)+2*config->chunk_depth);
        sprintf(path,"%s/medium",config->blockdata);
        break;
        path=s_zmalloc(1+strlen("/low")+strlen(config->blockdata)+2*config->chunk_depth);
        sprintf(path,"%s/low",config->blockdata);
        case LOW_PRIO:
        break;
        default:
        die_dataerr("chunk_write : Unknown prio");
    }
    len=strlen(path);
    len++;
    for ( d=0; d<config->chunk_depth*2;d+=2){
       path[len+d-1]='/';
       path[len+d]=aschash[d/2];
    }
    fullpath=as_sprintf(__FILE__,__LINE__,"%s/%s",path,aschash);
    s_free(aschash);
    s_free(path);
    return fullpath;
}

DAT *chunk_read(unsigned char *thehash,INUSE *inuse)
{
   DAT *encrypted;
   char *fullpath;
   int fd;

   fullpath=hash_to_path(thehash,inuse->offset);
   encrypted=s_zmalloc(sizeof(DAT));
   encrypted->data=s_zmalloc(inuse->size);
   if (-1 ==
            (fd =
             s_open2(fullpath, O_RDONLY | O_NOATIME, S_IRWXU)))
            die_syserr();
   encrypted->size=fullRead(fd,encrypted->data,inuse->size);
   if ( encrypted->size != inuse->size) 
        die_dataerr("chunk_read : unexpected short read %lu expected %lu: data corruption?", 
                     encrypted->size,inuse->size);
   close(fd);
   return(encrypted);
}

void chunk_delete(unsigned char *thehash,unsigned long long chunk_store)
{
   char *fullpath;
   fullpath=hash_to_path(thehash,chunk_store);
   if ( -1 == unlink(fullpath)) die_syserr();
   s_free(fullpath);
}

void chunk_write(unsigned char *thehash,DAT *compressed, unsigned long long chunk_store)
{
    int fd;
    char *fullpath;

    fullpath=hash_to_path(thehash,chunk_store);
    if (-1 ==
            (fd =
             s_open2(fullpath, O_CREAT | O_TRUNC| O_RDWR| O_NOATIME, S_IRWXU)))
            die_syserr();
    fullWrite(fd,compressed->data, compressed->size);
    close(fd);
    s_free(fullpath);
}

void fl_write_cache(CCACHEDTA * ccachedta, INOBNO * inobno)
{
    INUSE *inuse;
    DAT *compressed;

    FUNC;
    create_hash_note((unsigned char *) &ccachedta->hash);
    file_delete_stored(inobno);
    inuse = file_get_inuse((unsigned char *) &ccachedta->hash);
    if (NULL == inuse) {
        compressed = lfscompress(ccachedta->data, ccachedta->datasize);
        if ( config->blockdata_io_type == CHUNK_IO ) {
           inuse = s_zmalloc(sizeof(INUSE));
           inuse->offset = MEDIUM_PRIO;
           inuse->size = compressed->size;
           inuse->allocated_size = compressed->size;
           chunk_write((unsigned char *) &ccachedta->hash,compressed,inuse->offset);
        } else {
           inuse = get_offset(compressed->size);
           s_lckpwrite(fdbdta, compressed->data, compressed->size,
                       inuse->offset);
        }
        if (config->replication == 1 && config->replication_role == 0) {
            write_repl_data(FDBDTA, REPLWRITE, (char *) &inuse->offset,
                            sizeof(unsigned long long),
                            (char *) compressed->data, compressed->size,
                            MAX_ALLOWED_THREADS - 2);
        }
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
    inuse->inuse++;
    LDEBUG("inuse->inuse=%llu : %llu-%llu", inuse->inuse, inobno->inode,
           inobno->blocknr);
    file_update_inuse((unsigned char *) &ccachedta->hash, inuse);
    delete_hash_note((unsigned char *) &ccachedta->hash);
    bin_write_dbdata(DBB, (char *) inobno, sizeof(INOBNO), ccachedta->hash,
                     config->hashlen);
    s_free(inuse);
    ccachedta->dirty = 0;
    ccachedta->pending = 0;
    ccachedta->newblock = 0;
    EFUNC;
    return;
}

/* delete = 1 Do delete dbdata
   delete = 0 Do not delete dbdta */
unsigned int file_commit_block(unsigned char *dbdata,
                               INOBNO inobno, unsigned long dsize)
{
    unsigned char *stiger;
    DAT *compressed;
    INUSE *inuse;
    unsigned int ret = 0;

    FUNC;
    stiger = thash(dbdata, dsize);
    create_hash_note(stiger);
    inuse = file_get_inuse(stiger);
    if (NULL == inuse) {
        compressed = lfscompress((unsigned char *) dbdata, dsize);
        if ( config->blockdata_io_type == CHUNK_IO ) {
           inuse = s_zmalloc(sizeof(INUSE));
           inuse->offset = MEDIUM_PRIO;
           inuse->size = compressed->size;
           inuse->allocated_size = compressed->size;
           chunk_write((unsigned char *) stiger,compressed,inuse->offset);
        } else {
           inuse = get_offset(compressed->size);
           s_lckpwrite(fdbdta, compressed->data, compressed->size,
                    inuse->offset);
        }
        update_meta(inobno.inode, compressed->size, 1);
        if (config->replication == 1 && config->replication_role == 0) {
            write_repl_data(FDBDTA, REPLWRITE, (char *) &inuse->offset,
                            sizeof(unsigned long long),
                            (char *) compressed->data, compressed->size,
                            MAX_ALLOWED_THREADS - 2);
        }
        DATfree(compressed);
    } else {
        loghash("commit_block : only updated inuse for hash ", stiger);
    }
    inuse->inuse++;
    file_update_inuse(stiger, inuse);
    bin_write_dbdata(DBB, (char *) &inobno, sizeof(INOBNO), stiger,
                     config->hashlen);
    delete_hash_note(stiger);
    free(stiger);
    s_free(inuse);
    return (ret);
}

/* Read a block of data from file */
DAT *file_tgr_read_data(unsigned char *stiger)
{
    INUSE *inuse = NULL;
    DAT *encrypted = NULL;

    FUNC;
    inuse = file_get_inuse(stiger);
    if (inuse) {
        if (inuse->inuse == 0)
            die_dataerr("file_tgr_read_data : read empty block");
        LDEBUG("file_tgr_read_data : read offset %llu, size %lu",
               inuse->offset, inuse->size);
        if ( config->blockdata_io_type == CHUNK_IO ) {
           encrypted=chunk_read(stiger,inuse);
        } else {
           encrypted = s_malloc(sizeof(DAT));
           encrypted->data = s_malloc(inuse->size);
           encrypted->size =
               (unsigned long) s_lckpread(fdbdta, encrypted->data,
                                          inuse->size, inuse->offset);
           if ( inuse->size != encrypted->size ) {
              log_fatal_hash("Please report this bug and include hash and offset",stiger);
              die_dataerr("file_tgr_read_data : read block expected offset %llu, size %lu, got size %lu",
                           inuse->offset,inuse->size,encrypted->size);
           }
        }
        s_free(inuse);
    } else {
        loghash("file_tgr_read_data - unable to find dbdta block hash :",
                stiger);
        encrypted = NULL;
    }
    EFUNC;
    return encrypted;
}

unsigned long long file_read_block(unsigned long long blocknr,
                                   char *blockdata, size_t rsize,
                                   size_t block_offset,
                                   unsigned long long inode)
{
    char *cachedata;
    DAT *cdata;
    DAT *data;
    INOBNO inobno;
    int ret = 0;
    CCACHEDTA *ccachedta;
    DAT *tdata;
    int vsize;
    uintptr_t p;
    inobno.inode = inode;
    inobno.blocknr = blocknr;
    int locked;

    locked = inode_isnot_locked(inode);
    cachedata =
        (char *) tctreeget(readcachetree, (void *) &inobno, sizeof(INOBNO),
                           &vsize);
    if (NULL == cachedata) {
        tdata = check_block_exists(&inobno);
        if (NULL == tdata) {
            return (0);
        }
        cdata = file_tgr_read_data(tdata->data);
        if (NULL == cdata) {
            auto_repair(&inobno);
            DATfree(tdata);
            return (0);
        }
        DATfree(tdata);
        data = lfsdecompress(cdata);
        if ( NULL == data ) die_dataerr("file_read_block : inode %llu - %llu failed to decompress block", inobno.inode,inobno.blocknr);
        if (block_offset < data->size) {
            if (rsize > data->size - block_offset) {
                memcpy(blockdata, data->data + block_offset,
                       data->size - block_offset);
            } else {
                memcpy(blockdata, data->data + block_offset, rsize);
            }
        }
        DATfree(cdata);
        if (locked) {
// When we read a block < BLKSIZE there it is likely that we need
// to read it again so it makes sense to put it in a cache.
            if (rsize < BLKSIZE) {
// Make sure that we don't overflow the cache.
                if (tctreernum(workqtree) * 2 > config->cachesize ||
                    tctreernum(readcachetree) * 2 > config->cachesize) {
                    flush_wait(inobno.inode);
                    purge_read_cache(inobno.inode, 1,
                                     (char *) __PRETTY_FUNCTION__);
                }
                ccachedta = s_zmalloc(sizeof(CCACHEDTA));
                p = (uintptr_t) ccachedta;
                ccachedta->dirty = 0;
                ccachedta->pending = 0;
                ccachedta->creationtime = time(NULL);
                memcpy(&ccachedta->data, data->data, data->size);
                ccachedta->datasize = data->size;
                tctreeput(readcachetree, (void *) &inobno, sizeof(INOBNO),
                          (void *) &p, sizeof(p));
            }
        }
        DATfree(data);
        ret = BLKSIZE;
        return (ret);
    }
// Fetch the block from disk and put it in the cache.
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
    return (ret);
}

/* The freelist is a btree with as key the number
   of blocks (512 bytes). The value is offset.
*/
void put_on_freelist(INUSE * inuse, unsigned long long inode)
{
    FREEBLOCK freeblock;
    unsigned long long scalc;
    time_t thetime;

    FUNC;

    get_offset_lock((char *) __PRETTY_FUNCTION__);
    // Delete these bytes from real_size;
    update_meta(inode, inuse->size, 0);
    thetime = time(NULL);
    if (config->nospace != ENOSPC) {
        freeblock.reuseafter = thetime + (config->flushtime * 10);
    } else {
        freeblock.reuseafter = thetime;
    }
    freeblock.offset = inuse->offset;
    scalc = inuse->allocated_size / 512;
    btbin_write_dup(FREELIST, (void *) &scalc, sizeof(unsigned long long),
                    (void *) &freeblock, sizeof(FREEBLOCK), LOCK);
    LDEBUG("put_on_freelist : %llu blocks at offset %llu", scalc,
           inuse->offset);
    release_offset_lock();
    EFUNC;
    return;
}

CCACHEDTA *file_update_stored(unsigned char *hash, INOBNO * inobno,
                              off_t offsetblock)
{
    DAT *data;
    DAT *uncompdata;
    CCACHEDTA *ccachedta;

    ccachedta = s_zmalloc(sizeof(CCACHEDTA));
    ccachedta->creationtime = time(NULL);
    ccachedta->dirty = 1;
    ccachedta->pending = 0;
    ccachedta->newblock = 0;

    data = file_tgr_read_data(hash);
    if (NULL == data) {
        s_free(ccachedta);
        return NULL;
    }
    uncompdata = lfsdecompress(data);
    if ( NULL == uncompdata ) die_dataerr("file_update_stored : inode %llu - %llu failed to decompress block", inobno->inode,inobno->blocknr);
    memcpy(&ccachedta->data, uncompdata->data, uncompdata->size);
    ccachedta->datasize = uncompdata->size;
    ccachedta->updated = data->size;
    DATfree(uncompdata);
    DATfree(data);
    file_delete_stored(inobno);
    EFUNC;
    return ccachedta;
}

void file_delete_stored(INOBNO * inobno)
{
    unsigned char *hash;
    DAT *data;
    INUSE *inuse;

    data = search_dbdata(DBB, inobno, sizeof(INOBNO), LOCK);
    if (NULL == data)
        return;
    hash = data->data;
    inuse = file_get_inuse(hash);
    if (NULL == inuse)
        return;
    delete_dbb(inobno);
  
    if (inuse->inuse <= 1) {
        if ( config->blockdata_io_type == CHUNK_IO ) {
          chunk_delete(hash,inuse->offset);
        } else {
          put_on_freelist(inuse, inobno->inode);
        }
        delete_inuse(hash);
    } else {
        inuse->inuse--;
        file_update_inuse(hash, inuse);
    }
    DATfree(data);
    s_free(inuse);
    return;
}

void *file_truncate_worker(void *threadarg)
{
    unsigned long long blocknr;
    unsigned long long lastblocknr;
    struct truncate_thread_data *trunc_data;
    unsigned char *stiger;
    DAT *data;
    INOBNO inobno;
    unsigned int offsetblock;
    INUSE *inuse;

    trunc_data = (struct truncate_thread_data *) threadarg;
    lastblocknr = trunc_data->lastblocknr;
    blocknr = trunc_data->blocknr;
    offsetblock = trunc_data->offsetblock;
    inobno.blocknr = trunc_data->blocknr;
    inobno.inode = trunc_data->inode;

    LDEBUG("Start truncation thread : inode %llu", inobno.inode);

    while (lastblocknr >= blocknr) {
        if (config->background_delete) {
            truncation_wait();
            write_lock((char *) __PRETTY_FUNCTION__);
        }
        if (offsetblock != 0 && lastblocknr == blocknr) {
            if (config->background_delete)
                release_write_lock();
            break;
        }
        LDEBUG
            ("file_fs_truncate : Enter loop lastblocknr %llu : blocknr %llu",
             lastblocknr, blocknr);
        inobno.blocknr = lastblocknr;
        data = search_dbdata(DBB, &inobno, sizeof(INOBNO), LOCK);
        if (NULL == data) {
            LDEBUG
                ("file_fs_truncate: deletion of non existent block inode : %llu, blocknr %llu",
                 inobno.inode, inobno.blocknr);
            if (lastblocknr > 0)
                lastblocknr--;
            else {
                if (config->background_delete)
                    release_write_lock();
                break;
            }
            if (config->background_delete)
                release_write_lock();
// Need to continue in case of a sparse file.
            continue;
        }
        stiger = s_malloc(data->size);
        memcpy(stiger, data->data, data->size);
        LDEBUG
            ("file_fs_truncate : lessfs_truncate Search to delete blocknr %llu:",
             lastblocknr);
        DATfree(data);
        create_hash_note(stiger);
        delete_dbb(&inobno);
        inuse = file_get_inuse(stiger);
        if (inuse) {
            if (inuse->inuse == 1) {
                if ( config->blockdata_io_type == CHUNK_IO ) {
                      chunk_delete(stiger,inuse->offset);
                } else {
                   put_on_freelist(inuse, inobno.inode);
                }
                delete_inuse(stiger);
                LDEBUG("file_fs_truncate : delete dbb %llu-%llu",
                       inobno.inode, inobno.blocknr);
            } else {
                if (inuse->inuse > 1)
                    inuse->inuse--;
                LDEBUG
                    ("file_fs_truncate : delete dbb %llu-%llu : inuse = %llu",
                     inobno.inode, inobno.blocknr, inuse->inuse);
                file_update_inuse(stiger, inuse);
            }
        } else
            LDEBUG("file_fs_truncate : inuse is NULL %llu-%llu",
                   inobno.inode, inobno.blocknr);
        delete_hash_note(stiger);
        s_free(inuse);
        s_free(stiger);
        if (config->background_delete)
            release_write_lock();
        if (lastblocknr > 0)
            lastblocknr--;
        else
            break;
        if (config->background_delete) {
            if (config->shutdown) {
                LINFO("Truncation thread aborts truncating inode %llu",
                      inobno.inode);
                return NULL;
            }
        }
    }
    if (0 != offsetblock) {
        if (config->background_delete)
            write_lock((char *) __PRETTY_FUNCTION__);
        file_partial_truncate_block(inobno.inode, lastblocknr,
                                    offsetblock);
        if (config->background_delete)
            release_write_lock();
    }
    delete_inode_note(inobno.inode);
    LDEBUG("Completed truncating of inode %llu", inobno.inode);
    if ( config->background_delete ) write_lock((char *) __PRETTY_FUNCTION__);
    btdelete_curkey(FREELIST, "TRNCTD", strlen("TRNCTD"),
                    threadarg, sizeof(struct truncate_thread_data),
                    (char *) __PRETTY_FUNCTION__);
    release_write_lock();
    s_free(threadarg);
    return NULL;
}

int file_fs_truncate(struct stat *stbuf, off_t size, char *bname,
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
    LDEBUG("file_fs_truncate inode %llu - size %llu", stbuf->st_ino,
           (unsigned long long) size);
    thetime = time(NULL);
    blocknr = size / BLKSIZE;
    offsetblock = size - (blocknr * BLKSIZE);
    oldsize = stbuf->st_size;
    lastblocknr = oldsize / BLKSIZE;
    trunc_data->inode = stbuf->st_ino;
    trunc_data->blocknr = blocknr;
    trunc_data->lastblocknr = lastblocknr;
    trunc_data->offsetblock = offsetblock;
    trunc_data->unlink = unlink;
    memcpy(&trunc_data->stbuf, stbuf, sizeof(struct stat));
    // Truncate filesize.
    if (!unlink)
        update_filesize_cache(stbuf, size);
    if (config->background_delete == 0) {
        file_truncate_worker((void *) trunc_data);
    } else {
        write_trunc_todolist(trunc_data);
        if (0 !=
            pthread_create(&truncate_thread, NULL, file_truncate_worker,
                           (void *) trunc_data))
            die_syserr();
        if (0 != pthread_detach(truncate_thread))
            die_syserr();
    }
    return (0);
}

void file_partial_truncate_block(unsigned long long inode,
                                 unsigned long long blocknr,
                                 unsigned int offset)
{
    unsigned char *blockdata;
    DAT *uncompdata;
    INOBNO inobno;
    DAT *data;
    unsigned char *stiger;
    INUSE *inuse;

    FUNC;
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
    blockdata = s_zmalloc(BLKSIZE);
    data = file_tgr_read_data(stiger);
    if (NULL == data) {
        die_dataerr("Hmmm, did not expect this to happen.");
    }
    LDEBUG("file_partial_truncate_block : clz_decompress");
    uncompdata = lfsdecompress(data);
    if ( NULL == uncompdata ) die_dataerr("file_partial_truncate_block : inode %llu - %llu failed to decompress block", inobno.inode,inobno.blocknr); 
    if (uncompdata->size >= offset) {
        memcpy(blockdata, uncompdata->data, offset);
    } else {
        memcpy(blockdata, uncompdata->data, uncompdata->size);
    }
    DATfree(uncompdata);
    file_commit_block(blockdata, inobno, offset);
    DATfree(data);
    s_free(blockdata);
    create_hash_note(stiger);
    inuse = file_get_inuse(stiger);
    if (NULL == inuse)
        die_dataerr
            ("file_partial_truncate_block : unexpected block not found");
    if (inuse->inuse == 1) {
        loghash("file_partial_truncate_block : delete hash", stiger);
        if (config->blockdata_io_type == CHUNK_IO ) {
          chunk_delete(stiger,inuse->offset);
        } else {
          put_on_freelist(inuse, inobno.inode);
        }
        delete_inuse(stiger);
    } else {
        if (inuse->inuse > 1)
            inuse->inuse--;
        file_update_inuse(stiger, inuse);
    }
    delete_hash_note(stiger);
    s_free(inuse);
    s_free(stiger);
    return;
}

int file_unlink_file(const char *path)
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
    res = dbstat(dname, &dirst, 0);
    if (S_ISLNK(st.st_mode) && haslinks == 1) {
        LDEBUG("unlink symlink %s inode %llu", path, inode);
        delete_key(DBS, &inode, sizeof(unsigned long long),
                   (char *) __PRETTY_FUNCTION__);
        LDEBUG("unlink symlink done %s", path);
    }
    inobno.inode = inode;
    inobno.blocknr = st.st_size / BLKSIZE;
    if (inobno.blocknr * BLKSIZE < st.st_size)
        inobno.blocknr++;

// Start deleting the actual data blocks.
    (void) file_fs_truncate(&st, 0, bname, 1);
// Remove this inode from the path_to_inode cache
    LDEBUG("file_unlink_file %s haslinks =%u", path, haslinks);
    if (haslinks == 1) {
        if (0 !=
            (res =
             btdelete_curkey(DBDIRENT, &dirst.st_ino,
                             sizeof(unsigned long long), &inode,
                             sizeof(unsigned long long),
                             (char *) __PRETTY_FUNCTION__))) {
            s_free(bname);
            s_free(dname);
            LDEBUG("file_unlink_file : %lu : %llu", dirst.st_ino, inode);
            return (res);
        }
        delete_key(DBP, (unsigned char *) &inode,
                   sizeof(unsigned long long),
                   (char *) __PRETTY_FUNCTION__);
        LDEBUG("file_unlink_file : unlinked DBDIRENT %lu : %llu",
               dirst.st_ino, inode);
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
            LDEBUG
                ("unlink_file : Restore %s to regular file settings and clean up.",
                 ddstat->filename);
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
