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
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define  _GNU_SOURCE

#ifdef BERKELEYDB

#ifndef LFATAL
#include "lib_log.h"
#endif
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <db.h>
#include <errno.h>
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
#include <tcutil.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <mhash.h>
#include <mutils/mhash.h>

#include "lib_safe.h"
#include "lib_cfg.h"
#include "retcodes.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#include "lib_qlz.h"
#include "lib_common.h"
#include "lib_crypto.h"
#include "file_io.h"
#include "lib_repl.h"
#include "lib_bdb.h"

DB *dbb;
DB *dbu;
DB *dbp;
DB *dbl;
DB *dbs;
DB *dbdta;
DB *dbdirent;
DB *freelist;

DB_ENV *envp;
const char **data_name;

extern TCTREE *workqtree;       // Used to buffer incoming data (writes) 
extern TCTREE *readcachetree;   // Used to cache chunks of data that are likely to be read
extern TCTREE *metatree;        // Used to cache file metadata (e.g. inode -> struct stat).
extern TCTREE *hashtree;        // Used to lock hashes
extern TCTREE *inodetree;       // Used to lock inodes
extern TCTREE *path2inotree;    // Used to cache path to inode translations

extern struct configdata *config;
extern unsigned long long nextoffset;
int fdbdta;
int frepl = 0;
int freplbl = 0;
extern int BLKSIZE;

DB_TXN *txn;

static pthread_mutex_t bdb_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *bdb_lockedby;

void bdb_lock(const char *msg)
{
    FUNC;
    LDEBUG("bdb_lock : %s", msg);
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + GLOBAL_LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;

    err_code = pthread_mutex_timedlock(&bdb_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&bdb_mutex);
#endif
    bdb_lockedby = msg;
    LDEBUG("bdb_lockedby : %s", bdb_lockedby);
    EFUNC;
    return;
}

int try_bdb_lock()
{
    int res;
    res = pthread_mutex_trylock(&bdb_mutex);
    return (res);
}

void release_bdb_lock()
{
    FUNC;
    pthread_mutex_unlock(&bdb_mutex);
    EFUNC;
    return;
}



//u_int32_t db_flags, env_flags;

void bdb_sync()
{
    freelist->sync(freelist, 0);
    dbu->sync(dbu, 0);
    dbb->sync(dbb, 0);
    dbp->sync(dbp, 0);
    dbs->sync(dbs, 0);
    dbdirent->sync(dbdirent, 0);
    dbl->sync(dbl, 0);
}

void bdb_checkpoint()
{
    FUNC;
    if (0 != envp->txn_checkpoint(envp, 0, 0, 0))
        die_dberr("Failed to checkpoint database");
    LDEBUG("bdb_checkpoint returns");
}

void bdb_stat()
{
    envp->lock_stat_print(envp,DB_STAT_ALL);
    envp->memp_stat_print(envp,DB_STAT_ALL);
}

void bdb_close()
{
    int ret, ret_c;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    if (config->transactions) {
        commit_transactions();
    }
    bdb_checkpoint();
    /* Close the database */
    ret_c = freelist->close(freelist, 0);
    if (ret_c != 0) {
        LFATAL("freelist database close failed.");
        ret = ret_c;
    }
    ret_c = dbp->close(dbp, 0);
    if (ret_c != 0) {
        LFATAL("metadata database close failed.");
        ret = ret_c;
    }
    ret_c = dbu->close(dbu, 0);
    if (ret_c != 0) {
        LFATAL("blockusage database close failed.");
        ret = ret_c;
    }
    ret_c = dbs->close(dbs, 0);
    if (ret_c != 0) {
        LFATAL("symlink database close failed.");
        ret = ret_c;
    }
    ret_c = dbdirent->close(dbdirent, 0);
    if (ret_c != 0) {
        LFATAL("dirent database close failed.");
        ret = ret_c;
    }
    ret_c = dbl->close(dbl, 0);
    if (ret_c != 0) {
        LFATAL("hardlink database close failed.");
        ret = ret_c;
    }
    ret_c = dbb->close(dbb, 0);
    if (ret_c != 0) {
        LFATAL("fileblock database close failed.");
        ret = ret_c;
    }
    /* Close the environment */
    if (envp) {
        ret_c = envp->close(envp, 0);
        if (ret_c != 0) {
            LFATAL("environment close failed: %s\n", db_strerror(ret_c));
            ret = ret_c;
        }
    }
    close_trees();
    if (NULL == config->blockdatabs) {
        fsync(fdbdta);
        close(fdbdta);
        free(config->nexthash);
    }
    flock(frepl, LOCK_UN);
    fsync(frepl);
    close(frepl);
    if (config->replication && config->replication_role == 0)
        close(freplbl);
    s_free(config->replication_logfile);
    release_bdb_lock();
    envp = NULL;
    dbp = NULL;
}

void drop_databases()
{
    envp = NULL;
    int ret;

    LINFO("Drop_databases called on %s", config->meta);
    /* Open the environment */
    ret = db_env_create(&envp, 0);
    if (ret != 0) {
        die_dberr("Error creating environment handle: %s\n",
                  db_strerror(ret));
    }
    ret = envp->remove(envp, config->meta, 0);
    if (ret != 0) {
        die_dberr("Failed to drop databases: %s\n", db_strerror(ret));
    }
}

unsigned int which_dbb_huge(DB *db, DBT *key)
{
   INOBNO *inobno;
   unsigned long long nr;
   unsigned int ret=0;

   if ( key->size != sizeof(INOBNO) ) {
      return(ret);
   }
   inobno=(INOBNO *)key->data;
   nr=inobno->inode/1000;
   nr*=1000;
   nr/=2;
   ret=inobno->inode-nr;
   return(ret);
}

unsigned int which_dbb_medium(DB *db, DBT *key)
{
   INOBNO *inobno;
   unsigned long long nr;
   unsigned int ret=0;

   if ( key->size != sizeof(INOBNO) ) {
      return(ret);
   }
   inobno=(INOBNO *)key->data;
   nr=inobno->inode/100;
   nr*=100;
   ret=inobno->inode-nr;
   return(ret);
}


unsigned int which_dbu_medium(DB *db, DBT *key)
{
   unsigned int ret=0;
   unsigned char *thash;

   thash=key->data; 
   ret=thash[0]/16;
   return(ret);
}

unsigned int which_dbu_huge(DB *db, DBT *key)
{
   unsigned int retl=0;
   unsigned int reth=0;
   unsigned char *thash;

   thash=key->data;
   retl=thash[0]/16;
   reth=thash[1]/16;
   reth*=16;
   retl+=reth;
   return(retl);
}


void bdb_open()
{
    int ret;
    struct stat stbuf;
    char *hashstr;
    DAT *data;
    char *sp, *dbname;
    FILE *fp;

    dbp = NULL;
    envp = NULL;
    u_int32_t db_flags, env_flags;

    if ( config->blockdata_io_type == FILE_IO ) {
       sp = s_dirname(config->blockdata);
    } else sp=s_strdup(config->blockdata);
    dbname = as_sprintf(__FILE__, __LINE__, "%s/replog.dta", sp);
    config->replication_logfile = s_strdup(dbname);
    if (-1 == (frepl = s_open2(dbname, O_CREAT | O_RDWR | O_NOATIME, S_IRWXU)))
        die_syserr();
    if (0 != flock(frepl, LOCK_EX | LOCK_NB)) {
        LFATAL
            ("Failed to lock the replication logfile %s\nlessfs must be unmounted before using this option!",
             config->replication_logfile);
        exit(EXIT_USAGE);
    }
    if (config->replication && config->replication_role == 0) {
        if (-1 == (freplbl = s_open2(dbname, O_RDWR, S_IRWXU)))
            die_syserr();
    }
    s_free(dbname);
    s_free(sp);



    LDEBUG("Open the environment : %s", config->meta);
    /* Open the environment */
    ret = db_env_create(&envp, 0);
    if (ret != 0) {
        die_dberr("Error creating environment handle: %s\n",
                  db_strerror(ret));
    }
    if (config->tuneforspeed)
        envp->set_flags(envp, DB_TXN_WRITE_NOSYNC, 1);
    env_flags = DB_CREATE |     /* Create the environment if it does
                                 * not already exist. */
        DB_INIT_TXN |           /* Initialize transactions */
        DB_INIT_LOCK |          /* Initialize locking. */
        DB_INIT_LOG |           /* Initialize logging */
        DB_THREAD |             /* Enable threading */
        DB_LOG_AUTO_REMOVE |    /* Remove logs files when no longer needed */
        DB_RECOVER |            /* Run normal recovery */
        DB_INIT_MPOOL;          /* Initialize the in-memory cache. */

    if ( config->bdb_private ) env_flags|=DB_PRIVATE;
    /*
     * Configure maximum transactions.
     * We only need one.
     */
    ret = envp->set_tx_max(envp, 1);
    if (ret != 0) {
        die_dberr("Error setting transactions: %s\n", db_strerror(ret));
    }
#ifdef MEMTRACE
    envp->set_alloc(envp, *(bdb_malloc), *(bdb_realloc), (bdb_free));
#endif
    fp = fopen(BERKELEYDB_ERRORLOG, "w");
    if (NULL == fp)
        die_syserr();
    envp->set_errfile(envp, fp);
    envp->set_msgfile(envp, fp);
    ret = envp->open(envp, config->meta, env_flags, 0);
    if (ret != 0) {
        die_dberr("Error opening environment: %s\n", db_strerror(ret));
    }


    LDEBUG("Initialize the DB handles.");
    /* Initialize the DB handles */
    ret = db_create(&dbb, envp, 0);
    if (ret != 0)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    ret = db_create(&dbu, envp, 0);
    if (ret != 0)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    ret = db_create(&dbp, envp, 0);
    if (ret != 0)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    ret = db_create(&dbl, envp, 0);
    if (ret != 0)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    ret = db_create(&dbs, envp, 0);
    if (ret != 0)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    ret = db_create(&dbdirent, envp, 0);
    if (ret != 0)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    ret = db_create(&freelist, envp, 0);
    if (ret != 0)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));

    LDEBUG("Open databases.");

    if (config->transactions) {
        db_flags = DB_CREATE | DB_AUTO_COMMIT | DB_READ_UNCOMMITTED;
        /* Enable uncommitted reads */ ;
    } else
        db_flags = DB_CREATE;
    /*
     * Open the database. Note that we are using auto commit for the open,
     * so the database is able to support transactions.
     */
    ret = dbp->open(dbp,        /* Pointer to the database */
                    NULL,       /* Txn pointer */
                    "metadata.db",      /* File name */
                    NULL,       /* Logical db name */
                    DB_BTREE,   /* Database type (using btree) */
                    db_flags,   /* Open flags */
                    0);         /* File mode. Using defaults */
    if (0 != ret)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    LDEBUG("dbp is now open");
    if ( config->tune_for_size == LFSMEDIUM ) {
       dbu->set_partition(dbu,16,NULL,which_dbu_medium);
    }  else if ( config->tune_for_size == LFSHUGE ) {
       dbu->set_partition(dbu,256,NULL,which_dbu_huge);
    }
//    db_flags = DB_CREATE | DB_AUTO_COMMIT ;    /* Enable uncommitted reads */;
    ret = dbu->open(dbu,        /* Pointer to the database */
                    NULL,       /* Txn pointer */
                    "blockusage.db",    /* File name */
                    NULL,       /* Logical db name */
                    DB_HASH,    /* Database type (using btree) */
                    db_flags,   /* Open flags */
                    0);         /* File mode. Using defaults */
    if (0 != ret)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    LDEBUG("dbu is now open");
    if ( config->tune_for_size == LFSMEDIUM ) {
       dbb->set_partition(dbb,100,NULL,which_dbb_medium);
    } else if ( config->tune_for_size == LFSHUGE ) {
       dbb->set_partition(dbb,500,NULL,which_dbb_huge);
    }
//    db_flags = DB_CREATE | AUTO_COMMIT ;    /* Enable uncommitted reads */;
    ret = dbb->open(dbb,        /* Pointer to the database */
                    NULL,       /* Txn pointer */
                    "fileblock.db",     /* File name */
                    NULL,       /* Logical db name */
                    DB_HASH,    /* Database type (using btree) */
                    db_flags,   /* Open flags */
                    0);         /* File mode. Using defaults */
    if (0 != ret)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    LDEBUG("dbb is now open");
    ret = dbl->set_flags(dbl, DB_DUP);
    ret = dbl->open(dbl,        /* Pointer to the database */
                    NULL,       /* Txn pointer */
                    "hardlink.db",      /* File name */
                    NULL,       /* Logical db name */
                    DB_BTREE,   /* Database type (using btree) */
                    db_flags,   /* Open flags */
                    0);         /* File mode. Using defaults */
    if (0 != ret)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    LDEBUG("hardlink database is now open");
    ret = dbs->open(dbs,        /* Pointer to the database */
                    NULL,       /* Txn pointer */
                    "symlink.db",       /* File name */
                    NULL,       /* Logical db name */
                    DB_BTREE,   /* Database type (using btree) */
                    db_flags,   /* Open flags */
                    0);         /* File mode. Using defaults */
    if (0 != ret)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    LDEBUG("symlink database is now open");
    ret = dbdirent->set_flags(dbdirent, DB_DUPSORT);
    if (0 != ret)
        die_dberr("Setting DB_DUPSORT for dbdirent failed : %s",
                  db_strerror(ret));
    ret = dbdirent->open(dbdirent,      /* Pointer to the database */
                         NULL,  /* Txn pointer */
                         "dirent.db",   /* File name */
                         NULL,  /* Logical db name */
                         DB_BTREE,      /* Database type (using btree) */
                         db_flags,      /* Open flags */
                         0);    /* File mode. Using defaults */
    if (0 != ret)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    LDEBUG("dirent database is now open");
    ret = freelist->set_flags(freelist, DB_DUPSORT);
    if (0 != ret)
        die_dberr("Setting DB_DUPSORT for freelist failed : %s",
                  db_strerror(ret));
    ret = freelist->open(freelist,      /* Pointer to the database */
                         NULL,  /* Txn pointer */
                         "freelist.db", /* File name */
                         NULL,  /* Logical db name */
                         DB_BTREE,      /* Database type (using btree) */
                         db_flags,      /* Open flags */
                         0);    /* File mode. Using defaults */
    if (0 != ret)
        die_dberr("Open DB handle failed: %s", db_strerror(ret));
    LDEBUG("freelist database is now open");
    LINFO("Databases are now open.");
    start_transactions();
    open_trees();
    if (config->blockdata_io_type == FILE_IO ) {
        if (-1 ==
            (fdbdta =
             s_open2(config->blockdata, O_CREAT | O_RDWR| O_NOATIME, S_IRWXU)))
            die_syserr();
        if (-1 == (stat(config->blockdata, &stbuf)))
            die_syserr();
        hashstr = as_sprintf(__FILE__, __LINE__, "NEXTOFFSET");
        config->nexthash =
            (char *) thash((unsigned char *) hashstr, strlen(hashstr));
        s_free(hashstr);
        data = search_dbdata(DBU, config->nexthash, config->hashlen, LOCK);
        if (NULL == data) {
            //LFATAL("Filesystem upgraded to support transactions");
            nextoffset = stbuf.st_size;
        } else {
            memcpy(&nextoffset, data->data, sizeof(unsigned long long));
            DATfree(data);
        }
        if (config->transactions) {
            check_datafile_sanity();
        }
    }
    return;
}

void start_transactions()
{
    /* Get the txn handle */
    int ret;
    unsigned int options;
    FUNC;
    txn = NULL;
    if (config->tuneforspeed) {
        options = DB_TXN_NOSYNC | DB_TXN_NOWAIT | DB_READ_COMMITTED;
    } else
        options = DB_TXN_SYNC | DB_READ_COMMITTED;
    if (0 != (ret = envp->txn_begin(envp, NULL, &txn, options))) {
        //In a hurry?
        //if ( 0 != (ret=envp->txn_begin(envp,NULL, &txn, DB_TXN_NOSYNC | DB_TXN_NOWAIT ))){
        die_dberr("Transaction begin failed : %s", db_strerror(ret));
    }
    bdb_checkpoint();
    EFUNC;
}

void commit_transactions()
{
    /*
     * Commit the transaction. Note that the transaction handle
     * can no longer be used.
     */
    int ret;

    FUNC;
    if (0 != (ret = txn->commit(txn, 0))) {
        config->transactions = 0;
        die_dberr("Transaction commit failed : %s", db_strerror(ret));
    }
    EFUNC;
    return;
}

void abort_transactions()
{
    /*
     * Abort the transaction. Note that the transaction handle
     * can no longer be used.
     */
    int ret;

    FUNC;
    if (0 != (ret = txn->abort(txn))) {
        config->transactions = 0;
        die_dberr("Transaction abort failed : %s", db_strerror(ret));
    }
    start_transactions();
    EFUNC;
    return;
}

// Delete key from database, if caller (msg) is defined this operation
// must be successfull, otherwise we log and exit.
void delete_key(int database, void *keydata, int len, const char *msg)
{
    DB *db;
    int res;
    DBT key;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    db = bdb_set_db(database);
    key.data = keydata;
    key.size = len;
    key.flags = DB_DBT_USERMEM;
    if (0 != (res = db->del(db, txn, &key, 0))) {
        if (msg) {
           die_dataerr("Delete of key failed :%s", db_strerror(res));
        } else LINFO("Delete of key failed :%s", db_strerror(res));
    }
    release_bdb_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLDELETE, keydata, len, NULL, 0,
                        MAX_ALLOWED_THREADS - 2);
    }
}

void bin_write_dbdata(int database, void *keydata, int keylen,
                      void *valdata, int datalen)
{
    DB *db;
    int res;
    DBT key;
    DBT data;

    FUNC;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);

    key.data = keydata;
    key.size = keylen;
    key.flags = DB_DBT_USERMEM;
    data.data = valdata;
    data.size = datalen;
    data.flags = DB_DBT_USERMEM;

    db = bdb_set_db(database);
    if (0 != (res = db->put(db, txn, &key, &data, 0))) {
        txn->abort(txn);
        LFATAL("bin_write_dbdata : database %u keylen %u datalen %u",
               database, keylen, datalen);
        die_dberr("Database write failed : %s", db_strerror(res));
    }
    release_bdb_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE, keydata, keylen, valdata,
                        datalen, MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

void bin_write(int database, void *keydata, int keylen,
               void *valdata, int datalen)
{
    DB *db;
    int res;
    DBT key;
    DBT data;

    FUNC;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);

    key.data = keydata;
    key.size = keylen;
    key.flags = DB_DBT_USERMEM;
    data.data = valdata;
    data.size = datalen;
    data.flags = DB_DBT_USERMEM;

    db = bdb_set_db(database);
    if (0 != (res = db->put(db, txn, &key, &data, 0))) {
        txn->abort(txn);
        die_dberr("Database write failed : %s", db_strerror(res));
    }
    release_bdb_lock();
}

void end_flush_commit()
{
    FUNC;
    if (config->transactions) {
        bdb_lock((char *) __PRETTY_FUNCTION__);
        commit_transactions();
        start_transactions();
        release_bdb_lock();
    }
    EFUNC;
    return;
}

void btbin_write_dbdata(int database, void *keydata, int keylen,
                        void *valdata, int vallen)
{

    FUNC;
    DB *db;
    DBC *cur;
    DBT key;
    DBT data;
    int res;

    FUNC;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);

    key.data = keydata;
    key.size = keylen;
    key.flags = DB_DBT_USERMEM;
    data.data = valdata;
    data.size = vallen;
    data.flags = DB_DBT_USERMEM;

    db = bdb_set_db(database);
    /* Get the cursor */
    db->cursor(db, txn, &cur, 0);

    res = cur->put(cur, &key, &data, DB_KEYFIRST | DB_READ_UNCOMMITTED);
    if (0 != res)
        die_dberr("btbin_write_dbdata : write failed %s",
                  db_strerror(res));
    if (cur)
        cur->close(cur);
    release_bdb_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE, keydata, keylen, valdata,
                        vallen, MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

void start_flush_commit()
{
    unsigned long long lastoffset = 0;
    FUNC;
    if (config->transactions)
        lessfs_trans_stamp();
    if (NULL == config->blockdatabs) {
        if (lastoffset != nextoffset) {
            LDEBUG("write nextoffset=%llu", nextoffset);
            bin_write_dbdata(DBU, config->nexthash, config->hashlen,
                             (unsigned char *) &nextoffset,
                             sizeof(unsigned long long));
            lastoffset = nextoffset;
        }
    }
    sync_all_filesizes();
    if ( config->blockdata_io_type == FILE_IO ) {
        fsync(fdbdta);
    }
    if (config->replication && config->replication_role == 0) {
        fsync(frepl);
    }
    EFUNC;
    return;
}

void btbin_write_dup(int database, void *keydata, int keylen,
                     void *valdata, int vallen, bool lock)
{
    DB *db;
    DBC *cur;
    DBT key;
    DBT data;
    int res;
    unsigned long long *inode;

    FUNC;
    if (lock)
        bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);

    inode = (unsigned long long *) keydata;
    key.data = keydata;
    key.size = keylen;
    key.flags = DB_DBT_USERMEM;
    data.data = valdata;
    data.size = vallen;
    data.flags = DB_DBT_USERMEM;

    db = bdb_set_db(database);
    /* Get the cursor */
    db->cursor(db, txn, &cur, 0);
    res = cur->put(cur, &key, &data, DB_KEYLAST);
    if (0 != res)
        die_dberr("btbin_write_dup : write failed %s %llu database %u",
                  db_strerror(res), *inode, database);
    if (cur)
        cur->close(cur);
    if (lock)
        release_bdb_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLDUPWRITE, keydata, keylen, valdata,
                        vallen, MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

int bt_entry_exists(int database, void *parent, int parentlen, void *value,
                    int vallen)
{
    int res = 0;
    int found = 0;
    DB *db;
    DBC *cur;
    DBT key;
    DBT data;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);
    key.data = s_zmalloc(parentlen);
    memcpy(key.data, parent, parentlen);
    key.flags = DB_DBT_REALLOC;;
    key.size = parentlen;
    data.flags = DB_DBT_REALLOC;
    data.data = s_zmalloc(vallen);
    memcpy(data.data, value, vallen);
    data.size = vallen;

    db = bdb_set_db(database);
    db->cursor(db, txn, &cur, 0);
    res = cur->get(cur, &key, &data, DB_GET_BOTH | DB_READ_UNCOMMITTED);
    if (cur)
        cur->close(cur);
    s_free(data.data);
    s_free(key.data);
    release_bdb_lock();
    if (0 == res)
        found = 1;
    LDEBUG("bt_entry_exists return %u", found);
    EFUNC;
    return (found);
}

INUSE *get_offset_reclaim(unsigned long long mbytes,
                          unsigned long long offset)
{
    DBT key;
    DBT data;
    DBC *cur;
    FREEBLOCK *freeblock = NULL;
    time_t thetime;
    bool hasone = 0;
    unsigned long long asize;
    INUSE *inuse = NULL;

    FUNC;

    thetime = time(NULL);
    bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);

    key.data = s_zmalloc(sizeof(mbytes));
    memcpy(key.data, &mbytes, sizeof(mbytes));
    key.size = sizeof(mbytes);
    key.flags = DB_DBT_REALLOC;
    data.data = s_zmalloc(1);
    data.size = 1;
    data.flags = DB_DBT_REALLOC;

    LDEBUG("get_offset_reclaim : search for %llu blocks on the freelist",
           mbytes);
    freelist->cursor(freelist, txn, &cur, 0);
    while (1) {
        while (0 ==
               cur->get(cur, &key, &data, DB_NEXT | DB_READ_UNCOMMITTED)) {
            if (key.size == strlen("TRNCTD"))
                continue;
            memcpy(&asize, key.data, sizeof(unsigned long long));
            if (asize < mbytes)
                continue;
            if (data.size < sizeof(FREEBLOCK)) {
                memcpy(&offset, data.data, data.size);
            } else {
                freeblock = s_malloc(sizeof(FREEBLOCK));
                memcpy(freeblock, data.data, data.size);
                if (freeblock->reuseafter > thetime && hasone == 0) {
                    s_free(freeblock);
                    hasone = 1;
                    continue;
                }
                if (freeblock->reuseafter > thetime && hasone == 1)
                    LINFO
                        ("get_offset_reclaim : early space reclaim, low on space");
                offset = freeblock->offset;
            }
            inuse = s_zmalloc(sizeof(INUSE));
            inuse->allocated_size = asize * 512;
            LDEBUG("asize=%llu, inuse->allocated_size %llu", asize,
                   inuse->allocated_size);
            inuse->offset = offset;
            cur->del(cur, 0);
            break;
        }
        if (inuse)
            break;
        if (!hasone)
            break;
        if (cur)
            cur->close(cur);
        freelist->cursor(freelist, txn, &cur, 0);
    }
    if (cur)
        cur->close(cur);
    s_free(data.data);
    s_free(key.data);
    release_bdb_lock();
    if (inuse) {
        if (config->replication == 1 && config->replication_role == 0) {
            if (freeblock) {
                write_repl_data(FREELIST, REPLDELETECURKEY,
                                (char *) &asize,
                                sizeof(unsigned long long),
                                (char *) freeblock, sizeof(FREEBLOCK),
                                MAX_ALLOWED_THREADS - 2);
            } else {
                write_repl_data(FREELIST, REPLDELETECURKEY,
                                (char *) &asize,
                                sizeof(unsigned long long),
                                (char *) &offset,
                                sizeof(unsigned long long),
                                MAX_ALLOWED_THREADS - 2);
            }
        }
    }
    return inuse;
}

unsigned long long get_offset_fast(unsigned long long mbytes)
{
    DBT key;
    DBT data;
    DBC *cur;
    int res;
    bool found = 0;
    FREEBLOCK *freeblock = NULL;
    time_t thetime;
    unsigned long long offset;
    unsigned long long fsize;

    FUNC;

    thetime = time(NULL);
    bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);

    key.data = s_zmalloc(sizeof(mbytes));
    memcpy(key.data, &mbytes, sizeof(mbytes));
    key.size = sizeof(mbytes);
    key.flags = DB_DBT_REALLOC;
    data.data = s_zmalloc(1);
    data.size = 1;
    data.flags = DB_DBT_REALLOC;

    LDEBUG("get_offset : search for %llu blocks on the freelist", mbytes);
    freelist->cursor(freelist, txn, &cur, 0);
    res = cur->get(cur, &key, &data, DB_SET | DB_READ_UNCOMMITTED);
    if (res == 0) {
        do {
            if (data.size < sizeof(FREEBLOCK)) {
                memcpy(&offset, data.data, data.size);
            } else {
                freeblock = s_malloc(sizeof(FREEBLOCK));
                memcpy(freeblock, data.data, data.size);
                if (freeblock->reuseafter > thetime) {
                    s_free(freeblock);
                    continue;
                }
                offset = freeblock->offset;
            }
            memcpy(&fsize, key.data, key.size);
            cur->del(cur, 0);
            found = 1;
            break;
        } while (0 ==
                 cur->get(cur, &key, &data,
                          DB_NEXT_DUP | DB_READ_UNCOMMITTED));
    }
    if (cur)
        cur->close(cur);
    s_free(data.data);
    s_free(key.data);
    release_bdb_lock();
    if (found) {
        if (config->replication == 1 && config->replication_role == 0) {
            if (freeblock) {
                write_repl_data(FREELIST, REPLDELETECURKEY,
                                (char *) &mbytes,
                                sizeof(unsigned long long),
                                (char *) freeblock, sizeof(FREEBLOCK),
                                MAX_ALLOWED_THREADS - 2);
            } else {
                write_repl_data(FREELIST, REPLDELETECURKEY,
                                (char *) &mbytes,
                                sizeof(unsigned long long),
                                (char *) &offset,
                                sizeof(unsigned long long),
                                MAX_ALLOWED_THREADS - 2);
            }
        }
    } else
        offset = (0 - 1);
    LDEBUG("get_offset returns = %llu", offset);
    EFUNC;
    return (offset);
}

DB *bdb_set_db(int database)
{
    DB *db;
    switch (database) {
    case DBU:
        db = dbu;
        break;
    case DBB:
        db = dbb;
        break;
    case DBP:
        db = dbp;
        break;
    case DBS:
        db = dbs;
        break;
    case DBL:
        db = dbl;
        break;
    case DBDIRENT:
        db = dbdirent;
        break;
    case FREELIST:
        db = freelist;
        break;
    default:
        die_syserr();
    }
    return db;
}

// lock is not used with tc
DAT *search_dbdata(int database, void *keydata, int len, bool lock)
{
    DB *db;
    DAT *lfsdata;
    DBT key;
    DBT data;
    int res;

    FUNC;
    if (lock)
        bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);

    db = bdb_set_db(database);
    key.data = s_zmalloc(len);
    memcpy(key.data, keydata, len);
    key.size = len;
    key.flags = DB_DBT_USERMEM;
    data.flags = DB_DBT_REALLOC;
    data.data = NULL;

    res = db->get(db, txn, &key, &data, DB_READ_UNCOMMITTED);
    s_free(key.data);
    if (res != 0) {
        lfsdata = NULL;
        if (lock)
            release_bdb_lock();
        return lfsdata;
    }
    lfsdata = s_zmalloc(sizeof(DAT));
    lfsdata->data = s_zmalloc(data.size);
    lfsdata->size = data.size;
    memcpy(lfsdata->data, data.data, data.size);
    s_free(data.data);
    LDEBUG("search_dbdata : found data");
    if (lock)
        release_bdb_lock();
    return lfsdata;
}

/* Search in directory with inode dinode or key dinode for name bname 
   Return the inode of file bname                       */
DDSTAT *dnode_bname_to_inode(void *dinode, int dlen, char *bname)
{
    DBC *cur;
    DAT *statdata;
    DDSTAT *filestat = NULL, *lfilestat;
    DAT *fname;
    DINOINO dinoino;
    unsigned long long *valnode;
    unsigned long long *inode;
    DBT key;
    DBT data;
    int res;
#ifdef ENABLE_CRYPTO
    DAT *decrypted;
#endif

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    /* Get a cursor */
    dbdirent->cursor(dbdirent, txn, &cur, DB_READ_UNCOMMITTED);
    DBTzero(&key);
    DBTzero(&data);

    inode = s_zmalloc(sizeof(unsigned long long));
    data.data = s_zmalloc(1);
    memcpy(inode, dinode, dlen);
    key.data = inode;
    key.size = sizeof(unsigned long long);
    key.flags = DB_DBT_REALLOC;
    data.flags = DB_DBT_REALLOC;

    //LFATAL("dnode_bname_to_inode : search dinode %llu", *inode); 
    res = cur->get(cur, &key, &data, DB_SET | DB_READ_UNCOMMITTED);
    if (res != 0) {
        LDEBUG("dnode_bname_to_inode: not found %s", db_strerror(res));
        goto end;
    }
    //LFATAL("dnode_bname_to_inode : found dinode %llu size = %i, len = %i", *inode,key.size,dlen); 
    /* traverse records */
    do {
        valnode = (unsigned long long *) data.data;
        if (*inode == *valnode && *inode != 1)
            continue;
        LDEBUG("Search for inode %llu", *valnode);
        statdata =
            search_dbdata(DBP, valnode, sizeof(unsigned long long),
                          NOLOCK);
        if (NULL == statdata) {
            LINFO("Unable to find file existing in dbp.\n");
            break;
        }
#ifdef ENABLE_CRYPTO
        if (config->encryptmeta && config->encryptdata) {
            decrypted = lfsdecrypt(statdata);
            DATfree(statdata);
            statdata = decrypted;
        }
#endif
        filestat = (DDSTAT *) statdata->data;
        if (0 != filestat->filename[0]) {
            s_free(statdata);
            LDEBUG("compare bname %s with filestat->filename %s",
                   bname, filestat->filename);
            if (0 == strcmp(bname, filestat->filename))
                break;
        } else {
            memcpy(&dinoino.dirnode, dinode, sizeof(unsigned long long));
            dinoino.inode = filestat->stbuf.st_ino;
            fname =
                btsearch_keyval(DBL, &dinoino, sizeof(DINOINO), bname,
                                strlen(bname), NOLOCK);
            if (fname) {
                lfilestat = s_zmalloc(sizeof(DDSTAT));
                memcpy(lfilestat, statdata->data, statdata->size);
                s_free(statdata);
                ddstatfree(filestat);
                memcpy(&lfilestat->filename, fname->data, fname->size);
                lfilestat->filename[fname->size] = 0;
                LDEBUG("dnode_bname_to_inode : filename %c",
                       lfilestat->filename[0]);
                filestat = lfilestat;
                DATfree(fname);
                break;
            } else
                s_free(statdata);
        }
        ddstatfree(filestat);
        filestat = NULL;
    } while (0 ==
             cur->get(cur, &key, &data,
                      DB_NEXT_DUP | DB_READ_UNCOMMITTED));
  end:
    s_free(data.data);
    s_free(key.data);
    if (cur)
        cur->close(cur);
    release_bdb_lock();
    if (filestat) {
        LDEBUG("dnode_bname_to_inode : filestat->filename=%s inode %llu",
               filestat->filename,
               (unsigned long long) filestat->stbuf.st_ino);
    } else {
        LDEBUG("dnode_bname_to_inode : return NULL");
    }
    return filestat;
}

unsigned long long has_nodes(unsigned long long inode)
{
    unsigned long long res = 0;
    unsigned long long *filenode;
    DAT *filedata;
    bool dotdir = 0;
    DDSTAT *ddstat;
    DBC *cur = NULL;
    DBT key;
    DBT data;
    unsigned int ret;

    FUNC;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbdirent->cursor(dbdirent, txn, &cur, DB_READ_UNCOMMITTED);

    DBTzero(&key);
    DBTzero(&data);
    key.data = s_zmalloc(sizeof(inode));
    memcpy(key.data, &inode, sizeof(inode));
    key.flags = DB_DBT_REALLOC;
    key.size = sizeof(inode);
    data.flags = DB_DBT_REALLOC;

    ret = cur->get(cur, &key, &data, DB_SET | DB_READ_UNCOMMITTED);
    if (ret != 0)
        goto end;
    do {
        filenode = (unsigned long long *) data.data;
        filedata =
            search_dbdata(DBP, filenode, sizeof(unsigned long long),
                          NOLOCK);
        if (filedata) {
            ddstat = value_to_ddstat(filedata);
            DATfree(filedata);
            if (*filenode == inode)
                dotdir = 1;
            if (ddstat->filename) {
                if (0 == strcmp(ddstat->filename, "."))
                    dotdir = 1;
                if (0 == strcmp(ddstat->filename, ".."))
                    dotdir = 1;
            }
            if (!dotdir) {
                LDEBUG
                    ("has_nodes : Found file in directory %s filenode %llu inode %llu",
                     ddstat->filename, *filenode, inode);
                res++;
            }
            ddstatfree(ddstat);
            dotdir = 0;
        }
    } while (0 ==
             cur->get(cur, &key, &data,
                      DB_NEXT_DUP | DB_READ_UNCOMMITTED));
    s_free(key.data);
  end:
    s_free(data.data);
    if (cur)
        cur->close(cur);
    release_bdb_lock();
    LDEBUG("has_nodes inode %llu contains %llu files", inode, res);
    EFUNC;
    return (res);
}

int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
               off_t offset, struct fuse_file_info *fi)
{
    int retcode = 0;
    DBC *cur;
    int res;
    struct stat stbuf;
    DAT *filedata;
    DDSTAT *ddstat;
    DBT key;
    DBT data;

    FUNC;
    (void) offset;
    (void) fi;

    res = dbstat(path, &stbuf, 1);
    if (0 != res) {
        return -ENOENT;
    }
    if (0 == strcmp(path, "/.lessfs/locks")) {
        LDEBUG("fs_readdir : reading locks");
        locks_to_dir(buf, filler, fi);
    }
    bdb_lock((char *) __PRETTY_FUNCTION__);
    dbdirent->cursor(dbdirent, txn, &cur, DB_READ_UNCOMMITTED);

    DBTzero(&key);
    DBTzero(&data);
    key.data = s_zmalloc(sizeof(unsigned long long));
    memcpy(key.data, &stbuf.st_ino, sizeof(unsigned long long));
    key.flags = DB_DBT_REALLOC;;
    key.size = sizeof(unsigned long long);
    data.flags = DB_DBT_REALLOC;

    //LFATAL("Called fs_readdir with path %s inode %llu", (char *) path, stbuf.st_ino);
    res = cur->get(cur, &key, &data, DB_SET | DB_READ_UNCOMMITTED);
    if (res == 0) {
        do {
            if (0 != memcmp(data.data, key.data, key.size)) {
                filedata =
                    search_dbdata(DBP, data.data, data.size, NOLOCK);
                if (filedata) {
                    ddstat = value_to_ddstat(filedata);
                    DATfree(filedata);
                    if (ddstat->filename[0] == 0) {
                        fs_read_hardlink(stbuf, ddstat, buf, filler, fi);
                    } else {
                        fil_fuse_info(ddstat, buf, filler, fi);
                    }
                    ddstatfree(ddstat);
                }
            }
        } while (0 ==
                 cur->get(cur, &key, &data,
                          DB_NEXT_DUP | DB_READ_UNCOMMITTED));
    }
    if (data.data)
        s_free(data.data);
    s_free(key.data);
    if (cur)
        cur->close(cur);
    release_bdb_lock();
    LDEBUG("fs_readdir: return");
    return (retcode);
}

void fs_read_hardlink(struct stat stbuf, DDSTAT * ddstat, void *buf,
                      fuse_fill_dir_t filler, struct fuse_file_info *fi)
{
    DBC *cur;
    DINOINO dinoino;
    DBT key;
    DBT data;
    int res;

    FUNC;

    dinoino.dirnode = stbuf.st_ino;
    dinoino.inode = ddstat->stbuf.st_ino;
    dbl->cursor(dbl, txn, &cur, DB_READ_UNCOMMITTED);
    DBTzero(&key);
    DBTzero(&data);
    key.data = s_zmalloc(sizeof(DINOINO));
    memcpy(key.data, &dinoino, sizeof(DINOINO));
    key.flags = DB_DBT_REALLOC;
    key.size = sizeof(DINOINO);
    data.flags = DB_DBT_REALLOC;

    res = cur->get(cur, &key, &data, DB_SET | DB_READ_UNCOMMITTED);
    if (res == 0) {
        do {
            memcpy(&ddstat->filename, data.data, data.size);
            ddstat->filename[data.size] = 0;
            LDEBUG("fs_read_hardlink : fil_fuse_info %s size %i",
                   ddstat->filename, data.size);
            fil_fuse_info(ddstat, buf, filler, fi);
            ddstat->filename[0] = 0;
        } while (0 ==
                 cur->get(cur, &key, &data,
                          DB_NEXT_DUP | DB_READ_UNCOMMITTED));
    }
    if (cur)
        cur->close(cur);
    if (data.data)
        s_free(data.data);
    if (key.data)
        s_free(key.data);
    EFUNC;
    return;
}

int count_dirlinks(void *linkstr, int len)
{
    DBC *cur;
    DBT key;
    DBT data;
    int count = 0, res;

    FUNC;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);
    key.data = s_zmalloc(len);
    memcpy(key.data, linkstr, len);
    key.size = len;
    key.flags = DB_DBT_REALLOC;
    data.flags = DB_DBT_REALLOC;

    dbdirent->cursor(dbl, txn, &cur, 0);
    res = cur->get(cur, &key, &data, DB_SET | DB_READ_UNCOMMITTED);
    if (res != 0) {
        LFATAL("count_dirlinks : %s", db_strerror(res));
        goto end_exit;
    }
    do {
        count++;
        if (count > 1)
            break;
    } while (0 ==
             cur->get(cur, &key, &data,
                      DB_NEXT_DUP | DB_READ_UNCOMMITTED));
    if (data.data)
        s_free(data.data);
  end_exit:
    s_free(key.data);
    if (cur)
        cur->close(cur);
    release_bdb_lock();
    EFUNC;
    LDEBUG("count_dirlinks : return %i", count);
    return (count);
}

DAT *btsearch_keyval(int database, void *keydata, int keylen, void *val,
                     int vallen, bool lock)
{
    DB *db;
    DBC *cur;
    DBT key;
    DBT data;
    DAT *retdta = NULL;
    int res;

    FUNC;

    if (lock)
        bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);
    if (val) {
        data.data = s_zmalloc(vallen);
        memcpy(data.data, val, vallen);
        data.size = vallen;
        data.flags = DB_DBT_REALLOC;
    } else {
        data.data = s_zmalloc(1);
        data.size = 1;
        data.flags = DB_DBT_REALLOC;
    }
    db = bdb_set_db(database);
    key.data = s_zmalloc(keylen);
    memcpy(key.data, keydata, keylen);
    key.size = keylen;
    key.flags = DB_DBT_REALLOC;
    /* Get a cursor */
    db->cursor(db, txn, &cur, 0);
    if (val) {
        res =
            cur->get(cur, &key, &data, DB_GET_BOTH | DB_READ_UNCOMMITTED);
        if (res != 0) {
            LDEBUG("btsearch_keyval : get DB_GET_BOTH %s",
                   db_strerror(res));
            goto end;
        }
        retdta = s_zmalloc(sizeof(DAT));
        retdta->size = data.size;
        retdta->data = s_zmalloc(data.size);
        memcpy(retdta->data, data.data, data.size);
        LDEBUG("btsearch_keyval found key +data data.size=%i", data.size);
        goto end;
    } else {
        res = cur->get(cur, &key, &data, DB_SET | DB_READ_UNCOMMITTED);
        if (res != 0) {
            LFATAL("btsearch_keyval : get DB_SET %s", db_strerror(res));
            goto end;
        }
        retdta = s_zmalloc(sizeof(DAT));
        retdta->size = data.size;
        retdta->data = s_zmalloc(data.size);
        memcpy(retdta->data, data.data, data.size);
        LDEBUG("btsearch_keyval found key data.size=%i", data.size);
    }
  end:
    s_free(key.data);
    s_free(data.data);
    if (cur)
        cur->close(cur);
    if (lock)
        release_bdb_lock();
    return retdta;
}

int btdelete_curkey(int database, void *keydata, int keylen, void *value,
                    int vallen, const char *msg)
{
    DB *db;
    int ret = 0;
    DBC *cur;
    DBT key;
    DBT data;
    int res;

    FUNC;
    bdb_lock((char *) __PRETTY_FUNCTION__);
    DBTzero(&key);
    DBTzero(&data);

    db = bdb_set_db(database);
    key.data = s_zmalloc(keylen);
    memcpy(key.data, keydata, keylen);
    key.size = keylen;
    key.flags = DB_DBT_REALLOC;
    data.data = s_zmalloc(vallen);
    memcpy(data.data, value, vallen);
    data.size = vallen;
    data.flags = DB_DBT_REALLOC;

    db->cursor(db, txn, &cur, 0);
    res = cur->get(cur, &key, &data, DB_GET_BOTH | DB_READ_UNCOMMITTED);
    if (res != 0) {
        LDEBUG("btdelete_curkey : %s", db_strerror(res));
        if (cur)
            cur->close(cur);
        release_bdb_lock();
        ret = -ENOENT;
        goto end;
    }
    cur->del(cur, 0);
    if (cur)
        cur->close(cur);
    release_bdb_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLDELETECURKEY, keydata, keylen, value,
                        vallen, MAX_ALLOWED_THREADS - 2);
    }
  end:
    s_free(data.data);
    s_free(key.data);
    LDEBUG("btdelete_curkey  ret %i", ret);
    return (ret);
}

void bdb_restart_truncation()
{
    char *keystr = "TRNCTD";
    pthread_t truncate_thread;
    struct truncate_thread_data *trunc_data;
    DBC *cur;
    DBT key;
    DBT value;
    int ret;

    FUNC;
    if (!config->background_delete)
        return;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    memset(&key, 0, sizeof(DBT));
    key.data = s_malloc(strlen(keystr));
    memcpy(key.data, keystr, strlen(keystr));
    key.size = strlen(keystr);
    memset(&value, 0, sizeof(DBT));
    key.flags = DB_DBT_REALLOC;
    value.flags = DB_DBT_REALLOC;
    freelist->cursor(freelist, NULL, &cur, 0);

    ret = cur->get(cur, &key, &value, DB_SET | DB_READ_UNCOMMITTED);
    if (ret == DB_NOTFOUND)
        goto end_exit;
    do {
        trunc_data = s_malloc(value.size);
        memcpy(trunc_data, value.data, value.size);
        if (0 == trunc_data->inode) {
            s_free(trunc_data);
            break;
        }
        LINFO("Resume truncation of inode %llu", trunc_data->inode);
        create_inode_note(trunc_data->inode);
        if (config->blockdatabs) {
            if (0 !=
                pthread_create(&truncate_thread, NULL, tc_truncate_worker,
                               (void *) trunc_data))
                die_syserr();
        } else {
            if (0 !=
                pthread_create(&truncate_thread, NULL,
                               file_truncate_worker, (void *) trunc_data))
                die_syserr();
        }
        if (0 != pthread_detach(truncate_thread))
            die_syserr();
    } while (0 ==
             cur->get(cur, &key, &value, DB_NEXT | DB_READ_UNCOMMITTED));
  end_exit:
    if (key.data)
        s_free(key.data);
    cur->close(cur);
    release_bdb_lock();
    EFUNC;
    return;
}

char *lessfs_stats()
{
    char *lfsmsg = NULL;
    char *line;
    DDSTAT *ddstat;
    DAT data;
    unsigned long long inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    CRYPTO *crypto;
    const char **lines = NULL;
    int count = 1;
    unsigned int lcount = 0;
    int res;
    float ratio;
    unsigned long sp;
    DB_BTREE_STAT *bdbstat;
    DBC *cur;
    DBT key;
    DBT value;

    bdb_lock((char *) __PRETTY_FUNCTION__);
    res = dbp->stat(dbp, txn, &sp, 0);
    bdbstat = (DB_BTREE_STAT *) sp;
    lcount = bdbstat->bt_ndata;
    s_free(bdbstat);
    lcount++;
    lines = s_malloc(lcount * sizeof(char *));
    lines[0] =
        as_sprintf
        (__FILE__, __LINE__,
         "  INODE             SIZE  COMPRESSED_SIZE            RATIO FILENAME\n");

    /* traverse records */
    dbp->cursor(dbp, txn, &cur, 0);
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));
    key.flags = DB_DBT_REALLOC;
    value.flags = DB_DBT_REALLOC;
    while (0 == cur->get(cur, &key, &value, DB_NEXT | DB_READ_UNCOMMITTED)) {
        if (0 != memcmp(key.data, nfi, 3) && 0 != memcmp(key.data, seq, 3)) {
            memcpy(&inode, key.data, sizeof(unsigned long long));
            data.data = value.data;
            data.size = value.size;
            if (inode == 0) {
                crypto = (CRYPTO *) value.data;
            } else {
                ddstat = value_to_ddstat(&data);
                ratio=0;
                if ( ddstat->stbuf.st_size != 0 ) {
                   if ( ddstat->real_size == 0 ) {
                       ratio=1000; // 100% dedup requires datasize/1000 metadata space
                   } else ratio=(float)ddstat->stbuf.st_size/(float)ddstat->real_size;
                } else ratio=0;
                if (S_ISREG(ddstat->stbuf.st_mode)) {
#ifdef x86_64
                    line = as_sprintf
                        (__FILE__, __LINE__, "%7lu  %15lu  %15llu  %15.2f %s\n",
                         ddstat->stbuf.st_ino, ddstat->stbuf.st_size,
                         ddstat->real_size, ratio, ddstat->filename);
#else
                    line = as_sprintf
                        (__FILE__, __LINE__, "%7llu  %15llu  %15llu  %15.2f %s\n",
                         ddstat->stbuf.st_ino, ddstat->stbuf.st_size,
                         ddstat->real_size, ratio, ddstat->filename);
#endif
                    lines[count++] = line;
                } else
                    lcount--;
                ddstatfree(ddstat);
            }
        } else
            lcount--;
        if (count == lcount)
            break;
    }
    s_free(value.data);
    s_free(key.data);
    lfsmsg = as_strarrcat(lines, count);
    cur->close(cur);
    while (count) {
        s_free((char *) lines[--count]);
    }
    s_free(lines);
    release_bdb_lock();
    EFUNC;
    return lfsmsg;
}

void listdbp()
{
    DDSTAT *ddstat;
    DAT *data;
    unsigned long long *inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    CRYPTO *crypto;
    DBC *cur;
    DBT key;
    DBT value;
    int count = 0;


    /* traverse records */
    dbp->cursor(dbp, txn, &cur, 0);
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));
    key.flags = DB_DBT_MALLOC;
    value.flags = DB_DBT_MALLOC;

    while (0 == cur->get(cur, &key, &value, DB_NEXT | DB_READ_UNCOMMITTED)) {
        count++;
        inode = (unsigned long long *) key.data;
        if (0 == memcmp(key.data, nfi, 3) || 0 == memcmp(key.data, seq, 3)) {
            inode = (unsigned long long *) value.data;
            printf("NFI : %llu\n", *inode);
        } else {
            data =
                search_dbdata(DBP, inode, sizeof(unsigned long long),
                              LOCK);
            if (*inode == 0) {
                crypto = (CRYPTO *) data->data;
            } else {
                ddstat = value_to_ddstat(data);
#ifdef x86_64
                printf
                    ("ddstat->filename %s \n      ->inode %lu  -> size %lu  -> real_size %llu time %lu mode %u\n",
                     ddstat->filename, ddstat->stbuf.st_ino,
                     ddstat->stbuf.st_size, ddstat->real_size,
                     ddstat->stbuf.st_atim.tv_sec, ddstat->stbuf.st_mode);
#else
                printf
                    ("ddstat->filename %s \n      ->inode %llu -> size %llu -> real_size %llu time %lu mode %u\n",
                     ddstat->filename, ddstat->stbuf.st_ino,
                     ddstat->stbuf.st_size, ddstat->real_size,
                     ddstat->stbuf.st_atim.tv_sec, ddstat->stbuf.st_mode);
#endif
                if (S_ISDIR(ddstat->stbuf.st_mode)) {
                    printf("      ->filename %s is a directory\n",
                           ddstat->filename);
                }
                ddstatfree(ddstat);
            }
            DATfree(data);
        }
        s_free(value.data);
        s_free(key.data);
    }
    cur->close(cur);
}

void listdirent()
{
    DBC *cur;
    unsigned long long *dir;
    unsigned long long *ent;
    DBT key;
    DBT value;

    dbdirent->cursor(dbdirent, txn, &cur, 0);
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));
    key.flags = DB_DBT_MALLOC;
    value.flags = DB_DBT_MALLOC;

    while (0 == cur->get(cur, &key, &value, DB_NEXT | DB_READ_UNCOMMITTED)) {
        dir = (unsigned long long *) key.data;
        ent = (unsigned long long *) value.data;
        printf("%llu:%llu\n", *dir, *ent);
        s_free(key.data);
        s_free(value.data);
    }
    cur->close(cur);
}

void list_hardlinks()
{
    unsigned long long inode;
    DINOINO dinoino;
    DBC *cur;
    DBT key;
    DBT value;

    dbdirent->cursor(dbl, txn, &cur, 0);
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));
    key.flags = DB_DBT_REALLOC;
    value.flags = DB_DBT_REALLOC;

    while (0 == cur->get(cur, &key, &value, DB_NEXT | DB_READ_UNCOMMITTED)) {
        if (key.size == sizeof(DINOINO)) {
            memcpy(&dinoino, key.data, sizeof(DINOINO));
            printf("dinoino %llu-%llu : inode %s\n", dinoino.dirnode,
                   dinoino.inode, (char *) value.data);
        } else {
            memcpy(&inode, key.data, sizeof(unsigned long long));
            memcpy(&dinoino, value.data, sizeof(DINOINO));
            printf("inode %llu : %llu-%llu dinoino\n", inode,
                   dinoino.dirnode, dinoino.inode);
        }
        memset(key.data, 0, key.size);
        memset(value.data, 0, value.size);
    }
    s_free(value.data);
    s_free(key.data);
    cur->close(cur);
}

void listdbb()
{
    char *asc_hash = NULL;
    unsigned long long inode;
    unsigned long long blocknr;
    DBC *cur;
    DBT key;
    DBT value;

    dbb->cursor(dbb, txn, &cur, 0);
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));
    key.flags = DB_DBT_REALLOC;
    value.flags = DB_DBT_REALLOC;


    while (0 == cur->get(cur, &key, &value, DB_NEXT | DB_READ_UNCOMMITTED)) {
        asc_hash = ascii_hash((unsigned char *) value.data);
        memcpy(&inode, key.data, sizeof(unsigned long long));
        memcpy(&blocknr, key.data + sizeof(unsigned long long),
               sizeof(unsigned long long));
        printf("%llu-%llu : %s\n", inode, blocknr, asc_hash);
        s_free(asc_hash);
    }
    if (value.data)
        s_free(value.data);
    if (key.data)
        s_free(key.data);
    cur->close(cur);
}

void listfree(int freespace_summary)
{
    unsigned long long mbytes;
    unsigned long long offset;
    unsigned long freespace=0;
    struct truncate_thread_data *trunc_data;
    DBC *cur;
    DBT key;
    DBT value;
    FREEBLOCK *freeblock;

    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));
    key.flags = DB_DBT_REALLOC;
    value.flags = DB_DBT_REALLOC;
    freelist->cursor(freelist, txn, &cur, 0);
    while (0 == cur->get(cur, &key, &value, DB_NEXT | DB_READ_UNCOMMITTED)) {
        if (key.size == strlen("TRNCTD")) {
            trunc_data = (struct truncate_thread_data *) value.data;
            printf
                ("Truncation not finished for inode %llu : start %llu -> end %llu size %llu\n",
                 trunc_data->inode, trunc_data->blocknr,
                 trunc_data->lastblocknr,
                 (unsigned long long) trunc_data->stbuf.st_size);
        } else {
            memcpy(&mbytes, key.data, sizeof(unsigned long long));
            freespace+=(mbytes * 512);
            if (value.size < sizeof(FREEBLOCK)) {
                memcpy(&offset, value.data, sizeof(unsigned long long));
                if ( !freespace_summary) {
                   printf("offset = %llu : blocks = %llu : bytes = %llu\n",
                           offset, mbytes, mbytes * 512);
                }
            } else {
                freeblock = (FREEBLOCK *) value.data;
                if ( !freespace_summary) {
                   printf
                       ("offset = %llu : blocks = %llu : bytes = %llu reuseafter %lu\n",
                        freeblock->offset, mbytes, mbytes * 512,
                        freeblock->reuseafter);
                }
            }
        }
    }
    printf("Total available space in %s : %lu\n\n",config->blockdata,freespace);
    if (value.data)
        s_free(value.data);
    if (key.data)
        s_free(key.data);
    cur->close(cur);
    return;
}

void listdbu()
{
}

void listdta()
{
}

void list_symlinks()
{
}

void flistdbu()
{
    char *asc_hash = NULL;
    DBC *cur;
    DBT key;
    DBT value;
    INUSE *inuse;
    unsigned long long nexto;
    unsigned long rsize;


    dbb->cursor(dbu, txn, &cur, 0);
    memset(&key, 0, sizeof(DBT));
    memset(&value, 0, sizeof(DBT));
    key.flags = DB_DBT_REALLOC;
    value.flags = DB_DBT_REALLOC;


    while (0 == cur->get(cur, &key, &value, DB_NEXT | DB_READ_UNCOMMITTED)) {
        if (0 == memcmp(config->nexthash, key.data, config->hashlen)) {
            memcpy(&nexto, value.data, sizeof(unsigned long long));
            printf("\nnextoffset = %llu\n\n", nexto);
        } else {
            inuse = (INUSE *) value.data;
            asc_hash = ascii_hash((unsigned char *) key.data);
            printf("%s   : %llu\n", asc_hash, inuse->inuse);
            printf
                ("offset                                               : %llu\n",
                 inuse->offset);
            printf("size                                         : %lu\n",
                   inuse->size);
            rsize = inuse->allocated_size;
            printf
                ("allocated size                                   : %lu\n\n",
                 rsize);
            s_free(asc_hash);
            if (inuse->allocated_size - 512 > inuse->size)
                printf("Reclaimed oversized %lu - %llu\n", inuse->size,
                       inuse->allocated_size);
        }
    }
    if (value.data)
        s_free(value.data);
    if (key.data)
        s_free(key.data);
    cur->close(cur);
}

#endif
