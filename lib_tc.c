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

#include <tcutil.h>
#include <tcbdb.h>
#include <tchdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <aio.h>
#include <mhash.h>
#include <mutils/mhash.h>

#include "lib_safe.h"
#include "lib_cfg.h"
#include "retcodes.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#include "lib_qlz.h"
#include "lib_qlz15.h"
#include "lib_common.h"
#include "lib_tc.h"
#include "lib_crypto.h"
#include "file_io.h"
#include "lib_repl.h"

#ifndef BERKELEYDB
#ifndef HAMSTERDB
extern char *logname;
extern char *function;
extern int debug;
extern int BLKSIZE;
extern int max_threads;
extern char *passwd;

TCHDB *dbb;
TCHDB *dbu;
TCHDB *dbp;
TCBDB *dbl;
TCHDB *dbs;
TCHDB *dbdta;
TCBDB *dbdirent;
TCBDB *freelist;

extern TCTREE *workqtree;       // Used to buffer incoming data (writes) 
extern TCTREE *readcachetree;   // Used to cache chunks of data that are likely to be read
extern TCTREE *metatree;        // Used to cache file metadata (e.g. inode -> struct stat).
extern TCTREE *hashtree;        // Used to lock hashes
extern TCTREE *inodetree;       // Used to lock inodes
extern TCTREE *path2inotree;    // Used to cache path to inode translations
int fdbdta = 0;
int frepl = 0;
int freplbl = 0;

extern unsigned long long nextoffset;
int written = 0;

const char *offset_lockedby;

u_int32_t db_flags, env_flags;

int btree_test_transaction(TCBDB * bdb)
{
    char *testtransaction = "LESSFSTESTTRANSACTION";
    int ecode;
    int retcode = 0;

    if (config->transactions) {
        if (!tcbdbtranbegin(bdb))
            goto e_exit;
        if (!tcbdbput
            (bdb, testtransaction, strlen(testtransaction),
             testtransaction, strlen(testtransaction)))
            goto e_exit;
        if (!tcbdbtrancommit(bdb))
            goto e_exit;
        if (!tcbdbtranbegin(bdb))
            goto e_exit;
        if (!tcbdbout(bdb, testtransaction, strlen(testtransaction))) {
            ecode = tcbdbecode(bdb);
            die_dataerr
                ("check_hash_database - Failed to delete the initial test transaction");
        }
        if (!tcbdbtrancommit(bdb))
            goto e_exit;
    }
    retcode = 1;
  e_exit:
    return (retcode);
}

TCHDB *tc_set_hashdb(int database)
{
    TCHDB *db;
    switch (database) {
    case DBU:
        db = dbu;
        break;
    case DBB:
        db = dbb;
        break;
    case DBDTA:
        db = dbdta;
        break;
    case DBP:
        db = dbp;
        break;
    case DBS:
        db = dbs;
        break;
    default:
        die_syserr();
    }
    return db;
}

TCBDB *tc_set_btreedb(int database)
{
    TCBDB *db;
    switch (database) {
    case DBDIRENT:
        db = dbdirent;
        break;
    case DBL:
        db = dbl;
        break;
    case FREELIST:
        db = freelist;
        break;
    default:
        die_syserr();
    }
    return db;
}

int hash_test_transaction(TCHDB * hdb, char *dbpath)
{
    char *testtransaction = "LESSFSTESTTRANSACTION";
    int ecode;
    int retcode = 0;

    if (config->transactions) {
        if (!tchdbtranbegin(hdb))
            goto e_exit;
        if (!tchdbput
            (hdb, testtransaction, strlen(testtransaction),
             testtransaction, strlen(testtransaction)))
            goto e_exit;
        if (!tchdbtrancommit(hdb))
            goto e_exit;
        tchdbsync(hdb);
        if (!tchdbtranbegin(hdb))
            goto e_exit;
        if (!tchdbout(hdb, testtransaction, strlen(testtransaction))) {
            ecode = tchdbecode(hdb);
            die_dataerr
                ("check_hash_database - Failed to delete the initial test transaction");
        }
        if (!tchdbtrancommit(hdb))
            goto e_exit;
    }
    retcode = 1;
  e_exit:
    return (1);
}

void repair_hash_database(TCHDB * hdb, char *dbpath,
                          unsigned long long bucketsize)
{
    LFATAL
        ("repair_hash_database : database %s failed the initial test transaction : executing automatic repair",
         dbpath);
    if (!tchdboptimize(hdb, bucketsize, 0, 0, HDBTLARGE))
        die_dberr("Could not recover database : %s", dbpath);
}

void repair_btree_database(TCBDB * bdb, char *dbpath,
                           unsigned long long bucketsize)
{
    LFATAL
        ("repair_btree_database : database %s failed the initial test transaction : executing automatic repair",
         dbpath);
    if (!tcbdboptimize(bdb, 0, 0, bucketsize, -1, -1, BDBTLARGE))
        die_dberr("Could not recover database : %s", dbpath);
}

TCHDB *hashdb_open(char *dbpath, int cacherow,
                   unsigned long long bucketsize, bool force_optimize)
{
    TCHDB *hdb;
    int ecode;
    int count = 0;

    FUNC;


//retry_open:
    hdb = tchdbnew();
    if (cacherow > 0) {
        tchdbsetcache(hdb, cacherow);
    }

    if (config->defrag == 1) {
        if (!tchdbsetdfunit(hdb, 1)) {
            ecode = tchdbecode(hdb);
            die_dberr("Error on setting defragmentation : %s",
                      tchdberrmsg(ecode));
        }
    }
    tchdbsetmutex(hdb);
    LINFO("Tuning the bucketsize for %s to %llu", dbpath, bucketsize);
    if (!tchdbtune(hdb, bucketsize, 0, 0, HDBTLARGE)) {
        ecode = tchdbecode(hdb);
        die_dberr("Failed to tune database buckets : %s",
                  tchdberrmsg(ecode));
    }
    if (!tchdbsetxmsiz(hdb, bucketsize)) {
        ecode = tchdbecode(hdb);
        die_dberr("Failed to set extra mapped memory : %s",
                  tchdberrmsg(ecode));
    }
    if (!tchdbopen(hdb, dbpath, HDBOWRITER | HDBOCREAT) || force_optimize) {
        repair_hash_database(hdb, dbpath, bucketsize);
        count++;
    }

    if (!hash_test_transaction(hdb, dbpath)) {
        if (count == 0) {
            repair_hash_database(hdb, dbpath, bucketsize);
            count++;
        } else
            die_dberr
                ("hash_test_transaction failed on a database that was 'repaired'");
    }
    if (count == 1)
        LFATAL
            ("Database : %s, has been repaired : running lessfsck is recommended",
             dbpath);
    EFUNC;
    return hdb;
}

TCBDB *btreedb_open(char *dbpath, int cacherow,
                    unsigned long long bucketsize, bool force_optimize)
{
    TCBDB *bdb;
    int ecode;
    int count = 0;

    FUNC;

    bdb = tcbdbnew();
    if (config->defrag == 1) {
        if (!tcbdbsetdfunit(bdb, 1)) {
            ecode = tcbdbecode(bdb);
            die_dberr("Error on setting defragmentation : %s",
                      tcbdberrmsg(ecode));
        }
    }
    tcbdbsetmutex(bdb);
    tcbdbtune(bdb, 0, 0, bucketsize, -1, -1, BDBTLARGE);
    tcbdbsetxmsiz(bdb, bucketsize);

    if (!tcbdbopen(bdb, dbpath, BDBOWRITER | BDBOCREAT) || force_optimize) {
        repair_btree_database(bdb, dbpath, bucketsize);
        count++;
    }

    if (!btree_test_transaction(bdb)) {
        if (count == 0) {
            repair_btree_database(bdb, dbpath, bucketsize);
        } else
            die_dberr
                ("btree_test_transaction failed on a database that was 'repaired'");
    }
    if (count == 1)
        LFATAL
            ("Database : %s, has been repaired : running lessfsck is recommended",
             dbpath);
    EFUNC;
    return bdb;
}

void tc_defrag()
{
    start_flush_commit();
    if (!tchdboptimize(dbb, atol(config->fileblockbs), 0, 0, HDBTLARGE))
        LINFO("fileblock.tch not optimized");
    if (!tchdboptimize(dbu, atol(config->blockusagebs), 0, 0, HDBTLARGE))
        LINFO("blockusage.tch not optimized");
    if (!tchdboptimize(dbp, atol(config->metabs), 0, 0, HDBTLARGE))
        LINFO("metadata.tcb not optimized");
    if (!tchdboptimize(dbs, atol(config->symlinkbs), 0, 0, HDBTLARGE))
        LINFO("symlink.tch not optimized");
    if (config->blockdatabs) {
        if (!tchdboptimize
            (dbdta, atol(config->blockdatabs), 0, 0, HDBTLARGE))
            LINFO("blockdata.tch not optimized");
    }
    if (!tcbdboptimize
        (freelist, 0, 0, atol(config->freelistbs), -1, -1, BDBTLARGE))
        LINFO("freelist.tcb not optimized");
    if (!tcbdboptimize
        (dbdirent, 0, 0, atol(config->direntbs), -1, -1, BDBTLARGE))
        LINFO("dirent.tcb not optimized");
    if (!tcbdboptimize
        (dbl, 0, 0, atol(config->hardlinkbs), -1, -1, BDBTLARGE))
        LINFO("hardlink.tcb not optimized");
    end_flush_commit();
}

void tc_open(bool defrag, bool createpath, bool force_optimize)
{
    char *dbpath;
    struct stat stbuf;
    char *sp;
    char *hashstr;
    DAT *data;


    FUNC;
    LINFO("Lessfs with tokyocabinet is no longer recommended, please consider using BerkeleyDB");
    if ( config->blockdata_io_type == FILE_IO ) {
       sp = s_dirname(config->blockdata);
    } else sp=s_strdup(config->blockdata);
    if (createpath)
        mkpath(sp, 0744);
    dbpath = as_sprintf(__FILE__, __LINE__, "%s/replog.dta", sp);
    config->replication_logfile = s_strdup(dbpath);
    if (-1 == (frepl = s_open2(dbpath, O_CREAT | O_RDWR, S_IRWXU)))
        die_syserr();
    if (0 != flock(frepl, LOCK_EX | LOCK_NB)) {
        LFATAL
            ("Failed to lock the replication logfile %s\nlessfs must be unmounted before using this option!",
             config->replication_logfile);
        exit(EXIT_USAGE);
    }

    if (config->replication && config->replication_role == 0) {
        if (-1 == (freplbl = s_open2(dbpath, O_RDWR, S_IRWXU)))
            die_syserr();
    }
    s_free(dbpath);
    s_free(sp);

    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/fileblock.tch",
                   config->fileblock);
    if (createpath)
        mkpath(config->fileblock, 0744);
    LDEBUG("Open database %s", dbpath);
    dbb =
        hashdb_open(dbpath, 0, atol(config->fileblockbs), force_optimize);
    s_free(dbpath);

    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/blockusage.tch",
                   config->blockusage);
    if (createpath)
        mkpath(config->blockusage, 0744);
    LDEBUG("Open database %s", dbpath);
    dbu =
        hashdb_open(dbpath, 0, atol(config->blockusagebs), force_optimize);
    s_free(dbpath);

    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/metadata.tcb", config->meta);
    if (createpath)
        mkpath(config->meta, 0744);
    LDEBUG("Open database %s", dbpath);
    dbp = hashdb_open(dbpath, 0, atol(config->metabs), force_optimize);
    s_free(dbpath);
    if (config->blockdatabs) {
        dbpath =
            as_sprintf(__FILE__, __LINE__, "%s/blockdata.tch",
                       config->blockdata);
        if (createpath)
            mkpath(config->blockdata, 0744);
        LDEBUG("Open database %s", dbpath);
        dbdta =
            hashdb_open(dbpath, 0, atol(config->blockdatabs),
                        force_optimize);
        s_free(dbpath);
    }
    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/freelist.tcb",
                   config->freelist);
    if (createpath)
        mkpath(config->freelist, 0744);
    freelist =
        btreedb_open(dbpath, 0, atol(config->freelistbs), force_optimize);
    LDEBUG("Open database %s", dbpath);
    s_free(dbpath);
    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/symlink.tch", config->symlink);
    if (createpath)
        mkpath(config->symlink, 0744);
    LDEBUG("Open database %s", dbpath);
    dbs = hashdb_open(dbpath, 0, atol(config->symlinkbs), force_optimize);
    s_free(dbpath);
    /* The dirent database is a B-TREE DB with cursors */
    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/dirent.tcb", config->dirent);
    if (createpath)
        mkpath(config->dirent, 0744);
    dbdirent =
        btreedb_open(dbpath, 0, atol(config->direntbs), force_optimize);
    s_free(dbpath);
    /* The dbl database is a B-TREE DB with cursors */
    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/hardlink.tcb",
                   config->hardlink);
    if (createpath)
        mkpath(config->hardlink, 0744);
    dbl =
        btreedb_open(dbpath, 0, atol(config->hardlinkbs), force_optimize);
    LDEBUG("Open database %s", dbpath);
    s_free(dbpath);

    if (!defrag) {
        open_trees();
        if ( config->blockdata_io_type == FILE_IO ) {
            if (-1 ==
                (fdbdta =
                 s_open2(config->blockdata, O_CREAT | O_RDWR, S_IRWXU)))
                die_syserr();
            if (-1 == (stat(config->blockdata, &stbuf)))
                die_syserr();
            if (config->transactions) {
                check_datafile_sanity();
            }
        }
        if ( config->blockdata_io_type != TOKYOCABINET ) {
            hashstr = as_sprintf(__FILE__, __LINE__, "NEXTOFFSET");
            config->nexthash =
                (char *) thash((unsigned char *) hashstr, strlen(hashstr));
            data =
                search_dbdata(DBU, config->nexthash, config->hashlen,
                              NOLOCK);
            if (NULL == data) {
                LINFO("Filesystem upgraded to support transactions");
                nextoffset = stbuf.st_size;
            } else {
                memcpy(&nextoffset, data->data,
                       sizeof(unsigned long long));
                DATfree(data);
            }
        }
    }
    LDEBUG("All databases are open");
    if (config->transactions) {
        if (config->blockdatabs) {
            tchdbtranbegin(dbdta);
        }
        tcbdbtranbegin(freelist);
        tchdbtranbegin(dbu);
        tchdbtranbegin(dbb);
        tchdbtranbegin(dbs);
        tchdbtranbegin(dbp);
        tcbdbtranbegin(dbdirent);
        tcbdbtranbegin(dbl);
    }
    EFUNC;
    return;
}

void hashdb_close(TCHDB * hdb)
{
    int ecode;

    FUNC;
    /* close the database */
    if (!tchdbclose(hdb)) {
        ecode = tchdbecode(hdb);
        die_dberr("close error: %s", tchdberrmsg(ecode));
    }
    /* delete the object */
    tchdbdel(hdb);
}

void tc_close(bool defrag)
{
    int ecode;

    FUNC;

    if (config->blockdata_io_type != TOKYOCABINET ) {
        bin_write_dbdata(DBU, config->nexthash, config->hashlen,
                         (unsigned char *) &nextoffset,
                         sizeof(unsigned long long));
    }
    commit_transactions();
    hashdb_close(dbb);
    hashdb_close(dbp);
    hashdb_close(dbu);
    hashdb_close(dbs);
    if (config->blockdata_io_type == TOKYOCABINET ) {
        hashdb_close(dbdta);
    }
    /* close the B-TREE database */
    if (!tcbdbclose(freelist)) {
        ecode = tcbdbecode(freelist);
        die_dberr("close error: %s", tchdberrmsg(ecode));
    }
    /* delete the object */
    tcbdbdel(freelist);

    /* close the B-TREE database */
    if (!tcbdbclose(dbdirent)) {
        ecode = tcbdbecode(dbdirent);
        die_dberr("close error: %s", tchdberrmsg(ecode));
    }
    /* delete the object */
    tcbdbdel(dbdirent);

    /* close the B-TREE database */
    if (!tcbdbclose(dbl)) {
        ecode = tcbdbecode(dbl);
        die_dberr("close error: %s", tchdberrmsg(ecode));
    }
    /* delete the object */
    tcbdbdel(dbl);

    if (!defrag) {
        close_trees();
        if (config->blockdata_io_type != TOKYOCABINET ) {
            fsync(fdbdta);
            close(fdbdta);
            s_free(config->nexthash);
        }
    }
    fsync(frepl);
    close(frepl);
    if (config->replication && config->replication_role == 0)
        close(freplbl);
    EFUNC;
}

// lock is not used
void btbin_write_dup(int database, void *keydata, int keylen,
                     void *dataData, int datalen, bool lock)
{
    TCBDB *tcdb;
    int ecode;
    FUNC;

    tcdb = tc_set_btreedb(database);
    if (!tcbdbputdup(tcdb, keydata, keylen, dataData, datalen)) {
        ecode = tcbdbecode(tcdb);
        die_dberr("tcbdbputdup failed : %s", tcbdberrmsg(ecode));
    }
    if (0 == config->relax)
        tcbdbsync(tcdb);
    if (config->replication == 1 && config->replication_role == 0) {
        if (database == DBDIRENT) {
            write_repl_data(DBDIRENT, REPLDUPWRITE, keydata, keylen,
                            dataData, datalen, MAX_ALLOWED_THREADS - 2);
        }
        if (database == DBL) {
            write_repl_data(DBL, REPLDUPWRITE, keydata, keylen, dataData,
                            datalen, MAX_ALLOWED_THREADS - 2);
        }
        if (database == FREELIST) {
            write_repl_data(FREELIST, REPLDUPWRITE, keydata, keylen,
                            dataData, datalen, MAX_ALLOWED_THREADS - 2);
        }
    }
    EFUNC;
}

void btbin_write_dbdata(int database, void *keydata, int keylen,
                        void *dataData, int datalen)
{
    TCBDB *tcdb;
    int ecode;
    FUNC;
    tcdb = tc_set_btreedb(database);
    if (!tcbdbput(tcdb, keydata, keylen, dataData, datalen)) {
        ecode = tcbdbecode(tcdb);
        die_dberr("tcbdbput failed : %s", tchdberrmsg(ecode));
    }
    if (0 == config->relax)
        tcbdbsync(tcdb);
    if (config->replication == 1 && config->replication_role == 0) {
        if (database == DBDIRENT) {
            write_repl_data(DBDIRENT, REPLWRITE, keydata, keylen, dataData,
                            datalen, MAX_ALLOWED_THREADS - 2);
        }
        if (database == DBL) {
            write_repl_data(DBL, REPLWRITE, keydata, keylen, dataData,
                            datalen, MAX_ALLOWED_THREADS - 2);
        }
        if (database == FREELIST) {
            write_repl_data(FREELIST, REPLWRITE, keydata, keylen, dataData,
                            datalen, MAX_ALLOWED_THREADS - 2);
        }
    }
    EFUNC;
}

void mbin_write_dbdata(TCMDB * db, void *keydata, int keylen,
                       void *dataData, int datalen)
{
    FUNC;
    tcmdbput(db, keydata, keylen, dataData, datalen);
    EFUNC;
}

void nbin_write_dbdata(TCNDB * db, void *keydata, int keylen,
                       void *dataData, int datalen)
{
    FUNC;
    tcndbput(db, keydata, keylen, dataData, datalen);
    EFUNC;
}

void bin_write_dbdata(int database, void *keydata, int keylen,
                      void *dataData, int datalen)
{
    TCHDB *db;
    int ecode;
    db = tc_set_hashdb(database);
    if (!tchdbput(db, keydata, keylen, dataData, datalen)) {
        ecode = tchdbecode(db);
        die_dberr("tchdbput failed : %s", tchdberrmsg(ecode));
    }
    if (config->replication == 1 && config->replication_role == 0) {
        if (database == DBDTA) {
            write_repl_data(DBDTA, REPLWRITE, keydata, keylen, dataData,
                            datalen, MAX_ALLOWED_THREADS - 2);
        }
        if (database == DBU) {
            write_repl_data(DBU, REPLWRITE, keydata, keylen, dataData,
                            datalen, MAX_ALLOWED_THREADS - 2);
        }
        if (database == DBB) {
            write_repl_data(DBB, REPLWRITE, keydata, keylen, dataData,
                            datalen, MAX_ALLOWED_THREADS - 2);
        }
        if (database == DBP) {
            write_repl_data(DBP, REPLWRITE, keydata, keylen, dataData,
                            datalen, MAX_ALLOWED_THREADS - 2);
        }
        if (database == DBS) {
            write_repl_data(DBS, REPLWRITE, keydata, keylen, dataData,
                            datalen, MAX_ALLOWED_THREADS - 2);
        }
    }
}

void bin_write(int database, void *keydata, int keylen,
               void *dataData, int datalen)
{
    TCHDB *db;
    int ecode;
    db = tc_set_hashdb(database);
    if (!tchdbput(db, keydata, keylen, dataData, datalen)) {
        ecode = tchdbecode(db);
        die_dberr("tchdbput failed : %s", tchdberrmsg(ecode));
    }
}

/* Search in directory with inode dinode or key dinode for name bname 
   Return the inode of file bname                       */
DDSTAT *dnode_bname_to_inode(void *dinode, int dlen, char *bname)
{
    BDBCUR *cur;
    char *dbkey;
    int dbsize;
    char *dbvalue;
    DAT *statdata;
    DDSTAT *filestat = NULL, *lfilestat;
    DAT *fname;
    DINOINO dinoino;
    unsigned long long keynode;
    unsigned long long valnode;
    unsigned long long *inode;
    unsigned long long *curinode;
#ifdef ENABLE_CRYPTO
    DAT *decrypted;
#endif

    FUNC;
    cur = tcbdbcurnew(dbdirent);
    if (!tcbdbcurjump(cur, (char *) dinode, dlen)
        && tcbdbecode(dbdirent) != TCESUCCESS) {
        tcbdbcurdel(cur);
        return NULL;
    }

    inode = (unsigned long long *) dinode;
    /* traverse records */
    while (dbkey = tcbdbcurkey(cur, &dbsize)) {
        curinode = (unsigned long long *) dbkey;
        if (*curinode != *inode) {
            //if (0 != memcmp(dbkey, dinode, dlen)) {
            s_free(dbkey);
            break;
        }
        dbvalue = tcbdbcurval(cur, &dbsize);
        if (dbvalue) {
            memcpy(&valnode, dbvalue, sizeof(valnode));
            memcpy(&keynode, dbkey, sizeof(keynode));
            if (keynode == valnode && keynode != 1) {
                s_free(dbvalue);
                s_free(dbkey);
                tcbdbcurnext(cur);
                continue;
            }
            statdata =
                search_dbdata(DBP, &valnode, sizeof(unsigned long long),
                              NOLOCK);
            if (NULL == statdata) {
                LINFO("Unable to find file existing in dbp.\n");
                tcbdbcurdel(cur);
                s_free(dbvalue);
                s_free(dbkey);
                return NULL;
            }
#ifdef ENABLE_CRYPTO
            if (config->encryptmeta && config->encryptdata) {
                decrypted = lfsdecrypt(statdata);
                DATfree(statdata);
                statdata = decrypted;
            }
#endif
            filestat = (DDSTAT *) statdata->data;
            s_free(dbvalue);
            if (0 != filestat->filename[0]) {
                s_free(statdata);
                LDEBUG("compare bname %s with filestat->filename %s",
                       bname, filestat->filename);
                if (0 == strcmp(bname, filestat->filename)) {
                    s_free(dbkey);
                    break;
                }
            } else {
                memcpy(&dinoino.dirnode, dinode,
                       sizeof(unsigned long long));
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
                    lfilestat->filename[fname->size + 1] = 0;
                    filestat = lfilestat;
                    DATfree(fname);
                    s_free(dbkey);
                    break;
                } else
                    s_free(statdata);
            }
            ddstatfree(filestat);
            filestat = NULL;
        }
        s_free(dbkey);
        tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    if (filestat) {
        LDEBUG("dnode_bname_to_inode : filestat->filename=%s inode %lu",
               filestat->filename, filestat->stbuf.st_ino);
    } else {
        LDEBUG("dnode_bname_to_inode : return NULL");
    }
    return filestat;
}

// lock is not used with tc
DAT *search_dbdata(int database, void *key, int len, bool lock)
{
    TCHDB *tcdb;
    DAT *data;
    int size;

    tcdb = tc_set_hashdb(database);
    data = s_malloc(sizeof(DAT));
    data->data = tchdbget(tcdb, key, len, &size);
    data->size = (unsigned long) size;
    if (NULL == data->data) {
        LDEBUG("search_dbdata : return NULL");
        s_free(data);
        data = NULL;
    } else
        LDEBUG("search_dbdata : return %lu bytes", data->size);
    return data;
}

DAT *search_memhash(TCMDB * db, void *key, int len)
{
    DAT *data;
    int size;

    FUNC;
    data = s_malloc(sizeof(DAT));
    data->data = tcmdbget(db, key, len, &size);
    data->size = (unsigned long) size;
    if (NULL == data->data) {
        LDEBUG("search_memhash : return NULL");
        s_free(data);
        data = NULL;
    } else
        LDEBUG("search_memhash : return %lu bytes", data->size);
    EFUNC;
    return data;
}

int btdelete_curkey(int database, void *key, int keylen, void *kvalue,
                    int kvallen, const char *msg)
{
    TCBDB *db;
    BDBCUR *cur;
    char *value;
    int vsize;
    int ksize;
    char *dbkey;
    int ret = 1;

    FUNC;

    db = tc_set_btreedb(database);
    cur = tcbdbcurnew(db);
    if (!tcbdbcurjump(cur, key, keylen)) {
        tcbdbcurdel(cur);
        return (-ENOENT);
    }
    /* traverse records */
    while (dbkey = tcbdbcurkey(cur, &ksize)) {
        if (0 != memcmp(dbkey, key, ksize)) {
            s_free(dbkey);
            break;
        }
        value = tcbdbcurval(cur, &vsize);
        if (value) {
            if (kvallen == vsize) {
                if (0 == memcmp(value, kvalue, kvallen)) {
                    ret = 0;
                    if (!tcbdbcurout(cur)) {
                        die_dataerr
                            ("btdelete_curkey : Failed to delete key, this should never happen : caller %s",
                             msg);
                        ret = -ENOENT;
                    }
                    s_free(value);
                    s_free(dbkey);
                    break;
                }
            }
            s_free(value);
        }
        s_free(dbkey);
        tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    if (config->replication == 1 && config->replication_role == 0) {
        if (db == dbdirent) {
            write_repl_data(DBDIRENT, REPLDELETECURKEY, key, keylen,
                            kvalue, kvallen, MAX_ALLOWED_THREADS - 2);
        }
        if (db == dbl) {
            write_repl_data(DBL, REPLDELETECURKEY, key, keylen, kvalue,
                            kvallen, MAX_ALLOWED_THREADS - 2);
        }
        if (db == freelist) {
            write_repl_data(FREELIST, REPLDELETECURKEY, key, keylen,
                            kvalue, kvallen, MAX_ALLOWED_THREADS - 2);
        }
    }
    return (ret);
}

/* lock is not used with tc */
DAT *btsearch_keyval(int database, void *key, int keylen, void *val,
                     int vallen, bool lock)
{
    TCBDB *db;
    BDBCUR *cur;
    char *dbkey;
    char *dbvalue;
    DAT *ret = NULL;
    int size;

    FUNC;
    db = tc_set_btreedb(database);
    cur = tcbdbcurnew(db);
    if (!tcbdbcurjump(cur, key, keylen) && tcbdbecode(db) != TCESUCCESS) {
        tcbdbcurdel(cur);
        return ret;
    }
    /* traverse records */
    while (dbkey = tcbdbcurkey(cur, &size)) {
        if (0 != memcmp(dbkey, key, keylen)) {
            s_free(dbkey);
            break;
        }
        dbvalue = tcbdbcurval(cur, &size);
        if (dbvalue) {
            if (val) {
                if (vallen == size) {
                    if (0 == memcmp(val, dbvalue, size)) {
                        ret = s_zmalloc(sizeof(DAT));
                        ret->data = s_zmalloc(size + 1);
                        ret->size = size;
                        memcpy(ret->data, dbvalue, size);
                        s_free(dbvalue);
                        s_free(dbkey);
                        break;
                    }
                }
            } else {
                ret = s_zmalloc(sizeof(DAT));
                ret->data = s_zmalloc(size + 1);
                ret->size = size;
                memcpy(ret->data, dbvalue, size);
                s_free(dbvalue);
                s_free(dbkey);
                break;
            }
            s_free(dbvalue);
        }
        s_free(dbkey);
        tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    EFUNC;
    return ret;
}

/* return 0, 1 or 2 if more then 2 we stop counting */
int count_dirlinks(void *linkstr, int len)
{
    BDBCUR *cur;
    char *key;
    int size;
    int count = 0;

    cur = tcbdbcurnew(dbl);
    if (!tcbdbcurjump(cur, linkstr, len)
        && tcbdbecode(dbl) != TCESUCCESS) {
        tcbdbcurdel(cur);
        return (-ENOENT);
    }
    /* traverse records */
    while (key = tcbdbcurkey(cur, &size)) {
        if (len == size) {
            if (0 == memcmp(key, linkstr, len)) {
                count++;
            }
        }
        s_free(key);
        tcbdbcurnext(cur);
        if (count > 1)
            break;
    }
    tcbdbcurdel(cur);
    return (count);
}

// Delete key from database, if caller (msg) is defined this operation
// must be successfull, otherwise we log and exit.
void delete_key(int database, void *keydata, int len, const char *msg)
{
    TCHDB *db;
    int ecode;

    db = tc_set_hashdb(database);
    if (!tchdbout(db, keydata, len)) {
        ecode = tchdbecode(db);
        if (msg)
            die_dataerr("Delete of key failed %s:%s", msg,
                        tchdberrmsg(ecode));
    }
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLDELETE, keydata, len, NULL, 0,
                        MAX_ALLOWED_THREADS - 2);
    }
}

void tc_restart_truncation()
{
    BDBCUR *cur;
    char *dbkey;
    char *key = "TRNCTD";
    int ksize;
    int keylen;
    pthread_t truncate_thread;
    struct truncate_thread_data *trunc_data;


    FUNC;
    if (!config->background_delete)
        return;
    keylen = strlen(key);

    cur = tcbdbcurnew(freelist);
    if (!tcbdbcurjump(cur, key, keylen)
        && tcbdbecode(dbdirent) != TCESUCCESS) {
        tcbdbcurdel(cur);
        return;
    }
    /* traverse records */
    while (dbkey = tcbdbcurkey(cur, &ksize)) {
        if (0 != memcmp(dbkey, key, keylen) || ksize != keylen) {
            s_free(dbkey);
            break;
        }
        trunc_data =
            (struct truncate_thread_data *) tcbdbcurval(cur, &ksize);
        if (0 == trunc_data->inode)
            break;
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
        s_free(dbkey);
        tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
}

/* Return 0 when the directory is empty, 1 when it contains files */
unsigned long long has_nodes(unsigned long long inode)
{
    unsigned long long res = 0;
    BDBCUR *cur;
    unsigned long long filenode;
    char *filenodestr;
    DAT *filedata;
    bool dotdir = 0;
    char *key;
    DDSTAT *ddstat;
    int size;
    unsigned long long keyval;

    FUNC;
    cur = tcbdbcurnew(dbdirent);
    if (!tcbdbcurjump(cur, &inode, sizeof(unsigned long long))
        && tcbdbecode(dbdirent) != TCESUCCESS) {
        tcbdbcurdel(cur);
        return (-ENOENT);
    }
    while (key = tcbdbcurkey(cur, &size)) {
        memcpy(&keyval, key, sizeof(unsigned long long));
        if (keyval != inode) {
            s_free(key);
            break;
        }
        filenodestr = (char *) tcbdbcurval(cur, &size);
        memcpy(&filenode, filenodestr, sizeof(unsigned long long));
        filedata =
            search_dbdata(DBP, &filenode, sizeof(unsigned long long),
                          NOLOCK);
        if (filedata) {
            ddstat = value_to_ddstat(filedata);
            DATfree(filedata);
            LDEBUG("Compare : %llu %llu", filenode, inode);
            if (filenode == inode)
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
                     ddstat->filename, filenode, inode);
                res++;
            }
            ddstatfree(ddstat);
        }
        s_free(key);
        s_free(filenodestr);
        dotdir = 0;
        if (0 != res)
            break;
        tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    LDEBUG("inode %llu contains files", inode);
    EFUNC;
    return (res);
}

void fs_read_hardlink(struct stat stbuf, DDSTAT * ddstat, void *buf,
                      fuse_fill_dir_t filler, struct fuse_file_info *fi)
{
    BDBCUR *cur;
    char *linkkey;
    int ksize;
    DINOINO dinoino;
    char *filename;

    FUNC;
    cur = tcbdbcurnew(dbl);
    dinoino.dirnode = stbuf.st_ino;
    dinoino.inode = ddstat->stbuf.st_ino;
    if (!tcbdbcurjump(cur, &dinoino, sizeof(DINOINO))
        && tcbdbecode(dbl) != TCESUCCESS)
        die_dataerr("Unable to find linkname, run fsck.");
    while (linkkey = tcbdbcurkey(cur, &ksize)) {
        if (0 != memcmp(linkkey, &dinoino, sizeof(DINOINO))) {
            LDEBUG("fs_read_hardlink : linkkey != dinoino");
            s_free(linkkey);
            break;
        }
        filename = (char *) tcbdbcurval(cur, &ksize);
        memcpy(&ddstat->filename, filename, ksize);
        ddstat->filename[ksize] = 0;
        s_free(filename);
        LDEBUG("fs_read_hardlink : fil_fuse_info %s size %i",
               ddstat->filename, ksize);
        fil_fuse_info(ddstat, buf, filler, fi);
        ddstat->filename[0] = 0;
        s_free(linkkey);
        tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    EFUNC;
    return;
}

int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
               off_t offset, struct fuse_file_info *fi)
{
    int retcode = 0;
    BDBCUR *cur;
    int size, res;
    int ksize;
    char *key;
    struct stat stbuf;
    unsigned long long keynode;
    unsigned long long filenode;
    char *filenodestr;
    DAT *filedata;
    DDSTAT *ddstat;
    FUNC;

    (void) offset;
    (void) fi;
    LDEBUG("Called fs_readdir with path %s", (char *) path);

    res = dbstat(path, &stbuf, 1);
    if (0 != res)
        return -ENOENT;

    if (0 == strcmp(path, "/.lessfs/locks")) {
        LDEBUG("fs_readdir : reading locks");
        locks_to_dir(buf, filler, fi);
    }

    cur = tcbdbcurnew(dbdirent);
    if (!tcbdbcurjump(cur, &stbuf.st_ino, sizeof(unsigned long long))
        && tcbdbecode(dbdirent) != TCESUCCESS) {
        tcbdbcurdel(cur);
        return (-ENOENT);
    }
    while (key = tcbdbcurkey(cur, &ksize)) {
        memcpy(&keynode, key, sizeof(unsigned long long));
        if (stbuf.st_ino != keynode) {
            s_free(key);
            break;
        }
        filenodestr = tcbdbcurval(cur, &size);
        memcpy(&filenode, filenodestr, sizeof(unsigned long long));
        if (filenode == keynode) {
            s_free(filenodestr);
            s_free(key);
            tcbdbcurnext(cur);
            continue;
        }
        LDEBUG("GOT filenode %llu", filenode);
        filedata =
            search_dbdata(DBP, &filenode, sizeof(unsigned long long),
                          NOLOCK);
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
        s_free(key);
        s_free(filenodestr);
        tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
    LDEBUG("fs_readdir: return");
    return (retcode);
}

int bt_entry_exists(int database, void *parent, int parentlen, void *value,
                    int vallen)
{
    TCBDB *db;
    int res = 0;
    BDBCUR *cur;
    char *dbvalue;
    char *key;
    int ksize;

    FUNC;

    db = tc_set_btreedb(database);
    cur = tcbdbcurnew(db);
    if (!tcbdbcurjump(cur, parent, parentlen)
        && tcbdbecode(db) != TCESUCCESS) {
        tcbdbcurdel(cur);
        return (res);
    }
    while (key = tcbdbcurkey(cur, &ksize)) {
        if (ksize != parentlen) {
            s_free(key);
            break;
        }
        if (0 != memcmp(key, parent, parentlen)) {
            s_free(key);
            break;
        }
        dbvalue = tcbdbcurval2(cur);
        if (dbvalue) {
            if (0 == memcmp(value, dbvalue, vallen))
                res = 1;
            s_free(dbvalue);
        }
        s_free(key);
        tcbdbcurnext(cur);
        if (res == 1)
            break;
    }
    tcbdbcurdel(cur);
    LDEBUG("bt_entry_exists : returns %i", res);
    EFUNC;
    return (res);
}

void drop_databases()
{
    char *dbpath;
    struct stat stbuf;

    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/fileblock.tch",
                   config->fileblock);
    if (-1 != stat(dbpath, &stbuf))
        unlink(dbpath);
    s_free(dbpath);
    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/blockusage.tch",
                   config->blockusage);
    if (-1 != stat(dbpath, &stbuf))
        unlink(dbpath);
    s_free(dbpath);
    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/metadata.tcb", config->meta);
    if (-1 != stat(dbpath, &stbuf))
        unlink(dbpath);
    s_free(dbpath);
    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/symlink.tch", config->symlink);
    if (-1 != stat(dbpath, &stbuf))
        unlink(dbpath);
    s_free(dbpath);
    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/dirent.tcb", config->dirent);
    if (-1 != stat(dbpath, &stbuf))
        unlink(dbpath);
    s_free(dbpath);
    dbpath =
        as_sprintf(__FILE__, __LINE__, "%s/hardlink.tcb",
                   config->hardlink);
    if (-1 != stat(dbpath, &stbuf))
        unlink(dbpath);
    s_free(dbpath);
    if (config->blockdatabs) {
        dbpath =
            as_sprintf(__FILE__, __LINE__, "%s/blockdata.tch",
                       config->blockdata);
        if (-1 != stat(dbpath, &stbuf))
            unlink(dbpath);
        s_free(dbpath);
    } else {
        dbpath =
            as_sprintf(__FILE__, __LINE__, "%s/freelist.tcb",
                       config->freelist);
        if (-1 != stat(dbpath, &stbuf))
            unlink(dbpath);
        s_free(dbpath);
        unlink(config->blockdata);
    }
}

DAT *search_nhash(TCNDB * db, void *key, int len)
{
    DAT *data;
    int size;

    FUNC;
    data = s_malloc(sizeof(DAT));
    data->data = tcndbget(db, key, len, &size);
    data->size = (unsigned long) size;
    if (NULL == data->data) {
        LDEBUG("search_nhash : return NULL");
        s_free(data);
        data = NULL;
    } else
        LDEBUG("search_nhash : return %lu bytes", data->size);
    EFUNC;
    return data;
}

void abort_transactions()
{
    if (config->blockdatabs) {
        if (config->transactions)
            if (!tchdbtranabort(dbdta))
                die_dataerr
                    ("IO error, unable to abort dbdta transaction");
    } else {
        fsync(fdbdta);
        if (config->transactions)
            if (!tcbdbtranabort(freelist))
                die_dataerr
                    ("IO error, unable to abort freelist transaction");
    }
    if (config->transactions) {
        if (!tchdbtranabort(dbu))
            die_dataerr
                ("IO error, unable to abort blockusage transaction");
        if (!tchdbtranabort(dbb))
            die_dataerr
                ("IO error, unable to abort fileblock transaction");
        if (!tchdbtranabort(dbp))
            die_dataerr("IO error, unable to abort metadata transaction");
        if (!tchdbtranabort(dbs))
            die_dataerr("IO error, unable to abort symlink transaction");
        if (!tcbdbtranabort(dbdirent))
            die_dataerr("IO error, unable to abort dirent transaction");
        if (!tcbdbtranabort(dbl))
            die_dataerr("IO error, unable to abort hardlink transaction");
    }
    start_transactions();
    return;
}

void commit_transactions()
{
    if (config->blockdatabs) {
        if (config->transactions)
            if (!tchdbtrancommit(dbdta))
                die_dataerr
                    ("IO error, unable to commit dbdta transaction");
    } else {
        fsync(fdbdta);
        if (config->transactions)
            if (!tcbdbtrancommit(freelist))
                die_dataerr
                    ("IO error, unable to commit freelist transaction");
    }
    if (config->transactions) {
        if (!tchdbtrancommit(dbu))
            die_dataerr
                ("IO error, unable to commit blockusage transaction");
        if (!tchdbtrancommit(dbb))
            die_dataerr
                ("IO error, unable to commit fileblock transaction");
        if (!tchdbtrancommit(dbp))
            die_dataerr("IO error, unable to commit metadata transaction");
        if (!tchdbtrancommit(dbs))
            die_dataerr("IO error, unable to commit symlink transaction");
        if (!tcbdbtrancommit(dbdirent))
            die_dataerr("IO error, unable to commit dirent transaction");
        if (!tcbdbtrancommit(dbl))
            die_dataerr("IO error, unable to commit hardlink transaction");
    }
    return;
}

void start_flush_commit()
{
    unsigned long long lastoffset = 0;
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
    commit_transactions();
    /* Make sure that the meta data is updated every once in a while */
    tcbdbsync(dbdirent);
    tcbdbsync(dbl);
    tchdbsync(dbp);
    tchdbsync(dbs);
}

void start_transactions()
{
    if (config->transactions) {
        if (config->blockdatabs) {
            tchdbtranbegin(dbdta);
        } else
            tcbdbtranbegin(freelist);
        tchdbtranbegin(dbu);
        tchdbtranbegin(dbb);
        tchdbtranbegin(dbp);
        tchdbtranbegin(dbs);
        tcbdbtranbegin(dbdirent);
        tcbdbtranbegin(dbl);
    }
    return;
}

void end_flush_commit()
{
    if (config->blockdatabs == NULL) {
        tcbdbsync(freelist);
    } else
        tchdbsync(dbdta);
    tchdbsync(dbu);
    tchdbsync(dbb);
    start_transactions();
}

char *lessfs_stats()
{
    char *lfsmsg;
    char *line;
    char *key;
    int ksize;
    DDSTAT *ddstat;
    DAT *data;
    float ratio;
    unsigned long long *inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    const char **lines = NULL;
    int count = 1;

    lines = s_malloc((tchdbrnum(dbp) + 1) * sizeof(char *));
    lines[0] =
        as_sprintf(__FILE__, __LINE__,
                   "  INODE             SIZE  COMPRESSED_SIZE            RATIO  FILENAME\n");
    /* traverse records */
    tchdbiterinit(dbp);
    while (key = tchdbiternext(dbp, &ksize)) {
        if (0 != memcmp(key, nfi, 3) && 0 != memcmp(key, seq, 3)) {
            inode = (unsigned long long *) key;
            data =
                search_dbdata(DBP, (void *) inode,
                              sizeof(unsigned long long), NOLOCK);
            if (*inode != 0) {
                ddstat = value_to_ddstat(data);
                ratio=0;
                if ( ddstat->stbuf.st_size != 0 ) {
                   if ( ddstat->real_size == 0 ) {
                       ratio=1000; // 100% dedup requires datasize/1000 metadata space
                   } else ratio=(float)ddstat->stbuf.st_size/(float)ddstat->real_size;
                } else ratio=0;
                if (S_ISREG(ddstat->stbuf.st_mode)) {
#ifdef x86_64
                    line = as_sprintf
                        (__FILE__, __LINE__, "%7lu  %15lu  %15llu  %15.2f  %s\n",
                         ddstat->stbuf.st_ino, ddstat->stbuf.st_size,
                         ddstat->real_size, ratio, ddstat->filename);
#else
                    line = as_sprintf
                        (__FILE__, __LINE__, "%7llu  %15llu  %15llu  %15.2f  %s\n",
                         ddstat->stbuf.st_ino, ddstat->stbuf.st_size,
                         ddstat->real_size, ratio, ddstat->filename);
#endif
                    lines[count++] = line;
                }
                ddstatfree(ddstat);
            }
            DATfree(data);
        }
        s_free(key);
    }
    lfsmsg = as_strarrcat(lines, count);
    while (count) {
        s_free((char *) lines[--count]);
    }
    s_free(lines);
    return lfsmsg;
}

INUSE *get_offset_reclaim(unsigned long long mbytes,
                          unsigned long long offset)
{
    BDBCUR *cur;
    unsigned long long *dbkey;
    unsigned long long *dboffset;
    int dbsize;
    FREEBLOCK *freeblock;
    INUSE *inuse = NULL;
    time_t thetime;
    bool hasone = 0;

    FUNC;
    thetime = time(NULL);
    LDEBUG("get_offset_reclaim : search for %llu blocks on the freelist",
           mbytes);
    while (1) {
        cur = tcbdbcurnew(freelist);
        tcbdbcurfirst(cur);
        while (dbkey = tcbdbcurkey(cur, &dbsize)) {
            if (dbsize == strlen("TRNCTD") || *dbkey < mbytes) {
                s_free(dbkey);
                tcbdbcurnext(cur);
                continue;
            } else
                LDEBUG("Search %llu got %llu", mbytes, *dbkey);
            if ((dboffset = tcbdbcurval(cur, &dbsize)) == NULL)
                die_dataerr("get_offset_reclaim : No value for key");
            if (dbsize < sizeof(FREEBLOCK)) {
                memcpy(&offset, dboffset, sizeof(unsigned long long));
                if (config->replication == 1
                    && config->replication_role == 0) {
                    write_repl_data(FREELIST, REPLDELETECURKEY,
                                    (char *) dbkey,
                                    sizeof(unsigned long long),
                                    (char *) &offset,
                                    sizeof(unsigned long long),
                                    MAX_ALLOWED_THREADS - 2);
                }
            } else {
                freeblock = (FREEBLOCK *) dboffset;
                if (freeblock->reuseafter > thetime && hasone == 0) {
                    tcbdbcurnext(cur);
                    hasone = 1;
                    s_free(dbkey);
                    s_free(dboffset);
                    continue;
                }
                if (freeblock->reuseafter > thetime && hasone == 1)
                    LINFO
                        ("get_offset_reclaim : early space reclaim, low on space");
                offset = freeblock->offset;
                if (config->replication == 1
                    && config->replication_role == 0) {
                    write_repl_data(FREELIST, REPLDELETECURKEY,
                                    (char *) dbkey,
                                    sizeof(unsigned long long),
                                    (char *) freeblock,
                                    sizeof(FREEBLOCK),
                                    MAX_ALLOWED_THREADS - 2);
                }
            }
            inuse = s_zmalloc(sizeof(INUSE));
            inuse->allocated_size = *dbkey * 512;
            LDEBUG("*dbkey=%llu, inuse->allocated_size %llu", *dbkey,
                   inuse->allocated_size);
            inuse->offset = offset;

            LDEBUG
                ("get_offset_reclaim : reclaim %llu blocks on the freelist at offset %llu",
                 mbytes, offset);
            if (!tcbdbcurout(cur)) {
                die_dataerr
                    ("get_offset_reclaim : failed to delete key, this should never happen!");
            }
            s_free(dboffset);
            break;
            s_free(dbkey);
            tcbdbcurnext(cur);
        }
        tcbdbcurdel(cur);
        if (!hasone)
            break;
        if (inuse)
            break;
    }
    LDEBUG("get_offset_reclaim returns = %llu", offset);
    return inuse;
}

unsigned long long get_offset_fast(unsigned long long mbytes)
{
    unsigned long long offset;
    BDBCUR *cur;
    unsigned long long *dbkey;
    unsigned long long *dboffset;
    int dbsize;
    bool found = 0;
    time_t thetime;
    FREEBLOCK *freeblock;

    FUNC;

    thetime = time(NULL);
    offset = nextoffset;
    LDEBUG("get_offset_fast : search for %llu blocks on the freelist",
           mbytes);
    cur = tcbdbcurnew(freelist);
    if (tcbdbcurjump(cur, (void *) &mbytes, sizeof(unsigned long long))) {
        while (dbkey = tcbdbcurkey(cur, &dbsize)) {
            if (0 == memcmp(dbkey, &mbytes, sizeof(unsigned long long))) {
                if ((dboffset = tcbdbcurval(cur, &dbsize)) == NULL)
                    die_dataerr("get_offset_fast : No value for key");
                if (dbsize < sizeof(FREELIST)) {
                    memcpy(&offset, dboffset, sizeof(unsigned long long));
                    if (config->replication == 1
                        && config->replication_role == 0) {
                        write_repl_data(FREELIST, REPLDELETECURKEY,
                                        (char *) &mbytes,
                                        sizeof(unsigned long long),
                                        (char *) &offset,
                                        sizeof(unsigned long long),
                                        MAX_ALLOWED_THREADS - 2);
                    }
                } else {
                    freeblock = (FREEBLOCK *) dboffset;
                    if (freeblock->reuseafter > thetime) {
                        tcbdbcurnext(cur);
                        continue;
                    }
                    offset = freeblock->offset;
                    if (config->replication == 1
                        && config->replication_role == 0) {
                        write_repl_data(FREELIST, REPLDELETECURKEY,
                                        (char *) &mbytes,
                                        sizeof(unsigned long long),
                                        (char *) freelist,
                                        sizeof(FREELIST),
                                        MAX_ALLOWED_THREADS - 2);
                    }
                }
                found = 1;
                LDEBUG
                    ("get_offset_fast : reclaim %llu blocks on the freelist at offset %llu",
                     mbytes, offset);
                if (!tcbdbcurout(cur)) {
                    die_dataerr
                        ("get_offset_fast : failed to delete key, this should never happen!");
                }
                s_free(dboffset);
                break;
            } else
                break;
            s_free(dbkey);
        }
    }
    if (!found)
        offset = (0 - 1);
    tcbdbcurdel(cur);
    LDEBUG("get_offset_fast returns = %llu", offset);
    return (offset);
}

void listdirent()
{
    BDBCUR *cur;
    char *key, *value;
    int size;
    unsigned long long dir;
    unsigned long long ent;

    /* traverse records */
    cur = tcbdbcurnew(dbdirent);
    tcbdbcurfirst(cur);
    while (key = tcbdbcurkey2(cur)) {
        memcpy(&dir, key, sizeof(dir));
        value = tcbdbcurval(cur, &size);;
        if (value) {
            memcpy(&ent, value, sizeof(ent));
            printf("%llu:%llu\n", dir, ent);
            s_free(value);
        }
        s_free(key);
        tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
}

void listfree(int freespace_summary)
{
    unsigned long long mbytes;
    unsigned long long offset;
    unsigned long freespace=0;
    BDBCUR *cur;
    unsigned long long *dbkey;
    unsigned long long *dboffset;
    struct truncate_thread_data *trunc_data;
    int dbsize;
    FREEBLOCK *freeblock;

    cur = tcbdbcurnew(freelist);
    tcbdbcurfirst(cur);
    while (dbkey = tcbdbcurkey(cur, &dbsize)) {
        if (dbsize == strlen("TRNCTD")) {
            trunc_data =
                (struct truncate_thread_data *) tcbdbcurval(cur, &dbsize);
            printf
                ("Truncation not finished for inode %llu : start %llu -> end %llu size %llu\n",
                 trunc_data->inode, trunc_data->blocknr,
                 trunc_data->lastblocknr,
                 (unsigned long long) trunc_data->stbuf.st_size);
            s_free(trunc_data);
        } else {
            if ((dboffset = tcbdbcurval(cur, &dbsize)) == NULL) {
                fprintf(stderr, "No value for key");
                exit(EXIT_SYSTEM);
            }
            memcpy(&mbytes, dbkey, sizeof(unsigned long long));
            freespace+=(mbytes * 512);
            if (dbsize < sizeof(FREEBLOCK)) {
                memcpy(&offset, dboffset, sizeof(unsigned long long));
                if ( !freespace_summary) {
                   printf("offset = %llu : blocks = %llu : bytes = %llu\n",
                          offset, mbytes, mbytes * 512);
                }
            } else {
                freeblock = (FREEBLOCK *) dboffset;
                if ( !freespace_summary) {
                   printf
                       ("offset = %llu : blocks = %llu : bytes = %llu reuseafter %lu\n",
                        freeblock->offset, mbytes, mbytes * 512,
                        freeblock->reuseafter);
                }
            }
            s_free(dboffset);
        }
        s_free(dbkey);
        tcbdbcurnext(cur);
    }
    printf("Total available space in %s : %lu\n\n",config->blockdata,freespace);
    tcbdbcurdel(cur);
    return;
}

void listdbp()
{
    char *key, *value;
    int size;
    int ksize;
    DDSTAT *ddstat;
    DAT *data;
    unsigned long long inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    CRYPTO *crypto;


    /* traverse records */
    tchdbiterinit(dbp);
    while (key = tchdbiternext(dbp, &ksize)) {
        if (0 == memcmp(key, nfi, 3) || 0 == memcmp(key, seq, 3)) {
            value = tchdbget(dbp, key, strlen(key), &size);
            memcpy(&inode, value, sizeof(unsigned long long));
            printf("%s : %llu\n", key, inode);
            s_free(value);
        } else {
            memcpy(&inode, key, sizeof(unsigned long long));
            data =
                search_dbdata(DBP, &inode, sizeof(unsigned long long),
                              LOCK);
            if (inode == 0) {
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
        s_free(key);
    }
}

void list_hardlinks()
{
    char *key, *value;
    int size;
    int ksize;
    unsigned long long inode;
    DINOINO dinoino;
    BDBCUR *cur;

    /* traverse records */
    cur = tcbdbcurnew(dbl);
    tcbdbcurfirst(cur);
    while (key = tcbdbcurkey(cur, &ksize)) {
        value = tcbdbcurval(cur, &size);
        if (ksize == sizeof(DINOINO)) {
            memcpy(&dinoino, key, sizeof(DINOINO));
            printf("dinoino %llu-%llu : inode %s\n", dinoino.dirnode,
                   dinoino.inode, value);
        } else {
            memcpy(&inode, key, sizeof(unsigned long long));
            memcpy(&dinoino, value, sizeof(DINOINO));
            printf("inode %llu : %llu-%llu dinoino\n", inode,
                   dinoino.dirnode, dinoino.inode);
        }
        s_free(value);
        s_free(key);
        tcbdbcurnext(cur);
    }
    tcbdbcurdel(cur);
}

/* List the symlink database */
void list_symlinks()
{
    char *key, *value;
    int size;
    int sp;

    unsigned long long inode;
    /* traverse records */
    tchdbiterinit(dbs);
    while (key = tchdbiternext(dbs, &size)) {
        value = tchdbget(dbs, key, size, &sp);
        memcpy(&inode, key, sizeof(unsigned long long));
        printf("%llu : %s\n", inode, value);
        s_free(value);
        s_free(key);
    }
}

void listdbb()
{
    char *asc_hash = NULL;
    char *key, *value;
    int size;
    int vsize;
    unsigned long long inode;
    unsigned long long blocknr;

    /* traverse records */
    tchdbiterinit(dbb);
    while (key = tchdbiternext(dbb, &size)) {
        value = tchdbget(dbb, key, size, &vsize);
        asc_hash = ascii_hash((unsigned char *) value);
        memcpy(&inode, key, sizeof(unsigned long long));
        memcpy(&blocknr, key + sizeof(unsigned long long),
               sizeof(unsigned long long));
        printf("%llu-%llu : %s\n", inode, blocknr, asc_hash);
        s_free(asc_hash);
        s_free(value);
        s_free(key);
    }
}


void listdta()
{
    char *asc_hash;
    char *key;

    /* traverse records */
    tchdbiterinit(dbdta);
    while (key = tchdbiternext2(dbdta)) {
        asc_hash = ascii_hash((unsigned char *) key);
        printf("%s\n", asc_hash);
        s_free(asc_hash);
        s_free(key);
    }
}

void flistdbu()
{
    char *asc_hash;
    char *key;
    int size;
    INUSE *inuse;
    unsigned long rsize;
    unsigned long long nextoffet;

    /* traverse records */
    tchdbiterinit(dbu);
    while (key = tchdbiternext2(dbu)) {
        if (0 == memcmp(config->nexthash, key, config->hashlen)) {
            inuse = tchdbget(dbu, key, config->hashlen, &size);
            memcpy(&nextoffet, inuse, sizeof(unsigned long long));
            printf("\nnextoffset = %llu\n\n", nextoffset);
        } else {
            inuse = tchdbget(dbu, key, config->hashlen, &size);
            asc_hash = ascii_hash((unsigned char *) key);
            printf("%s   : %llu\n", asc_hash, inuse->inuse);
            printf
                ("offset                                               : %llu\n",
                 inuse->offset);
            printf("size                                         : %lu\n",
                   inuse->size);
            rsize = round_512(inuse->size);
            printf
                ("round size                                   : %lu\n\n",
                 rsize);
            s_free(asc_hash);
            s_free(inuse);
        }
        s_free(key);
    }
}

void listdbu()
{
    char *asc_hash;
    char *key, *value;
    int size;
    unsigned long long counter;

    /* traverse records */
    tchdbiterinit(dbu);
    while (key = tchdbiternext2(dbu)) {
        value = tchdbget(dbu, key, config->hashlen, &size);
        asc_hash = ascii_hash((unsigned char *) key);
        memcpy(&counter, value, sizeof(counter));
        printf("%s : %llu\n", asc_hash, counter);
        s_free(asc_hash);
        s_free(value);
        s_free(key);
    }
}
#endif
#endif
