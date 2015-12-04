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

#ifdef HAMSTERDB
#ifndef LFATAL
#include "lib_log.h"
#endif
#include <ham/hamsterdb.h>
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
#include "lib_common.h"
#include "lib_crypto.h"
#include "file_io.h"
#include "lib_repl.h"
#include "lib_hamster.h"

#define TOTAL_DATABASES 8
ham_db_t *db[TOTAL_DATABASES];
ham_env_t *env;
ham_txn_t *txn = 0;
ham_status_t st;

int dbb = DBB;
int dbu = DBU;
int dbp = DBP;
int dbl = DBL;
int dbs = DBS;
int dbdta = DBDTA;
int dbdirent = DBDIRENT;
int freelist = FREELIST;

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

static pthread_mutex_t hamster_mutex = PTHREAD_MUTEX_INITIALIZER;
const char *ham_lockedby;

void ham_lock(const char *msg)
{
    FUNC;
    LDEBUG("ham_lock : %s", msg);
#ifdef DBGLOCK
    struct timespec deltatime;
    deltatime.tv_sec = time(NULL) + GLOBAL_LOCK_TIMEOUT;
    deltatime.tv_nsec = 0;
    int err_code;

    err_code = pthread_mutex_timedlock(&hamster_mutex, &deltatime);
    if (err_code != 0) {
        die_lock_report(msg, __PRETTY_FUNCTION__);
    }
#else
    pthread_mutex_lock(&hamster_mutex);
#endif
    ham_lockedby = msg;
    LDEBUG("ham_lockedby : %s", ham_lockedby);
    EFUNC;
    return;
}

int try_ham_lock()
{
    int res;
    res = pthread_mutex_trylock(&hamster_mutex);
    return (res);
}

void release_ham_lock()
{
    FUNC;
    pthread_mutex_unlock(&hamster_mutex);
    EFUNC;
    return;
}

void ham_error(const char *foo)
{
    LFATAL("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
    die_dberr("hamsterdb error");
}

void hm_open()
{
    char *dbname;
    struct stat stbuf;
    char *hashstr;
    DAT *data;
    int i;
    char *sp;

    ham_parameter_t envparameters[] = {
        {HAM_PARAM_CACHESIZE, config->hamsterdb_cachesize },
        {0, 0}
    };

    FUNC;
    sp = s_dirname(config->blockdata);
    dbname = as_sprintf(__FILE__, __LINE__, "%s/replog.dta", sp);
    config->replication_logfile = s_strdup(dbname);
    if (-1 == (frepl = s_open2(dbname, O_CREAT | O_RDWR, S_IRWXU)))
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
    s_free(sp);
    s_free(dbname);
    dbname = as_sprintf(__FILE__, __LINE__, "%s/lessfs.db", config->meta);
    if (config->transactions) {
        st = ham_env_open(&env, dbname,
                             HAM_DISABLE_MMAP | HAM_ENABLE_TRANSACTIONS | HAM_ENABLE_FSYNC,
                             envparameters);
    } else {
        st = ham_env_open(&env, dbname, HAM_DISABLE_MMAP, envparameters);
    }
    if (st != HAM_SUCCESS)
        ham_error("ham_env_open failed to open the databases");
    s_free(dbname);
    /*
    for (i = 0; i < TOTAL_DATABASES; i++) {
        LDEBUG("ham_new %i", i);
        st = ham_new(&db[i]);
        if (st != HAM_SUCCESS)
            ham_error("ham_new");
    }*/
    st = ham_env_open_db(env, &db[DBDTA], 1 + DBDTA, 0, 0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_open_db (DBDTA)");
    st = ham_env_open_db(env, &db[DBU], 1 + DBU, 0, 0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_open_db (DBU)");
    st = ham_env_open_db(env, &db[DBB], 1 + DBB, 0, 0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_open_db (DBB)");
    st = ham_env_open_db(env, &db[DBP], 1 + DBP, 0, 0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_open_db (DBP)");
    st = ham_env_open_db(env, &db[DBS], 1 + DBS, 0, 0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_open_db (DBS)");
    st = ham_env_open_db(env, &db[DBL], 1 + DBL, 0, 0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_open_db (DBL)");
    st = ham_env_open_db(env, &db[FREELIST], 1 + FREELIST, 0, 0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_open_db (FREELIST)");
    st = ham_env_open_db(env, &db[DBDIRENT], 1 + DBDIRENT, 0, 0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_open_db (DBDIRENT)");

    if (config->transactions) {
#ifdef HAM_API_REVISION
       ham_txn_begin(&txn, env, "lessfs",NULL,0);
#else
       ham_txn_begin(&txn, db[0], 0);
#endif
    }
    open_trees();
    if ( config->blockdata_io_type == FILE_IO ) {
        if (-1 ==
            (fdbdta =
             s_open2(config->blockdata, O_CREAT | O_RDWR, S_IRWXU)))
            die_syserr();
        if (-1 == (stat(config->blockdata, &stbuf)))
            die_syserr();
        hashstr = as_sprintf(__FILE__, __LINE__, "NEXTOFFSET");
        config->nexthash =
            (char *) thash((unsigned char *) hashstr, strlen(hashstr));
        s_free(hashstr);
        data = search_dbdata(DBU, config->nexthash, config->hashlen, LOCK);
        if (!data) {
            LINFO("Filesystem upgraded to support transactions");
            nextoffset = stbuf.st_size;
        } else {
            memcpy(&nextoffset, data->data, sizeof(unsigned long long));
            DATfree(data);
        }
        if (config->transactions) {
            check_datafile_sanity();
        }
    }
    EFUNC;
    LDEBUG("Database are open");
    return;
}

void drop_databases()
{
    char *dbname;
    struct stat stbuf;

    dbname = as_sprintf(__FILE__, __LINE__, "%s/lessfs.db", config->meta);
    if (-1 != stat(dbname, &stbuf))
        unlink(dbname);
    s_free(dbname);
    if ( config->blockdata_io_type == FILE_IO ) {
        if (-1 != stat(config->blockdata, &stbuf))
            unlink(config->blockdata);
    }
    return;
}

void hm_create(bool createpath)
{
    char *dbname, *sp;
    struct stat stbuf;
    char *hashstr;
    DAT *data;
    int i;

    FUNC;
    dbname = as_sprintf(__FILE__, __LINE__, "%s/lessfs.db", config->meta);
    if (createpath)
        mkpath(config->meta, 0744);
    ham_parameter_t dbuparameters[] = {
        {HAM_PARAM_KEYSIZE, config->hashlen}
        ,
        {0, 0}
    };

    ham_parameter_t dbbparameters[] = {
        {HAM_PARAM_KEYSIZE, 2 * sizeof(unsigned long long)},
        {0, 0}
    };

    ham_parameter_t dbpparameters[] = {
        {HAM_PARAM_KEYSIZE, sizeof(unsigned long long)},
        {0, 0}
    };


    ham_parameter_t envparameters[] = {
        {HAM_PARAM_CACHESIZE, config->hamsterdb_cachesize },
        {0, 0}
    };
    st = ham_env_create(&env, dbname,
                        HAM_DISABLE_MMAP | HAM_ENABLE_TRANSACTIONS | HAM_ENABLE_FSYNC |
                        HAM_AUTO_RECOVERY, 0660, envparameters);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_new %s");
    s_free(dbname);
    /*for (i = 0; i < TOTAL_DATABASES; i++) {
        LDEBUG("ham_new %i", i);
        st = ham_new(&db[i]);
        if (st != HAM_SUCCESS)
            ham_error("ham_new");
    }*/
    st = ham_env_create_db(env, &db[DBDTA], 1 + DBDTA, 0, dbuparameters);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_create_db (DBDTA)");
    st = ham_env_create_db(env, &db[DBU], 1 + DBU, 0, dbuparameters);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_create_db (DBU)");
    st = ham_env_create_db(env, &db[DBB], 1 + DBB, 0, dbbparameters);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_create_db (DBB)");
    st = ham_env_create_db(env, &db[DBP], 1 + DBP, 0, dbpparameters);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_create_db (DBP)");
    st = ham_env_create_db(env, &db[DBS], 1 + DBS, 0, 0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_create_db (DBS)");
    st = ham_env_create_db(env, &db[DBL], 1 + DBL, HAM_ENABLE_DUPLICATES,
                           0);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_create_db (DBL)");
    st = ham_env_create_db(env, &db[FREELIST], 1 + FREELIST,
                           HAM_ENABLE_DUPLICATES, dbpparameters);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_create_db (FREELIST)");
    st = ham_env_create_db(env, &db[DBDIRENT], 1 + DBDIRENT,
                           HAM_ENABLE_DUPLICATES, dbpparameters);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_create_db (DBDIRENT)");
    if (config->transactions) {
#ifdef HAM_API_REVISION
        ham_txn_begin(&txn, env, "lessfs",NULL,0);
#else
        ham_txn_begin(&txn, db[0], 0);
#endif
    }
    open_trees();
    if ( config->blockdata_io_type == FILE_IO ) {
        if (-1 ==
            (fdbdta =
             s_open2(config->blockdata, O_CREAT | O_RDWR, S_IRWXU)))
            die_syserr();
        if (-1 == (stat(config->blockdata, &stbuf)))
            die_syserr();
        hashstr = as_sprintf(__FILE__, __LINE__, "NEXTOFFSET");
        config->nexthash =
            (char *) thash((unsigned char *) hashstr, strlen(hashstr));
        data = search_dbdata(DBU, config->nexthash, config->hashlen, LOCK);
        if (!data) {
            LINFO("Filesystem upgraded to support transactions");
            nextoffset = stbuf.st_size;
        } else {
            memcpy(&nextoffset, data->data, sizeof(unsigned long long));
            DATfree(data);
        }
        if (config->transactions) {
            check_datafile_sanity();
        }
    }
    sp = s_dirname(config->blockdata);
    if (createpath)
        mkpath(sp, 0744);
    dbname = as_sprintf(__FILE__, __LINE__, "%s/replog.dta", sp);
    if (-1 == (frepl = s_open2(dbname, O_CREAT | O_RDWR, S_IRWXU)))
        die_syserr();
    s_free(sp);
    s_free(dbname);
    return;
}

int btdelete_curkey(int database, void *key, int keylen, void *kvalue,
                    int kvallen, const char *msg)
{
    ham_cursor_t *cursor;
    int ret = 1;
    ham_key_t hamkey;
    ham_record_t hamrecord;

    FUNC;
    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&hamkey, 0, sizeof(hamkey));
    memset(&hamrecord, 0, sizeof(hamrecord));

    st = ham_cursor_create(&cursor, db[database],txn, 0);
    if (st != HAM_SUCCESS) {
        LFATAL("ham_cursor_create() failed with error %d\n", st);
        release_ham_lock();
        return (-ENOENT);
    }

    hamkey.data = key;
    hamkey.size = keylen;
    /* traverse records */

    st = ham_cursor_find(cursor, &hamkey, 0, 0);
    if (st == HAM_SUCCESS) {
        /* traverse records */
        do {
            if ((st =
                 ham_cursor_move(cursor, 0, &hamrecord, 0)) != HAM_SUCCESS)
                die_dberr
                    ("btdelete_curkey : expected a value after a succesfull find : caller %s",
                     msg);
            if (0 == memcmp(hamrecord.data, kvalue, kvallen)) {
                ret = 0;
                st = ham_cursor_erase(cursor, 0);
                if (st != HAM_SUCCESS) {
                    LDEBUG("ham_erase, failed to erase key : caller %s",
                           msg);
                    ret = -ENOENT;
                }
                break;
            }
        } while (HAM_SUCCESS ==
                 (st =
                  ham_cursor_move(cursor, &hamkey, &hamrecord,
                                  HAM_CURSOR_NEXT)));
    }
    ham_cursor_close(cursor);
    release_ham_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLDELETECURKEY, key, keylen, kvalue,
                        kvallen, MAX_ALLOWED_THREADS - 2);
    }
    return (ret);
}

DAT *search_dbdata(int database, void *key, int len, bool lock)
{
    DAT *data;
    ham_key_t hamkey;
    ham_record_t record;

    FUNC;
    data = s_malloc(sizeof(DAT));
    if (lock)
        ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&hamkey, 0, sizeof(hamkey));
    memset(&record, 0, sizeof(record));
    hamkey.data = key;
    hamkey.size = len;
    if ((st =
        ham_db_find(db[database], txn, &hamkey, &record,
                  0)) != HAM_SUCCESS) {
        LDEBUG("search_dbdata : nothing found in database %i return NULL",
               database);
        s_free(data);
        data = NULL;
    } else {
        data->data = s_malloc(record.size);
        memcpy(data->data, record.data, record.size);
        data->size = record.size;
    }
    if (lock)
        release_ham_lock();
    EFUNC;
    return data;
}

/* Search in directory with inode dinode or key dinode for name bname 
   Return the inode of file bname                       */
DDSTAT *dnode_bname_to_inode(void *dinode, int dlen, char *bname)
{
    ham_cursor_t *cursor;
    DAT *statdata;
    DDSTAT *filestat = NULL;
    DINOINO dinoino;
    unsigned long long keynode;
    unsigned long long valnode;
    DAT *fname;

    FUNC;
    ham_key_t key;
    ham_record_t record;

    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));
    key.data = (char *) dinode;
    key.size = dlen;

    st = ham_cursor_create(&cursor, db[DBDIRENT], txn, 0);
    if (st != HAM_SUCCESS) {
        LFATAL("ham_cursor_create() failed with error %d\n", st);
        die_syserr();
    }
    st = ham_cursor_find(cursor, &key, 0, 0);
    if (st == HAM_KEY_NOT_FOUND) {
        LINFO("Unable to find file existing in dbp.\n");
        goto end_error;
    }
    /* traverse records */
    do {
        memcpy(&keynode, key.data, key.size);
        if ((st = ham_cursor_move(cursor, 0, &record, 0)) != HAM_SUCCESS) {
            LINFO("Key %llu without value dinode.\n", keynode);
            filestat = NULL;
            goto end_error;
        }
        memcpy(&valnode, record.data, record.size);
        if (keynode == valnode && keynode != 1) {
            LDEBUG("keynode == valnode && keynode != 1 %llu", keynode);
            continue;
        }
        statdata =
            search_dbdata(DBP, &valnode, sizeof(unsigned long long),
                          NOLOCK);
        if (NULL == statdata) {
            LINFO("Unable to find file existing in dbp...\n");
            goto end_error;
        }
        filestat = value_to_ddstat(statdata);
        DATfree(statdata);
        if (0 != filestat->filename[0]) {
            LDEBUG("compare bname %s with filestat->filename %s",
                   bname, filestat->filename);
            if (0 == strcmp(bname, filestat->filename)) {
                LDEBUG("%s == %s, break", bname, filestat->filename);
                break;
            }
        } else {
            memcpy(&dinoino.dirnode, dinode, sizeof(unsigned long long));
            dinoino.inode = filestat->stbuf.st_ino;
            fname =
                btsearch_keyval(DBL, &dinoino, sizeof(DINOINO), bname,
                                strlen(bname), NOLOCK);
            if (fname) {
                memcpy(&filestat->filename, fname->data, fname->size);
                filestat->filename[fname->size + 1] = 0;
                DATfree(fname);
                break;
            }
        }
        ddstatfree(filestat);
        filestat = NULL;
    } while (HAM_SUCCESS ==
             (st =
              ham_cursor_move(cursor, &key, &record,
                              HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES)));
  end_error:
    ham_cursor_close(cursor);
    release_ham_lock();
    return filestat;
}

DAT *btsearch_keyval(int database, void *key, int keylen, void *val,
                     int vallen, bool lock)
{
    ham_cursor_t *cursor;
    ham_key_t hamkey;
    ham_record_t hamrec;
    DAT *ret = NULL;

    FUNC;

    if (lock)
        ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&hamkey, 0, sizeof(hamkey));
    memset(&hamrec, 0, sizeof(hamrec));

    /* create a new cursor */
    st = ham_cursor_create(&cursor, db[database], txn, 0);
    if (st)
        ham_error("ham_cursor_create");

    hamkey.data = key;
    hamkey.size = keylen;

    /* traverse records */
    st = ham_cursor_find(cursor, &hamkey, 0, 0);
    if (st == HAM_SUCCESS) {
        do {
            if ((st =
                 ham_cursor_move(cursor, 0, &hamrec, 0)) != HAM_SUCCESS)
                die_dberr
                    ("btsearch_keyval : expected a value after a succesfull find");
            if (val) {
                if (vallen == hamrec.size) {
                    if (0 == memcmp(val, hamrec.data, hamrec.size)) {
                        ret = s_zmalloc(sizeof(DAT));
                        ret->data = s_zmalloc(hamrec.size + 1);
                        memcpy(ret->data, hamrec.data, hamrec.size);
                        ret->size = hamrec.size;
                        break;
                    }
                }
            } else {
                ret = s_zmalloc(sizeof(DAT));
                ret->data = s_zmalloc(hamrec.size + 1);
                memcpy(ret->data, hamrec.data, hamrec.size);
                ret->size = hamrec.size;
                break;
            }
        } while (HAM_SUCCESS ==
                 (st =
                  ham_cursor_move(cursor, &hamkey, &hamrec,
                                  HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES)));
    }
    ham_cursor_close(cursor);
    if (lock)
        release_ham_lock();
    EFUNC;
    return ret;
}

int count_dirlinks(void *linkstr, int len)
{
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;

    FUNC;
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    int count = 0;
    ham_lock((char *) __PRETTY_FUNCTION__);
    st = ham_cursor_create(&cursor, db[DBL], txn, 0);
    if (st)
        ham_error("ham_cursor_create");
    key.data = linkstr;
    key.size = len;
    st = ham_cursor_find(cursor, &key, 0, 0);
    if (st == HAM_SUCCESS) {
        /* traverse records */
        do {
            count++;
            if (count > 1)
                break;
        } while (0 ==
                 (st =
                  ham_cursor_move(cursor, &key, &rec,
                                  HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES)));
    }
    ham_cursor_close(cursor);
    release_ham_lock();
    EFUNC;
    return (count);
}

// Delete key from database, if caller (msg) is defined this operation
// must be successfull, otherwise we log and exit.
void delete_key(int database, void *keydata, int len, const char *msg)
{
    ham_key_t key;              /* the structure for a key */

    FUNC;
    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&key, 0, sizeof(key));
    key.data = keydata;
    key.size = len;
    st = ham_db_erase(db[database], txn, &key, 0);
    if (st != HAM_SUCCESS) {
        if (msg) {
            LFATAL("Failed to delete data from database %i : caller %s",
                   database, msg);
            ham_error("ham_erase");
        }
    }
    release_ham_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLDELETE, keydata, len, NULL, 0,
                        MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

char *lessfs_stats()
{
    char *lfsmsg = NULL;
    char *line;
    DDSTAT *ddstat;
    DAT *data;
    unsigned long long inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    CRYPTO *crypto;
    const char **lines = NULL;
    int count = 1;
    float ratio;
    ham_cursor_t *cursor;
    unsigned long lcount;
    ham_key_t key;
    ham_record_t rec;

    FUNC;

    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    st = ham_db_get_key_count(db[DBP], txn, 0, (ham_u64_t *) & lcount);
    if (st)
        ham_error("ham_get_key_count");
    lcount++;
    lines = s_malloc(lcount * sizeof(char *));
    lines[0] =
        as_sprintf
        (__FILE__, __LINE__,
         "  INODE             SIZE  COMPRESSED_SIZE            RATIO FILENAME\n");
    /* traverse records */

    /* create a new cursor */
    st = ham_cursor_create(&cursor, db[DBP], txn, 0);
    if (st)
        ham_error("ham_cursor_create");

    while (1) {
        st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT);
        if (st != HAM_SUCCESS) {
            /* reached end of the database? */
            if (st == HAM_KEY_NOT_FOUND)
                break;
            else {
                die_dataerr("ham_cursor_next() failed with error %d\n",
                            st);
            }
        }
        if (0 != memcmp(key.data, nfi, 3) && 0 != memcmp(key.data, seq, 3)) {
            memcpy(&inode, key.data, sizeof(unsigned long long));
            data =
                search_dbdata(DBP, &inode, sizeof(unsigned long long),
                              NOLOCK);
            if (inode == 0) {
                crypto = (CRYPTO *) data->data;
            } else {
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
                }
                ddstatfree(ddstat);
            }
            DATfree(data);
        }
    }
    lfsmsg = as_strarrcat(lines, count);
    ham_cursor_close(cursor);
    release_ham_lock();
    while (count) {
        s_free((char *) lines[--count]);
    }
    s_free(lines);
    EFUNC;
    return lfsmsg;
}

void bin_write_dbdata(int database, void *keydata, int keylen,
                      void *dataData, int datalen)
{
    ham_key_t key;              /* the structure for a key */
    ham_record_t record;        /* the structure for a record */
    FUNC;

    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));
    key.data = keydata;
    key.size = keylen;
    record.size = datalen;
    record.data = dataData;
    st = ham_db_insert(db[database], txn, &key, &record, HAM_OVERWRITE);
    if (st != HAM_SUCCESS)
        ham_error("bin_write_dbdata");
    release_ham_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE, keydata, keylen, dataData,
                        datalen, MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
    return;
}

void bin_write(int database, void *keydata, int keylen,
               void *dataData, int datalen)
{
    ham_key_t key;              /* the structure for a key */
    ham_record_t record;        /* the structure for a record */
    FUNC;

    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));
    key.data = keydata;
    key.size = keylen;
    record.size = datalen;
    record.data = dataData;
    st = ham_db_insert(db[database], txn, &key, &record, HAM_OVERWRITE);
    if (st != HAM_SUCCESS)
        ham_error("bin_write");
    release_ham_lock();
    EFUNC;
    return;
}

void btbin_write_dup(int database, void *keydata, int keylen,
                     void *dataData, int datalen, bool lock)
{
    ham_key_t key;              /* the structure for a key */
    ham_record_t record;        /* the structure for a record */

    FUNC;
    if (lock)
        ham_lock((char *) __PRETTY_FUNCTION__);

    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    key.data = keydata;
    key.size = keylen;
    record.size = datalen;
    record.data = dataData;

    LDEBUG("btbin_write_dup : before insert database %i", database);
    st = ham_db_insert(db[database], txn, &key, &record, HAM_DUPLICATE);
    if (st != HAM_SUCCESS)
        ham_error("btbin_write_dup");
    LDEBUG("btbin_write_dup : after insert");
    if (lock)
        release_ham_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLDUPWRITE, keydata, keylen,
                        dataData, datalen, MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

void hm_close(bool abort)
{
    FUNC;
    ham_lock((char *) __PRETTY_FUNCTION__);
    if (config->transactions) {
        if (abort) {
            st = ham_env_close(env, HAM_AUTO_CLEANUP | HAM_TXN_AUTO_ABORT);
        } else {
            st = ham_env_close(env,
                               HAM_AUTO_CLEANUP | HAM_TXN_AUTO_COMMIT);
        }
    } else
        st = ham_env_close(env, HAM_AUTO_CLEANUP);
    if (st != HAM_SUCCESS)
        ham_error("ham_env_close failed");
    release_ham_lock();
    close_trees();
    if ( config->blockdata_io_type == FILE_IO ) {
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
    EFUNC;
    return;
}

int fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
               off_t offset, struct fuse_file_info *fi)
{
    int retcode = 0;
    ham_cursor_t *cursor;
    int res;
    struct stat stbuf;
    DAT *filedata;
    DDSTAT *ddstat;
    ham_key_t key;
    ham_record_t rec;

    FUNC;
    (void) offset;
    (void) fi;
    LDEBUG("Called fs_readdir with path %s", (char *) path);

    res = dbstat(path, &stbuf, 1);
    if (0 != res) {
        return -ENOENT;
    }
    if (0 == strcmp(path, "/.lessfs/locks")) {
        LDEBUG("fs_readdir : reading locks");
        locks_to_dir(buf, filler, fi);
    }

    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));
    st = ham_cursor_create(&cursor, db[DBDIRENT], txn, 0);
    if (st)
        ham_error("fs_readdir : failed to create cursor");

    key.data = &stbuf.st_ino;
    key.size = sizeof(unsigned long long);
    st = ham_cursor_find(cursor, &key, 0, 0);
    if (st == HAM_SUCCESS) {
        do {
            if ((st = ham_cursor_move(cursor, 0, &rec, 0)) != HAM_SUCCESS)
                die_dberr
                    ("btdelete_curkey : expected a value after a succesfull find");
            if (0 == memcmp(rec.data, key.data, key.size))
                continue;
            filedata = search_dbdata(DBP, rec.data, rec.size, NOLOCK);
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
        } while (HAM_SUCCESS ==
                 (st =
                  ham_cursor_move(cursor, &key, &rec,
                                  HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES)));
    }
    ham_cursor_close(cursor);
    release_ham_lock();
    LDEBUG("fs_readdir: return");
    return (retcode);
}

void fs_read_hardlink(struct stat stbuf, DDSTAT * ddstat, void *buf,
                      fuse_fill_dir_t filler, struct fuse_file_info *fi)
{
    ham_cursor_t *cursor;
    DINOINO dinoino;
    ham_key_t key;
    ham_record_t rec;

    FUNC;
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    LDEBUG("fs_read_hardlink : %llu", (unsigned long long) stbuf.st_ino);
    st = ham_cursor_create(&cursor, db[DBL], txn, 0);
    if (st)
        ham_error("ham_cursor_create");

    dinoino.dirnode = stbuf.st_ino;
    dinoino.inode = ddstat->stbuf.st_ino;

    key.data = &dinoino;
    key.size = sizeof(DINOINO);
    st = ham_cursor_find(cursor, &key, 0, 0);
    if (st == HAM_SUCCESS) {
        do {
            if ((st = ham_cursor_move(cursor, 0, &rec, 0)) != HAM_SUCCESS) {
                LINFO("fs_read_hardlink : Key without value dinode.\n");
                goto end_error;
            }
            memcpy(&ddstat->filename, rec.data, rec.size);
            ddstat->filename[rec.size] = 0;
            fil_fuse_info(ddstat, buf, filler, fi);
            ddstat->filename[0] = 0;
        } while (HAM_SUCCESS ==
                 (st =
                  ham_cursor_move(cursor, &key, &rec,
                                  HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES)));
    }
  end_error:
    ham_cursor_close(cursor);
    EFUNC;
    return;
}

int bt_entry_exists(int database, void *parent, int parentlen, void *value,
                    int vallen)
{
    int res = 0;
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;

    FUNC;

    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));
    ham_lock((char *) __PRETTY_FUNCTION__);
    st = ham_cursor_create(&cursor, db[database], txn, 0);
    if (st)
        ham_error("bt_entry_exists : ham_cursor_create");
    key.data = parent;
    key.size = parentlen;
    st = ham_cursor_find(cursor, &key, 0, 0);
    if (st == HAM_SUCCESS) {
        do {
            if ((st = ham_cursor_move(cursor, 0, &rec, 0)) != HAM_SUCCESS)
                ham_error("bt_entry_exists");
            if (0 != memcmp(rec.data, value, vallen))
                continue;
            res = 1;
        } while (0 ==
                 (st =
                  ham_cursor_move(cursor, &key, &rec,
                                  HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES)));
    }
    ham_cursor_close(cursor);
    release_ham_lock();
    EFUNC;
    return (res);
}

unsigned long long has_nodes(unsigned long long inode)
{
    unsigned long long res = 0;
    ham_cursor_t *cursor;
    unsigned long long filenode;
    DAT *filedata;
    bool dotdir = 0;
    DDSTAT *ddstat;
    unsigned long long keyval;
    ham_key_t key;
    ham_record_t rec;

    FUNC;
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    ham_lock((char *) __PRETTY_FUNCTION__);
    st = ham_cursor_create(&cursor, db[DBDIRENT], txn, 0);
    if (st != HAM_SUCCESS) {
        return (-ENOENT);
    }

    key.data = &inode;
    key.size = sizeof(unsigned long long);
    st = ham_cursor_find(cursor, &key, 0, 0);
    if (st == HAM_SUCCESS) {
        do {
            if ((st = ham_cursor_move(cursor, 0, &rec, 0)) != HAM_SUCCESS)
                ham_error("hasnodes");
            memcpy(&keyval, key.data, sizeof(unsigned long long));
            if (keyval != inode) {
                break;
            }
            memcpy(&filenode, rec.data, rec.size);
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
            dotdir = 0;
            if (0 != res)
                break;
        } while (0 ==
                 (st =
                  ham_cursor_move(cursor, &key, &rec,
                                  HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES)));
    }
    ham_cursor_close(cursor);
    release_ham_lock();
    LDEBUG("inode %llu contains files", inode);
    EFUNC;
    return (res);
}

void commit_transactions()
{
    if (config->transactions) {
        ham_lock((char *) __PRETTY_FUNCTION__);
        st = ham_txn_commit(txn, 0);
        if (st != HAM_SUCCESS)
            ham_error("start_flush_commit : failed to commit transaction");
        release_ham_lock();
    }
}

void abort_transactions()
{
    if (config->transactions) {
        ham_lock((char *) __PRETTY_FUNCTION__);
        st = ham_txn_abort(txn, 0);
        if (st != HAM_SUCCESS)
            ham_error("abort_transactions : failed to abort transaction");
        release_ham_lock();
    }
    start_transactions();
}

void start_transactions()
{
    if (config->transactions) {
        ham_lock((char *) __PRETTY_FUNCTION__);
#ifdef HAM_API_REVISION
        st = ham_txn_begin(&txn, env, "lessfs",NULL,0);
#else
        st = ham_txn_begin(&txn, db[0], 0);
#endif
        if (st != HAM_SUCCESS)
            ham_error("start_flush_commit : failed to start transaction");
        release_ham_lock();
    }
}

void start_flush_commit()
{
    unsigned long long lastoffset = 0;
    FUNC;
    if (config->transactions)
        lessfs_trans_stamp();
    if ( config->blockdata_io_type == FILE_IO ) {
        if (lastoffset != nextoffset) {
            LDEBUG("write nextoffset=%llu", nextoffset);
            bin_write_dbdata(DBU, config->nexthash, config->hashlen,
                             (unsigned char *) &nextoffset,
                             sizeof(unsigned long long));
            lastoffset = nextoffset;
        }
        fsync(fdbdta);
    }
    sync_all_filesizes();
    EFUNC;
    return;
}

void end_flush_commit()
{
    FUNC;
    if (config->transactions) {
        commit_transactions();
        start_transactions();
    }
    EFUNC;
    return;
}

void hm_restart_truncation()
{
    ham_cursor_t *cursor;
    char *keystr = "TRNCTD";
    int keylen;
    pthread_t truncate_thread;
    struct truncate_thread_data trunc_data;
    ham_key_t key;
    ham_record_t rec;

    FUNC;
    if (!config->background_delete)
        return;
    keylen = strlen(keystr);

    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    st = ham_cursor_create(&cursor, db[FREELIST], txn, 0);
    if (st != HAM_SUCCESS) {
        LWARNING("Failed to create cursor : cm_restart_truncation");
        release_ham_lock();
        return;
    }
    key.data = keystr;
    key.size = strlen(keystr);
    st = ham_cursor_find(cursor, &key, 0, 0);
    if (st == HAM_SUCCESS) {
        /* traverse records */
        do {
            if ((st = ham_cursor_move(cursor, 0, &rec, 0)) != HAM_SUCCESS)
                ham_error("cm_restart_truncation");
            memcpy(&trunc_data, &rec.data, sizeof(trunc_data));
            if (0 == trunc_data.inode)
                break;
            LINFO("Resume truncation of inode %llu", trunc_data.inode);
            create_inode_note(trunc_data.inode);
            if ( config->blockdata_io_type == FILE_IO ) {
                if (0 !=
                    pthread_create(&truncate_thread, NULL,
                                   tc_truncate_worker,
                                   (void *) &trunc_data))
                    die_syserr();
            } else {
                if (0 !=
                    pthread_create(&truncate_thread, NULL,
                                   file_truncate_worker,
                                   (void *) &trunc_data))
                    die_syserr();
            }
            if (0 != pthread_detach(truncate_thread))
                die_syserr();
        } while (0 ==
                 (st =
                  ham_cursor_move(cursor, &key, &rec,
                                  HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES)));
    }
    ham_cursor_close(cursor);
    release_ham_lock();
    EFUNC;
    return;
}

void btbin_write_dbdata(int database, void *keydata, int keylen,
                        void *dataData, int datalen)
{
    ham_key_t key;              /* the structure for a key */
    ham_record_t record;        /* the structure for a record */

    FUNC;

    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    key.data = keydata;
    key.size = keylen;
    record.size = datalen;
    record.data = dataData;
    st = ham_db_insert(db[database], txn, &key, &record, HAM_DUPLICATE);
    if (st != HAM_SUCCESS)
        ham_error("bin_write_dbdata");
    release_ham_lock();
    if (config->replication == 1 && config->replication_role == 0) {
        write_repl_data(database, REPLWRITE, keydata, keylen, dataData,
                        datalen, MAX_ALLOWED_THREADS - 2);
    }
    EFUNC;
}

INUSE *get_offset_reclaim(unsigned long long mbytes,
                          unsigned long long offset)
{
    unsigned long long asize;
    INUSE *inuse = NULL;
    ham_cursor_t *cursor;
    ham_key_t hamkey;
    ham_record_t hamrec;
    time_t thetime;
    FREEBLOCK *freeblock;
    bool hasone = 0;

    FUNC;
    thetime = time(NULL);
    LDEBUG("get_offset : search for %llu blocks on the freelist", mbytes);

    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&hamkey, 0, sizeof(hamkey));
    memset(&hamrec, 0, sizeof(hamrec));

    /* create a new cursor */
    st = ham_cursor_create(&cursor, db[FREELIST], txn, 0);
    if (st)
        ham_error("ham_cursor_create");

    hamkey.data = &mbytes;
    hamkey.size = sizeof(unsigned long long);
    while (1) {
        while (1) {
            st = ham_cursor_move(cursor, &hamkey, &hamrec,
                                 HAM_CURSOR_NEXT);
            if (st != HAM_SUCCESS) {
                /* reached end of the database? */
                if (st == HAM_KEY_NOT_FOUND) {
                    break;
                } else
                    exit(EXIT_SYSTEM);
            }
            if (hamkey.size == strlen("TRNCTD"))
                continue;
            memcpy(&asize, hamkey.data, sizeof(unsigned long long));
            if (asize < mbytes)
                continue;

            if (hamrec.size < sizeof(FREEBLOCK)) {
                memcpy(&offset, hamrec.data, hamrec.size);
                if (config->replication == 1
                    && config->replication_role == 0) {
                    write_repl_data(FREELIST, REPLDELETECURKEY,
                                    (char *) &asize,
                                    sizeof(unsigned long long),
                                    (char *) &offset,
                                    sizeof(unsigned long long),
                                    MAX_ALLOWED_THREADS - 2);
                }
            } else {
                freeblock = (FREEBLOCK *) hamrec.data;
                if (freeblock->reuseafter > thetime && hasone == 0) {
                    s_free(freeblock);
                    hasone = 1;
                    continue;
                }
                if (freeblock->reuseafter > thetime && hasone == 1)
                    LINFO
                        ("get_offset_reclaim : early space reclaim, low on space");
                offset = freeblock->offset;
                if (config->replication == 1
                    && config->replication_role == 0) {
                    write_repl_data(FREELIST, REPLDELETECURKEY,
                                    (char *) &asize,
                                    sizeof(unsigned long long),
                                    (char *) freeblock, sizeof(FREEBLOCK),
                                    MAX_ALLOWED_THREADS - 2);
                }
            }
            inuse = s_zmalloc(sizeof(INUSE));
            inuse->allocated_size = asize * 512;
            LDEBUG
                ("get_offset_reclaim : asize=%llu, inuse->allocated_size %llu",
                 asize, inuse->allocated_size);
            inuse->offset = offset;
            st = ham_cursor_erase(cursor, 0);
            if (st != HAM_SUCCESS) {
                die_dataerr("get_offset_reclaim, failed to erase key");
            }
            break;
        }
        if (inuse)
            break;
        if (!hasone)
            break;
        ham_cursor_close(cursor);
        st = ham_cursor_create(&cursor, db[FREELIST], txn, 0);
        if (st)
            ham_error("ham_cursor_create");
    }
    ham_cursor_close(cursor);
    release_ham_lock();
    EFUNC;
    return inuse;
}

unsigned long long get_offset_fast(unsigned long long mbytes)
{
    unsigned long long offset;
    bool found = 0;
    ham_cursor_t *cursor;
    ham_key_t hamkey;
    ham_record_t hamrec;
    time_t thetime;
    FREEBLOCK *freeblock;


    FUNC;
    thetime = time(NULL);
    LDEBUG("get_offset : search for %llu blocks on the freelist", mbytes);

    ham_lock((char *) __PRETTY_FUNCTION__);
    memset(&hamkey, 0, sizeof(hamkey));
    memset(&hamrec, 0, sizeof(hamrec));

    /* create a new cursor */
    st = ham_cursor_create(&cursor, db[FREELIST], txn, 0);
    if (st)
        ham_error("ham_cursor_create");

    hamkey.data = &mbytes;
    hamkey.size = sizeof(unsigned long long);

    /* Move to first matching key */
    st = ham_cursor_find(cursor, &hamkey, 0, 0);
    if (st != HAM_SUCCESS) {
        LDEBUG("get_offset : key not found in database %i.\n", FREELIST);
        goto end_error;
    }

    do {
        /* Get the value from this key */
        if ((st = ham_cursor_move(cursor, 0, &hamrec, 0)) != HAM_SUCCESS) {
            break;
        }
        if (hamrec.size < sizeof(FREEBLOCK)) {
            memcpy(&offset, hamrec.data, hamrec.size);
            if (config->replication == 1 && config->replication_role == 0) {
                write_repl_data(FREELIST, REPLDELETECURKEY,
                                (char *) &mbytes,
                                sizeof(unsigned long long),
                                (char *) &offset,
                                sizeof(unsigned long long),
                                MAX_ALLOWED_THREADS - 2);
            }
        } else {
            freeblock = (FREEBLOCK *) hamrec.data;
            if (freeblock->reuseafter > thetime) {
                continue;
            }
            offset = freeblock->offset;
            if (config->replication == 1 && config->replication_role == 0) {
                write_repl_data(FREELIST, REPLDELETECURKEY,
                                (char *) &mbytes,
                                sizeof(unsigned long long),
                                (char *) freeblock, sizeof(FREEBLOCK),
                                MAX_ALLOWED_THREADS - 2);
            }
        }
        found = 1;
        LDEBUG
            ("get_offset : reclaim %llu blocks on the freelist at offset %llu",
             mbytes, offset);
        st = ham_cursor_erase(cursor, 0);
        if (st != HAM_SUCCESS) {
            die_dataerr("get_offset, failed to erase key");
        }
        break;
    } while (HAM_SUCCESS ==
             (st =
              ham_cursor_move(cursor, &hamkey, &hamrec,
                              HAM_CURSOR_NEXT | HAM_ONLY_DUPLICATES)));
  end_error:
    ham_cursor_close(cursor);
    release_ham_lock();
    if (!found)
        offset = (0 - 1);
    EFUNC;
    return (offset);
}

void list_symlinks()
{
    unsigned long long inode;
    ham_key_t key;
    ham_record_t record;
    ham_cursor_t *cursor;


    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    st = ham_cursor_create(&cursor, db[DBS], txn, 0);
    if (st != HAM_SUCCESS) {
        fprintf(stderr, "ham_cursor_create() failed with error %d\n", st);
        exit(-1);
    }
    st = ham_cursor_move(cursor, &key, &record, HAM_CURSOR_FIRST);
    if (st != HAM_SUCCESS)
        goto end_error;

    do {
        memcpy(&inode, key.data, sizeof(unsigned long long));
        printf("%llu : %s\n", inode, (char *) record.data);
    } while (HAM_SUCCESS ==
             (st =
              ham_cursor_move(cursor, &key, &record, HAM_CURSOR_NEXT)));
  end_error:
    ham_cursor_close(cursor);
    return;
}

void list_hardlinks()
{
    unsigned long long inode;
    DINOINO dinoino;
    ham_key_t key;
    ham_record_t record;
    ham_cursor_t *cursor;


    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    st = ham_cursor_create(&cursor, db[DBL], txn, 0);
    if (st != HAM_SUCCESS) {
        fprintf(stderr, "ham_cursor_create() failed with error %d\n", st);
        exit(-1);
    }
    st = ham_cursor_move(cursor, &key, &record, HAM_CURSOR_FIRST);
    if (st != HAM_SUCCESS)
        goto end_error;

    do {
        if (key.size == sizeof(DINOINO)) {
            memcpy(&dinoino, key.data, sizeof(DINOINO));
            printf("dinoino %llu-%llu : inode %s\n", dinoino.dirnode,
                   dinoino.inode, (char *) record.data);
        } else {
            memcpy(&inode, key.data, sizeof(unsigned long long));
            memcpy(&dinoino, record.data, record.size);
            printf("inode %llu : %llu-%llu dinoino\n", inode,
                   dinoino.dirnode, dinoino.inode);
        }
    } while (HAM_SUCCESS ==
             (st =
              ham_cursor_move(cursor, &key, &record, HAM_CURSOR_NEXT)));
  end_error:
    ham_cursor_close(cursor);
    return;
}

// Empty stub, is never used with hamsterdb
void listdbu()
{
}

void listdta()
{
}

void flistdbu()
{
    char *asc_hash;
    INUSE *inuse;
    unsigned long rsize;
    unsigned long long nexto;

    ham_key_t key;
    ham_record_t record;
    ham_cursor_t *cursor;


    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    st = ham_cursor_create(&cursor, db[DBU], txn, 0);
    if (st != HAM_SUCCESS) {
        fprintf(stderr, "ham_cursor_create() failed with error %d\n", st);
        exit(-1);
    }
    st = ham_cursor_move(cursor, &key, &record, HAM_CURSOR_FIRST);
    if (st != HAM_SUCCESS)
        goto end_error;
    do {
        if (0 == memcmp(config->nexthash, key.data, config->hashlen)) {
            memcpy(&nexto, record.data, sizeof(unsigned long long));
            printf("\nnextoffset = %llu\n\n", nexto);
        } else {
            inuse = (INUSE *) record.data;
            asc_hash = ascii_hash((unsigned char *) key.data);
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
        }
    } while (HAM_SUCCESS ==
             (st =
              ham_cursor_move(cursor, &key, &record, HAM_CURSOR_NEXT)));
  end_error:
    ham_cursor_close(cursor);
    return;
}

void listdbp()
{
    DDSTAT *ddstat;
    DAT *data;
    unsigned long long inode;
    char *nfi = "NFI";
    char *seq = "SEQ";
    CRYPTO *crypto;

    ham_key_t key;
    ham_record_t record;
    ham_cursor_t *cursor;


    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    st = ham_cursor_create(&cursor, db[DBP], txn, 0);
    if (st != HAM_SUCCESS) {
        fprintf(stderr, "ham_cursor_create() failed with error %d\n", st);
        exit(-1);
    }
    st = ham_cursor_move(cursor, &key, &record, HAM_CURSOR_FIRST);
    if (st != HAM_SUCCESS)
        goto end_error;

    do {
        if (0 == memcmp(key.data, nfi, 3) || 0 == memcmp(key.data, seq, 3)) {
            memcpy(&inode, record.data, sizeof(unsigned long long));
            printf("%s : %llu\n", (char *) key.data, inode);
        } else {
            memcpy(&inode, key.data, sizeof(unsigned long long));
// FIX ME, POINTLESS SLOW
            data =
                search_dbdata(DBP, &inode, sizeof(unsigned long long),
                              LOCK);
            if (inode == 0) {
                crypto = (CRYPTO *) data->data;
            } else {
                ddstat = value_to_ddstat(data);
#ifdef x86_64
                printf
                    ("ddstat->filename %s \n      ->inode %lu  -> size %lu  -> real_size %llu time %lu\n",
                     ddstat->filename, ddstat->stbuf.st_ino,
                     ddstat->stbuf.st_size, ddstat->real_size,
                     ddstat->stbuf.st_atim.tv_sec);
#else
                printf
                    ("ddstat->filename %s \n      ->inode %llu -> size %llu -> real_size %llu time %lu\n",
                     ddstat->filename, ddstat->stbuf.st_ino,
                     ddstat->stbuf.st_size, ddstat->real_size,
                     ddstat->stbuf.st_atim.tv_sec);
#endif
                if (S_ISDIR(ddstat->stbuf.st_mode)) {
                    printf("      ->filename %s is a directory\n",
                           ddstat->filename);
                }
                if (S_ISLNK(ddstat->stbuf.st_mode)) {
                    printf("      ->filename |%s| is a symlink\n",
                           ddstat->filename);
                }

                ddstatfree(ddstat);
            }
            DATfree(data);
        }
    } while (HAM_SUCCESS ==
             (st =
              ham_cursor_move(cursor, &key, &record, HAM_CURSOR_NEXT)));
  end_error:
    ham_cursor_close(cursor);
    return;
}

void listdirent()
{
    unsigned long long dir;
    unsigned long long ent;

    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t record;

    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    st = ham_cursor_create(&cursor, db[DBDIRENT], txn, 0);
    if (st != HAM_SUCCESS) {
        fprintf(stderr, "ham_cursor_create() failed with error %d\n", st);
        exit(-1);
    }

    while (1) {
        st = ham_cursor_move(cursor, &key, &record, HAM_CURSOR_NEXT);
        if (st != HAM_SUCCESS) {
            /* reached end of the database? */
            if (st == HAM_KEY_NOT_FOUND) {
                break;
            } else
                exit(EXIT_SYSTEM);
        }
        memcpy(&dir, key.data, sizeof(dir));
        memcpy(&ent, record.data, sizeof(ent));
        printf("%llu:%llu\n", dir, ent);
    }
    ham_cursor_close(cursor);
}

void listdbb()
{
    char *asc_hash = NULL;
    unsigned long long inode;
    unsigned long long blocknr;

    ham_key_t key;
    ham_record_t record;
    ham_cursor_t *cursor;

    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    st = ham_cursor_create(&cursor, db[DBB], txn, 0);
    if (st != HAM_SUCCESS) {
        fprintf(stderr, "ham_cursor_create() failed with error %d\n", st);
        exit(-1);
    }
    st = ham_cursor_move(cursor, &key, &record, HAM_CURSOR_FIRST);
    if (st != HAM_SUCCESS)
        return;

    do {
        asc_hash = ascii_hash((unsigned char *) record.data);
        memcpy(&inode, key.data, sizeof(unsigned long long));
        memcpy(&blocknr, key.data + sizeof(unsigned long long),
               sizeof(unsigned long long));
        printf("%llu-%llu : %s\n", inode, blocknr, asc_hash);
        s_free(asc_hash);
    } while (HAM_SUCCESS ==
             (st =
              ham_cursor_move(cursor, &key, &record, HAM_CURSOR_NEXT)));
    return;
}


void listfree(int freespace_summary)
{
    unsigned long long mbytes;
    unsigned long long offset;
    unsigned long freespace=0;
    struct truncate_thread_data *trunc_data;
    FREEBLOCK *freeblock;

    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t record;

    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    st = ham_cursor_create(&cursor, db[FREELIST], txn, 0);
    if (st != HAM_SUCCESS) {
        fprintf(stderr, "ham_cursor_create() failed with error %d\n", st);
        exit(-1);
    }

    while (1) {
        st = ham_cursor_move(cursor, &key, &record, HAM_CURSOR_NEXT);
        if (st != HAM_SUCCESS) {
            /* reached end of the database? */
            if (st == HAM_KEY_NOT_FOUND) {
                break;
            } else
                exit(EXIT_SYSTEM);
        }
        if (key.size == strlen("TRNCTD")) {
            trunc_data = (struct truncate_thread_data *) record.data;
            printf
                ("Truncation not finished for inode %llu : start %llu -> end %llu size %llu\n",
                 trunc_data->inode, trunc_data->blocknr,
                 trunc_data->lastblocknr,
                 (unsigned long long) trunc_data->stbuf.st_size);
        } else {
            if (record.size == 0) {
                fprintf(stderr, "No value for key");
                exit(EXIT_SYSTEM);
            }
            memcpy(&mbytes, key.data, sizeof(unsigned long long));
            freespace+=(mbytes * 512);
            if (record.size < sizeof(FREEBLOCK)) {
                memcpy(&offset, record.data, sizeof(unsigned long long));
                if ( !freespace_summary) {
                   printf("offset = %llu : blocks = %llu : bytes = %llu\n",
                          offset, mbytes, mbytes * 512);
                }
            } else {
                freeblock = (FREEBLOCK *) record.data;
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
    ham_cursor_close(cursor);
    return;
}
#endif
