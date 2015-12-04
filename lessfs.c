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

#define FUSE_USE_VERSION 26
#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifndef LFATAL
#include "lib_log.h"
#endif

#define LFSVERSION "1.7.0"

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
#include "lib_safe.h"
#include "lib_common.h"
#ifdef LZO
#include "lib_lzo.h"
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

#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include "lib_cfg.h"
#include "lib_str.h"
#include "lib_repl.h"
#include "retcodes.h"
#ifdef ENABLE_CRYPTO
#include "lib_crypto.h"
#endif

#include "commons.h"
#ifdef BERKELEYDB
extern char *bdb_lockedby;
#endif

unsigned long working = 0;
unsigned long sequence = 0;

void segvExit()
{
    LFATAL("Exit caused by segfault!, exitting\n");
    db_close(0);
    exit(EXIT_OK);
}

void normalExit()
{
    LFATAL("Exit signal received, exitting\n");
    db_close(0);
    exit(EXIT_OK);
}

void libSafeExit()
{
    db_close(0);
    LFATAL("Exit signal received from lib_safe, exitting\n");
    exit(EXIT_OK);
}

int check_path_sanity(const char *path)
{
    char *name;
    char *p;
    char *q;
    int retcode = 0;

    FUNC;
    LDEBUG("check_path_sanity : %s", path);

    name = s_strdup((char *) path);
    p = name;
    q = p;
    while (1) {
        p = strrchr(q, '/');
        if (p == NULL)
            break;
        q = p;
        q++;
        p[0] = 0;
        if (q) {
            if (strlen(q) > 255) {
                LDEBUG("check_path_sanity : error q>255");
                retcode = -ENAMETOOLONG;
            }
        }
        q = p--;
    }
    s_free(name);
    EFUNC;
    return (retcode);
}

void dbsync()
{
#ifndef BERKELEYDB
#ifndef HAMSTERDB
    tcbdbsync(dbdirent);
    tchdbsync(dbu);
    tchdbsync(dbb);
    tchdbsync(dbp);
#endif
#endif
    if ( config->blockdata_io_type == FILE_IO ) {
        fsync(fdbdta);
    }
    if (config->replication == 1 && config->replication_role == 0) {
        fsync(frepl);
    }
}

static int lessfs_getattr(const char *path, struct stat *stbuf)
{
    int res = 0;
    char *bname, *sqstr;
    unsigned long sequence;
    static int last_sequence = 1;

    FUNC;
    LDEBUG("lessfs_getattr %s", path);
    res = check_path_sanity(path);
    if (res == -ENAMETOOLONG)
        return (res);

    get_global_lock((char *) __PRETTY_FUNCTION__);
    if (0 == strncmp("/.lessfs/locks/", path, strlen("/.lessfs/locks/"))) {
        bname = s_basename((char *) path);
        get_inode_lock((char *) __PRETTY_FUNCTION__);
        res = lckname_to_stat(bname, stbuf);
        release_inode_lock();
        s_free(bname);
        goto end;
    }
    res = dbstat(path, stbuf, 1);
    if (!inode_isnot_locked(stbuf->st_ino))
        stbuf->st_mode ^= S_ISVTX;
    LDEBUG("lessfs_getattr : %s size %llu : result %i", path,
           (unsigned long long) stbuf->st_size, res);
    if (0 == strcmp("/.lessfs/lessfs_stats", path)) {
        s_free(config->lfsstats);
        config->lfsstats = lessfs_stats();
        stbuf->st_size = strlen(config->lfsstats);
    }
    if (0 == strcmp("/.lessfs/replication/enabled", path))
        stbuf->st_size = 2;
    if (0 == strcmp("/.lessfs/replication/backlog", path))
        stbuf->st_size = 2;
    if (0 == strcmp("/.lessfs/replication/rotate_replog", path))
        stbuf->st_size = 2;
    if (0 == strcmp("/.lessfs/replication/sequence", path)) {
        sequence = get_sequence();
        sqstr = as_sprintf(__FILE__, __LINE__, "%lu", sequence);
        // There is no such thing as invalidate cache for the
        // High level API. This is a crude hack to make sure that
        // the content is not read from cache.
        stbuf->st_size = strlen(sqstr) + last_sequence;
        last_sequence++;
        if (last_sequence > 2)
            last_sequence = 1;
        s_free(sqstr);
    }
  end:
    release_global_lock();
    return (res);
}

static int lessfs_access(const char *path, int mask)
{
    int res = 0;
    FUNC;
// Empty stub
    return (res);
}

static int lessfs_readlink(const char *path, char *buf, size_t size)
{
    int res = 0;

    FUNC;

    LDEBUG("lessfs_readlink : size = %i", (int) size);
    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = fs_readlink(path, buf, size);
    release_global_lock();
    return (res);
}


static int lessfs_readdir(const char *path, void *buf,
                          fuse_fill_dir_t filler, off_t offset,
                          struct fuse_file_info *fi)
{
    int retcode;
    get_global_lock((char *) __PRETTY_FUNCTION__);
    retcode = fs_readdir(path, buf, filler, offset, fi);
    release_global_lock();
    return (retcode);
}

static int lessfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
    int retcode = 0;
    char *dname;
    struct stat stbuf;
    time_t thetime;

    FUNC;
    if (config->replication == 1 && config->replication_role == 1) {
        LFATAL("lessfs_mknod");
        return (-EPERM);
    }
    get_global_lock((char *) __PRETTY_FUNCTION__);
    thetime = time(NULL);
    dname = s_dirname((char *) path);
    retcode = dbstat(dname, &stbuf, 0);
    LDEBUG("lessfs_mknod : dbstat returns %i", retcode);
    if (0 == retcode) {
        dbmknod(path, mode, NULL, rdev);
        stbuf.st_ctim.tv_sec = thetime;
        stbuf.st_ctim.tv_nsec = 0;
        stbuf.st_mtim.tv_sec = thetime;
        stbuf.st_mtim.tv_nsec = 0;
        retcode = update_stat(dname, &stbuf);
        LDEBUG("lessfs_mknod : update_stat returns %i", retcode);
        s_free(dname);
    }
    release_global_lock();
    return (retcode);
}

static int lessfs_mkdir(const char *path, mode_t mode)
{
    int ret;
    if (config->replication == 1 && config->replication_role == 1) {
        return (-EPERM);
    }
    get_global_lock((char *) __PRETTY_FUNCTION__);
    ret = fs_mkdir(path, mode);
    release_global_lock();
    return (ret);
}

static int lessfs_unlink(const char *path)
{
    int res;
    struct stat *stbuf;
    FUNC;

    LDEBUG("lessfs_unlink : %s", path);
    if (config->replication == 1 && config->replication_role == 1) {
        return (-EPERM);
    }
// /.lessfs_stats can not be deleted
    if (0 == strcmp(path, "/.lessfs/replication/rotate_replog"))
        return (0);
    if (0 == strcmp(path, "/.lessfs/replication/enabled"))
        return (0);
    if (0 == strcmp(path, "/.lessfs/replication/backlog"))
        return (0);
    if (0 == strcmp(path, "/.lessfs/lessfs_stats"))
        return (0);
    if (0 == strncmp(path, "/.lessfs/locks", strlen("/.lessfs/locks")))
        return (0);
    stbuf = s_malloc(sizeof(struct stat));
    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = dbstat(path, stbuf, 0);
    if (res != 0) {
        release_global_lock();
        s_free(stbuf);
        return (res);
    }
    write_lock((char *) __PRETTY_FUNCTION__);
    flush_wait(stbuf->st_ino);
    purge_read_cache(stbuf->st_ino, 1, (char *) __PRETTY_FUNCTION__);

    meta_lock((char *) __PRETTY_FUNCTION__);
    update_filesize_onclose(stbuf->st_ino);
    tctreeout(metatree, &stbuf->st_ino, sizeof(unsigned long long));
    release_meta_lock();

    if (config->blockdatabs) {
        res = db_unlink_file(path);
    } else {
        res = file_unlink_file(path);
    }
    if (config->relax == 0)
        dbsync();
    invalidate_p2i((char *) path);
    release_write_lock();
    release_global_lock();
    s_free(stbuf);
    if (config->nospace == ENOSPC)
        config->nospace = -ENOSPC;
    EFUNC;
    return res;
}

static int lessfs_rmdir(const char *path)
{
    int res;
    if (config->replication == 1 && config->replication_role == 1) {
        return (-EPERM);
    }
    if (0 == strncmp(path, "/.lessfs/locks", strlen("/.lessfs/locks")))
        return (0);
    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = fs_rmdir(path);
    invalidate_p2i((char *) path);
    release_global_lock();
    return (res);

}

static int lessfs_symlink(const char *from, const char *to)
{
    int res = 0;

    FUNC;
    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = fs_symlink((char *) from, (char *) to);
    invalidate_p2i((char *) from);
    release_global_lock();
    return (res);
}

static int lessfs_rename(const char *from, const char *to)
{
    int res = 0;
    struct stat stbuf;

    FUNC;
    LDEBUG("lessfs_rename : from %s , to %s", from, to);
    if (config->replication == 1 && config->replication_role == 1) {
        return (-EPERM);
    }
    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = dbstat(from, &stbuf, 0);
    if (res == 0) {
        update_filesize_onclose(stbuf.st_ino);
        if (stbuf.st_nlink > 1 && !S_ISDIR(stbuf.st_mode)) {
            res = fs_rename_link(from, to, stbuf);
        } else {
            res = fs_rename(from, to, stbuf);
        }
    }
    if (S_ISDIR(stbuf.st_mode)) {
        erase_p2i();
    } else {
       invalidate_p2i((char *) from);
       invalidate_p2i((char *) to);
    }
    release_global_lock();
    return (res);
}

static int lessfs_link(const char *from, const char *to)
{
    int res = 0;

    FUNC;
    if (config->replication == 1 && config->replication_role == 1) {
        return (-EPERM);
    }
    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = fs_link((char *) from, (char *) to);
    release_global_lock();
    return (res);
}

static int lessfs_chmod(const char *path, mode_t mode)
{
    int res;
    time_t thetime;
    struct stat stbuf;
    const char *data;
    int vsize;
    MEMDDSTAT *memddstat;
    DAT *ddbuf;

    FUNC;
    if (config->replication == 1 && config->replication_role == 1) {
        LFATAL("lessfs_chmod");
        return (-EPERM);
    }
    LDEBUG("lessfs_chmod : %s", path);
    get_global_lock((char *) __PRETTY_FUNCTION__);
    thetime = time(NULL);
    res = dbstat(path, &stbuf, 0);
    if (res != 0)
        return (res);

//  Since we use the stickybit to indicate that the file is locked
//  during truncation and deletion we do not allow
//  to be set on a regular file in this situation.
    if (config->sticky_on_locked && S_ISREG(stbuf.st_mode)) {
        if (S_ISVTX == (S_ISVTX & stbuf.st_mode)) {
            stbuf.st_mode = stbuf.st_mode ^ S_ISVTX;
        }
    }

    meta_lock((char *) __PRETTY_FUNCTION__);
    data = tctreeget(metatree, &stbuf.st_ino,
                     sizeof(unsigned long long), &vsize);
    if (data) {
        memddstat = (MEMDDSTAT *) data;
        memddstat->stbuf.st_ctim.tv_sec = thetime;
        memddstat->stbuf.st_ctim.tv_nsec = 0;
        memddstat->stbuf.st_mode = mode;
        memddstat->updated = 1;
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &stbuf.st_ino,
                  sizeof(unsigned long long), (void *) ddbuf->data,
                  ddbuf->size);
        cache_p2i((char *) path, &memddstat->stbuf);
        release_meta_lock();
        DATfree(ddbuf);
    } else {
        stbuf.st_mode = mode;
        stbuf.st_ctim.tv_sec = thetime;
        stbuf.st_ctim.tv_nsec = 0;
        release_meta_lock();
        res = update_stat((char *) path, &stbuf);
    }
    release_global_lock();
    return (res);
}

static int lessfs_chown(const char *path, uid_t uid, gid_t gid)
{
    int res;
    time_t thetime;
    struct stat stbuf;
    const char *data;
    int vsize;
    MEMDDSTAT *memddstat;
    DAT *ddbuf;

    FUNC;
    if (config->replication == 1 && config->replication_role == 1) {
        return (-EPERM);
    }
    get_global_lock((char *) __PRETTY_FUNCTION__);
    thetime = time(NULL);
    invalidate_p2i((char *) path);
    res = dbstat(path, &stbuf, 0);
    if (res != 0)
        goto end_unlock;
    meta_lock((char *) __PRETTY_FUNCTION__);
    data = tctreeget(metatree, &stbuf.st_ino,
                     sizeof(unsigned long long), &vsize);
    if (data) {
        memddstat = (MEMDDSTAT *) data;
        memddstat->stbuf.st_ctim.tv_sec = thetime;
        memddstat->stbuf.st_ctim.tv_nsec = 0;
        memddstat->stbuf.st_uid = uid;
        memddstat->stbuf.st_gid = gid;
        memddstat->updated = 1;
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &stbuf.st_ino,
                  sizeof(unsigned long long), (void *) ddbuf->data,
                  ddbuf->size);
        cache_p2i((char *) path, &memddstat->stbuf);
        release_meta_lock();
        DATfree(ddbuf);
    } else {
        if (-1 != uid)
            stbuf.st_uid = uid;
        if (-1 != gid)
            stbuf.st_gid = gid;
        stbuf.st_ctim.tv_sec = thetime;
        stbuf.st_ctim.tv_nsec = 0;
        release_meta_lock();
        res = update_stat((char *) path, &stbuf);
    }
  end_unlock:
    release_global_lock();
    return (res);
}

static int lessfs_truncate(const char *path, off_t size)
{
    int res = 0;
    struct stat *stbuf;
    char *bname;
    if (config->replication == 1 && config->replication_role == 1) {
        if (0 != strcmp(path, "/.lessfs/replication/enabled"))
            return (-EPERM);
    }
    if (0 == strcmp(path, "/.lessfs/replication/sequence"))
        return (-EPERM);
    LDEBUG("lessfs_truncate : %s truncate to %llu", path, size);
    get_global_lock((char *) __PRETTY_FUNCTION__);
    bname = s_basename((char *) path);
    stbuf = s_malloc(sizeof(struct stat));
    res = dbstat(path, stbuf, 0);
    if (res != 0) {
        release_global_lock();
        s_free(stbuf);
        return (res);
    }
    if (S_ISDIR(stbuf->st_mode)) {
        release_global_lock();
        s_free(stbuf);
        return (-EISDIR);
    }
    /* Flush the blockcache before we continue. */
    create_inode_note(stbuf->st_ino);
    write_lock((char *) __PRETTY_FUNCTION__);
    flush_wait(stbuf->st_ino);
    purge_read_cache(stbuf->st_ino, 1, (char *) __PRETTY_FUNCTION__);
    if (size < stbuf->st_size) {
        if (config->blockdatabs) {
            res = db_fs_truncate(stbuf, size, bname, 0);
        } else {
            res = file_fs_truncate(stbuf, size, bname, 0);
        }
    } else {
        LDEBUG("lessfs_truncate : %s only change size to %llu", path,
               size);
        update_filesize_cache(stbuf, size);
        delete_inode_note(stbuf->st_ino);
    }
    invalidate_p2i((char *) path);
    release_write_lock();
    s_free(stbuf);
    s_free(bname);
    release_global_lock();
    if (config->nospace == ENOSPC)
        config->nospace = -ENOSPC;
    return (res);
}

static int lessfs_utimens(const char *path, const struct timespec ts[2])
{
    int res = 0;
    struct stat stbuf;
    DDSTAT *ddstat;
    DAT *ddbuf;
    DAT *dskdata;
    const char *data;
    int vsize;
    MEMDDSTAT *memddstat;

    FUNC;
    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = dbstat(path, &stbuf, 0);
    if (res != 0)
        goto out;
    data = tctreeget(metatree, &stbuf.st_ino,
                     sizeof(unsigned long long), &vsize);
    if (data) {
        memddstat = (MEMDDSTAT *) data;
        memddstat->stbuf.st_atim.tv_sec = ts[0].tv_sec;
        memddstat->stbuf.st_atim.tv_nsec = ts[0].tv_nsec;
        memddstat->stbuf.st_mtim.tv_sec = ts[1].tv_sec;
        memddstat->stbuf.st_mtim.tv_nsec = ts[1].tv_nsec;
        memddstat->updated = 1;
        ddbuf = create_mem_ddbuf(memddstat);
        tctreeput(metatree, &stbuf.st_ino,
                  sizeof(unsigned long long), (void *) ddbuf->data,
                  ddbuf->size);
        cache_p2i((char *) path, &memddstat->stbuf);
        DATfree(ddbuf);
    } else {
        dskdata =
            search_dbdata(DBP, &stbuf.st_ino, sizeof(unsigned long long),
                          LOCK);
        if (dskdata == NULL) {
#ifdef x86_64
            die_dataerr("Failed to find file %lu", stbuf.st_ino);
#else
            die_dataerr("Failed to find file %llu", stbuf.st_ino);
#endif
        }
        ddstat = value_to_ddstat(dskdata);
        ddstat->stbuf.st_atim.tv_sec = ts[0].tv_sec;
        ddstat->stbuf.st_atim.tv_nsec = ts[0].tv_nsec;
        ddstat->stbuf.st_mtim.tv_sec = ts[1].tv_sec;
        ddstat->stbuf.st_mtim.tv_nsec = ts[1].tv_nsec;
        ddbuf =
            create_ddbuf(ddstat->stbuf, ddstat->filename,
                         ddstat->real_size);
        bin_write_dbdata(DBP, &stbuf.st_ino, sizeof(unsigned long long),
                         (void *) ddbuf->data, ddbuf->size);
        cache_p2i((char *) path, &ddstat->stbuf);
        DATfree(dskdata);
        DATfree(ddbuf);
        ddstatfree(ddstat);
    }
  out:
    release_global_lock();
    return (res);
}

unsigned long long get_real_size(unsigned long long inode)
{
    DAT *data;
    DDSTAT *ddstat;
    unsigned long long real_size = 0;

    data = search_dbdata(DBP, &inode, sizeof(unsigned long long), LOCK);
    if (NULL == data) {
        return (real_size);
    }
    ddstat = value_to_ddstat(data);
    DATfree(data);
    real_size = ddstat->real_size;
    ddstatfree(ddstat);
    return (real_size);
}

static int lessfs_open(const char *path, struct fuse_file_info *fi)
{
    struct stat stbuf;
    char *bname;
    DAT *ddbuf;
    const char *dataptr;
    unsigned long long blocknr = 0;
    MEMDDSTAT *memddstat;
    unsigned long long inode;
    int vsize;

    int res = 0;

    FUNC;
    LDEBUG("lessfs_open : %s strlen %i : uid %u", path,
           strlen((char *) path), fuse_get_context()->uid);

    get_global_lock((char *) __PRETTY_FUNCTION__);
    res = dbstat(path, &stbuf, 0);
    if (res == -ENOENT) {
        fi->fh = 0;
        stbuf.st_mode = fi->flags;
        stbuf.st_uid = fuse_get_context()->uid;
        stbuf.st_gid = fuse_get_context()->gid;
        LDEBUG("lessfs_open %s is a new file", path);
    } else {
        fi->fh = stbuf.st_ino;
        inode = stbuf.st_ino;
        bname = s_basename((char *) path);
//  Check if we have already something in cache for this inode
        meta_lock((char *) __PRETTY_FUNCTION__);
        dataptr =
            tctreeget(metatree, &stbuf.st_ino,
                      sizeof(unsigned long long), &vsize);
        if (dataptr == NULL) {
            blocknr--;          /* Invalid block in cache */
//  Initialize the cache
            memddstat = s_malloc(sizeof(MEMDDSTAT));
            memcpy(&memddstat->stbuf, &stbuf, sizeof(struct stat));
            memcpy(&memddstat->filename, bname, strlen(bname) + 1);
            memddstat->blocknr = blocknr;
            memddstat->updated = 0;
            memddstat->real_size = get_real_size(stbuf.st_ino);
            memddstat->opened = 1;
            memddstat->stbuf.st_atim.tv_sec = time(NULL);
            memddstat->stbuf.st_atim.tv_nsec = 0;
            LDEBUG("lessfs_open : initial open memddstat->opened = %u",
                   memddstat->opened);
            LDEBUG("memddstat->stbuf.st_atim.tv_sec = %lu",
                   memddstat->stbuf.st_atim.tv_sec);
            ddbuf = create_mem_ddbuf(memddstat);
            tctreeput(metatree, &inode, sizeof(unsigned long long),
                      (void *) ddbuf->data, ddbuf->size);
            DATfree(ddbuf);
            memddstatfree(memddstat);
        } else {
            memddstat = (MEMDDSTAT *) dataptr;
            memddstat->opened++;
            memddstat->stbuf.st_atim.tv_sec = time(NULL);
            memddstat->stbuf.st_atim.tv_nsec = 0;
            LDEBUG("lessfs_open : repeated open ddstat->opened = %u %lu",
                   memddstat->opened, memddstat->stbuf.st_atim.tv_sec);
            ddbuf = create_mem_ddbuf(memddstat);
            tctreeput(metatree, &inode, sizeof(unsigned long long),
                      (void *) ddbuf->data, ddbuf->size);
            DATfree(ddbuf);
        }
        release_meta_lock();
        s_free(bname);
    }
    release_global_lock();
    EFUNC;
    return (res);
}

static int lessfs_read(const char *path, char *buf, size_t size,
                       off_t offset, struct fuse_file_info *fi)
{
    unsigned long long blocknr;
    size_t done = 0;
    size_t got = 0;
    size_t block_offset = 0;
    struct stat stbuf;
    unsigned long sequence;

    FUNC;

    get_global_lock((char *) __PRETTY_FUNCTION__);
    write_lock((char *) __PRETTY_FUNCTION__);
    memset(buf, 0, size);
    LDEBUG("lessfs_read called offset : %llu, size bytes %llu",
           (unsigned long long) offset, (unsigned long long) size);
    if (fi->fh == 10) {
        strncpy(buf, &config->lfsstats[offset], size);
        done = size;
        goto end_error;
    }
    if (fi->fh == 14) {
        sprintf(buf, "%i\n", config->replication_enabled);
        done = size;
        goto end_error;
    }
    if (fi->fh == 15) {
        sprintf(buf, "%i\n", config->replication_backlog);
        done = size;
        goto end_error;
    }
    if (fi->fh == 16) {
        sequence = get_sequence();
        memset(buf, 0, size);
        sprintf(buf, "%lu\n", sequence);
        done = size;
        goto end_error;
    }
    if (fi->fh == 17) {
        sprintf(buf, "%u\n", 0);
        done = size;
        goto end_error;
    }
// Change this to creating a buffer that is allocated once.
// Disable reads beyond EOF
    if (get_realsize_fromcache(fi->fh, &stbuf)) {
        LDEBUG("lessfs_read : get_realsize_fromcache returns %llu",
               (unsigned long long) stbuf.st_size);
        if (offset + size > stbuf.st_size) {
            if (offset > stbuf.st_size)
                goto end_error;
            size = stbuf.st_size - offset;
        }
    }
    while (done < size) {
        blocknr = (offset + done) / BLKSIZE;
        block_offset = (done + offset) - (blocknr * BLKSIZE);
        if (config->blockdatabs) {
            got =
                readBlock(blocknr, buf + done, size - done, block_offset,
                          fi->fh);
        } else {
            got =
                file_read_block(blocknr, buf + done, size - done,
                                block_offset, fi->fh);
        }
        done = done + BLKSIZE - block_offset;
        if (done > size)
            done = size;
    }
  end_error:
    release_write_lock();
    release_global_lock();
    return (done);
}

CCACHEDTA *update_stored(unsigned char *hash, INOBNO * inobno,
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

    data = search_dbdata(DBDTA, hash, config->hashlen, LOCK);
    if (NULL == data) {
        s_free(ccachedta);
        return NULL;
    }
    uncompdata = lfsdecompress(data);
    LDEBUG("got uncompsize : %lu", uncompdata->size);
    memcpy(&ccachedta->data, uncompdata->data, uncompdata->size);
    ccachedta->datasize = uncompdata->size;
    ccachedta->updated = data->size;
    DATfree(uncompdata);
    DATfree(data);
    db_delete_stored(inobno);
    EFUNC;
    return ccachedta;
}

void add2cache(INOBNO * inobno, const char *buf, off_t offsetblock,
               size_t bsize)
{
    CCACHEDTA *ccachedta;
    uintptr_t p;
    int size;
    char *key;
    DAT *data;
    bool found = 0;


  pending:
    write_lock((char *) __PRETTY_FUNCTION__);
    key =
        (char *) tctreeget(readcachetree, (void *) inobno, sizeof(INOBNO),
                           &size);
    if (key) {
        memcpy(&p, key, size);
        ccachedta = get_ccachedta(p);
        while (ccachedta->pending == 1) {
            release_write_lock();
            goto pending;
        }
        ccachedta->creationtime = time(NULL);
        ccachedta->dirty = 1;
        memcpy((void *) &ccachedta->data[offsetblock], buf, bsize);
        if (ccachedta->datasize < offsetblock + bsize)
            ccachedta->datasize = offsetblock + bsize;
        update_filesize(inobno->inode, bsize, offsetblock,
                        inobno->blocknr);
        release_write_lock();
        return;
    }
    update_filesize(inobno->inode, bsize, offsetblock, inobno->blocknr);
    if (bsize < BLKSIZE) {
        data = check_block_exists(inobno);
        if (data) {
            create_hash_note(data->data);
            if (NULL == config->blockdatabs) {
                ccachedta =
                    file_update_stored(data->data, inobno, offsetblock);
            } else {
                ccachedta =
                    update_stored((unsigned char *) data->data, inobno,
                                  offsetblock);
            }
            delete_hash_note(data->data);
            if (ccachedta) {
                memcpy(&ccachedta->data[offsetblock], buf, bsize);
                if (ccachedta->datasize < offsetblock + bsize)
                    ccachedta->datasize = offsetblock + bsize;
                found = 1;
            }
            DATfree(data);
        }
    }

    if (!found) {
        ccachedta = s_zmalloc(sizeof(CCACHEDTA));
        memcpy(&ccachedta->data[offsetblock], buf, bsize);
        ccachedta->datasize = offsetblock + bsize;
        ccachedta->creationtime = time(NULL);
        ccachedta->dirty = 1;
        ccachedta->pending = 0;
        ccachedta->newblock = 1;
    }

    if (tctreernum(workqtree) * 2 > config->cachesize ||
        tctreernum(readcachetree) * 2 > config->cachesize) {
        flush_wait(0);
        purge_read_cache(0, 1, (char *) __PRETTY_FUNCTION__);
    }
    p = (uintptr_t) ccachedta;
// A full block can be processed right away.
    if (bsize == BLKSIZE) {
        ccachedta->pending = 1;
        tctreeput(workqtree, (void *) inobno, sizeof(INOBNO), (void *) &p,
                  sizeof(p));
    }
// When this is not a full block a separate thread is used.
    tctreeput(readcachetree, (void *) inobno, sizeof(INOBNO), (void *) &p,
              sizeof(p));
    release_write_lock();
    return;
}

void flush_rotate_replog()
{
   write_lock((char *) __PRETTY_FUNCTION__);
   flush_wait(0);
   purge_read_cache(0, 1, (char *) __PRETTY_FUNCTION__);
   if ( config->blockdata_io_type == CHUNK_IO ) sync();
   start_flush_commit();
   end_flush_commit();
   config->replication_last_rotated = time(NULL);
   rotate_replog();
   release_repl_lock();
   release_write_lock();
}

static int lessfs_write(const char *path, const char *buf, size_t size,
                        off_t offset, struct fuse_file_info *fi)
{
    off_t offsetblock;
    size_t bsize;
    size_t done = 0;
    INOBNO inobno;

    FUNC;
    if (config->nospace == ENOSPC)
        return (-ENOSPC);
    wait_inode_pending2(fi->fh);
    get_global_lock((char *) __PRETTY_FUNCTION__);
    if (fi->fh == 14) {
        if (0 == memcmp(buf, "0", 1)) {
            LINFO("Replication is now disabled");
            config->replication_enabled = 0;
        }
        if (0 == memcmp(buf, "1", 1)) {
            LINFO("Replication is now enabled");
            config->replication_enabled = 1;
        }
        goto end_exit;
    }
    if (fi->fh == 16) {
        size = -EPERM;
        goto end_exit;
    }
    if (fi->fh == 17) {
        if (config->replication == 1 && config->replication_role == 0) {
          if (0 == memcmp(buf, "1", 1)) {
            LINFO("Rotated replog after receiving rotation request"); 
            flush_rotate_replog();   
          }
        } else size = -EPERM;
        goto end_exit;
    }
    if (config->replication == 1 && config->replication_role == 1) {
        size = -EPERM;
        goto end_exit;
    }
    LDEBUG("lessfs_write offset %llu size %lu", offset,
           (unsigned long) size);
    inobno.inode = fi->fh;
    while (1) {
        inobno.blocknr = (offset + done) / BLKSIZE;
        offsetblock = offset + done - (inobno.blocknr * BLKSIZE);
        bsize = size - done;
        if (bsize + offsetblock > BLKSIZE)
            bsize = BLKSIZE - offsetblock;
        //Just put it in cache. We might need to fetch the block.
        //This is done by add2cache.
        add2cache(&inobno, buf + done, offsetblock, bsize);
        done = done + bsize;
        bsize = size - done;
        if (done >= size)
            break;
    }
  end_exit:
    release_global_lock();
    EFUNC;
    return (size);
}

static int lessfs_statfs(const char *path, struct statvfs *stbuf)
{
    int res;
    char *blockdatadir;

    if (config->blockdatabs) {
        res = statvfs(config->blockdata, stbuf);
    } else {
        blockdatadir = s_dirname(config->blockdata);
        res = statvfs(blockdatadir, stbuf);
        s_free(blockdatadir);
    }
    if (res == -1)
        return -errno;
    return 0;
}

static int lessfs_release(const char *path, struct fuse_file_info *fi)
{
    const char *dataptr;
    int vsize;
    MEMDDSTAT *memddstat;
    DAT *ddbuf;

    FUNC;
    LDEBUG("lessfs_release");
    get_global_lock((char *) __PRETTY_FUNCTION__);
    meta_lock((char *) __PRETTY_FUNCTION__);
    dataptr =
        tctreeget(metatree, &fi->fh, sizeof(unsigned long long), &vsize);
    if (dataptr) {
        memddstat = (MEMDDSTAT *) dataptr;
        if (memddstat->opened == 1) {
            // Update the filesize when needed.
            update_filesize_onclose(fi->fh);
            tctreeout(metatree, &fi->fh, sizeof(unsigned long long));
            invalidate_p2i((char *) path);
            LDEBUG("lessfs_release : Delete cache for %llu", fi->fh);
        } else {
            memddstat->opened--;
            ddbuf = create_mem_ddbuf(memddstat);
            LDEBUG("lessfs_release : %llu is now %u open", fi->fh,
                   memddstat->opened);
            tctreeput(metatree, &fi->fh, sizeof(unsigned long long),
                      (void *) ddbuf->data, ddbuf->size);
            DATfree(ddbuf);
        }
    }
    (void) path;
    (void) fi;
    release_meta_lock();
    release_global_lock();
    EFUNC;
    return 0;
}

static int lessfs_fsync(const char *path, int isdatasync,
                        struct fuse_file_info *fi)
{
    FUNC;
    (void) path;
    (void) isdatasync;
    get_global_lock((char *) __PRETTY_FUNCTION__);
    write_lock((char *) __PRETTY_FUNCTION__);
    flush_wait(fi->fh);
    purge_read_cache(fi->fh,0,(char *)__PRETTY_FUNCTION__);
    release_write_lock();
    /* Living on the edge, wait for pending I/O but do not flush the caches. */
    if (config->relax < 2) {
        update_filesize_onclose(fi->fh);
    }
    /* When config->relax == 1 dbsync is not called but the caches within lessfs
       are flushed making this a safer option. */
    if (config->relax == 0)
        dbsync();
    release_global_lock();
    EFUNC;
    return 0;
}

static void lessfs_destroy(void *unused __attribute__ ((unused)))
{
    FUNC;
    LINFO("Lessfs is going down, please wait");
    config->shutdown = 1;
    if (config->replication_enabled) {
        LFATAL("Wait for active replication threads to shutdown");
        while (!config->safe_down) {
            sleep(1);
        }
        LFATAL("No active replication threads, going down");
        sleep(1);
    }
    get_global_lock((char *) __PRETTY_FUNCTION__);
    write_lock((char *) __PRETTY_FUNCTION__);
    purge_read_cache(0, 1, (char *) __PRETTY_FUNCTION__);
    flush_wait(0);
    if (0 != tctreernum(workqtree) || 0 != tctreernum(readcachetree))
        LFATAL("Going down with data in queue, this should never happen");
    release_write_lock();
    release_global_lock();
    config->replication = 0;
    if (config->transactions)
        lessfs_trans_stamp();
    clear_dirty();
    db_close(0);
    s_free(config->lfsstats);
#ifdef ENABLE_CRYPTO
    if (config->encryptdata) {
        s_free(config->passwd);
        s_free(config->iv);
    }
#endif
    if (config->transactions)
        free(config->commithash);
    if (config->replication_watchdir)
        s_free(config->replication_watchdir);
    s_free(config);
#ifdef MEMTRACE
    leak_report();
#endif
    LFATAL("Lessfs is down");
    EFUNC;
    return;
}

/*  Return 0 when enough space s_free
    Return 1 at MIN_SPACE_FREE
    Return 2 at MIN_SPACE_CLEAN 
    Return 3 at MIN_SPACE_FREE+3% 
*/
int check_free_space(char *dbpath)
{
    float dfree;
    int mf, mc, sr;
    char *minfree;
    char *minclean;
    char *startreclaim;
    struct statfs sb;

    if (-1 == statfs(dbpath, &sb))
        die_dataerr("Failed to stat : %s", dbpath);
    dfree = (float) sb.f_blocks / (float) sb.f_bfree;
    dfree = 100 / dfree;
    minfree = getenv("MIN_SPACE_FREE");
    minclean = getenv("MIN_SPACE_CLEAN");
    if (minfree) {
        mf = atoi(minfree);
        if (mf > 100 || mf < 1)
            mf = 10;
    } else {
        mf = 10;
    }
    startreclaim=getenv("START_RECLAIM");
    if (startreclaim ) {
        sr = atoi(startreclaim);
        if (sr > 100 || sr < 1)
            sr = 15;
    }  else sr=1;
    if ( 100-dfree >= sr ) {
      config->reclaim=1;
    } else config->reclaim=0;
    /* Freeze has a higher prio then clean */
    if (dfree <= mf) {
        return (1);
    }
    if (minclean) {
        mc = atoi(minclean);
        if (mc > 100 || mc < 1)
            mc = 15;
    } else {
        mc = 0;
    }
    if (dfree <= mc) {
        return (2);
    }
    if (dfree <= mf + 1)
        return (3);
    return (0);
}

void freeze_nospace(char *dbpath)
{
    config->frozen = 1;
    get_global_lock((char *) __PRETTY_FUNCTION__);
    write_lock((char *) __PRETTY_FUNCTION__);
    flush_wait(0);
    purge_read_cache(0, 1, (char *) __PRETTY_FUNCTION__);
    if (config->nospace != 0) {
        release_write_lock();
        release_global_lock();
        if (config->nospace == 1) {
            config->nospace = -ENOSPC;
            LFATAL
                ("Filesystem for database %s has insufficient space to continue, ENOSPC",
                 dbpath);
            LINFO
                ("Writes will return ENOSPC when new blocks need to be allocated.");
        }
        return;
    } else {
        LFATAL
            ("Filesystem for database %s has insufficient space to continue, freezing I/O",
             dbpath);
        LFATAL("All IO is now suspended until space comes available.");
        LFATAL
            ("If no other options are available it should be safe to kill lessfs.");
    }
    while (1) {
        if (0 == check_free_space(dbpath)) {
            if (config->nospace == 0) {
                config->frozen = 0;
                release_write_lock();
                release_global_lock();
            }
            break;
        }
        sleep(1);
    }
    if (config->nospace == 0)
        LFATAL("Resuming IO : sufficient space available.");
}

void exec_clean_program()
{
    char *program;
    pid_t rpid;
    int res = 1;
    /* We can read and log output from the program. */
    int commpipe[2];

    FUNC;

    program = getenv("CLEAN_PROGRAM");
    if (NULL == program) {
        LINFO("CLEAN_PROGRAM is not defined");
        return;
    }

    LINFO("exec_clean_program : execute %s", program);
    if (pipe(commpipe) == -1) {
        LFATAL("Failed to open pipe.");
        return;
    }
    /* Attempt to fork and check for errors */
    if ((rpid = fork()) == -1) {
        LFATAL("Fork error. Exiting.\n");       /* something went wrong */
        return;
    }
    if (rpid) {
        /* A positive (non-negative) PID indicates the parent process */
        dup2(commpipe[0], 0);   /* Replace stdout with out side of the pipe */
        close(commpipe[1]);     /* Close unused side of pipe (in side) */
        setvbuf(stdout, (char *) NULL, _IONBF, 0);      /* Set non-buffered output on stdout */
        wait(&res);
    } else {
        /* A zero PID indicates that this is the child process */
        dup2(commpipe[1], 1);   /* Replace stdin with the in side of the pipe */
        close(commpipe[0]);     /* Close unused side of pipe (out side) */
        /* Replace the child fork with a new process */
        if (execl(program, program, NULL) == -1) {
            LFATAL("exec_clean_program : execl %s failed", program);
            exit(-1);
        }
    }
    return;
    EFUNC;
}

/* This thread does general housekeeping.
   For now it checks that there is enough diskspace s_free for
   the databases. If not it syncs the database, 
   closes them and freezes I/O. */
void *housekeeping_worker(void *arg)
{
    char *dbpath = NULL;
    int result;
    int count = 0;

    while (1) {
        while (count <= 6) {
            switch (count) {
#ifdef BERKELEYDB
            case 0:
                dbpath =
                    as_sprintf(__FILE__, __LINE__, "%s/metadata.db",
                               config->meta);
                count = 7;
                break;
            case 1:
                if ( config->blockdata_io_type == FILE_IO ) {
                    dbpath = s_strdup(config->blockdata);
                }
                count = 7;
                break;
#else
#ifdef HAMSTERDB
            case 0:
                dbpath =
                    as_sprintf(__FILE__, __LINE__, "%s/lessfs.db",
                               config->meta);
                count = 7;
                break;
            case 1:
                if ( config->blockdata_io_type == FILE_IO ) {
                    dbpath = s_strdup(config->blockdata);
                }
                count = 7;
                break;
#else
            case 0:
                dbpath =
                    as_sprintf(__FILE__, __LINE__, "%s/fileblock.tch",
                               config->fileblock);
                break;
            case 1:
                dbpath =
                    as_sprintf(__FILE__, __LINE__, "%s/blockusage.tch",
                               config->blockusage);
                break;
            case 2:
                dbpath =
                    as_sprintf(__FILE__, __LINE__, "%s/metadata.tcb",
                               config->meta);
                break;
            case 3:
                if (config->blockdatabs) {
                    dbpath =
                        as_sprintf(__FILE__, __LINE__, "%s/blockdata.tch",
                                   config->blockdata);
                } else {
                    dbpath = s_strdup(config->blockdata);
                }
                break;
            case 4:
                dbpath =
                    as_sprintf(__FILE__, __LINE__, "%s/symlink.tch",
                               config->symlink);
                break;
            case 5:
                dbpath =
                    as_sprintf(__FILE__, __LINE__, "%s/dirent.tcb",
                               config->dirent);
                break;
            case 6:
                dbpath =
                    as_sprintf(__FILE__, __LINE__, "%s/hardlink.tcb",
                               config->hardlink);
                break;
#endif
#endif
            default:
                break;
            }
            result = check_free_space(dbpath);
            switch (result) {
            case 1:
                freeze_nospace(dbpath);
                break;
            case 2:
                exec_clean_program();
                break;
            case 3:
                config->nospace = RECLAIM_AGRESSIVE;
            default:
                break;
            }
            s_free(dbpath);
            count++;
            sleep(config->inspectdiskinterval);
        }
        count = 0;
        repl_lock((char *) __PRETTY_FUNCTION__);
        release_repl_lock();
    }
    return NULL;
}

void show_lock_status(int csocket)
{
    char *msg;
    timeoutWrite(3, csocket,
                 "---------------------\n",
                 strlen("---------------------\n"));
    timeoutWrite(3, csocket,
                 "normally unset\n\n", strlen("normally unset\n\n"));
    if (0 != try_global_lock()) {
        msg =
            as_sprintf(__FILE__, __LINE__, "global_lock : 1 (set) by %s\n",
                       global_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_global_lock();
        timeoutWrite(3, csocket,
                     "global_lock : 0 (not set)\n",
                     strlen("global_lock : 0 (not set)\n"));
    }
    if (0 != try_meta_lock()) {
        msg =
            as_sprintf(__FILE__, __LINE__, "meta_lock : 1 (set) by %s\n",
                       meta_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_meta_lock();
        timeoutWrite(3, csocket,
                     "meta_lock : 0 (not set)\n",
                     strlen("meta_lock : 0 (not set)\n"));
    }
    if (0 != try_write_lock()) {
        msg =
            as_sprintf(__FILE__, __LINE__, "write_lock : 1 (set) by %s\n",
                       write_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_write_lock();
        timeoutWrite(3, csocket,
                     "write_lock : 0 (not set)\n",
                     strlen("write_lock : 0 (not set)\n"));
    }
#ifdef BERKELEYDB
    if (0 != try_bdb_lock()) {
        msg =
            as_sprintf(__FILE__, __LINE__, "bdb_lock : 1 (set) by %s\n",
                       bdb_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_bdb_lock();
        timeoutWrite(3, csocket,
                     "bdb_lock : 0 (not set)\n",
                     strlen("bdb_lock : 0 (not set)\n"));
    }
#endif
    if (0 != try_offset_lock()) {
        msg =
            as_sprintf(__FILE__, __LINE__, "offset_lock : 1 (set) by %s\n",
                       offset_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_offset_lock();
        timeoutWrite(3, csocket,
                     "offset_lock : 0 (not set)\n",
                     strlen("offset_lock : 0 (not set)\n"));
    }
    if (0 != try_hash_lock()) {
        msg =
            as_sprintf(__FILE__, __LINE__, "hash_lock : 1 (set) by %s\n",
                       hash_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_hash_lock();
        timeoutWrite(3, csocket,
                     "hash_lock : 0 (not set)\n",
                     strlen("hash_lock : 0 (not set)\n"));
    }
    if (0 != try_cachep2i_lock()) {
        msg =
            as_sprintf(__FILE__, __LINE__,
                       "cachep2i_lock : 1 (set) by %s\n",
                       cachep2i_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_cachep2i_lock();
        timeoutWrite(3, csocket,
                     "cachep2i_lock : 0 (not set)\n",
                     strlen("cachep2i_lock : 0 (not set)\n"));
    }
#ifdef HAMSTERDB
    if (0 != try_ham_lock()) {
        msg =
            as_sprintf(__FILE__, __LINE__, "ham_lock : 1 (set) by %s\n",
                       ham_lockedby);
        timeoutWrite(3, csocket, msg, strlen(msg));
        s_free(msg);
    } else {
        release_ham_lock();
        timeoutWrite(3, csocket,
                     "ham_lock : 0 (not set)\n",
                     strlen("ham_lock : 0 (not set)\n"));
    }
#endif
    timeoutWrite(3, csocket,
                 "---------------------\n",
                 strlen("---------------------\n"));
    timeoutWrite(3, csocket,
                 "normally set\n\n", strlen("normally set\n\n"));
}

void *ioctl_worker(void *arg)
{
    int msocket;
    int csocket;
    const char *port;
    const char *proto = "tcp";
    struct sockaddr_un client_address;
    socklen_t client_len;
    char buf[1028];
    char *addr;
    int err = 0;
    char *result;
    char *message = NULL;
    bool isfrozen = 0;

    msocket = -1;
    while (1) {
        addr = getenv("LISTEN_IP");
        port = getenv("LISTEN_PORT");
        if (NULL == port)
            port = "100";
        if (NULL == addr)
            LWARNING
                ("The administration session is not limited to localhost, this is not recommended.");
        msocket = serverinit(addr, port, proto);
        if (msocket != -1)
            break;
        sleep(1);
        close(msocket);
    }

    client_len = sizeof(client_address);
    while (1) {
        csocket =
            accept(msocket, (struct sockaddr *) &client_address,
                   &client_len);
        while (1) {
            result = NULL;
            if (-1 == timeoutWrite(3, csocket, ">", 1))
                break;
            if (-1 == readnlstring(10, csocket, buf, 1024)) {
                result = "timeout";
                err = 1;
                break;
            }
            if (0 == strncmp(buf, "\r", strlen("\r")))
                continue;
            if (0 == strncmp(buf, "quit\r", strlen("quit\r"))
                || 0 == strncmp(buf, "exit\r", strlen("exit\r"))) {
                err = 0;
                result = "bye";
                break;
            }
            if (0 == strncmp(buf, "freeze\r", strlen("freeze\r"))) {
                if (0 == isfrozen) {
                    result = "All i/o is now suspended.";
                    err = 0;
                    get_global_lock((char *) __PRETTY_FUNCTION__);
                    start_flush_commit();
                    isfrozen = 1;
                } else {
                    result = "i/o is already suspended.";
                    err = 1;
                }
            }
#ifdef BERKELEYDB
            if (0 == strncmp(buf, "bdb_status\r", strlen("bdb_status\r"))) {
               bdb_stat();
               result = "Please look at the lessfs bdb error log.";
               err=0;
            }
#endif
            if (0 == strncmp(buf, "defrost\r", strlen("defrost\r"))) {
                if (1 == isfrozen) {
                    result = "Resuming i/o.";
                    err = 0;
                    end_flush_commit();
                    release_global_lock();
                    isfrozen = 0;
                } else {
                    result = "i/o is not suspended.";
                    err = 1;
                }
            }
#ifndef BERKELEYDB
#ifndef HAMSTERDB
            if (0 == strncmp(buf, "defrag\r", strlen("defrag\r"))) {
                result = "Resuming i/o after defragmentation.";
                err = 1;
                if (-1 ==
                    timeoutWrite(3, csocket,
                                 "Suspending i/o for defragmentation\n",
                                 strlen
                                 ("Suspending i/o for defragmentation\n")))
                    break;
                err = 0;
                get_global_lock((char *) __PRETTY_FUNCTION__);
                tc_defrag();
                db_close(1);
                tc_open(1, 0, 0);
                release_global_lock();
            }
#endif
#endif
            if (0 == strncmp(buf, "help\r", strlen("help\r"))
                || 0 == strncmp(buf, "h\r", strlen("h\r"))) {
#ifdef BERKELEYDB
                result =
                    "valid commands: bdb_status defrag defrost freeze help lockstatus quit|exit";
#else
                result =
                    "valid commands: defrag defrost freeze help lockstatus quit|exit";
#endif
                err = 0;
            }
            if (0 == strncmp(buf, "lockstatus\r", strlen("lockstatus\r"))) {
                show_lock_status(csocket);
                result = "lockstatus listed";
                err = 0;
            }
            if (NULL == result) {
                err = -1;
                result = "unknown command";
            }
            if (err == 0) {
                message =
                    as_sprintf(__FILE__, __LINE__, "+OK %s\n", result);
                if (-1 ==
                    timeoutWrite(3, csocket, message, strlen(message)))
                    break;
            } else {
                message =
                    as_sprintf(__FILE__, __LINE__, "-ERR %s\n", result);
                if (-1 ==
                    timeoutWrite(3, csocket, message, strlen(message)))
                    break;
            }
            s_free(message);
            message = NULL;
        }
        if (message)
            s_free(message);
        if (err == 0) {
            message = as_sprintf(__FILE__, __LINE__, "+OK %s\n", result);
            timeoutWrite(3, csocket, message, strlen(message));
        } else {
            message = as_sprintf(__FILE__, __LINE__, "-ERR %s\n", result);
            timeoutWrite(3, csocket, message, strlen(message));
        }
        s_free(message);
        message = NULL;
        close(csocket);
    }
    return NULL;
}

void *init_worker(void *arg)
{
    int count;
    char *a;
    uintptr_t p;
    CCACHEDTA *ccachedta;
    char *key;
    int size;
    int vsize;
    int found[max_threads];
    char *dupkey;
    int index;
    TCLIST *keylist;

    memcpy(&count, arg, sizeof(int));   // count is thread number.
    s_free(arg);
    found[count] = 0;
    while (1) {
        if (found[count] == 0) {
            usleep(50000);
            if (config->replication && config->replication_role == 0
                && 0 != strcmp(config->replication_partner_ip, "-1")) {
                if (0 == try_replbl_lock((char *) __PRETTY_FUNCTION__)) {
                    send_backlog();
                    release_replbl_lock();
                }
            } else if (config->shutdown
                       || config->replication_enabled == 0)
                config->safe_down = 1;
        }
        found[count] = 0;
        write_lock((char *) __PRETTY_FUNCTION__);
        keylist = tctreekeys(workqtree);
        index = 0;
        if (0 != tclistnum(keylist)) {
            inobnolistsort(keylist);
            key = (char *) tclistval(keylist, 0, &size);
            a = (char *) tctreeget(workqtree, (void *) key, size, &vsize);
            if (a) {
                memcpy(&p, a, vsize);
                ccachedta = get_ccachedta(p);
                dupkey = s_malloc(size);
                memcpy(dupkey, key, size);
                tctreeout(workqtree, key, size);
                found[count]++;
                tclistdel(keylist);
                release_write_lock();
                worker_lock((char *) __PRETTY_FUNCTION__);
                working++;
                sequence++;
                release_worker_lock();
                cook_cache(dupkey, size, ccachedta, sequence);
                s_free(dupkey);
                worker_lock((char *) __PRETTY_FUNCTION__);
                working--;
                release_worker_lock();
            } else
                die_dataerr("Key without value in workers");
        } else
            tclistdel(keylist);
        if (0 == found[count])
            release_write_lock();
    }
    LFATAL("Thread %u exits", count);
    pthread_exit(NULL);
}

/* Write the hash for string LESSFS_DIRTY to DBU
   When this hash is present during mount the filesystem
   has not been unmounted cleanly previously.*/
void mark_dirty()
{
    unsigned char *stiger;
    char *brand;
    INUSE finuse;
    time_t thetime;
    unsigned long long inuse;

    FUNC;
    brand = as_sprintf(__FILE__, __LINE__, "LESSFS_DIRTY");
    stiger = thash((unsigned char *) brand, strlen(brand));
    thetime = time(NULL);
    inuse = thetime;
    if ( config->blockdata_io_type == TOKYOCABINET ) {
        update_inuse(stiger, inuse);
    } else {
        finuse.inuse = BLKSIZE;
        finuse.size = inuse;
        finuse.offset = 0;
        file_update_inuse(stiger, &finuse);
    }
    s_free(brand);
    free(stiger);
#ifndef BERKELEYDB
#ifndef HAMSTERDB
    tchdbsync(dbu);
#endif
#endif
    EFUNC;
    return;
}

/* Return 1 when filesystem is dirty */
int check_dirty()
{
    unsigned char *stiger;
    char *brand;
    unsigned long long inuse;
    INUSE *finuse;
    int dirty = 0;
    brand = as_sprintf(__FILE__, __LINE__, "LESSFS_DIRTY");
    stiger = thash((unsigned char *) brand, strlen(brand));
    if (NULL == config->blockdatabs) {
        finuse = file_get_inuse(stiger);
        if (finuse) {
            s_free(finuse);
            dirty = 1;
        }
    } else {
        inuse = getInUse(stiger);
        if (0 != inuse)
            dirty = 1;
    }
    free(stiger);
    s_free(brand);
    return (dirty);
}

void check_blocksize()
{
    int blksize;

    blksize = get_blocksize();
    if (blksize != BLKSIZE)
        die_dataerr
            ("Not allowed to mount lessfs with blocksize %u when previously used with blocksize %i",
             BLKSIZE, blksize);
    return;
}

// Flush data every flushtime seconds.
void *lessfs_flush(void *arg)
{
    //static int masterslave = 0;
    struct stat stbuf;
    time_t curtime;


    while (1) {
        curtime = time(NULL);
        if (config->replication) {
            sleep(config->flushtime / 2);
        } else
            sleep(config->flushtime);
// When in slave mode the master tells when to commit.
        if (config->replication != 1 || config->replication_role != 1) {
            get_global_lock((char *) __PRETTY_FUNCTION__);
            write_lock((char *) __PRETTY_FUNCTION__);
            flush_wait(0);
            purge_read_cache(0, 1, (char *) __PRETTY_FUNCTION__);
            if ( config->blockdata_io_type == CHUNK_IO ) sync();
            start_flush_commit();
            end_flush_commit();
            if (0 == strcmp(config->replication_partner_ip, "-1")
                && config->replication && config->replication_role == 0) {
                repl_lock((char *) __PRETTY_FUNCTION__);
                if (-1 == fstat(frepl, &stbuf))
                    die_syserr();
                if (stbuf.st_size > config->rotate_replog_size || (curtime - config->replication_last_rotated) > REPLOG_DELAY) {
                    config->replication_last_rotated = time(NULL);
                    rotate_replog();
                } else fsync(frepl);
                release_repl_lock(); 
            }
            release_write_lock();
            release_global_lock();
            if (config->replication == 1 && config->replication_role == 0) {
                 if (-1 == fstat(frepl, &stbuf))
                     die_syserr();
                 if (0 != config->max_backlog_size
                     && stbuf.st_size > config->max_backlog_size) {
                     LINFO("Waiting for the replication log to drain");
                     trunc_lock((char *) __PRETTY_FUNCTION__);
                     while (1) {
                         if (-1 == fstat(frepl, &stbuf))
                             die_syserr();
                         if (stbuf.st_size < config->max_backlog_size)
                             break;
                         usleep(50000);
                     }
                     release_trunc_lock();
                 }
                 write_repl_data(WRITTEN, TRANSACTIONCOMMIT, " ", 1,
                                 NULL, 0, MAX_ALLOWED_THREADS - 2);
            }
        }
    }
    pthread_exit(NULL);
}

static void *lessfs_init()
{
    unsigned int count = 0;
    unsigned int *cnt;
    int ret;
    unsigned char *stiger;
    char *hashstr;
    INUSE *finuse;
    struct tm *timeinfo = NULL;

    FUNC;
#ifdef LZO
    initlzo();
#endif
    if (getenv("MAX_THREADS"))
        max_threads = atoi(getenv("MAX_THREADS"));
    if (max_threads > MAX_ALLOWED_THREADS - 1)
        die_dataerr
            ("Configuration error : MAX_ALLOWED_THREADS should be less then %i",
             MAX_ALLOWED_THREADS - 1);
    pthread_t worker_thread[max_threads];
    pthread_t ioctl_thread;
    pthread_t replication_thread;
    pthread_t housekeeping_thread;
    pthread_t flush_thread;

    for (count = 0; count < max_threads; count++) {
        cnt = s_malloc(sizeof(int));
        memcpy(cnt, &count, sizeof(int));
        ret =
            pthread_create(&(worker_thread[count]), NULL, init_worker,
                           (void *) cnt);
        if (ret != 0)
            die_syserr();
        if (0 != pthread_detach(worker_thread[count]))
            die_syserr();
    }
    ret = pthread_create(&ioctl_thread, NULL, ioctl_worker, (void *) NULL);
    if (ret != 0)
        die_syserr();
    if (0 != pthread_detach(ioctl_thread))
        die_syserr();

    if (config->replication) {
        if (config->replication_role == 1) {
// A slave needs a listener.
            ret =
                pthread_create(&replication_thread, NULL,
                               replication_worker, (void *) NULL);
            if (ret != 0)
                die_syserr();
            if (0 != pthread_detach(replication_thread))
                die_syserr();
        }
    }

    ret =
        pthread_create(&housekeeping_thread, NULL, housekeeping_worker,
                       (void *) NULL);
    if (ret != 0)
        die_syserr();
    if (0 != pthread_detach(housekeeping_thread))
        die_syserr();

    ret = pthread_create(&flush_thread, NULL, lessfs_flush, (void *) NULL);
    if (ret != 0)
        die_syserr();
    if (0 != pthread_detach(flush_thread))
        die_syserr();
    check_blocksize();
    hashstr =
        as_sprintf(__FILE__, __LINE__, "%s%i", config->hash,
                   config->hashlen);
    stiger = thash((unsigned char *) hashstr, strlen(hashstr));
    if (NULL == config->blockdatabs) {
        if (NULL == (finuse = file_get_inuse(stiger))) {
            die_dataerr
                ("Invalid hashsize or hash found, do not change hash or hashsize after formatting lessfs.");
        } else
            s_free(finuse);
    } else {
        if (0 == getInUse(stiger))
            die_dataerr
                ("Invalid hashsize or hash found, do not change hash or hashsize after formatting lessfs.");
    }
    s_free(hashstr);
    free(stiger);

    timeinfo = init_transactions();

    if (check_dirty()) {
        LINFO("Lessfs has not been unmounted cleanly.");
        if (config->transactions) {
            LINFO("Rollback to : %s", asctime(timeinfo));
            write_repl_data(WRITTEN, TRANSACTIONABORT, " ", 1,
                            NULL, 0, MAX_ALLOWED_THREADS - 2);
        } else
            LINFO
                ("Lessfs has not been unmounted cleanly, you are advised to run lessfsck.");
    } else {
        LINFO("The filesystem is clean.");
        if (config->transactions) {
            LINFO("Last used at : %s", asctime(timeinfo));
        }
        mark_dirty();
    }
    config->lfsstats = s_zmalloc(2);
    if (config->replication != 1 || config->replication_role != 1) {
#ifdef BERKELEYDB
       bdb_restart_truncation();
#else
#ifdef HAMSTERDB
       hm_restart_truncation();
#else
       tc_restart_truncation();
#endif
#endif
    }
    EFUNC;
    return NULL;
}

static struct fuse_operations lessfs_oper = {
    .getattr = lessfs_getattr,
    .access = lessfs_access,
    .readlink = lessfs_readlink,
    .readdir = lessfs_readdir,
    .mknod = lessfs_mknod,
    .mkdir = lessfs_mkdir,
    .symlink = lessfs_symlink,
    .unlink = lessfs_unlink,
    .rmdir = lessfs_rmdir,
    .rename = lessfs_rename,
    .link = lessfs_link,
    .chmod = lessfs_chmod,
    .chown = lessfs_chown,
    .truncate = lessfs_truncate,
    .utimens = lessfs_utimens,
    .open = lessfs_open,
    .read = lessfs_read,
    .write = lessfs_write,
    .statfs = lessfs_statfs,
    .release = lessfs_release,
    .fsync = lessfs_fsync,
    .destroy = lessfs_destroy,
    .init = lessfs_init,
};

void usage(char *appName)
{
    char **argv = (char **) s_malloc(2 * sizeof(char *));
    argv[0] = appName;
    argv[1] = (char *) s_malloc(3 * sizeof(char));
    memcpy(argv[1], "-h\0", 3);
    fuse_main(2, argv, &lessfs_oper, NULL);
    FUNC;
    printf("\n"
           "-------------------------------------------------------\n"
           "lessfs %s\n"
           "\n"
           "Usage: %s [/path_to_config.cfg] [mount_point] <FUSE OPTIONS>\n"
           "\n"
           "Example :\nmklessfs /etc/lessfs.cfg \nlessfs   /etc/lessfs.cfg /mnt\n\n"
           "A high performance example with big_writes.\n(Requires kernel 2.6.26 or higher and a recent version of fuse.)\n"
           "lessfs /etc/lessfs.cfg /fuse -o use_ino,readdir_ino,default_permissions,\\\n       allow_other,big_writes,max_read=131072,max_write=131072\n"
           "-------------------------------------------------------\n",
           LFSVERSION, appName);
    exit(EXIT_USAGE);
}

int verify_kernel_version()
{
    struct utsname un;
    char *begin;
    char *end;
    int count;

    uname(&un);
    begin = un.release;

    for (count = 0; count < 3; count++) {
        end = strchr(begin, '.');
        if (end) {
            end[0] = 0;
            end++;
        }
        if (count == 0 && atoi(begin) < 2)
            return (-1);
        if (count == 0 && atoi(begin) > 2)
            break;
        if (count == 1 && atoi(begin) < 6)
            return (-2);
        if (count == 1 && atoi(begin) > 6)
            break;
        if (count == 2 && atoi(begin) < 26)
            return (-3);
        begin = end;
    }
    return (0);
}

int main(int argc, char *argv[])
{
    int res;
    char *p=NULL, *maxwrite, *maxread;
    char **argv_new = (char **) s_malloc(argc * sizeof(char *));
    int argc_new = argc - 1;
    struct rlimit lessfslimit;

    FUNC;

    if ((argc > 1) && (strcmp(argv[1], "-h") == 0)) {
        usage(argv[0]);
    }

    if (argc < 3) {
        usage(argv[0]);
    }

    if (-1 == r_env_cfg(argv[1]))
        usage(argv[0]);

    argv_new[0] = argv[0];
    int i;
    for (i = 1; i < argc - 1; i++) {
        argv_new[i] = argv[i + 1];
        if (strstr(argv[i + 1], "big_writes")) {
            maxwrite = strstr(argv[i + 1], "max_write=");
            maxread = strstr(argv[i + 1], "max_read=");
            if (maxwrite && maxread) {
                p = strchr(maxwrite, '=');
                p++;
                BLKSIZE = atoi(p);
                p = strchr(maxread, '=');
                p++;
                if (atoi(p) != BLKSIZE) {
                    LFATAL
                        ("lessfs : Supplied values for max_read and max_write must match.");
                    fprintf(stderr,
                            "Supplied values for max_read and max_write must match.\n");
                    exit(EXIT_SYSTEM);
                }
                if (BLKSIZE > 4096 && 0 != verify_kernel_version()) {
                    LFATAL
                        ("The kernel used is to old for larger then 4k blocksizes, kernel >= 2.6.26 is required.");
                    exit(EXIT_SYSTEM);
                }
            } else {
                LFATAL
                    ("lessfs : big_writes specified without max_write or max_read.");
                fprintf(stderr,
                        "big_writes specified without max_write or max_read.\n");
                exit(EXIT_SYSTEM);
            }
        }
    }
// Enable dumping of core for debugging.
   if (getenv("COREDUMPSIZE")) {
        lessfslimit.rlim_cur = lessfslimit.rlim_max =
            atoi(getenv("COREDUMPSIZE"));
        if (0 != setrlimit(RLIMIT_CORE, &lessfslimit)) {
            fprintf(stderr, "Failed to set COREDUMPSIZE to %i : error %s",
                    atoi(getenv("COREDUMPSIZE")), strerror(errno));
            exit(EXIT_SYSTEM);
        }
    } else {
        signal(SIGSEGV, segvExit);
    }
    LDEBUG("lessfs : blocksize is set to %u", BLKSIZE);
    signal(SIGHUP, normalExit);
    signal(SIGTERM, normalExit);
    signal(SIGALRM, normalExit);
    signal(SIGINT, normalExit);
    signal(SIGUSR1, libSafeExit);
    if (getenv("DEBUG"))
        debug = atoi(getenv("DEBUG"));
    parseconfig(0, 0);

    struct fuse_args args = FUSE_ARGS_INIT(argc_new, argv_new);
    fuse_opt_parse(&args, NULL, NULL, NULL);
    p = as_sprintf(__FILE__, __LINE__, "-omax_readahead=128,max_write=%u,max_read=%u",
                   BLKSIZE, BLKSIZE);
    fuse_opt_add_arg(&args, p);
    s_free(p);
    if (BLKSIZE > 4096) {
        if (0 != verify_kernel_version()) {
            LFATAL
                ("The kernel used is to old for larger then 4k blocksizes, kernel >= 2.6.26 is required.");
            exit(EXIT_SYSTEM);
        }
        fuse_opt_add_arg(&args, "-obig_writes");
    }
    fuse_opt_add_arg(&args,
                     "-ohard_remove,kernel_cache,negative_timeout=0,entry_timeout=0,attr_timeout=1,use_ino,readdir_ino,default_permissions,allow_other");
    s_free(argv_new);
    return fuse_main(args.argc, args.argv, &lessfs_oper, NULL);
    return (res);
}
