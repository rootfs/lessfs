/*
 *   Lessfs: A data deduplicating filesystem.
 *   Copyright (C) 2008 Mark Ruijter <mruijter@lessfs.com>
 *
 *   This program is s_free software;  you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 3 of the License, or
 *   (at your option) any later version.
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
#include <sys/file.h>
#include <fcntl.h>
#include <semaphore.h>
#include <pthread.h>
#include <fuse.h>

#include <tcutil.h>
#include <tchdb.h>
#include <tcbdb.h>
#include <stdlib.h>
#include <stdbool.h>

#include "lib_cfg.h"
#include "lib_safe.h"
#include "lib_io.h"
#include "lib_net.h"
#include "lib_str.h"
#include "retcodes.h"
#ifdef LZO
#include "lib_lzo.h"
#else
#include "lib_qlz.h"
#endif
#include "lib_common.h"
#include "lib_tc.h"
#include "lib_repl.h"
#include "commons.h"

#ifdef ENABLE_CRYPTO
unsigned char *passwd = NULL;
#endif

extern struct configdata *config;
extern unsigned long long nextoffset;
extern int fdbdta;
extern int frepl;
extern int rrepl;
extern int freplbl;             /* Backlog processing */
extern int BLKSIZE;

#define die_dataerr(f...) { LFATAL(f); exit(EXIT_DATAERR); }

struct option_info {
    char *configfile;
    char *replogfile;
    bool notwritten;
    unsigned int blocksize;
};
struct option_info mkoptions;

unsigned long working;

void usage(char *name)
{
    printf
        ("Usage:\nShow replog content :\n%s -r /path_to_replogfile -b blocksize \n",
         name);
    printf
        ("\n\nMerge replog with a lessfs slave :\n%s -r /path_to_replogfile -c /path_to_config.cfg\n\n",
         name);
    printf
        ("-r Path and name of the lessfs replication log : -b is mandatory\n");
    printf
        ("-c Path and name of the lessfs configuation file, the replog be merged with the lessfs slave filesystem\n");
    printf("-h Displays this usage message\n");
    exit(-1);
}

int get_opts(int argc, char *argv[])
{

    int c;

    mkoptions.replogfile = NULL;
    mkoptions.configfile = NULL;
    mkoptions.notwritten = 0;
    mkoptions.blocksize = 0;
    while ((c = getopt(argc, argv, "b:whr:c:")) != -1)
        switch (c) {
        case 'b':
            if (optopt == 'c')
                printf
                    ("Option -%c requires a lessfs the blocksize as argument.\n",
                     optopt);
            else {
                mkoptions.blocksize = atoi(optarg);
                BLKSIZE = mkoptions.blocksize;
            }
            break;
        case 'c':
            if (optopt == 'c')
                printf
                    ("Option -%c requires a lessfs configuration file as argument.\n",
                     optopt);
            else
                mkoptions.configfile = optarg;
            break;
        case 'r':
            if (optopt == 'r')
                printf
                    ("Option -%c requires a lessfs replication log file as argument.\n",
                     optopt);
            else
                mkoptions.replogfile = optarg;
            break;
        case 'w':
            mkoptions.notwritten = 1;
            break;
        case 'h':
            usage(argv[0]);
            break;
        default:
            abort();
        }
    return 0;
}

char *asc_hash(unsigned char *bhash, int hashlen)
{
    char *ascii_hash = NULL;
    int n;
    char *p1, *p2;

    for (n = 0; n < hashlen; n++) {
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


void parsedbp(char *action, REPLICATIONMSG replicationmsg, char *wmsg)
{
    unsigned long long *node;
    DDSTAT *ddstat;

    if (replicationmsg.ksize == 3) {
        node = (unsigned long long *) replicationmsg.value;
        if (0 == memcmp("NFI", replicationmsg.value, replicationmsg.vsize)) {
            printf("%s DBP\t:\tNFI %llu\t\t\t\t\t\t\t\t\t\t%s\n", action,
                   *node, wmsg);
        } else
            printf("%s DBP\t:\tSEQ %llu\t\t\t\t\t\t\t\t\t\t%s\n", action,
                   *node, wmsg);
        return;
    }
    node = (unsigned long long *) replicationmsg.key;
    ddstat = (DDSTAT *) replicationmsg.value;
    printf
        ("%s DBP\t:\tInode\t%llu\t:\tfilename %s\t- size\t%lu\t\t\t\t%s\n",
         action, *node, ddstat->filename, ddstat->stbuf.st_size, wmsg);
    return;
}

void parseline(char *message, unsigned int msgsize, int is_written)
{
//message : db (char), op (char), ksize (int),vsize (int), key (char *),value (char *)
    REPLICATIONMSG replicationmsg;
    char *wmsg;
    char *hsh;
    INOBNO *inobno;

    if (is_written)
        wmsg = as_sprintf(__FILE__, __LINE__, "IW");
    else
        wmsg = as_sprintf(__FILE__, __LINE__, "NW");

    replicationmsg.database = message[0];
    replicationmsg.operation = message[1];
    memcpy(&replicationmsg.ksize, &message[2], sizeof(int));
    memcpy(&replicationmsg.vsize, &message[2 + sizeof(int)], sizeof(int));
    replicationmsg.key = (unsigned char *) &message[2 + (2 * sizeof(int))];
    replicationmsg.value =
        (unsigned char *) &message[2 + (2 * sizeof(int)) +
                                   replicationmsg.ksize];
    if (replicationmsg.operation == REPLWRITE) {
        if (replicationmsg.database == DBDTA) {
            printf("WRT DBDTA\t: keysize %u\t:\tmsgsize\t%u\t%s\n",
                   replicationmsg.ksize, replicationmsg.vsize, wmsg);
        }
        if (replicationmsg.database == DBU) {
            hsh = asc_hash(replicationmsg.key, replicationmsg.ksize);
            printf("WRT DBU\t:\t%s\t:\tvalsize\t%u\t%s\n", hsh,
                   replicationmsg.ksize, wmsg);
            s_free(hsh);
        }
        if (replicationmsg.database == DBB) {
            inobno = (INOBNO *) replicationmsg.key;
            hsh = asc_hash(replicationmsg.value, replicationmsg.vsize);
            printf("WRT DBB\t:\t%llu-%llu\t: %s\t\t\t%s\n", inobno->inode,
                   inobno->blocknr, hsh, wmsg);
            s_free(hsh);
        }
        if (replicationmsg.database == DBP) {
            parsedbp("WRT", replicationmsg, wmsg);
        }
        if (replicationmsg.database == DBS) {
            printf("WRT DBS\t: keysize %u\t: msgsize\t%u\t%s\n",
                   replicationmsg.ksize, replicationmsg.vsize, wmsg);
        }
    }
    if (replicationmsg.operation == REPLDELETE) {
        if (replicationmsg.database == DBDTA) {
            printf("DEL DBDTA\t: keysize %u\t: msgsize\t%u\t%s\n",
                   replicationmsg.ksize, replicationmsg.vsize, wmsg);
        }
        if (replicationmsg.database == DBU) {
            hsh = asc_hash(replicationmsg.key, replicationmsg.ksize);
            printf("DEL DBU\t:\t%s\t\t\t\t%s\n", hsh, wmsg);
            s_free(hsh);
        }
        if (replicationmsg.database == DBB) {
            printf("DEL DBB\t: keysize %u\t: msgsize\t%u\t%s\n",
                   replicationmsg.ksize, replicationmsg.vsize, wmsg);
        }
        if (replicationmsg.database == DBP) {
            parsedbp("DEL", replicationmsg, wmsg);
        }
        if (replicationmsg.database == DBS) {
            printf("DEL DBS\t: keysize %u\t: msgsize\t%u\t%s\n",
                   replicationmsg.ksize, replicationmsg.vsize, wmsg);
        }
    }
    if (replicationmsg.operation == TRANSACTIONCOMMIT) {
        printf("TRC\t\t\t\t\t\t\t\t\t\t\t\t%s\n", wmsg);
    }
    if (replicationmsg.operation == TRANSACTIONABORT) {
        printf("TRA\t\t\t\t\t\t\t\t\t\t\t\t%s\n", wmsg);
    }
    s_free(wmsg);
    return;
}

void create_report()
{
    unsigned int msgsize;
    int result;
    char *data;
    struct stat stbuf;
    unsigned long offset = 0;
    unsigned long line = 0;
    int is_written = 0;

    if (-1 == fstat(rrepl, &stbuf))
        die_syserr();
    if (0 == stbuf.st_size) {
        fprintf(stderr, "This replication logfile is empty\n");
        return;
    }
    lseek(rrepl, offset, SEEK_SET);
    while (1) {
        line++;
        result = fullRead(rrepl, (unsigned char *) &msgsize, sizeof(int));
        if (sizeof(int) != result) {
            break;
        }
        offset += sizeof(int);
        if (msgsize > 2 * BLKSIZE || msgsize == 0 ) {
            fprintf(stderr,
                    "create_report : corrupt replog file detected : msgsize %u",
                    msgsize);
            die_dataerr("create_report : corrupt replog file detected");
        }
        data = s_zmalloc(msgsize);
        result = fullRead(rrepl, (unsigned char *) data, msgsize);
        if (result != msgsize)
            die_syserr();
        if (data[msgsize - 1] != '~') {
            if (mkoptions.notwritten) {
                offset += msgsize;
                s_free(data);
                lseek(rrepl, offset, SEEK_SET);
                printf("SKIP\n");
                continue;
            }
            is_written = 1;
        }
        is_written = 0;
        parseline(data, msgsize - 1, is_written);
        offset += msgsize;
        s_free(data);
        lseek(rrepl, offset, SEEK_SET);
    }
}

int main(int argc, char *argv[])
{
    int analyze_only = 1;
    char *p;

    debug = 1;
    if (argc < 2)
        usage(argv[0]);

    get_opts(argc, argv);
    if (NULL != mkoptions.configfile) {
        analyze_only = 0;
        if (-1 == r_env_cfg(mkoptions.configfile))
            usage(argv[0]);
    }
    if (0 == mkoptions.replogfile)
        usage(argv[0]);
    if (-1 == (rrepl = s_open2(mkoptions.replogfile, O_RDWR, S_IRWXU)))
        die_syserr();
    if (analyze_only) {
        if (mkoptions.blocksize == 0)
            usage(argv[0]);
        create_report();
        exit(0);
    }
    if (-1 == r_env_cfg(mkoptions.configfile))
        usage(argv[0]);
    p = getenv("BLKSIZE");
    if (NULL != p) {
        BLKSIZE = atoi(p);
        if (BLKSIZE > 131072) {
            LFATAL
                ("Fuse does not support a blocksize greater then 131072 bytes.");
            fprintf(stderr,
                    "Fuse does not support a blocksize greater then 131072 bytes.\n");
            exit(EXIT_SYSTEM);
        }
    } else {
        if (0 == mkoptions.blocksize) {
            fprintf(stderr, "BLKSIZE not found in config nor specified\n");
            exit(EXIT_USAGE);
        }
    }
    parseconfig(0, 0);
    merge_replog();
    next_sequence();
    db_close(0);
    s_free(config);
    printf("The replication log has been processed\n");
#ifdef MEMTRACE
    leak_report();
#endif
    exit(0);
}
