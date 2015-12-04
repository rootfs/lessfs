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
#include <sys/types.h>
#include <fuse.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/file.h>


#include <tcutil.h>
#include <tcbdb.h>
#include <tchdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <aio.h>
#include <mhash.h>
#include <mutils/mhash.h>
#include <sys/types.h>
#include <dirent.h>


#include "lib_safe.h"
#include "lib_cfg.h"
#include "lib_net.h"
#include "retcodes.h"
#ifdef LZO
#include "lib_lzo.h"
#endif
#include "lib_qlz.h"
#include "lib_common.h"
#include "lib_repl.h"
#include "lib_crypto.h"
#include "file_io.h"

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
extern TCTREE *path2inotree;
extern int fdbdta;
extern int frepl;
extern int freplbl;
int rrepl;
extern int BLKSIZE;
extern unsigned long long nextoffset;

unsigned char *crc32(unsigned char *buf, int size, int thread_number)
{
    MHASH td[MAX_ALLOWED_THREADS];
    unsigned char *hash[thread_number];

    td[thread_number] = mhash_init(MHASH_CRC32);
    if (td[thread_number] == MHASH_FAILED)
        exit(1);

    mhash(td[thread_number], buf, size);
    hash[thread_number] = mhash_end(td[thread_number]);
    return hash[thread_number];
}

int check_abort()
{
    int ret = OK;
    if (config->shutdown || config->replication_enabled == 0) {
        config->safe_down = 1;
        ret = FAIL;
    }
    return (ret);
}

void merge_replog()
{
    unsigned int msgsize;
    int result;
    char *data;
    struct stat stbuf;
    unsigned long offset = 0;
    unsigned long line = 0;
    REPLICATIONMSG replicationmsg;
    char *crccalc;
    char *crcrec;

    LINFO("merge_replog");
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
        if (msgsize > 2 * BLKSIZE) {
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
            offset += msgsize;
            s_free(data);
            lseek(rrepl, offset, SEEK_SET);
            printf("SKIP\n");
            continue;
        }
        replicationmsg.database = data[0];
        replicationmsg.operation = data[1];
        memcpy(&replicationmsg.ksize, &data[2], sizeof(int));
        memcpy(&replicationmsg.vsize, &data[2 + sizeof(int)], sizeof(int));
        replicationmsg.key =
            (unsigned char *) &data[2 + (2 * sizeof(int))];
        replicationmsg.value =
            (unsigned char *) &data[2 + (2 * sizeof(int)) +
                                    replicationmsg.ksize];
        crccalc =
            (char *) crc32((unsigned char *) data,
                           msgsize - ((sizeof(int) + 1)), 1);
        crcrec = &data[msgsize - (sizeof(int) + 1)];
        if (0 != memcmp(crccalc, crcrec, sizeof(int))) {
            LINFO("crcsend != crcrecv %02x %02x : %02x %02x",
                  data[0], data[1],
                  data[msgsize - sizeof(int) - 1],
                  data[msgsize - sizeof(int)]);
            free(crccalc);
            die_dataerr("CRC errors in replication logfile, abort");
        }
        free(crccalc);
        if (replicationmsg.operation != TRANSACTIONCOMMIT &&
            replicationmsg.operation != TRANSACTIONABORT) {
            process_message(data, msgsize - (1 + sizeof(int)));
        }
        offset += msgsize;
        s_free(data);
        lseek(rrepl, offset, SEEK_SET);
    }
    return;
}


// Try to send the replication backlog.
// 0 if OK, 1 if failed.
int send_backlog()
{
    unsigned int msgsize;
    int ret = OK;
    int retry = 0;
    int result;
    unsigned char *flagwritten = (unsigned char *) "Z";
    char *data;
    char *msgstart;
    unsigned int remote;
    struct stat stbuf;
    unsigned long long offset;
    bool dotrunc = 0;
    bool connected = 0;

    FUNC;
    if (config->frozen)
        return (FAIL);
    if (config->replication_enabled == 0)
        return (FAIL);
    lseek(freplbl, 0, SEEK_SET);
    offset = 0;
    if (-1 == fstat(freplbl, &stbuf))
        die_syserr();
    if (0 == stbuf.st_size) {
        LDEBUG("send_backlog : replog is empty %lu, %lu", stbuf.st_size,
               stbuf.st_ino);
        return (ret);
    }
    LDEBUG("send_backlog : start sending the backlog to the slave");
  final_run:
    while (1) {
        result =
            fullRead(freplbl, (unsigned char *) &msgsize, sizeof(int));
        if (sizeof(int) != result)
            break;
        offset = offset + sizeof(int);
        LDEBUG("msgsize=%i", msgsize);
        if ( 0 == msgsize ) die_dataerr("send_backlog : corrupt replog file detected, msgsize=0");
        if (msgsize > 2 * BLKSIZE)
            die_dataerr("send_backlog : corrupt replog file detected");
        data = s_zmalloc(msgsize);
        result = fullRead(freplbl, (unsigned char *) data, msgsize);
        if (result != msgsize) {
            s_free(data);
            LFATAL("send_backlog : !msgsize");
            return (FAIL);
        }
        if (data[msgsize - 1] != '~') {
            s_free(data);
            offset = offset + msgsize;
            lseek(freplbl, offset, SEEK_SET);
            LDEBUG
                ("send_backlog : has been written before, skip (not an error)");
            continue;
        }
        if (!connected) {
            while (OK != reconnect()) {
                sleep(1);       // Make sure we don't flood the other size with SYN's
                LINFO("send_backlog : failed to connect to the slave");
            }
            connected = 1;
        }
        msgstart =
            as_sprintf(__FILE__, __LINE__, "START : %i", msgsize - 1);
        while (1) {
            if (OK != (ret = check_abort()))
                break;
            result =
                fulltimWrite(5, config->replication_socket,
                             (unsigned char *) msgstart,
                             strlen(msgstart) + 1);
            if (result != strlen(msgstart) + 1) {
                LINFO("send_backlog : write msgstart failed");
                while (OK != reconnect()) {
                    sleep(1);   // Make sure we don't flood the other size with SYN's
                    LINFO("send_backlog : failed to connect to the slave");
                }
                continue;
            }
            result =
                fulltimRead(5, config->replication_socket,
                            (unsigned char *) &remote,
                            sizeof(unsigned int));
            if (OK != (ret = check_abort()))
                break;
            if (remote != msgsize - 1) {
                if (OK != reconnect()) {
                    ret = FAIL;
                    break;
                }
                LINFO("send_backlog : invalid message size");
                continue;
            }

            result =
                fulltimWrite(5, config->replication_socket,
                             (unsigned char *) data, msgsize - 1);
            if (OK != (ret = check_abort()))
                break;
            if (result != msgsize - 1) {
                LINFO
                    ("send_backlog : failed to write the message, reset connection");
                while (OK != reconnect()) {
                    sleep(1);   // Make sure we don't flood the other size with SYN's
                    LINFO("send_backlog : failed to connect to the slave");
                }
                continue;
            }
            result =
                fulltimRead(5, config->replication_socket,
                            (unsigned char *) &remote,
                            sizeof(unsigned int));
            if (OK != (ret = check_abort()))
                break;
            if (remote == ACK) {
                retry = 0;
                LDEBUG("send_backlog : remote == ACK");
                break;
            }
            LINFO("send_backlog : no ACK on %02x %02x : %02x %02x",
                  (u_char) data[0], (u_char) data[1],
                  (u_char) data[msgsize - sizeof(int) - 1],
                  (u_char) data[msgsize - sizeof(int)]);
            while (OK != reconnect()) {
                sleep(1);       // Make sure we don't flood the other size with SYN's
                LINFO("send_backlog : failed to connect to the slave");
            }
            if (OK != (ret = check_abort()))
                break;
        }
        retry = 0;
        s_free(msgstart);
        s_free(data);
        if (ret == FAIL)
            break;
        lseek(freplbl, offset + msgsize - 1, SEEK_SET);
        result = fullWrite(freplbl, flagwritten, 1);
        if (result != sizeof(unsigned char))
            die_dataerr
                ("send_backlog : write error on disk: this should never happen");
        offset = offset + msgsize;
        lseek(freplbl, offset, SEEK_SET);
    }
    if (ret == OK) {
        if (dotrunc == 0)
            repl_lock((char *) __PRETTY_FUNCTION__);
        if (0 == dotrunc) {
            dotrunc = 1;
            goto final_run;
        }
        lseek(freplbl, 0, SEEK_SET);
        if (-1 == ftruncate(freplbl, 0))
            die_dataerr("Failed to truncate replog");
        release_repl_lock();
    }
    if (ret == OK) {
        LDEBUG("send_backlog : send_backlog returns OK");
    } else {
        if (config->shutdown == 0) {
            LINFO("send_backlog : return FAIL");
        } else
            LINFO
                ("send_backlog : refuse request, going down after shutdown request");
    }
    close(config->replication_socket);
    return (ret);
}

void rotate_replog()
{
    char *newfile;
    struct stat stbuf;
    unsigned long sequence;

    FUNC;
    sequence = get_sequence();
    newfile =
        as_sprintf(__FILE__, __LINE__, "%s-%lu",
                   config->replication_logfile, sequence);
    if (-1 != stat(newfile, &stbuf)) {
        die_dataerr("Replication logfile with name %s already exists",
                    newfile);
    }
    flock(frepl, LOCK_UN);
    fsync(frepl);
    close(frepl);
    close(freplbl);
    if (0 != rename(config->replication_logfile, newfile))
        die_dataerr("Failed to rename %s to %s",
                    config->replication_logfile, newfile);
    if (-1 ==
        (frepl =
         s_open2(config->replication_logfile, O_CREAT | O_RDWR, S_IRWXU)))
        die_syserr();
    if (0 != flock(frepl, LOCK_EX | LOCK_NB)) {
        LFATAL("Failed to lock the replication logfile %s",
               config->replication_logfile);
        exit(EXIT_USAGE);
    }
    if (-1 ==
        (freplbl = s_open2(config->replication_logfile, O_RDWR, S_IRWXU)))
        die_syserr();
    next_sequence();
    sequence = get_sequence();

    s_free(newfile);
    EFUNC;
    return;
}

void write_replication_data(unsigned char db, unsigned char op, char *key,
                            int ksize, char *value, int vsize,
                            int threadnr)
{
    char *replicationmsg;
    int replicationmsg_size;
    int result;
    char *crc;

// replication_msg : db (char), op (char), ksize (int),vsize (int), key (char *),value (char *), crc32 (int)
    replicationmsg_size =
        ksize + vsize + (sizeof(unsigned char) * 2) + (sizeof(int) * 2) +
        sizeof(int);
    replicationmsg = s_zmalloc(replicationmsg_size + 1);
    replicationmsg[0] = db;
    replicationmsg[1] = op;
    memcpy(replicationmsg + (sizeof(unsigned char) * 2), &ksize,
           sizeof(int));
    memcpy(replicationmsg + (sizeof(unsigned char) * 2) + sizeof(int),
           &vsize, sizeof(int));
    memcpy(replicationmsg + (sizeof(unsigned char) * 2) +
           (sizeof(int) * 2), key, ksize);
    if (op == REPLWRITE || op == REPLDUPWRITE || op == REPLDELETECURKEY) {
        memcpy(replicationmsg + ksize + (sizeof(unsigned char) * 2) +
               (2 * sizeof(int)), value, vsize);
// For safety we will calc a crc32 checksum of the message.
// drbd has seen cases where data received was garbled despite the
// use of tcp as transfer protocol.
    }
    crc =
        (char *) crc32((unsigned char *) replicationmsg,
                       replicationmsg_size - sizeof(int), threadnr);
    memcpy(replicationmsg + replicationmsg_size - sizeof(int), crc,
           sizeof(int));
    free(crc);

    config->replication_backlog = 1;
    LDEBUG("write to replog, msgsize = %u", replicationmsg_size);
    lseek(frepl, 0, SEEK_END);
    replicationmsg_size++;
    result =
        fullWrite(frepl, (unsigned char *) &replicationmsg_size,
                  sizeof(int));
    replicationmsg[replicationmsg_size - 1] = '~';
    // Add one byte that will be used later to flag when a message has been send succesfully.
    result =
        fullWrite(frepl, (unsigned char *) replicationmsg,
                  replicationmsg_size);
    if (result != replicationmsg_size)
        die_dataerr("Failed to write to replication log");
    s_free(replicationmsg);
}

void write_repl_data(unsigned char db, unsigned char op, char *key,
                     int ksize, char *value, int vsize, int threadnr)
{
    struct stat stbuf;
    //unsigned int curtime = time(NULL);

    FUNC;
    repl_lock((char *) __PRETTY_FUNCTION__);
    if (-1 == fstat(frepl, &stbuf))
        die_syserr();
    /*if (0 == strcmp(config->replication_partner_ip, "-1")
        && config->replication && config->replication_role == 0) {
        if (stbuf.st_size > config->rotate_replog_size
            || curtime - config->replication_last_rotated > REPLOG_DELAY) {
            config->replication_last_rotated = time(NULL);
            rotate_replog();
        }
    }*/
    if (0 != stbuf.st_size) {
        config->replication_backlog = 1;
    } else
        config->replication_backlog = 0;
    write_replication_data(db, op, key, ksize, value, vsize, threadnr);
    release_repl_lock();
    EFUNC;
    return;
}

int reconnect()
{
    int retry = 0;
    close(config->replication_socket);
    while (1) {
        config->replication_socket =
            clientconnect(config->replication_partner_ip,
                          config->replication_partner_port, "tcp");
        if (config->replication_socket != -1)
            break;
        if (OK != check_abort())
            return (FAIL);
        LINFO("Retry connect to %s:%s", config->replication_partner_ip,
              config->replication_partner_port);
        retry++;
        sleep(retry);
        if (retry > 3)
            return (FAIL);
    }
    return (OK);
}


void flush_recv_buffer()
{
    char trash;
    while (0 <
           fulltimRead(1, config->replication_socket,
                       (unsigned char *) &trash, sizeof(char)));
    return;
}

void watchdir()
{
    struct stat stbuf;
    DIR *dp = NULL;
    struct dirent *entry;
    char *name;
    unsigned long sequence;
    bool found = 0;
    int sleeptime = WATCHDIR_SLEEPINTERVAL;

    LINFO("watchdir : open %s", config->replication_watchdir);
    if (-1 == stat(config->replication_watchdir, &stbuf))
        die_dataerr("watchdir : Failed to stat %s",
                    config->replication_watchdir);
    if (-1 == chdir(config->replication_watchdir))
        die_dataerr("watchdir : Failed to chdir to %s",
                    config->replication_watchdir);
    while (1) {
        if (config->shutdown)
            break;
        while (config->frozen) {
            sleep(1);
        }
        if (NULL == (dp = (opendir(config->replication_watchdir)))) {
            LFATAL("watchdir : Failed to open %s\n",
                   config->replication_watchdir);
            exit(EXIT_WDIR);
        }
        while (entry = readdir(dp)) {
            sequence = get_sequence();
            if (-1 == stat(entry->d_name, &stbuf)) {
                LINFO("watchdir: Error on stat %s\n", entry->d_name);
                continue;
            }
            if (S_ISDIR(stbuf.st_mode)) {
                continue;
            }
            name =
                as_sprintf(__FILE__, __LINE__, "replog.dta-%lu", sequence);
            if (0 != strcmp(name, entry->d_name)) {
                s_free(name);
                continue;
            }
            if (-1 == (rrepl = s_open2(name, O_RDWR, S_IRWXU)))
                die_syserr();
            merge_replog();
            found = 1;
            next_sequence();
            commit_transactions();
            start_transactions();
            s_free(name);
            name =
                as_sprintf(__FILE__, __LINE__, "replog.dta-%lu-processed",
                           sequence);
            if (0 != rename(entry->d_name, name))
                die_dataerr("Failed to rename %s to %s", entry->d_name,
                            name);
            s_free(name);
        }
        closedir(dp);
        if (found) {
            sleeptime = 0;
        } else if (sleeptime < WATCHDIR_SLEEPINTERVAL)
            sleeptime++;
        sleep(sleeptime);
        found = 0;
    }
    config->safe_down = 1;
}

void *replication_worker(void *arg)
{
    int msocket;
    const char *proto = "tcp";
    struct sockaddr_un client_address;
    socklen_t client_len;
    char *message = NULL;
    int result;
    int msglen = 0;
    char *p;
    char *crcrec;
    char *crccalc;
    unsigned int nakcount = 0;

    if (config->replication_watchdir) {
        watchdir();
        pthread_exit(NULL);
    }
    msocket = -1;
    if (NULL == config->replication_listen_port)
        config->replication_listen_port = "101";

    while (1) {
        msocket =
            serverinit(config->replication_listen_ip,
                       config->replication_listen_port, proto);
        if (msocket != -1)
            break;
        LINFO("replication_worker : serverinit failed: retry");
        sleep(1);
        close(msocket);
    }
    message = s_zmalloc(BLKSIZE * 2);   // Enough space to hold the message and the kitchen sink
    client_len = sizeof(client_address);
    while (1) {
        nakcount = 0;
        config->safe_down = 1;
        config->replication_socket =
            accept(msocket, (struct sockaddr *) &client_address,
                   &client_len);
        config->safe_down = 0;
        LDEBUG("replication_worker : connected");
        while (1) {
            if (OK != check_abort()) {
                LINFO
                    ("Refuse replication data while filesystem is frozen or shutting down");
                close(config->replication_socket);
                break;
            }
            if (config->replication_enabled == 0) {
                LINFO
                    ("replication_worker : replication is disabled, disconnect");
                close(config->replication_socket);
                break;
            }
            if (nakcount > 3) {
                LINFO("replication_worker : nakcount > 3, disconnect");
                close(config->replication_socket);
                break;
            }
            result =
                readstring(300, config->replication_socket, message, 64);
            if (result == TIMEOUT) {
                LINFO("No START message after connect, handshake failure");
                close(config->replication_socket);
                break;
            }
            if (result == -1) {
                LDEBUG
                    ("replication_worker : readstring %i close connection",
                     result);
                close(config->replication_socket);
                break;
            }
            if (result == 0) {
                if (0 != strncmp("START :", message, strlen("START :"))) {
                    send_nak();
                    LDEBUG
                        ("replication_worker : readstring no valid start of message");
                    nakcount++;
                    continue;
                }
                p = strchr(message, ':');
                p++;
                msglen = atoi(p);
            } else {
                send_nak();
                LINFO("replication_worker : readstring no valid message");
                nakcount++;
                continue;
            }
            result =
                fulltimWrite(5, config->replication_socket,
                             (unsigned char *) &msglen, sizeof(int));
            if (result != sizeof(int)) {
                LINFO
                    ("replication_worker : failed to send ACK, disconnect");
                close(config->replication_socket);
                break;
            }
            result =
                fulltimRead(10, config->replication_socket,
                            (unsigned char *) message, msglen);
            if (result != msglen) {
                LINFO("replication_worker : got %i expected %i", result,
                      msglen);
                send_nak();
                nakcount++;
                continue;
            }
            if (msglen < sizeof(int)) {
#ifdef x86_64
                LINFO("replication_worker : got %i bytes expected %lu",
                      msglen, sizeof(int));
#else
                LINFO("replication_worker : got %i bytes expected %u",
                      msglen, sizeof(int));
#endif
                send_nak();
                nakcount++;
                continue;
            }
            crccalc =
                (char *) crc32((unsigned char *) message,
                               msglen - (sizeof(int)), 1);
            crcrec = &message[msglen - sizeof(int)];
            if (0 != memcmp(crccalc, crcrec, sizeof(int))) {
                LINFO("crcsend != crcrecv %02x %02x : %02x %02x",
                      message[0], message[1],
                      message[msglen - sizeof(int) - 1],
                      message[msglen - sizeof(int)]);
                free(crccalc);
                send_nak();
                nakcount++;
                continue;
            }
            free(crccalc);
            send_ack();
            nakcount = 0;
            process_message(message, msglen - sizeof(int));
        }
    }
// We never get here.
    s_free(message);
    pthread_exit(NULL);
}

void process_message(char *message, int msglen)
{
//message : db (char), op (char), ksize (int),vsize (int), key (char *),value (char *)
    REPLICATIONMSG replicationmsg;
    unsigned long long offset;
    unsigned long cursequence;
    unsigned long newsequence;

    replicationmsg.database = message[0];
    replicationmsg.operation = message[1];
    memcpy(&replicationmsg.ksize, &message[2], sizeof(int));
    memcpy(&replicationmsg.vsize, &message[2 + sizeof(int)], sizeof(int));
    replicationmsg.key = (unsigned char *) &message[2 + (2 * sizeof(int))];
    replicationmsg.value =
        (unsigned char *) &message[2 + (2 * sizeof(int)) +
                                   replicationmsg.ksize];
    while (config->frozen) {
        usleep(10000);
    }
    if (replicationmsg.operation == REPLWRITE) {
        LDEBUG("process_message : write keylen %i, vsize %i",
               replicationmsg.ksize, replicationmsg.vsize);
        if (replicationmsg.database == DBDTA) {
            bin_write_dbdata(DBDTA, replicationmsg.key,
                             replicationmsg.ksize, replicationmsg.value,
                             replicationmsg.vsize);
        }
        if (replicationmsg.database == DBU) {
            bin_write_dbdata(DBU, replicationmsg.key, replicationmsg.ksize,
                             replicationmsg.value, replicationmsg.vsize);
        }
        if (replicationmsg.database == DBB) {
            bin_write_dbdata(DBB, replicationmsg.key, replicationmsg.ksize,
                             replicationmsg.value, replicationmsg.vsize);
        }
        if (replicationmsg.database == DBP) {
            if (3 == replicationmsg.ksize) {
                if (0 == memcmp(replicationmsg.key, "SEQ", 3)) {
                    cursequence = get_sequence();
                    memcpy(&newsequence, replicationmsg.value,
                           sizeof(newsequence));
                    if (newsequence != cursequence)
                        die_dataerr
                            ("replication log with sequence %lu while expecting %lu",
                             newsequence, cursequence);
                }
            }
            bin_write_dbdata(DBP, replicationmsg.key, replicationmsg.ksize,
                             replicationmsg.value, replicationmsg.vsize);
            cachep2i_lock((char *) __PRETTY_FUNCTION__);
            tctreeclear(path2inotree);
            release_cachep2i_lock();
        }
        if (replicationmsg.database == DBS) {
            bin_write_dbdata(DBS, replicationmsg.key, replicationmsg.ksize,
                             replicationmsg.value, replicationmsg.vsize);
        }
        if (replicationmsg.database == DBDIRENT) {
            btbin_write_dbdata(DBDIRENT, replicationmsg.key,
                               replicationmsg.ksize, replicationmsg.value,
                               replicationmsg.vsize);
            cachep2i_lock((char *) __PRETTY_FUNCTION__);
            tctreeclear(path2inotree);
            release_cachep2i_lock();
        }
        if (replicationmsg.database == FREELIST) {
            btbin_write_dbdata(FREELIST, replicationmsg.key,
                               replicationmsg.ksize, replicationmsg.value,
                               replicationmsg.vsize);
        }
        if (replicationmsg.database == DBL) {
            btbin_write_dbdata(DBL, replicationmsg.key,
                               replicationmsg.ksize, replicationmsg.value,
                               replicationmsg.vsize);
            cachep2i_lock((char *) __PRETTY_FUNCTION__);
            tctreeclear(path2inotree);
            release_cachep2i_lock();
        }
        if (replicationmsg.database == FDBDTA) {
// With a write to fdbdta we use the key as store for the offset.
            memcpy(&offset, replicationmsg.key, replicationmsg.ksize);
            s_lckpwrite(fdbdta, replicationmsg.value, replicationmsg.vsize,
                        offset);
        }
    }
    if (replicationmsg.operation == REPLDUPWRITE) {
        if (replicationmsg.database == DBDIRENT) {
            btbin_write_dup(DBDIRENT, replicationmsg.key,
                            replicationmsg.ksize, replicationmsg.value,
                            replicationmsg.vsize, LOCK);
        }
        if (replicationmsg.database == FREELIST) {
            btbin_write_dup(FREELIST, replicationmsg.key,
                            replicationmsg.ksize, replicationmsg.value,
                            replicationmsg.vsize, LOCK);
        }
        if (replicationmsg.database == DBL) {
            btbin_write_dup(DBL, replicationmsg.key, replicationmsg.ksize,
                            replicationmsg.value, replicationmsg.vsize,
                            LOCK);
        }
    }
    if (replicationmsg.operation == REPLDELETE) {
        if (replicationmsg.database == DBDTA) {
            delete_key(DBDTA, replicationmsg.key, replicationmsg.ksize,
                       NULL);
        }
        if (replicationmsg.database == DBU) {
            delete_key(DBU, replicationmsg.key, replicationmsg.ksize,
                       NULL);
        }
        if (replicationmsg.database == DBB) {
            delete_key(DBB, replicationmsg.key, replicationmsg.ksize,
                       NULL);
        }
        if (replicationmsg.database == DBP) {
            delete_key(DBP, replicationmsg.key, replicationmsg.ksize,
                       NULL);
        }
        if (replicationmsg.database == DBS) {
            delete_key(DBS, replicationmsg.key, replicationmsg.ksize,
                       NULL);
        }
    }
    if (replicationmsg.operation == TRANSACTIONCOMMIT) {
        get_global_lock((char *) __PRETTY_FUNCTION__);
        write_lock((char *) __PRETTY_FUNCTION__);
        flush_wait(0);
        purge_read_cache(0, 1, (char *) __PRETTY_FUNCTION__);
        release_write_lock();
        start_flush_commit();
        end_flush_commit();
        release_global_lock();
    }
    if (replicationmsg.operation == TRANSACTIONABORT) {
        abort_transactions();
    }
    if (replicationmsg.operation == REPLDELETECURKEY) {
        if (replicationmsg.database == DBDIRENT) {
            btdelete_curkey(DBDIRENT, replicationmsg.key,
                            replicationmsg.ksize, replicationmsg.value,
                            replicationmsg.vsize,
                            (char *) __PRETTY_FUNCTION__);
        }
        if (replicationmsg.database == FREELIST) {
            btdelete_curkey(FREELIST, replicationmsg.key,
                            replicationmsg.ksize, replicationmsg.value,
                            replicationmsg.vsize,
                            (char *) __PRETTY_FUNCTION__);
        }
        if (replicationmsg.database == DBL) {
            btdelete_curkey(DBL, replicationmsg.key, replicationmsg.ksize,
                            replicationmsg.value, replicationmsg.vsize,
                            (char *) __PRETTY_FUNCTION__);
        }
    }
    if (replicationmsg.operation == REPLSETNEXTOFFSET) {
        memcpy(&nextoffset, replicationmsg.key, replicationmsg.ksize);
    }
    return;
}

int send_nak()
{
    int confirm = NAK;
    int result;
    FUNC;
    flush_recv_buffer();
    result =
        fulltimWrite(3, config->replication_socket,
                     (unsigned char *) &confirm, sizeof(int));
    if (result != sizeof(unsigned int)) {
        close(config->replication_socket);
        return (-1);
    }
    EFUNC;
    return (0);
}

int send_ack()
{
    int confirm = ACK;
    int result;
    FUNC;
    result =
        fulltimWrite(3, config->replication_socket,
                     (unsigned char *) &confirm, sizeof(int));
    if (result != sizeof(unsigned int)) {
        close(config->replication_socket);
        return (-1);
    }
    EFUNC;
    return (0);
}
