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

#define USEC_MAX 1000000

struct configdata {
    char *blockdata;
    char *blockdatabs;
    int blockdata_io_type;
    char *blockusage;
    char *blockusagebs;
    char *dirent;
    char *direntbs;
    char *fileblock;
    char *fileblockbs;
    char *meta;
    char *metabs;
    char *hardlink;
    char *hardlinkbs;
    char *symlink;
    char *symlinkbs;
    char *freelist;
    char *freelistbs;
    char *nexthash;
    char *replication_logfile;
    char *replication_watchdir;
    time_t replication_last_rotated;
    unsigned char *commithash;
    char *hash;
    char *lfsstats;
    unsigned char compression;
    unsigned char *iv;
    unsigned char *passwd;
    unsigned long long cachesize;
    unsigned int flushtime;
    unsigned int inspectdiskinterval;
    unsigned int defrag;
    unsigned int relax;
    unsigned int encryptdata;
    unsigned int encryptmeta;
    unsigned int selected_hash;
    int hashlen;
    int transactions;
    int replication;
    int replication_role;       // 0=master, 1=slave
    int replication_socket;
    char *replication_partner_ip;
    char *replication_partner_port;
    char *replication_listen_ip;
    char *replication_listen_port;
    int background_delete;
    int replication_enabled;
    int replication_backlog;
    unsigned long rotate_replog_size;
// Set stickybit on the file during truncation to indicate that
// it's locked.
    int sticky_on_locked;
    int shutdown;
    int frozen;
    int safe_down;
    int tuneforspeed;
    int nospace;
    int chunk_depth;
    int reclaim;
    int tune_for_size; // 0 <=1TB 1<=10TB 2 >10TB
    int bdb_private;
    unsigned long hamsterdb_cachesize;
    unsigned long max_backlog_size;
// Sleep utime in the truncation loop.
// Counter is increase by normal ops, decreased by truncation.
// (background) Truncation will therefore become slower when other tasks are running.
};
struct configdata *config;

int read_s_cfg(char *cfgfile, char *value, int size);
int read_m_cfg(char *cfgfile, char *value, char *value2, int size);
int r_env_cfg(char *configfile);
char *read_val(char *token);
