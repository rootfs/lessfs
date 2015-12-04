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

typedef struct {
    unsigned long long offset;
    time_t reuseafter;
} FREEBLOCK;

typedef struct {
    unsigned long long available_blocks;
    unsigned long long offset;
} FLIST;

INUSE *file_get_inuse(unsigned char *);
unsigned int file_commit_block(unsigned char *, INOBNO, unsigned long);
void file_sync_flush_dtaq();
void file_partial_truncate_block(unsigned long long, unsigned long long,
                                 unsigned int);
int file_unlink_file(const char *);
CCACHEDTA *file_update_stored(unsigned char *, INOBNO *, off_t);
void fl_write_cache(CCACHEDTA *, INOBNO *);
unsigned long long file_read_block(unsigned long long, char *, size_t,
                                   size_t, unsigned long long);
int file_fs_truncate(struct stat *, off_t, char *, bool);
void file_update_inuse(unsigned char *, INUSE *);
DAT *file_tgr_read_data(unsigned char *);
unsigned long long round_512(unsigned long long);
void file_delete_stored(INOBNO *);
void *file_truncate_worker(void *);
void write_trunc_todolist(struct truncate_thread_data *);
void set_new_offset(unsigned long long);
