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
void bin_write_dbdata(int, void *, int, void *, int);
void bin_write(int, void *, int, void *, int);
void mbin_write_dbdata(TCMDB *, void *, int, void *, int);
void delete_key(int, void *, int, const char *);
void tc_open(bool, bool, bool);
void tc_close(bool);
void db_update_block(const char *, unsigned long long,
                     unsigned int, unsigned long long, unsigned long long,
                     unsigned char *);
int fs_mkdir(const char *, mode_t);
int fs_readdir(const char *, void *, fuse_fill_dir_t, off_t,
               struct fuse_file_info *);
char *fs_search_topdir(char *);
void btbin_write_dbdata(int, void *, int, void *, int);
void btbin_curwrite_dbdata(int, BDBCUR *, char *, int);
unsigned long long has_nodes(unsigned long long);
void bt_curwrite(int, char *, char *);
int bt_entry_exists(int, void *, int, void *, int);
DDSTAT *dnode_bname_to_inode(void *, int, char *);
DAT *search_memhash(TCMDB *, void *, int);
void tc_defrag();
void binhash(unsigned char *, int, word64[3]);
unsigned char *sha_binhash(unsigned char *, int);
int inode_block_pending(unsigned long long, unsigned long long);
void flush_dta_queue();
void loghash(char *, unsigned char *);
int btdelete_curkey(int, void *, int, void *, int, const char *);
void log_fatal_hash(char *, unsigned char *);
void write_dbb_to_cache(INOBNO *, unsigned char *);
void brand_blocksize();
void drop_databases();
DAT *search_nhash(TCNDB *, void *, int);
void flush_queue(unsigned long long, bool);
void flush_abort(unsigned long long);
void start_flush_commit();
void end_flush_commit();
void wait_hash_pending(unsigned char *);
void delete_stored(INOBNO *, unsigned char *);
void auto_repair(INOBNO *);
void restore_trunc_mode(unsigned long long, struct stat *);
void truncation_wait();
void restart_truncation();
TCBDB *tc_set_btreedb(int);
TCHDB *tc_set_hashdb(int);
INUSE *get_offset_reclaim(unsigned long long, unsigned long long);
DAT *btsearch_keyval(int, void *, int, void *, int, bool);
void tc_restart_truncation();
void start_transactions();
void commit_transactions();
void listdbu();
void flistdbu();
void listfree();
void listdbb();
void listdirent();
void list_symlinks();
void list_hardlinks();
unsigned long long get_offset_fast(unsigned long long);
void listdbp();
void listdta();
void abort_transactions();
