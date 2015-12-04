#define BERKELEYDB_ERRORLOG  "/var/log/lessfs-bdb_err.txt"
#define DBTzero(t)                    (memset((t), 0, sizeof(DBT)))
#define die_dataerr(f...) { LFATAL(f); exit(EXIT_DATAERR); }
#define die_syserr() { LFATAL("Fatal system error : %s",strerror(errno)); exit(EXIT_SYSTEM); }

void bdb_close();
void bdb_checkpoint();
void start_transactions();
void commit_transactions();
void bdb_open();
DAT *search_dbdata(int, void *, int, bool);
void drop_databases();
DB *bdb_set_db(int);
void delete_key(int, void *, int, const char *);
void bin_write_dbdata(int, void *, int, void *, int);
void btbin_write_dup(int, void *, int, void *, int, bool);
void btbin_write_dbdata(int, void *, int, void *, int);
void start_flush_commit();
int bt_entry_exists(int, void *, int, void *, int);
DDSTAT *dnode_bname_to_inode(void *, int, char *);
DAT *btsearch_keyval(int, void *, int, void *, int, bool);
int count_dirlinks(void *, int);
int fs_readdir(const char *, void *, fuse_fill_dir_t, off_t,
               struct fuse_file_info *);
void fs_read_hardlink(struct stat, DDSTAT *, void *, fuse_fill_dir_t,
                      struct fuse_file_info *);
void bdb_restart_truncation();
unsigned long long get_offset_fast(unsigned long long);
INUSE *get_offset_reclaim(unsigned long long, unsigned long long);
unsigned long long has_nodes(unsigned long long);
void bin_write(int, void *, int, void *, int);
int btdelete_curkey(int, void *, int, void *, int, const char *);
void release_bdb_lock();
int try_bdb_lock();
void bdb_lock(const char *);
void end_flush_commit();
void listdbp();
void listdbb();
void listdbu();
void listdta();
void list_symlinks();
void flistdbu();
void listfree();
void listdirent();
void list_hardlinks();
void bdb_stat();
void abort_transactions();
