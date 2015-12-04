void ham_error(const char *);
int fs_readdir(const char *, void *, fuse_fill_dir_t, off_t,
               struct fuse_file_info *);
void fs_read_hardlink(struct stat, DDSTAT *, void *, fuse_fill_dir_t,
                      struct fuse_file_info *);
unsigned long long has_nodes(unsigned long long);
void hm_create(bool);
void hm_open();
void start_flush_commit();
void end_flush_commit();
unsigned long long get_offset_fast(unsigned long long);
INUSE *get_offset_reclaim(unsigned long long, unsigned long long);
DAT *btsearch_keyval(int, void *, int, void *, int, bool);
void bin_write_dbdata(int, void *, int, void *, int);
void bin_write(int, void *, int, void *, int);
void delete_key(int, void *, int, const char *);
int btdelete_curkey(int, void *, int, void *, int, const char *);
int try_com_lock();
void hm_close(bool);
void drop_databases();
int bt_entry_exists(int, void *, int, void *, int);
void hm_restart_truncation();
DAT *x_search_dbdata(int, void *, int, bool, char *, int);
int try_ham_lock();
void release_ham_lock();
void ham_lock(const char *);
void start_transactions();
void listdirent();
void listdbp();
void list_hardlinks();
void list_symlinks();
void listdbu();
void listdbb();
void listdta();
void listfree(int);
void abort_transactions();
void commit_transactions();
void btbin_write_dbdata(int , void *, int , void *, int);

