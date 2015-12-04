#define die_dberr(f...) { LFATAL(f); exit(EXIT_DBERR); }
#define die_dataerr(f...) { LFATAL(f); exit(EXIT_DATAERR); }
#define die_syserr() { LFATAL("Fatal system error : %s",strerror(errno)); exit(EXIT_SYSTEM); }

#define LOCK 1
#define NOLOCK 0
#define MAX_THREADS 1
#define MAX_POSIX_FILENAME_LEN 256
#define MAX_FUSE_BLKSIZE 131072
#define METAQSIZE 2048
#define MAX_HASH_LEN 64
#define CACHE_MAX_AGE 3
#define MAX_ALLOWED_THREADS 1024
/* Used for debugging locking */
#define GLOBAL_LOCK_TIMEOUT 7200        //Since deleting or truncating can take a long time...
#define LOCK_TIMEOUT 3600
#define MAX_META_CACHE 100000   //This consumes approx 1.5 MB memory
#define ROTATE_REPLOG_SIZE 1073741824
#define RECLAIM_AGRESSIVE 128
#define HIGH_PRIO 0
#define MEDIUM_PRIO 1
#define LOW_PRIO 2
#define MAX_CHUNK_DEPTH 5
#define LFSSMALL 0                 // Filesystem <=1TB & BDB
#define LFSMEDIUM 1                // Filesystem <=10TB & BDB
#define LFSHUGE 2                  // Filesystem >10TB & BDB

typedef unsigned long long int word64;
typedef unsigned long word32;
typedef unsigned char byte;
void tiger(byte *, word64, word64 *);

enum IOTYPE {TOKYOCABINET,FILE_IO,MULTIFILE_IO,CHUNK_IO};

typedef struct {
    unsigned long long offset;
    unsigned long size;
    unsigned long long allocated_size;
    unsigned long long inuse;
} INUSE;

typedef struct {
    unsigned long long offset;
    unsigned long size;
    unsigned long long inuse;
} OLDINUSE;

typedef struct {
    unsigned long long inode;
    unsigned long long blocknr;
} INOBNO;

typedef struct {
    //unsigned int snapshotnr;
    unsigned long long dirnode;
    unsigned long long inode;
} DINOINO;

typedef struct {
    unsigned long size;
    unsigned char *data;
} DAT;

typedef struct {
    struct stat stbuf;
    unsigned long long real_size;
    char filename[MAX_POSIX_FILENAME_LEN + 1];
} DDSTAT;

typedef struct {
    char passwd[64];
    char iv[8];
} CRYPTO;

typedef struct {
    struct stat stbuf;
    unsigned int updated;
    unsigned long long blocknr;
    unsigned int opened;
    unsigned long long real_size;
    char filename[MAX_POSIX_FILENAME_LEN + 1];
} MEMDDSTAT;

struct truncate_thread_data {
    unsigned long long blocknr;
    unsigned long long lastblocknr;
    unsigned long long inode;
    unsigned int offsetblock;
    struct stat stbuf;
    bool unlink;
};

typedef struct {
    unsigned char data[MAX_FUSE_BLKSIZE];
    unsigned long datasize;
    unsigned char hash[MAX_HASH_LEN];
    int dirty;
    time_t creationtime;
    int pending;
    char newblock;
    unsigned long updated;
} CCACHEDTA;

static inline CCACHEDTA *get_ccachedta(uintptr_t p)
{
    return (CCACHEDTA *) p;
}

unsigned char *thash(unsigned char *, int);
void check_datafile_sanity();
int fs_readlink(const char *, char *, size_t);
int fs_symlink(char *, char *);
void invalidate_p2i(char *);
void erase_p2i();
void cache_p2i(char *, struct stat *);
void logiv(char *, unsigned char *);
void loghash(char *, unsigned char *);
void log_fatal_hash(char *, unsigned char *);
void create_hash_note(unsigned char *);
void wait_hash_pending(unsigned char *);
void delete_hash_note(unsigned char *);
void create_inode_note(unsigned long long);
void wait_inode_pending(unsigned long long);
int inode_isnot_locked(unsigned long long);
void wait_inode_pending2(unsigned long long);
void delete_inode_note(unsigned long long);
void die_lock_report(const char *, const char *);
void get_global_lock(const char *);
void get_global_lock(const char *);
int try_global_lock();
void release_global_lock();
void write_lock(const char *);
void release_write_lock();
int try_write_lock();
void get_hash_lock(const char *);
void release_hash_lock();
void get_inode_lock(const char *);
void release_inode_lock();
void get_offset_lock(const char *);
void release_offset_lock();
void meta_lock(const char *);
void repl_lock(const char *);
void release_repl_lock();
void release_replbl_lock();
void release_meta_lock();
int try_meta_lock();
void trunc_lock();
void release_trunc_lock();
void truncation_wait();
DAT *create_mem_ddbuf(MEMDDSTAT *);
DDSTAT *value_to_ddstat(DAT *);
DAT *create_ddbuf(struct stat, char *, unsigned long long);
void dbmknod(const char *, mode_t, char *, dev_t);
int get_realsize_fromcache(unsigned long long, struct stat *);
int dbstat(const char *, struct stat *, bool);
void comprfree(compr *);
void memddstatfree(MEMDDSTAT *);
MEMDDSTAT *value_tomem_ddstat(char *, int);
void ddstatfree(DDSTAT *);
int path_from_cache(char *, struct stat *);
int get_dir_inode(char *, struct stat *, bool);
void formatfs();
void write_file_ent(const char *, unsigned long long, mode_t mode, char *,
                    dev_t);
void write_nfi(unsigned long long);
void write_seq(unsigned long);
DAT *lfsdecompress(DAT *);
void auto_repair(INOBNO *);
unsigned long long readBlock(unsigned long long,
                             char *, size_t, size_t, unsigned long long);
void delete_inuse(unsigned char *);
void delete_dbb(INOBNO *);
unsigned long long getInUse(unsigned char *);
void DATfree(DAT *);
void update_inuse(unsigned char *, unsigned long long);
unsigned long long get_next_inode();
MEMDDSTAT *inode_meta_from_cache(unsigned long long);
void update_filesize_onclose(unsigned long long);
int update_filesize_cache(struct stat *, off_t);
void update_filesize(unsigned long long, unsigned long long,
                     unsigned int, unsigned long long);
void hash_update_filesize(MEMDDSTAT *, unsigned long long);
DAT *lfscompress(unsigned char *, unsigned long);
DAT *check_block_exists(INOBNO *);
int fs_mkdir(const char *, mode_t);
int db_unlink_file(const char *);
void partial_truncate_block(unsigned long long, unsigned long long,
                            unsigned int);
void *tc_truncate_worker(void *);
DAT *search_dbdata(int, void *key, int, bool);
DDSTAT *dnode_bname_to_inode(void *, int, char *);
char *lessfs_stats();
int count_dirlinks(void *, int);
struct tm *init_transactions();
int update_parent_time(char *, int);
int update_stat(char *, struct stat *);
void flush_wait(unsigned long long);
void db_close(bool);
void cook_cache(char *, int, CCACHEDTA *, unsigned long);
unsigned long long get_inode(const char *);
void purge_read_cache(unsigned long long, bool, char *);
void update_meta(unsigned long long, unsigned long, int);
void lessfs_trans_stamp();
int db_fs_truncate(struct stat *, off_t, char *, bool);
void write_trunc_todolist(struct truncate_thread_data *);
void tc_write_cache(CCACHEDTA *, INOBNO *);
void db_delete_stored(INOBNO *);
void fil_fuse_info(DDSTAT *, void *, fuse_fill_dir_t,
                   struct fuse_file_info *);
void locks_to_dir(void *, fuse_fill_dir_t, struct fuse_file_info *);
int lckname_to_stat(char *, struct stat *);
int fs_link(char *, char *);
int fs_rename_link(const char *, const char *, struct stat);
int fs_rename(const char *, const char *, struct stat);
int fs_rmdir(const char *);
void clear_dirty();
void parseconfig(int, bool);
void sync_all_filesizes();
int get_blocksize();
void brand_blocksize();
void btbin_write_dup(int database, void *, int, void *, int, bool);
void checkpasswd(char *);
int try_offset_lock();
int try_hash_lock();
int try_repl_lock();
int try_replbl_lock();
void worker_lock();
void release_worker_lock();
void cachep2i_lock(const char *);
void release_cachep2i_lock();
int try_cachep2i_lock();
unsigned long get_sequence();
void next_sequence();
char *ascii_hash(unsigned char *);
void open_trees();
void close_trees();
INUSE *get_offset(unsigned long long);
void inobnolistsort(TCLIST *list);
void mkchunk_dir(char *);
unsigned char *hash_to_ascii(unsigned char *);
