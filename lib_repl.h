// replication_msg : db (char), op (char), ksize (int),vsize (int), key (char *),value (char *), crc32 (int)
typedef struct {
    unsigned char database;     // 0=dbdta,1=dbu,2=dbb
    unsigned char operation;    // 0=write,1=delete,2=read
    unsigned char *key;
    int ksize;
    unsigned char *value;
    int vsize;
    int crc32;
} REPLICATIONMSG;

#define REPLWRITE         0
#define REPLDELETE        1
#define REPLREAD          2
#define REPLDUPWRITE      3
#define REPLDELETECURKEY  4
#define REPLSETNEXTOFFSET 5
#define TRANSACTIONCOMMIT 6
#define TRANSACTIONABORT  7 

#define DBDTA       0
#define DBU         1
#define DBB         2
#define DBP         3
#define DBS         4
#define DBL         5
#define FREELIST    6
#define DBDIRENT    7
#define TRANSACTIONS 8
#define NEXTOFFSET 98
#define FDBDTA     99
#define WRITTEN   254

#define ACK  6
#define NAK 21
#define WATCHDIR_SLEEPINTERVAL 10
#define REPLOG_DELAY 15*60      // Allow a 15 minutes delay before sending an incompleted replog

int reconnect();
void *replication_worker(void *);
void process_message(char *, int);
int send_nak();
int send_ack();
void flush_recv_buffer();
int send_backlog();
void rotate_replog();
void write_replication_data(unsigned char, unsigned char, char *,
                            int, char *, int, int);
void write_repl_data(unsigned char, unsigned char, char *,
                     int, char *, int, int);
void merge_replog();
