#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.01)
AC_INIT(lessfs, 1.5.8, bugs@lessfs.com)
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_SRCDIR([listdb.c])
AC_CONFIG_HEADER([config.h])

AC_SUBST(DEPS_CFLAGS)
# Checks for programs.
AC_PROG_CC

# Checks for header files.
AC_FUNC_ALLOCA
AC_HEADER_DIRENT
AC_HEADER_STDC

# Checks for typedefs, structures, and compiler characteristics.
AM_PROG_CC_C_O
AC_HEADER_STDBOOL
AC_C_CONST
AC_TYPE_UID_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_STRUCT_ST_BLOCKS
AC_CHECK_MEMBERS([struct stat.st_rdev])
AC_HEADER_TIME
AC_C_VOLATILE
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_REALLOC
AC_FUNC_SELECT_ARGTYPES
AC_TYPE_SIGNAL
AC_FUNC_STAT
AC_FUNC_STRCOLL
AC_CHECK_FUNCS([getpass uname strcasecmp bzero gethostbyaddr gethostbyname gettimeofday memmove memset mkdir select setenv socket strchr strdup strerror strrchr strstr mhash_init])

AC_CHECK_HEADERS([sys/vfs.h arpa/inet.h fcntl.h inttypes.h limits.h malloc.h memory.h netdb.h netinet/in.h stddef.h stdint.h stdlib.h string.h strings.h sys/socket.h sys/time.h syslog.h unistd.h utime.h mhash.h])
# Check whether to enable filesystem encryption
CRYPTO_LIBS="$LIBS -lmhash -lpthread"
CRYPTO_MSG="no"


AC_CHECK_HEADER(mhash.h,
                [AC_DEFINE(HAVE_MHASH_H)],
                [AC_MSG_ERROR([Please install mhash])])


AC_ARG_WITH(crypto,
        [  --with-crypto           Enable lessfs filesystem encryption.],
        [
                if test "x$withval" != "xno" ; then
                        AC_DEFINE(ENABLE_CRYPTO, 1,
                                [Define if you want to allow filesystem encryption])
			CRYPTO_LIBS="$LIBS -lssl -lmhash"
                        CRYPTO_MSG="yes"
			AC_CHECK_HEADERS(openssl/ssl.h, [], [], [
			])
			MYDEPS="$MYDEPS openssl > 0.9.5"
                fi
        ]
)

LZO_MSG="no"
AC_ARG_WITH(lzo,
        [  --with-lzo              Enable the use of LZO compression.],
        [
                if test "x$withval" != "xno" ; then
                        AC_DEFINE(LZO, 1,
                                [Define if you want to enable LZO compression])
			LZO_LIBS="$LIBS -llzo2"
                        EXTRAFILES=lib_lzo.c
			AC_CHECK_HEADERS(lzo/lzoconf.h, [], [], [
			])
			AC_CHECK_LIB(lzo2, lzo1a_compress, [LZO_LIBS="-llzo2"], [AC_MSG_ERROR("No liblzo2 found")])
                        LZO_MSG="yes"
                fi
        ]
)

SNAPPY_MSG="no"
AC_ARG_WITH(snappy,
        [  --with-snappy           Enable the use of googles snappy compression.],
        [
                if test "x$withval" != "xno" ; then
                        AC_DEFINE(SNAPPY, 1,
                                [Define if you want to enable SNAPPY compression])
                        SNAPPY_LIBS="$LIBS -lsnappy"
                        EXTRAFILES=lib_snappy.c
                        AC_CHECK_HEADERS(snappy-c.h, [], [], [
                        ])
                        AC_CHECK_LIB(snappy, snappy_compress, [SNAPPY_LIBS="-lsnappy"], [AC_MSG_ERROR("No libsnappy found")])
                        LZO_MSG="yes"
                fi
        ]
)



HAMSTERDB_MSG="no"
AC_ARG_WITH(hamsterdb,
        [  --with-hamsterdb        Use hamsterdb instead of tokyocabinet.],
        [
                if test "x$withval" != "xno" ; then
                        AC_DEFINE(HAMSTERDB, 1,
                                [Define if you want to use hamsterdb instead of tokyocabinet])
                        HAMSTERDB_LIBS="$LIBS -lhamsterdb"
                        EXTRAFILES=lib_hamster.c
                        AC_CHECK_HEADERS(ham/hamsterdb.h, [], [], [
                        ])
                        AC_CHECK_LIB(hamsterdb, ham_env_create, [HAMSTERDB_LIBS="-lhamsterdb"], [AC_MSG_ERROR("Hamsterdb is not found")])
                        HAMSTERDB_MSG="yes"
                fi
        ]
)

BERKELEYDB_MSG="no"
AC_ARG_WITH(berkeleydb,
        [  --with-berkeleydb       Use berkeleydb instead of tokyocabinet.],
        [
                if test "x$withval" != "xno" ; then
                        AC_DEFINE(BERKELEYDB, 1,
                                [Define if you want to use berkeleydb instead of tokyocabinet])
                        BERKELEYDB_LIBS="$LIBS -ldb"
                        EXTRAFILES=lib_bdb.c
                        AC_CHECK_HEADERS(db.h, [], [], [
                        ])
                        AC_CHECK_LIB(db, db_env_create , [BERKELEYDB_LIBS="-ldb"], [AC_MSG_ERROR("Berkeleydb is not found")])
                        HAMSTERDB_MSG="yes"
                fi
        ]
)

DEBUG_MSG="no"
AC_ARG_ENABLE(debug,
        [  --enable-debug          Enable debug messages],
        [
                if test "$enable_debug" = "yes"; then
                        AC_DEFINE(DEBUG, 1,
                                [Define if you want debug information])
                        DEBUG_MSG="yes"
                fi
        ]
)

MEMTRACE_MSG="no"
AC_ARG_ENABLE(memtrace,
        [  --enable-memtrace       Enable tracing memory allocation],
        [
                if test "$enable_memtrace" = "yes"; then
                        AC_DEFINE(MEMTRACE, 1,
                                [Define if you want to trace memory allocation])
                        MEMTRACE_MSG="yes"
                fi
        ]
)

DBGLOCK_MSG="no"
AC_ARG_ENABLE(lckdebug,
        [  --enable-lckdebug       Enable debugging of mutex deadlocks],
        [
                if test "$enable_lckdebug" = "yes"; then
                        AC_DEFINE(DBGLOCK, 1,
                                [Define if you want to debug locking])
                        DBGLOCK_MSG="yes"
                fi
        ]
)

DBGLOCK_MSG="no"
AC_ARG_ENABLE(filelog,
        [  --enable-filelog        Enable writing log messages to a file instead of syslog],
        [
                if test "$enable_filelog" = "yes"; then
                        AC_DEFINE(FILELOG, 1,
                                [Define if you want to debug locking])
                        FILELOCK_MSG="yes"
                fi
        ]
)

AC_SUBST(CRYPTO_FLAGS)
AC_SUBST(CRYPTO_LIBS)
AC_SUBST(LZO_LIBS)
AC_SUBST(SNAPPY_LIBS)
AC_SUBST(HAMSTERDB_LIBS)
AC_SUBST(BERKELEYDB_LIBS)
PKG_CHECK_MODULES(DEPS, tokyocabinet >= 1.4.21 fuse >= 2.8.0 ${MYDEPS})
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
