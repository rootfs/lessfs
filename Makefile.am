prefix=@prefix@
exec_prefix=@exec_prefix@
libdir=@libdir@
includedir=@includedir@

ARCH := $(shell uname -m)
LIBS=$(CRYPTO_LIBS) $(DEPS_LIBS) $(LZO_LIBS) $(HAMSTERDB_LIBS) $(BERKELEYDB_LIBS) $(SNAPPY_LIBS)
bin_PROGRAMS=lessfs
sbin_PROGRAMS=mklessfs listdb lessfsck replogtool
man1_MANS=lessfs.1 replogtool.1
lessfs_SOURCES=lib_hamster.c lib_qlz15.c lib_common.c lib_lzo.c lessfs.c lib_crypto.c lib_safe.c lib_tc.c file_io.c lib_cfg.c lib_str.c lib_net.c lib_qlz.c lib_log.h lib_repl.c lib_bdb.c lib_snappy.c
lessfs_CFLAGS=-D_REENTRANT -D__USE_LARGEFILE64 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -D${ARCH} -D SYSLOG ${DEPS_CFLAGS}
mklessfs_SOURCES=lib_hamster.c lib_common.c lib_qlz15.c lib_lzo.c file_io.c mklessfs.c lib_crypto.c lib_safe.c lib_tc.c lib_cfg.c lib_str.c lib_net.c lib_qlz.c lib_repl.c lib_bdb.c lib_snappy.c
mklessfs_CFLAGS=-D_REENTRANT -D__USE_LARGEFILE64 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -D${ARCH} -D SYSLOG ${DEPS_CFLAGS}
listdb_SOURCES=lib_hamster.c lib_common.c lib_lzo.c lib_qlz15.c file_io.c listdb.c lib_crypto.c lib_safe.c lib_tc.c lib_cfg.c lib_str.c lib_net.c lib_qlz.c lib_repl.c lib_bdb.c lib_snappy.c
listdb_CFLAGS=-D_REENTRANT -D__USE_LARGEFILE64 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -D${ARCH} ${DEPS_CFLAGS}
lessfsck_SOURCES=lib_hamster.c lib_common.c lib_lzo.c lib_qlz15.c file_io.c lessfsck.c lib_crypto.c lib_safe.c lib_tc.c lib_cfg.c lib_str.c lib_net.c lib_qlz.c lib_repl.c lib_bdb.c lib_snappy.c
lessfsck_CFLAGS=-D_REENTRANT -D__USE_LARGEFILE64 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -D${ARCH} -D SYSLOG ${DEPS_CFLAGS} 
replogtool_SOURCES=lib_hamster.c lib_common.c lib_lzo.c lib_qlz15.c file_io.c replogtool.c lib_crypto.c lib_safe.c lib_tc.c lib_cfg.c lib_str.c lib_net.c lib_qlz.c lib_repl.c lib_bdb.c lib_snappy.c
replogtool_CFLAGS=-D_REENTRANT -D__USE_LARGEFILE64 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -D${ARCH} ${DEPS_CFLAGS}

Name: lessfs
Description: lessfs a data deduplicating filesystem
Version: @VERSION@

pkgconfigdir = @pkgconfigdir@
$(pkgconfig_DATA): config.status
