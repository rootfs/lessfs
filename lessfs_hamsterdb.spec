Summary:	Lessfs is an inline data deduplicating filesystem
Name:		lessfs
Version:	1.5.11
Release:	hm%{?dist}
License:	GPLv3+
Group:		Applications/System
URL:            http://www.lessfs.com
Source:         http://downloads.sourceforge.net/%{name}/%{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}
BuildRequires:  tokyocabinet-devel 
BuildRequires:  openssl-devel 
BuildRequires:  mhash-devel
BuildRequires:  fuse-devel
BuildRequires:  autoconf

Requires: fuse
Requires: mhash
Requires: tokyocabinet
Requires: hamsterdb

%description
Lessfs is an inline data deduplicating filesystem.

%prep
%setup -q

%build
autoconf
export CFLAGS="-O2"
%configure --with-crypto --with-hamsterdb
make %{?_smp_mflags}

%install
rm -rf %{buildroot}
make DESTDIR=%{buildroot} install
install -D -m 755 etc/lessfs-init_example %{buildroot}/etc/init.d/lessfs-init_example
install -D -m 755 etc/lessfs.cfg-hamsterdb %{buildroot}/etc/lessfs.cfg-hamsterdb

rm -rf %{buildroot}%{_datadir}/%{name}
rm -rf %{buildroot}%{_libdir}/lib%{name}.a

%clean
rm -rf %{buildroot}

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files
%defattr(-, root, root, -)
%doc FAQ ChangeLog COPYING README
%{_bindir}/lessfs
%{_sbindir}/mklessfs
%{_sbindir}/replogtool
%{_sbindir}/lessfsck
%{_sbindir}/listdb
%{_mandir}/man1/lessfs.1.gz
%{_mandir}/man1/replogtool.1.gz
/etc/init.d/lessfs-init_example
/etc/lessfs.cfg-hamsterdb

%changelog
* Sat Apr 21 2011 Mark Ruijter <mruijter@lessfs.com> - 1.5.11
- A bug in configure.in caused DEBUG to support
- to be compiled in when --disable-debug was specified.
- This was a major cause of slow performance reports.
* Sat Apr 21 2011 Mark Ruijter <mruijter@lessfs.com> - 1.5.10
- Added support for hamsterdb-2.0.x
* Thu Sep 29 2011 Mark Ruijter <mruijter@lessfs.com> - 1.5.8
- Code clean-ups.
* Sat Sep 17 2011 Mark Ruijter <mruijter@lessfs.com> - 1.5.7
- Fix a possible deadlock (timing) introduced in 1.5.6
- Adds support for google SNAPPY compression.
* Fri Sep 16 2011 Mark Ruijter <mruijter@lessfs.com> - 1.5.6
- Added support to force replog rotation
- echo 1 >/.lessfs/replication/rotate_replog
- Solved a bug that caused the replog not to rotate after
- REPLOG_DELAY had expired.
- lessfs_stats now shows the compression/deduplication ratio.
- Bug fix that increases the IOPS performance of lessfs from
- approx 1000 IOPS to > 6000 IOPS. (Measured with BDB on an
- Intel X25M SSD).
* Tue Sep 6 2011 Mark Ruijter <mruijter@lessfs.com> - 1.5.3
- More replication fixes after unclean shutdown of the 
- master or the slave.
* Mon Sep 5 2011 Mark Ruijter <mruijter@lessfs.com> - 1.5.2
- Fixes with replication after an unclean shutdown of the master.
- In this case the master would revert the running transactions but
- failed to remove them from the replication logfile.
- Removes flock when used with chunk_io
* Thu Aug 4 2011 Mark Ruijter <mruijter@lessfs.com> - 1.5.0
- Added chunk_io
* Wed June 22 2011 Mark Ruijter <mruijter@lessfs.com> - 1.4.9
- Lessfs can now log to a file as well as syslog.
- File logging can be enabled with configure --enable-filelog 
- after which lessfs will log to : /var/log/lessfs.log
* Mon June 6 2011 Mark Ruijter <mruijter@lessfs.com> - 1.4.8
- Solves a problem that would cause lessfschk to segfault.
- Fixes a problem with rename and inode caching.
- Adds backward compatability with <= lessfs-1.4.2
* Tue May 31 2011 Mark Ruijter <mruijter@lessfs.com> - 1.4.7
- Solves a problem with the new block sort routines that
- can cause a segfault. Also fixes a problem where hardlinking
- a hardlink would sometimes return a not null terminated string.
* Sun May 29 2011 Mark Ruijter <mruijter@lessfs.com> - 1.4.6
- A problem with the block sorting routine has been solved.
* Fri May 27 2011 Mark Ruijter <mruijter@lessfs.com> - 1.4.5
- The worker threads now sort the work that needs to be done
- one inode-blocknr so that data will be written sequential
- on disk. -ENOSPC handling. Lessfs now no longer freezes
- when the disk fills up. Instead it returns -ENOSPC.
- When the disk is filled to MIN_SPACE-FREE - 3% lessfs 
- will now reclaim space much more agressive.
- Lessfs now uses HASH databases instead of BTREE databases
- when compiled with BerkeleyDB.
* Thu May 09 2011 Mark Ruijter <mruijter@lessfs.com> - 1.4.2
- Since BerkeleyDB 4.3          
- Berkeley DB supports degree 2 isolation, which discards 
- read locks held by a transaction in order to minimize 
- the number of locks required by, for example, a cursor 
- iterating through a database. Lessfs now uses this isolation
- level and as a result use less locks and is 10% faster then
- lessfs-1.4.0 with BDB. A nasty bug with hamsterdb was solved.
* Thu May 05 2011 Mark Ruijter <mruijter@lessfs.com> - 1.4.0
- This is a new major release of lessfs.
- It's focus has been on reliability and crash recovery.
- Lessfs now comes with support for Berkeleydb which is the
- only backend that survived all our tests. Also changed is
- the way that deleted chunks are recycled. 
- Other changes : The lessfs replication sequence can be read
- from .lessfs/replication/sequence. This file is only used
- with batch replication.
* Thu Apr 28 2011 Mark Ruijter <mruijter@lessfs.com> - 1.3.3.12
- Added support for hamsterdb in listdb.
- When used with batch replication the master will now
- close the replication log after reaching the configured size
- or after 15 minutes, whatever comes first.
- The interval can be adjusted at compile time in lib_repl.h
- #define REPLOG_DELAY 15*60
* Wed Apr 17 2011 Mark Ruijter >mruijter@lessfs.com> - 1.3.3.11
- This version of lessfs introduces watchdir replication.
- Introduces a new configure option to debug memory
- allocation. 
* Wed Apr 06 2011 Mark Ruijter >mruijter@lessfs.com> - 1.3.3.9
- A new tool that allows commiting replication logfiles to
- a slave. This enables the use of for example sftp to transfer
- replication logfiles. Replication has been improved.
* Tue Mar 22 2011 Mark Ruijter >mruijter@lessfs.com> - 1.3.3.8
- lessfs_read has been optimized
* Sat Mar 19 2011 Mark Ruijter >mruijter@lessfs.com> - 1.3.3.7
- Solves a problem where entering 0.0.0.0 as replication
- listen IP failed to work. Solves a problem that would
- sometimes cause fsx-linux to fail on lessfs running on i386.
* Fri Mar 18 2011 Mark Ruijter >mruijter@lessfs.com> - 1.3.3.6
- Minor code cleanups. Lessfs now compiles with -Werror
* Tue Mar 15 2011 Mark Ruijter >mruijter@lessfs.com> - 1.3.3.5
- Minor code cleanups.
* Tue Mar 08 2011 Mark Ruijter >mruijter@lessfs.com> - 1.3.3.4
- Replication connect fix.
* Sun Mar 06 2011 Mark Ruijter >mruijter@lessfs.com> - 1.3.3.3
- Major replication code rewrite.
* Wed Feb 09 2011 Mark Ruijter <mruijter@lessfs.com> - 1.3.3.1
- Fixes a cache corruption bug.
* Wed Feb 02 2011 Mark Ruijter <mruijter@lessfs.com> - 1.3.3
- Disable mmap use with hamsterdb.
- Improved replication. Many code cleanups.
* Wed Jan 19 2011 Mark Ruijter <mruijter@lessfs.com> - 1.3.2
- Make hamsterdb work without transactions.
* Sat Jan 15 2011 Mark Ruijter <mruijter@lessfs.com> - 1.3.1
- Adds support for hamsterdb. Fixes a memory leak and a
- problem when lessfs is used with encryption.
* Wed Dec 23 2010 Mark Ruijter <mruijter@lessfs.com> - 1.2.2.6
- Fixes a silly mistake that may cause a segfault upon mounting.
* Wed Dec 22 2010 Mark Ruijter <mruijter@lessfs.com> - 1.2.2.5
- Fixes a problem with lessfs_stats
* Wed Dec 22 2010 Mark Ruijter <mruijter@lessfs.com> - 1.2.2.4
- Improved cache eviction.
- When a lot of small files are copied, exceeding cache size,
- this change improves the performance with 40%.
* Sun Dec 19 2010 Mark Ruijter <mruijter@lessfs.com> - 1.2.2.3
- This release improves the speed of metadata operations 
- by 30~50%.
* Sun Dec 12 2010 Mark Ruijter <mruijter@lessfs.com> - 1.2.2.2
- This release enables some sane cache settings by default.
- It also solves a problem with sending the replication backlog.
- The lib_tc code has been split in two files.
* Sun Oct 31 2010 Mark Ruijter <mruijter@lessfs.com> - 1.2.0
- Improves the speed of metadata operation by 8%.
- Lessfsck now only checks all the metadata by default.
- It used to check the actual integrity of the files,
- which is very time consuming. 
- The -t option still allows this.
* Wed Oct 27 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.9.10
- This release introduces a cache layer for metadata which makes
- this code up to 12 times faster when small files are copied.
* Mon Oct 18 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.9.8
- Introduces a cache layer for metadata making this
- release up to 12 times faster when small files are copied.
* Sun Oct 17 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.9.7
- Fixes a bug where truncation would fail when encryption had been
- selected with the tc datastore.
* Wed Oct 13 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.9.6
- This enables lessfs background truncation threads
- to resume after an umount or shutdown. Background truncation
- is now enabled by default. Because lessfs uses the freelist
- database to keep truncation state this database now needs
- to be configured in lessfs.cfg even when using the tc datastore.
* Fri Oct  1 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.9.3
- Fixes a problem with replication.
- Prioritizes reads and writes above truncation.
* Wed Sep 29 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.9.1
- Fixes some nasty bugs with replication. Improves the du, ls -als
- statistics as well as better .lessfs_stats when lessfs
- truncates a file. .lessfs_stats is now fairly accurate when
- used with file_io. A problem that is not solved is that when
- a file is stored twice on lessfs, the second time it will
- show that the file uses 0 bytes. This is true until we remove
- the first copy of the file. A that time the second copy of the
- file is the reason that the blocks are still on the filesystem.
- It is however very difficult if not impossible with the current
- implementation to link these blocks back to file2.
* Sat Sep 25 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.9
- This release adds sync and async replication.
* Tue Sep 21 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.8.1
- Fixes a small bug that disabled lessfs_stats
* Mon Sep 20 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.8
- This release contains a number of small bug fixes as well
- as improved truncation code. Lessfs can now truncate
- files as a background process. Files that are being truncated
- are no longer read/write locked but are now only write locked.
- During truncation a file with the inodenumber of the truncated file
- as filename will be visible in /.lessfs/locks/ to indicate that
- the inode is still being processed.
* Sun Aug 22 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.6
- This release adds a number of features to lessfs.
- A. Lessfs is now able to spawn a background thread for
- delete operations. Truncate and delete operations will
- appear to be very fast when this feature is enabled.
- B. Lessfs now supports native synchronous replication.
- This should be considered alpha code, although it
- works for me ;-). For now the lessfs master will wait
- for ever on the slave. So if the slave dies or disconnects
- the master will try to reconnect and freeze until this
- has succeeded. In the future it will be possible to
- have the master write a backlog after a certain amount of time
- or after manual intervention. 
* Wed Jul 21 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.5
- This release fixes a deadlock when lessfs is used with 
- samba. It also improves crash resilience.
* Fri Jul 16 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.4
- Improves lessfschk.
* Thu Jul 15 2010 Mark Ruijter <mruijter@lessfs.com> - 1.1.3
- This release fixes a problem where lessfs would leave
- orphaned data chunks in the system under high load.
- A number of problems with lessfschk has been solved.
* Wed Jul 07 2010 Mark Ruijter <mruijter@gmail.com> - 1.1.2
- This release fixes a bug discovered by Dave.
- Lessfs now survives this test script:
- #! /bin/bash
- for n in $(seq -w 1 10 ); do
-         mkdir /data1/data/fsx$n
-         cd /data1/data/fsx$n
-         fsx-linux all >/tmp/fsx$n &
- 
-         mkdir /data1/data/bonnie$n
-         bonnie++ -u root -d /data1/data/bonnie$n >/tmp/bonnie$n 2>&1 &
- 
-         mkdir /data1/data/iozone$n
-         cd /data1/data/iozone$n
-         iozone -a >/tmp/iozone$n &
- done
* Fri Jul 02 2010 Mark Ruijter <mruijter@gmail.com> - 1.1.1
- Fixed a race condition that would manifest itself
- with highly concurrent IO.
* Wed Jun 30 2010 Mark Ruijter <mruijter@gmail.com> - 1.1.0
- Marc Christiansen has provided a patch that fixed a crash
- and enhances lessfs_stats output. Many problems have
- been fixed with the new cache code. 
* Sun Jun 20 2010 Mark Ruijter <mruijter@gmail.com> - 1.1.0-beta5
- Fixes a bug where reads would mostly mis the cache.
- Read performance has now dramatically (300%) increased
- for chunks of data that are found in the cache.
* Tue Jun 19 2010 Mark Ruijter <mruijter@gmail.com> - 1.1.0-beta4
- Fixes a (rare) race condition with the file_io backend.
- Improved write performance when writing smaller then BLKSIZE
- data chunks. General code cleaning.
* Tue Jun 15 2010 Mark Ruijter <mruijter@gmail.com> - 1.1.0-beta3
- Under some circumstances a newly written block of data
- with hash (A) could be overwritten before the previous
- write had finished. A new 'per hash' locking mechanisme    
- now makes sure that this never happens.
- See create_hash_note for details.
* Thu Jun 10 2010 Mark Ruijter <mruijter@gmail.com> - 1.1.0-beta2
- Fixed a deadlock. Lessfs now supports deadlock reporting
- Telnet localhost 100 -> lockstatus will show details about locking
* Thu Jun 10 2010 Mark Ruijter <mruijter@gmail.com> - 1.1.0-beta1
- A number of race conditions has been fixed.
* Wed Jun 2 2010 Mark Ruijter <mruijter@gmail.com> - 1.1.0-alpha1
- This release changes lessfs internals in a major way.
- Lessfs-1.1.0 is _not_ compatible with previous lessfs versions.
- This new version provides a much improved cache layer and 
- way better performance. Threading has been improved and lessfs
- is now capable of using many threads /CPU's.
* Tue Mar 30 2010 Mark Ruijter <mruijter@gmail.com> - 1.0.8
- This release enables lessfs to be mounted without the
- need to specify other options then the configuration file
- and the mountpoint. Please consult the manual for more
- details. Eric D. Garver contributed a patch that
- makes the build process less picky about missing
- GNU files like INSTALL and NEWS. A bug in lessfs_read
- has been found by extensive testing with fsx-linux. In
- cases where a sparse block of data would be followed by
- a normal block lessfs_read would return wrong data in 
- some cases. Added automatic migration support for older
- lessfs versions.
* Sat Mar 27 2010 Mark Ruijter <mruijter@gmail.com> - 1.0.7
- This release fixes a problem where data copied
- from windows to lessfs (samba) would show the wrong
- nr of blocks. This would result in du reporting wrong
- numbers.
* Thu Mar 11 2010 Mark Ruijter <mruijter@gmail.com> - 1.0.6
- Fixes a segfault that may occur when lessfs is used 
- without transactions enabled. The segfault occurs when
- lessfs is unmounted after closing the databases. The 
- impact of the bug is therefore low.
* Sun Mar 07 2010 Mark Ruijter <mruijter@gmail.com> - 1.0.5
- Fixes a small problem with logging.
* Thu Mar 03 2010 Mark Ruijter <mruijter@gmail.com> - 1.0.4
- This release enables support for transactions/checkpointing.
- Lessfs now no longer needs fsck after a crash. Also new is the ability to
- run a program when disk space drops below a certain amount of space. 
- This program can be used to free up space when the tokyocabinet 
- datastore is used.
* Sun Jan 24 2010 Mark Ruijter <mruijter@gmail.com> - 1.0.1
- Fixes a rare race condition that can cause lessfs to crash.
* Mon Dec 30 2009 Mark Ruijter <mruijter@gmail.com> - 1.0.0
- Removed all the bugs. ;-)
* Mon Dec 21 2009 Mark Ruijter <mruijter@gmail.com> - 0.9.6
- Fix an erroneous free() that can crash lessfs upon startup
- when the tiger hash is selected. Changes mklessfs so that
- it supports automatic directory creation and database overwrites.
- mklessfs now has improved error reporting.
* Wed Dec 16 2009 Mark Ruijter <mruijter@gmail.com> - 0.9.4
- Fixes two memory leaks that are related to hardlink operations.
- Solves a problem caused by not initializing st_[a/c/m]time.tv_nsec.
- Thanks to Wolfgang Zuleger for doing a great job on analyzing these bugs.
- Fixed a memory leak in file_io.
* Sun Dec 13 2009 Mark Ruijter <mruijter@gmail.com> - 0.9.3
- Partial file truncation encryption caused data corruption.
* Sat Dec 12 2009 Mark Ruijter <mruijter@gmail.com> - 0.9.2
- This release fixes some problems where permissions where not properly
- set on open files. It also fixes a problem with the link count
- of directories. Performance for some meta data operations has improved.
* Fri Dec 11 2009 Mark Ruijter <mruijter@gmail.com> - 0.9.1
- Fix permission problems with open files.
* Wed Dec 09 2009 Mark Ruijter <mruijter@gmail.com> - 0.9.0
- Lessfs now passes fsx-linux. The problems reported with rsync
- have now been solved. Major changes of the truncation code.
* Sun Nov 15 2009 Mark Ruijter <mruijter@gmail.com> - 0.8.3
- Fixes a major bug in the truncation code.
- This bug will crash lessfs when used with ccrypt or rsync –inplace.
* Sat Nov 09 2009 Mark Ruijter <mruijter@gmail.com> - 0.8.2
- Fixes a bug that causes lessfsck and mklessfs to segfault when compiled
- with encryption support and encryption disabled in the config.
- Fixes a bug that causes lessfs to segfault on umount when compiled
- with encryption support and encryption disabled in the config.
- lessfsck,listdb and mklessfs are now installed in /usr/sbin
- instead of /usr/bin.
* Sat Nov 07 2009 Mark Ruijter <mruijter@gmail.com> - 0.8.1
- Fixes a bug that causes mklessfs to segfault when DEBUG is not set.
- Adds lessfsck. lessfsck can be used  to check, optimize and repair 
- a lessfs filesystem.
* Mon Oct 26 2009 Mark Ruijter <mruijter@gmail.com> - 0.8.0
- Fixes a possible segfault when lessfs is used with lzo compression.
- Fixes a problem when compiling lessfs without encryption on
- a system without openssl-devel.
- Enhances the logging facility.
- Performance has improved for higher latency storage like iscsi, drbd.
- Reduces the number of fsync operations when sync_relax>0.
- 
- Thanks to : Roland Kletzing for finding and assisting
- with solving some of the problems mentioned.
 
* Fri Oct 22 2009 Adam Miller <maxamillion@fedoraproject.org> - 0.7.5-4
- Fixed missing URL field as well as missing Require for fuse
- Removed period from summary 

* Thu Oct 22 2009 Adam Miller <maxamillion@fedoraproject.org> - 0.7.5-3
  -Added fuse-devel and autoconf as build dependencies

* Wed Oct 21 2009 Adam Miller <maxamillion@fedoraproject.org> - 0.7.5-2
  -First attempt to build for Fedora review request
  -Based on upstream .spec, full credit of initial work goes to Mark Ruijter

* Fri Oct 16 2009 Mark Ruijter <mruijter@lessfs.com> - 0.7.5-1
  Fix a segfault on free after unmounting lessfs without
  encryption support. Fix a problem that could lead to a
  deadlock when using file_io with NFS.
  A performance improvement, changed a mutex lock for a
  spinlock.
* Sun Oct 11 2009 Mark Ruijter <mruijter@lessfs.com> - 0.7.4
  This version of lessfs introduces a new hash named
  Blue Midnight Whish : http://www.q2s.ntnu.no/sha3_nist_competition/start
  This is a very fast hash that increases lessfs performance
  significantly. The implementation makes it easy to use any
  of the hashes from the NIST hash competition. MBW was 
  choosen for lessfs because of the speed.
  To use BMW : configure --with-sha3
* Tue Oct 06 2009 Mark Ruijter <mruijter@lessfs.com> - 0.7.2
  Fix a typo in lib_tc.c that can lead to data corruption.
* Mon Oct 05 2009 Mark Ruijter <mruijter@lessfs.com> - 0.7.1
  Introduced a new data storage backend, file-io.
  Higher overall performance.
* Sun Sep 06 2009 Mark Ruijter <mruijter@lessfs.com> - 0.6.1
  Never improve your code minutes before releasing it.
  Fix a silly bug with mklessfs.
* Sun Sep 06 2009 Mark Ruijter <mruijter@lessfs.com> - 0.6.0
  Added encryption support to lessfs.
  Fixed one small bug that would leave orphaned meta data in the
  metadatabase when hardlinks where removed.
* Wed Aug 26 2009 Mark Ruijter <mruijter@lessfs.com> - 0.5.0
  Improved thread locking that leads to much better performance.
  Many NFS related problems have been solved and debugging
  is now easier.
* Mon Aug 17 2009 Mark Ruijter <mruijter@lessfs.com> - 0.2.8
  Many bugfixes, including incorrect filesize on writing
  in a file with various offsets using lseek. This also
  caused problems with NFS.
* Fri Aug 14 2009 Mark Ruijter <mruijter@lessfs.com> - 0.2.7
  Fixed a problem where dbstat failed to return the proper
  filesize. One other bug could leak to a deadlock of lessfs.
* Fri Jul 17 2009 Mark Ruijter <mruijter@lessfs.com> - 0.2.6
  Fixed two bugs, one which could lead to data corruption.
  One other that would leave deleted data in the database.
* Wed Jul 08 2009 Mark Ruijter <mruijter@lessfs.com> - 0.2.5
  This release fixes to one minor and one major bug.
  One bug in the code would actually crash lessfs
  upon renaming a file or directory. lessfs-0.2.4
  is no longer available for download.
* Sun Jul 05 2009 Mark Ruijter <mruijter@lessfs.com> - 0.2.4
  Added support for automatic defragmentation.
* Tue Jun 23 2009 Mark Ruijter <mruijter@lessfs.com> - 0.2.3
  This release fixes a small memory leak and improves
  write performance in general approx 12%.
  Known issues : 
  Using direct_io with kernel 2.6.30 causes reads to
  continue for ever. I am not sure if this is a kernel
  issue or a lessfs bug. With earlier kernels direct_io
  works fine.
* Sun Jun 21 2009 Mark Ruijter <mruijter@lessfs.com> - 0.2.2
  NFS support and improved caching code.
  WARNING : nfs will only work with kernel >= 2.6.30
* Wed Jun 10 2009 Mark Ruijter <mruijter@lessfs.com> - 0.2.1
  Improved the performance of writes smaller then 
  max_write in size. These writes will now remain long
  enough in the cache so that subsequent writes to the 
  same block will update the cache instead of the database.
  Mounting lessfs without formatting the filesystem now
  logs a warning instead of creating a segfault.
  Creating of sparse files now works again after being 
  broken in release 0.1.19.
* Mon May 25 2009 Mark Ruijter <mruijter@lessfs.com> - 0.2.0
  Added a cache that improves performance with approx 30%.
* Thu May 14 2009 Mark Ruijter <mruijter@lessfs.com> - 0.1.22
  Fixed a data corruption bug (workaround) when the 
  underlying filesystems run out of space. Fixed a problem
  with hardlinking symlinks.
* Wed Apr 22 2009 Mark Ruijter <mruijter@lessfs.com> - 0.1.20
  Fixed two bugs:
  1. Truncate operations would sometimes fail.
  2. unlink of hardlinked files would sometimes fail.
* Wed Apr 04 2009 Mark Ruijter <mruijter@lessfs.com> - 0.1.19 
  Fixed a bug in the truncation routine where a delete chunk 
  would remain in the database. Cleaned up the init script.
* Mon Mar 30 2009 Mark Ruijter <mruijter@lessfs.com> - 0.1.18 
* Mon Mar 27 2009 Mark Ruijter <mruijter@lessfs.com> - 0.1.17 
  Bug fix, reenable syslog.
* Mon Mar 27 2009 Mark Ruijter <mruijter@lessfs.com> - 0.1.16
* Mon Mar 23 2009 Mark Ruijter <mruijter@gmail.com>  - 0.1.15
* Sat Mar 21 2009 Mark Ruijter <mruijter@gmail.com>  - 0.1.14
* Tue Feb 24 2009 Mark Ruijter <mruijter@gmail.com>  - 0.1.13
- Initial package
