#!/bin/sh
##################################################
# A simple regression script for testing lessfs  #
##################################################

export CFLAGS="-Wall -march=core2 -O2 -I /usr/local/BerkeleyDB.4.8/include -L /usr/local/BerkeleyDB.4.8/lib"


end_exit()
{
  echo $MSG
  exit 1
}

check_prerequisites()
{
  MSG="libsnappy not found"
  ldconfig  -v | grep snappy
  [ $? != 0 ] && end_exit
  MSG="liblzo not found"
  ldconfig  -v | grep lzo
  [ $? != 0 ] && end_exit
  MSG="hamsterdb not found"
  ldconfig  -v | grep hamsterdb
  [ $? != 0 ] && end_exit
  MSG="BerkeleyDB not found"
  ldconfig  -v | grep db-4.8
  [ $? != 0 ] && end_exit

  MSG="iozone needs to be installed"
  IOZONE=`which iozone`
  [ $? != 0 ] && end_exit

  MSG="fsx-linux needs to be installed"
  FSX=`which fsx-linux`
  [ $? != 0 ] && end_exit

  MSG="prove needs to be installed"
  PROVE=`which prove`
  [ $? != 0 ] && end_exit

}

test_backends()
{
# GENERAL TEST
   BACKENDS="--with-tokyocabinet --with-berkeleydb --with-hamsterdb"
   for b in $BACKENDS
   do
      ./configure $b $CUROPT
      make clean; make -j6
      MSG="mklessfs failed"
      cp DB_CONFIG /tmp/mta
      ./mklessfs -f -c /tmp/lessfs.cfg
      [ $? != 0 ] && end_exit
      MSG="Failed to mount lessfs"
      ./lessfs /tmp/lessfs.cfg $MOUNTPOINT
      [ $? != 0 ] && end_exit
      cd $MOUNTPOINT
      MSG="iozone returned non zero"
      iozone -a
      [ $? != 0 ] && end_exit
      MSG="fsx-linux failed"
      $FSX -N 100000 all
      [ $? != 0 ] && end_exit
      MSG="Failed the POSIX filesystem test"
      mkdir prove
      cd prove
      $PROVE -r $CURDIR/pjd-fstest-20080816-lessfs
      [ $? != 0 ] && end_exit
      cd $CURDIR; cd ..
      umount $MOUNTPOINT
      rm -rf /tmp/dta/*
      rm -rf /tmp/mta/*
   done
cd $CURDIR; cd ..
}


###################################
#  MAIN
###################################

check_prerequisites

umount /tmp/lessfs 2>/dev/null
[ -d /tmp/dta ] && rm -rf /tmp/dta
[ -d /tmp/mta ] && rm -rf /tmp/mta
[ -d /tmp/lessfs ] && rm -rf /tmp/lessfs

CURDIR=`pwd`
MSG="Failed to compile pjd-fstest"
cd pjd-fstest-20080816-lessfs
make
[ $? != 0 ] && end_exit
cd ..; cd ..

MOUNTPOINT=/tmp/lessfs

mkdir /tmp/dta
[ $? != 0 ] && exit 1
mkdir /tmp/mta
[ $? != 0 ] && exit 1
mkdir $MOUNTPOINT
[ $? != 0 ] && exit 1

cp regression/lessfs.cfg /tmp/lessfs.cfg
CUROPT=""
test_backends
echo "First basic backend test PASSED"

export PASSWORD="1234"
CUROPT="--with-lzo --with-crypto"
cat regression/lessfs.cfg | grep -v COMPRE >/tmp/lessfs.cfg 
echo "COMPRESSION=lzo" >>/tmp/lessfs.cfg
test_backends
echo "Second backend test (LZO + ENCRYPTION) PASSED"

CUROPT="--with-snappy"
cat regression/lessfs.cfg | grep -v COMPRE >/tmp/lessfs.cfg
echo "COMPRESSION=snappy" >>/tmp/lessfs.cfg
test_backends
echo "Third backend test (SNAPPY) PASSED"

CUROPT="--with-snappy --with-encryption"
cat regression/lessfs.cfg | grep -v COMPRE >/tmp/lessfs.cfg
echo "COMPRESSION=snappy" >>/tmp/lessfs.cfg
test_backends
echo "Last backend test (SNAPPY/ENCRYPTED) PASSED"
echo "ALL TESTS PASSSED : DON'T WORRY, BE HAPPY"
cd $CURDIR/pjd-fstest-20080816-lessfs
make clean
cd $CURDIR
