#!/bin/sh
# $FreeBSD: src/tools/regression/fstest/tests/link/17.t,v 1.1 2007/01/17 01:42:09 pjd Exp $

desc="Test repeated linking in different directories"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..40"

n0=`namegen`
n1=`namegen`
n2=`namegen`
n3=`namegen`
n4=`namegen`
n5=`namegen`

for i in 1 2 3 4 5 6 7 8 9 10
do
   expect 0 mkdir $i 0644
done
expect 0 create 1/${n0} 0644
for i in 2 3 4 5 6 7 8 9 10
do
   expect 0 link 1/${n0} ${i}/${n0}
done
for i in 1 2 3 4 5 6 7 8 9 10
do
   expect 0 unlink ${i}/${n0}
   expect 0 rmdir ${i}
done
