#!/bin/sh
# $FreeBSD: src/tools/regression/fstest/tests/link/17.t,v 1.1 2007/01/17 01:42:09 pjd Exp $

desc="Make sure that nothing goes wrong with repeated linking"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..12"

n0=`namegen`
n1=`namegen`
n2=`namegen`
n3=`namegen`
n4=`namegen`
n5=`namegen`

expect 0 create ${n0} 0644
expect 0 link ${n0} ${n1}
expect 0 link ${n0} ${n2}
expect 0 link ${n0} ${n3}
expect 0 link ${n0} ${n4}
expect 0 link ${n0} ${n5}
expect 0 unlink ${n0}
expect 0 unlink ${n1}
expect 0 unlink ${n2}
expect 0 unlink ${n3}
expect 0 unlink ${n4}
expect 0 unlink ${n5}
