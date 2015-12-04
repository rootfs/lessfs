#!/bin/sh
cd /fuse
while true
do
iozone -a
lpid=`ps ax  | grep lessfs | grep -v grep | awk '{ print $1}'`
pmap -x $lpid >>/tmp/lfsham
done
