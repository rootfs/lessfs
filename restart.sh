#!/bin/sh -x
umount /fuse
rm -rf /data/mta/*
rm -rf /data/dta/*
logrotate -f /etc/logrotate.conf
cp DB_CONFIG /data/mta
./mklessfs -c /etc/lessfs.cfg
# Use the new lessfs.cfg syntax and let lessfs worry about the rest.
./lessfs /etc/lessfs.cfg /fuse
