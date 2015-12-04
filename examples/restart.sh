#!/bin/sh -x
umount /master
umount /slave
rm -rf /data/master/mta/*
rm -rf /data/master/dta/*
rm -rf /data/slave/mta/*
rm -rf /data/slave/dta/*
logrotate -f /etc/logrotate.conf
../mklessfs -f -c /etc/lessfs.cfg-master
../mklessfs -f -c /etc/lessfs.cfg-slave
cp ../DB_CONFIG /data/slave/mta/
cp ../DB_CONFIG /data/master/mta/
# Use the new lessfs.cfg syntax and let lessfs worry about the rest.
../lessfs /etc/lessfs.cfg-slave /slave
../lessfs /etc/lessfs.cfg-master /master
