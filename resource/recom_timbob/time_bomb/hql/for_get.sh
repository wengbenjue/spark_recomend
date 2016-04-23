#!/bin/bash

echo $1
echo $2
echo $3

y=$1
m=$2
d=$3



function getlog()
{
log=$1
echo $log
nohup sudo wget -c ftp://222.73.49.39:2001/$log-0[1-9].zip --ftp-user=XMSFTP --ftp-password=123LOG#@! -P /home/log_data/ &
nohup sudo wget -c ftp://222.73.49.39:2001/$log-1[1-9].zip --ftp-user=XMSFTP --ftp-password=123LOG#@! -P /home/log_data/ &
nohup sudo wget -c ftp://222.73.49.39:2001/$log-2[1-9].zip --ftp-user=XMSFTP --ftp-password=123LOG#@! -P /home/log_data/ &
nohup sudo wget -c ftp://222.73.49.39:2001/$log-3[1-9].zip --ftp-user=XMSFTP --ftp-password=123LOG#@! -P /home/log_data/ &
}

echo $y $m $d

mm=`echo $m | cut -d- -f 2`
m1=`echo $m | cut -d- -f 1`

if [ $mm -lt 10 ];then
  echo $mm