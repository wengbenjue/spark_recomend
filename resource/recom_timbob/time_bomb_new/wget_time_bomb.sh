#!/bin/bash

last_date=`date +%Y-%m-%-d --date="-1 day"` 
echo $last_date
echo "start....."
wget -c ftp://222.73.49.39:2001/$last_date.zip --ftp-user=XMSFTP --ftp-password=123LOG#@! -P /home/log_data/ > /home/time_bomb/get.log 1>&2 
echo "download ovet...."
unzip -n /home/log_data/$last_date -d /home/log_data/log/
echo "解压 完毕"

#加载数据至hive
