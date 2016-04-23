#!/bin/bash

last_date=`date +%Y-%m-%-d --date="-1 day"` 
echo "start....."
wget -c ftp://xx.xx.xx.xx:xxxx/$last_date.zip --ftp-user=xxxx --ftp-password=xxxx -P /home/log_data/ > /home/time_bomb/get.log 1>&2
echo "download ovet...."
unzip -n /home/log_data/$last_date -d /home/log_data/log/
echo "解压 完毕"

#加载数据至hive

~                
