#!/bin/bash

export PATH=$PATH:/opt/cloudera/parcels/CDH/lib/hive/bin/hive

hive -f /home/time_bomb/hql/create_table.hql
