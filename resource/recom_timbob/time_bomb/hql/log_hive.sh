#!/bin/bash

export PATH=$PATH:/opt/cloudera/parcels/CDH/lib/hive/bin/hive

hive -f /home/time_bomb/hql/log_hive.hql
hive -f /home/time_bomb/hql/export_memberid.hql > /home/recomend/to_spark_recomend/userid.txt
hive -f /home/time_bomb/hql/export_resid.hql > /home/recomend/to_spark_recomend/resid.txt
hive -f /home/time_bomb/hql/export_scores.hql > /home/recomend/to_spark_recomend/scores.txt
hive -f /home/time_bomb/hql/export_resmapping.hql > /home/recomend/to_spark_recomend/resmapping.txt


#映射文件 同步至HBASE
java -jar /home/spark/wbj/res_mapping/scala_mapping.jar /home/recomend/to_spark_recomend/resmapping.txt > /home/spark/wbj/res_mapping/mapping.log

#运行spark进行推荐
/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --class com.xiaomishu.com.cf.ResRecomendALS /home/spark/wbj/spark_recomend/spark_resrecomend-assembly-1.0.jar --rank 5 --numIterations 20 --lambda 1.0 --kryo /user/hadoop/mllib/scores.txt /user/hadoop/mllib/userid.txt /user/hadoop/mllib/resid.txt
