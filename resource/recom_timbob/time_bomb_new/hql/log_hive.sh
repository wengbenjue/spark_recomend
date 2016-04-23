#!/bin/bash

export PATH=$PATH:/opt/cloudera/parcels/CDH/lib/hive/bin/hive

start=$(date +%s)

hive -f /home/time_bomb/hql/log_hive.hql
hive -f /home/time_bomb/hql/export_memberid.hql > /home/recomend/to_spark_recomend/userid.txt
hive -f /home/time_bomb/hql/export_resid.hql > /home/recomend/to_spark_recomend/resid.txt
hive -f /home/time_bomb/hql/export_scores.hql > /home/recomend/to_spark_recomend/scores.txt
hive -f /home/time_bomb/hql/export_resmapping.hql > /home/recomend/to_spark_recomend/resmapping.txt

sleep 3m

#去掉第一行数据，清理
sed '1d' /home/recomend/to_spark_recomend/resid.txt > /home/recomend/to_spark_recomend/resid.c
sed '1d' /home/recomend/to_spark_recomend/resmapping.txt > /home/recomend/to_spark_recomend/resmapping.c
sed '1d' /home/recomend/to_spark_recomend/scores.txt > /home/recomend/to_spark_recomend/scores.c
sed '1d' /home/recomend/to_spark_recomend/userid.txt > /home/recomend/to_spark_recomend/userid.c

#上传数据至hdfs
hadoop fs -put -f /home/recomend/to_spark_recomend /user/hadoop/mllib

sleep 5m

#映射文件 同步至HBASE
java -jar /home/spark/wbj/res_mapping/scala_mapping.jar --zookeeper_quorum spark1.xiaomishu.com,spark2.xiaomishu.com,spark4.xiaomishu.com,spark5.xiaomishu.com,spark7.xiaomishu.com /home/recomend/to_spark_recomend/resmapping.c

sleep 5m

#运行spark进行推荐
/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit --class com.xiaomishu.com.cf.ResRecomendALS /home/spark/wbj/spark_recomend/spark_resrecomend-assembly-1.0.jar --rank 5 --numIterations 20 --lambda 1.0 --kryo /user/hadoop/mllib/to_spark_recomend/scores.c /user/hadoop/mllib/to_spark_recomend/userid.c /user/hadoop/mllib/to_spark_recomend/resid.c

end=$(date +%s)

echo $(($end-$start))
