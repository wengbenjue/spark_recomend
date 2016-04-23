--加载数据


--load data into acesslog
load data local inpath '/home/spark/data/logs/*.log' into table acesslog;  



--insert into etl_acesslog
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;

insert overwrite table etl_acesslog
partition (year,month,day)
select 
from_unixtime(unix_timestamp(concat(date,' ',time))) as date,
regexp_extract(cs_cookie,'userinfo=[^=\\|]+\\|[^=\\|]\\|(\\d+)',1) as userid,
regexp_extract(cs_uri_query,'itemid=([a-zA-Z0-9]{12})',1) as itemid,
FROM_UNIXTIME( CAST( REGEXP_EXTRACT(cs_cookie, 'fruitr=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 5) AS BIGINT)) as fruitr_current_visit,
CAST( REGEXP_EXTRACT(cs_cookie, 'fruits=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 2) AS int) as fruits_pageviews,
TO_DATE( FROM_UNIXTIME( CAST( REGEXP_EXTRACT(cs_cookie, 'fruits=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 4) AS BIGINT))) as fruits_timestamps,
year(from_unixtime(unix_timestamp(concat(date,' ',time)))) as year,
month(from_unixtime(unix_timestamp(concat(date,' ',time)))) as month,
day(from_unixtime(unix_timestamp(concat(date,' ',time))))  as day
from acesslog where date not like '#%' and cs_cookie RLIKE 'fruitr' 
and from_unixtime(unix_timestamp(concat(date,' ',time))) >='1970-09-01'
and from_unixtime(unix_timestamp(concat(date,' ',time))) <'2049-09-02'
distribute by year,month,day;


 
--add jar 加载时间衰减函数timeDecay
ADD JAR /home/spark/hive/jars/fruitrecomend-1.0.jar;
CREATE TEMPORARY FUNCTION timeDecay AS 'com.soledede.udf.DecayUDF';

--插入数据到 iis_pageview_scores 
insert overwrite table iis_pageview_scores  select ct.userid,ct.itemid, if(ct.pageviews < 5,round(0.2*ct.pageviews,1),0.9)  as page_score,timeDecay(datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),ct.last_visit_time)) as time_score ,ct.last_visit_time from (select userid,itemid,sum(fruits_pageviews) as pageviews,max(fruitr_current_visit) as last_visit_time from etl_acesslog where userid  <>  '' and userid is not null  and  itemid <> '' and itemid is not null group by userid,itemid ) as ct where userid  <>  '' and userid is not null  and  itemid <> '' and itemid is not null;


--加载数据到item
load data local inpath '/home/spark/data/recomend/item/*' into table item;  


--加载数据到item_mapping
load data local inpath '/home/spark/data/recomend/item_mapping/*' into table item_mapping;
select * from item_mapping;



--插入数据  item_mapping_distinct
insert overwrite table item_mapping_distinct select itemid,itemcharc,max(update_time),city_id  from item_mapping  group by itemid ,itemcharc,city_id ;




--上传数据至hive数据仓库
load data local inpath '/home/spark/data/recomend/order/*' into table order_scores ;  

	
--插入数据到   order_scores_sum

insert overwrite table order_scores_sum select userid,itemcharc,sum(order_num_score) from order_scores group by userid,itemcharc ;


--插入数据到  scores

insert overwrite table scores select p.userid,m.itemid,p.view_num_score+p.view_time_score+o.order_num_score as score from  iis_pageview_scores p inner join item i on p.itemid = i.itemid inner join item_mapping_distinct m on p.itemid = m.itemcharc left outer join order_scores_sum o where p.userid=o.userid and p.itemid = o.itemcharc;

insert overwrite table scores select o.userid,m.itemid,o.order_num_score as score from order_scores_sum o left outer join  item_mapping_distinct m on o.itemcharc = m.itemcharc where o.itemcharc <> '';

