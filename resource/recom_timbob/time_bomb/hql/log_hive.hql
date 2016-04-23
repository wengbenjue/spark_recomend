--加载数据


--load data into weblog
load data local inpath '/home/log_data/log/*/*.log' into table weblog;  



--insert into weblog_etl
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nostrict;

insert overwrite table weblog_etl
partition (year,month,day)
select
from_unixtime(unix_timestamp(concat(date1,' ',time))) as date1,
weekofyear(from_unixtime(unix_timestamp(concat(date1,' ',time))))  as weeknum,
hour(from_unixtime(unix_timestamp(concat(date1,' ',time))))  as hour1  ,
cs_uri_stem,
cs_uri_query,
c_ip,
cs_user_agent,
cs_referer,
cs_host,
sc_status,
sc_substatus,
sc_win32_status,
sc_bytes,
cs_bytes,
time_taken ,
SUBSTR( REGEXP_EXTRACT(cs_cookie, 'comfg114vpp=(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)\\-(\\w+)',0) ,13 )  AS vpp,
REGEXP_EXTRACT(cs_cookie, 'utma=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 2) as utma_uniqueid,
REGEXP_EXTRACT(cs_cookie, 'comfg114sss=(.[^\;]*)', 1) as sessionid,
regexp_extract(cs_cookie,'isshow=[^=\\|]+\\|[^=\\|]\\|(\\d+)',1) as memberid,
if(regexp_extract(cs_uri_stem,'/([a-zA-Z0-9]{12})/',1) <> '' ,regexp_extract(cs_uri_stem,'/([a-zA-Z0-9]{12})/',1),
regexp_extract(cs_uri_query,'resid=([a-zA-Z0-9]{12})',1)) as resid,
FROM_UNIXTIME( CAST( REGEXP_EXTRACT(cs_cookie, 'utma=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 3) AS BIGINT)) as utma_first_visit,
FROM_UNIXTIME( CAST( REGEXP_EXTRACT(cs_cookie, 'utma=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 4) AS BIGINT)) as utma_previous_visit,
FROM_UNIXTIME( CAST( REGEXP_EXTRACT(cs_cookie, 'utma=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 5) AS BIGINT)) as utma_current_visit ,
CAST( REGEXP_EXTRACT(cs_cookie, 'utmb=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 2) AS int) as utmb_pageviews,
TO_DATE( FROM_UNIXTIME( CAST( REGEXP_EXTRACT(cs_cookie, 'utmb=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 4) AS BIGINT))) as utmb_timestamps,
CAST( REGEXP_EXTRACT(cs_cookie, 'utmz=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 3) AS int)  utmz_sessions,
CAST( REGEXP_EXTRACT(cs_cookie, 'utmz=(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)', 4) AS int) as utmz_campaigns,
REGEXP_EXTRACT(cs_cookie, 'utmcsr=\\(?(\\w+)\\)?', 1) as utmscr,
REGEXP_EXTRACT(cs_cookie, 'utmccn=\\(?(\\w+)\\)?', 1)  as utmccn,
REGEXP_EXTRACT(cs_cookie, 'utmcmd=\\(?(\\w+)\\)?', 1)  as utmcmd,
REFLECT("java.net.URLDecoder", "decode", CAST( REGEXP_EXTRACT(cs_cookie, 'utmctr=(\.[^\;^\|]*)', 1) AS STRING)) as utmctr,
year(from_unixtime(unix_timestamp(concat(date1,' ',time)))) as year,
month(from_unixtime(unix_timestamp(concat(date1,' ',time)))) as month,
day(from_unixtime(unix_timestamp(concat(date1,' ',time))))  as day
from weblog where date1 not like '#%' and cs_cookie RLIKE 'utma'
and from_unixtime(unix_timestamp(concat(date1,' ',time))) >='1990-09-01'
and from_unixtime(unix_timestamp(concat(date1,' ',time))) <'2049-09-02'
distribute by year,month,day;

--插入数据到 iis_pageview_scores 
insert overwrite table iis_pageview_scores  select ct.memberid,ct.resid, if(ct.pageviews < 5,round(0.2*ct.pageviews,1),0.9)  as page_score,if(datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),ct.last_visit_time) < 30,0.4,0) as time_score ,ct.last_visit_time from (select memberid,resid,sum(utmb_pageviews) as pageviews,max(utma_current_visit) as last_visit_time from weblog_etl where memberid  <>  ' ' and memberid is not null  and  resid <> ' ' and resid is not null group by memberid,resid ) as ct where memberid  <>  ' ' and memberid is not null  and  resid <> ' ' and resid is not null;


--加载数据到红餐厅
load data local inpath '/home/recomend/res/*' into table red_res;  


--加载数据到res_mapping
load data local inpath '/home/recomend/res_mapping/*' into table res_mapping;


--插入数据  res_mapping_distinct
insert overwrite table res_mapping_distinct select resid,rescharc,max(update_time),city_id  from res_mapping group by resid ,rescharc,city_id ;


--上传数据至hive数据仓库
load data local inpath '/home/recomend/order/*' into table order_scores ;  

	
--插入数据到   order_scores_sum

insert overwrite table order_scores_sum select memberid,rescharc,sum(order_num_score) from order_scores group by memberid,rescharc ;


--插入数据到  scores

insert overwrite table scores select p.memberid,m.resid,p.view_num_score+p.view_time_score+o.order_num_score as score from  iis_pageview_scores p inner join red_res r on p.rescharc = r.rescharc inner join res_mapping_distinct m on p.rescharc = m.rescharc left outer join order_scores_sum o where p.memberid=o.memberid and p.rescharc = o.rescharc;
