create external table weblog (
date string,
time string,
cs_method string,
cs_uri_stem string,
cs_uri_query string,
cs_uri_username string,
c_ip string,
cs_user_agent string,
cs_cookie string,
cs_referer string,
cs_host string,
sc_status string,
sc_substatus string,
sc_win32_status string,
sc_bytes string,
cs_bytes string,
time_taken int)
row format delimited fields terminated by ' ' lines
terminated by '\n';

--weblog_etl 建表脚本
create table weblog_etl (
date1 string,
weeknum int,
hour1 int,
cs_uri_stem string,
cs_uri_query string,
c_ip string,
cs_user_agent string,
cs_referer string,
cs_host string,
sc_status string,
sc_substatus string,
sc_win32_status string,
sc_bytes string,
cs_bytes string,
time_taken int,
vpp string,
utma_uniqueid string,
sessionid string,
memberid string,
resid string,
utma_first_visit string,
utma_previous_visit string,
utma_current_visit string,
utmb_pageviews int,
utmb_timestamps string,
utmz_sessions int,
utmz_campaigns int,
utmscr string,
utmccn string,
utmcmd string,
utmctr string
)
partitioned by (year int,month int,day int)
row format delimited fields terminated by '\t' lines
terminated by '\n';

alter table  weblog_etl set serdeproperties('serialization.null.format' = '');
alter table weblog set serdeproperties('serialization.null.format' = '');

--创建iis_pageview_scores
create table iis_pageview_scores (
memberid int,
rescharc string,
view_num_score double,
view_time_score  double,
view_latest_time string
)
row format delimited fields terminated by '\t' lines
terminated by '\n';

ALTER TABLE iis_pageview_scores SET SERDEPROPERTIES('serialization.null.format' = '');


--创建红餐厅表 red_res
create table red_res(
rescharc string,
red_res_time string
)
row format delimited fields terminated by '\t' lines
terminated by '\n';

ALTER TABLE red_res SET SERDEPROPERTIES('serialization.null.format' = '');

--创建res_mapping表
create table res_mapping(
resid int,
rescharc string,
update_time string,
city_id int
)
row format delimited fields terminated by '\t' lines
terminated by '\n';

ALTER TABLE res_mapping SET SERDEPROPERTIES('serialization.null.format' = '');

--创建res_mapping_distinct
create table res_mapping_distinct(
resid int,
rescharc string,
update_time string,
city_id int
)
row format delimited fields terminated by '\t' lines
terminated by '\n';


ALTER TABLE res_mapping_distinct SET SERDEPROPERTIES('serialization.null.format' = '');

--创建order_scores 表

create table order_scores (
memberid int,
rescharc string,
order_num_score  double,
order_latest_time string
)
row format delimited fields terminated by '\t' lines
terminated by '\n';

ALTER TABLE order_scores SET SERDEPROPERTIES('serialization.null.format' = '');



--创建order_scores_sum

create table order_scores_sum(
memberid int,
rescharc string,
order_num_score  double
)
row format delimited fields terminated by '\t' lines
terminated by '\n';
alter table order_scores_sum set serdeproperties('serialization.null.format' = '');

--创建总的score表

create table scores(
memberid int,
resid int,
 scores  double
)
row format delimited fields terminated by '\t' lines
terminated by '\n';
ALTER TABLE scores SET SERDEPROPERTIES('serialization.null.format' = '');