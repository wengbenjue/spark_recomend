select m.userid from (select *,row_number() over (partition by userid) as num from scores ) m where m.num=1;
