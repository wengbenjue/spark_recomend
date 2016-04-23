select m.itemid from (select *,row_number() over (partition by itemid) as num from scores ) m where m.num=1;
