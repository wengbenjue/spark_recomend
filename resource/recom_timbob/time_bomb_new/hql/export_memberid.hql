select m.memberid from (select *,row_number() over (partition by memberid) as num from scores ) m where m.num=1;
