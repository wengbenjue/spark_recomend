select m.resid from (select *,row_number() over (partition by resid) as num from scores ) m where m.num=1;
