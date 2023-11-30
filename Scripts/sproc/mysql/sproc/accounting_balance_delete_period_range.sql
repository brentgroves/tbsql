select * from Plex.accounting_balance_update_period_range

-- drop procedure Plex.accounting_balance_delete_period_range
create procedure Plex.accounting_balance_delete_period_range
(
	IN v_pcn int
)
BEGIN   
	declare v_start_period int;
	declare v_end_period int;

	select start_period,end_period into v_start_period,v_end_period 
	from Plex.accounting_period_ranges where pcn = v_pcn;

--   	select v_pcn pcn,v_start_period start_period,v_end_period end_period;
	delete from Plex.accounting_balance 
	WHERE pcn = v_pcn 
	and period between v_start_period and v_end_period;

END; 
call Plex.accounting_balance_delete_period_range(123681)

-- select count(*)
-- select top 10 *
FROM Plex.accounting_balance 
where pcn = 123681 
-- and period between 202205 and 202304  -- 2878
-- and period = 202301  -- 244
-- and period = 202302  -- 241
-- and period = 202303  -- 240
-- and period between 202206 and 202304  -- 2641
-- and period = 202304  -- 243
and period between 202207 and 202305  -- 2646
 and period = 202305  -- 248


call Plex.accounting_balance_delete_period_range(300758)

select distinct pcn,period from Plex.accounting_balance  order by pcn,period  

select count(*) from Plex.accounting_balance  -- 7,172,2,995
select * from Plex.accounting_period_ranges apr where pcn=123681
