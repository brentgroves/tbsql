

select * from Plex.accounting_period_ranges
-- drop PROCEDURE Plex.account_period_balance_delete_open_period_range
CREATE PROCEDURE Plex.account_period_balance_delete_open_period_range
( 
	in v_pcn int
)
BEGIN   
	declare v_start_open_period int;
	declare v_end_open_period int;

	select start_open_period,end_open_period into v_start_open_period,v_end_open_period 
	from Plex.accounting_period_ranges where pcn = v_pcn;
--  	select v_pcn pcn,v_start_open_period start_open_period,v_end_open_period end_open_period;
	delete from Plex.account_period_balance 
	WHERE pcn = v_pcn 
	and period between v_start_open_period and v_end_open_period;
END; 

call Plex.account_period_balance_delete_open_period_range(300758)
call Plex.account_period_balance_delete_open_period_range(123681)
-- call Plex.account_period_balance_recreate_period_range(123681)

select * from Plex.accounting_period_ranges -- 202105/202204
-- update Plex.accounting_period_ranges 
-- set start_period = 202210,no_update = 0
-- set no_update=0
where pcn = 123681


select distinct pcn,period 
-- select count(*)
from Plex.account_period_balance 
where pcn = 123681 
-- and period between 202210 and 202212 -- 13,851
and period >= 202210
order by pcn,period
