select * from Plex.accounting_period_ranges
-- drop PROCEDURE Plex.account_period_balance_delete_period_range
CREATE PROCEDURE Plex.account_period_balance_delete_period_range
( 
	in v_pcn int
)
BEGIN   
	declare v_start_period int;
	declare v_end_period int;

	select start_period,end_period into v_start_period,v_end_period 
	from Plex.accounting_period_ranges where pcn = v_pcn;
--   	select v_pcn pcn,v_start_period start_period,v_end_period end_period;
	delete from Plex.account_period_balance 
	WHERE pcn = v_pcn 
	and period between v_start_period and v_end_period;
END; 

call Plex.account_period_balance_delete_period_range(300758)
call Plex.account_period_balance_delete_period_range(123681)

select distinct pcn,period from Plex.account_period_balance  order by pcn,period 
select count(*) from Plex.account_period_balance apb -- 140,713/101,879 132,428,123,659,131,900, 123,615
