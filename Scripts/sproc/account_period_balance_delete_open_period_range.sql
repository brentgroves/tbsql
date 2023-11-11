select count(*) from Plex.account_period_balance apb -- 157,283,148,998/140,713/100,365 132,428,123,659,131,900, 123,615
select count(*) 
from Plex.account_period_balance apb 
where pcn=123681
-- and period = 202207  -- 4,617
-- and period = 202208  -- 4,617
and period between 202207 and 202208 -- 50,787/0




-- select distinct pcn,period from Scratch.account_period_balance order by pcn,period  -- 41,293
select distinct pcn,period from Plex.account_period_balance order by pcn,period  -- 41,293
select * FROM Plex.accounting_balance_update_period_range 

exec Plex.account_period_balance_delete_open_period_range 123681

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


--drop procedure Plex.account_period_balance_delete_open_period_range
create procedure Plex.account_period_balance_delete_open_period_range
(
	@pcn int
)
as 
begin
	-- debug variable;
--	declare @pcn int;
--	set @pcn = 123681;

	declare @start_open_period int;
	declare @end_open_period int;
	select @start_open_period=start_open_period,@end_open_period=end_open_period 
	from Plex.accounting_period_ranges where pcn = @pcn
--	SELECT @pcn pcn, @start_open_period start_open_period, @end_open_period end_open_period
	delete from Plex.account_period_balance 
	WHERE pcn = @pcn 
	and period between @start_open_period and @end_open_period

end; -- Plex.account_period_balance_delete_open_period_range

