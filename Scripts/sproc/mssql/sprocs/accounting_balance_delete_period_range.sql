select * from Plex.accounting_balance_update_period_range


-- exec Plex.accounting_balance_delete_period_range 123681
-- exec Plex.accounting_balance_delete_period_range 300758
-- drop procedure Plex.accounting_balance_delete_period_range
create procedure Plex.accounting_balance_delete_period_range
(
	@pcn int
)
as
begin
	
	declare @start_period int;
	declare @end_period int;
	select @start_period=start_period,@end_period=end_period 
	from Plex.accounting_period_ranges where pcn = @pcn
--	SELECT @pcn pcn, @start_period start_period, @end_period end_period
	delete from Plex.accounting_balance WHERE pcn = @pcn and period between @start_period and @end_period	
end;
select count(*)
from Plex.accounting_balance -- 48,697,44,520,41,525
select * from Plex.accounting_balance_update_period_range
select * 
from Plex.accounting_period_ranges
