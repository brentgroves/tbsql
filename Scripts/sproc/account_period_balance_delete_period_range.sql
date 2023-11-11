-- exec Plex.account_period_balance_delete_period_range 123681
-- exec Plex.account_period_balance_delete_period_range 300758
-- drop procedure Plex.account_period_balance_delete_period_range
create procedure Plex.account_period_balance_delete_period_range
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
	delete from Plex.account_period_balance WHERE pcn = @pcn and period between @start_period and @end_period
end 

select * from Plex.accounting_period_ranges

select * from Plex.account_period_balance ab 
select count(*) from Plex.account_period_balance ab --157,283/101,879
select distinct pcn,period 
from Plex.account_period_balance  order by pcn,period

/*
 * Make backup
 */
select *
  into Archive.account_period_balance_2022_07_28 -- 157,283
--into Archive.account_period_balance_06_01_2022 -- 132428
--into Archive.account_period_balance_2022_04_04 -- 115,374
--into Archive.account_period_balance_2022_03_21 -- 107,133
--into Archive.account_period_balance_2022_02_16 -- 98,892
--select count(*)
from Plex.account_period_balance  -- 115,374

declare @pcn int;
set @pcn = 123681
declare @period_start int;
declare @period_end int;
select @period_start = 202102,@period_end = 202111;
--select @pcn,@period_start,@period_end;select * from Archive.account_period_balance_01_03_2022 

insert into Plex.account_period_balance
--select count(*) from Archive.account_period_balance_01_03_2022  WHERE pcn = @pcn and period between @period_start and @period_end  -- 39,267
select * from Archive.account_period_balance_01_03_2022 WHERE pcn = @pcn and period between @period_start and @period_end

select * from Archive.account_period_balance_12_30 
select count(*) from Archive.account_period_balance_12_30 -- 43,630
select distinct pcn,period from Archive.account_period_balance_12_30 order by pcn,period

select * from Archive.account_period_balance_01_03_2022 
select count(*) from Archive.account_period_balance_01_03_2022 -- 43,630
select distinct pcn,period from Archive.account_period_balance_01_03_2022 order by pcn,period

select * from Plex.account_period_balance ab 
select count(*) from Plex.account_period_balance ab --4,363/43,630
select distinct pcn,period from Plex.account_period_balance  order by pcn,period

select *
--into Archive.accounting_balance_01_03_2022
select distinct pcn,period 
from Plex.accounting_balance 
order by pcn,period 



select * 
--select count(*) from Archive.account_period_balance_01_03_2022 ab -- 43,630
--into Archive.account_period_balance_01_03_2022
--select count(*) from Plex.account_period_balance ab -- 43,630
from Plex.account_period_balance b -- 43,630

