-- Plex.max_fiscal_period_view source
-- drop view Plex.max_fiscal_period_view
select max_fiscal_period  from Plex.max_fiscal_period_view
where pcn = 123681
and year = 2022

	select pcn,year(begin_date) year,period 
	from Plex.accounting_period 
	where pcn = 123681
	and year(begin_date) = 2022 

declare @max_fiscal_period int;
exec Plex.sp_max_fiscal_period 123681,2022, @max_fiscal_period output;
select @max_fiscal_period;
-- drop procedure Plex.sp_max_fiscal_period
create procedure Plex.sp_max_fiscal_period
(
	@pcn int,
	@year int,
	@max_fiscal_period int output
)
as 
BEGIN

WITH fiscal_period(pcn,year,period)
as
(
	select pcn,year(begin_date) year,period 
	from Plex.accounting_period 
	where pcn = @pcn
	and year(begin_date) = @year 
)
-- select * from fiscal_period;
,max_fiscal_period(pcn,year,max_fiscal_period)
as
(
  SELECT pcn,year,max(period) max_fiscal_period
  FROM fiscal_period
  group by pcn,[year]
)
--select * from max_fiscal_period
select @max_fiscal_period = max_fiscal_period from max_fiscal_period;
	
end

