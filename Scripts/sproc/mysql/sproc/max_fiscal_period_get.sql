-- Plex.max_fiscal_period_view source
-- drop view Plex.max_fiscal_period_view
select max_fiscal_period  from Plex.max_fiscal_period_view
where pcn = 123681
and year = 2022

call Plex.max_fiscal_period(123681,2022,@max_fiscal_period);
select @max_fiscal_period;

-- drop procedure Plex.max_fiscal_period
create procedure Plex.max_fiscal_period
(
	IN v_pcn int,
	IN v_year int,
	OUT v_max_fiscal_period int
)
BEGIN

WITH fiscal_period(pcn,year,period)
as
(
	select pcn,year(begin_date) year,period 
	from Plex.accounting_period 
	where pcn = v_pcn
	and year(begin_date) = v_year 
)
-- select * from fiscal_period;
,max_fiscal_period(pcn,year,max_fiscal_period)
as
(
  SELECT pcn,year,max(period) max_fiscal_period
  FROM fiscal_period
  group by pcn,year
)
--	select count(*) cnt from max_fiscal_period
select max_fiscal_period into v_max_fiscal_period from max_fiscal_period;
	
end
	