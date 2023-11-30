-- Plex.max_fiscal_period_view source
-- drop view Plex.max_fiscal_period_view
select max_fiscal_period  from Plex.max_fiscal_period_view
where pcn = 123681
and year = 2022

create view Plex.max_fiscal_period_view(pcn,`year`,max_fiscal_period)
	as
	WITH fiscal_period(pcn,year,period)
	as
	(
		select pcn,year(begin_date) year,period from Plex.accounting_period 
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
	select * from max_fiscal_period;
	