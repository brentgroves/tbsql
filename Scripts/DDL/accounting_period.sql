select *
-- select count(*)
from Plex.accounting_period ap 
-- where ordinal =0 -- 1,418
where ordinal =1 -- 1,418

Set update_date of ordinal 1 to a more recent value for testing.
-- update Plex.accounting_period
-- set update_date = 2022-07-01  -- 2021-12-13 11:49:00
set update_date = '2022-07-01'  -- 2021-12-13 11:49:00
where pcn = 123681
and period = 202111
and ordinal = 1

-- update Plex.accounting_period
set update_date = '2022-07-27' -- 2022-07-21 15:14:00
--set update_date = '2022-07-21 15:14:00'
where pcn = 123681
and period = 202206
and ordinal = 1

-- update Plex.accounting_period
set update_date = '2022-07-01'  -- 2021-11-05 15:42:00.000
--set update_date = 2022-07-01  -- 2021-11-05 15:42:00.000
where pcn = 300758
and period = 202110
and ordinal = 1


-- update Plex.accounting_period
set update_date = '2022-07-27'  -- 2022-07-18 10:01:00
--set update_date = '2022-07-18 10:01:00'
where pcn = 300758
and period = 202206
and ordinal = 1

-- Original values
select update_date 
from Plex.accounting_period
where pcn = 123681
and period = 202111
and ordinal = 1  -- 2021-12-13 11:49:00
and ordinal = 0  -- 2021-12-13 11:49:00


select update_date 
from Plex.accounting_period
where pcn = 123681
and period = 202206
and ordinal = 1  -- 2022-07-21 15:14:00
and ordinal = 0  -- 2022-07-21 15:14:00

select update_date 
from Plex.accounting_period
where pcn = 300758
and period = 202110
and ordinal = 1  -- 2021-11-05 15:42:00.000
and ordinal = 0  -- 2021-11-05 15:42:00.000

select update_date 
from Plex.accounting_period
where pcn = 300758
and period = 202206
--and ordinal = 1  -- 2022-07-27 00:00:00
and ordinal = 0  -- 2022-07-18 10:01:00



-- select count(*)
from Plex.accounting_period_2 p1 -- 2,836
-- call sp_calc_start_period()
select *
from Plex.accounting_balance_update_period_range 

-- Drop table

-- DROP TABLE mgdw.Plex.accounting_period;
declare @dt datetime= '1900-01-01';
-- mgdw.Plex.accounting_period_2 definition

-- Drop table

-- DROP TABLE mgdw.Plex.accounting_period;

CREATE TABLE mgdw.Plex.accounting_period (
	pcn int NOT NULL,
	period_key int NOT NULL,
	period int NULL,
	fiscal_order int NULL,
	begin_date datetime NULL,
	end_date datetime NULL,
	period_display varchar(7) NULL,
	quarter_group tinyint NULL,
	period_status int NULL,
	add_date datetime NULL,
	update_date datetime NULL,
	ordinal int NOT NULL,
	CONSTRAINT IX_accounting_period_pcn_period_no_newest UNIQUE (pcn,period,ordinal),
	CONSTRAINT PK__accounting_period PRIMARY KEY (pcn,period_key,ordinal)
);
EXEC sp_helpindex 'Plex.accounting_period'

select *
from Plex.accounting_period_2 ap 

-- exec sp_calc_start_period;
-- drop procedure sp_calc_start_period;
create procedure sp_calc_start_period
as
begin 
	declare @start_period int;

	with account_period_diff 
	as 
	(
		select p1.pcn,p1.period,p1.update_date prev_update_date,p2.update_date cur_update_date 
		from Plex.accounting_period_2 p1  
		join Plex.accounting_period_2 p2
		on p1.pcn=p2.pcn 
		and p1.period_key=p2.period_key
		and p1.ordinal = 0
		and p2.ordinal = 1
		where p1.update_date <> p2.update_date 
	)
	-- select * from account_period_diff;
	,new_start_period
	as 
	( 
		select min(period) start_period 
		from account_period_diff 
		group by pcn 
	)
	-- select * from new_start_period;
	select @start_period=start_period from new_start_period;
	if @start_period is not null 
	begin 
		select 'update Plex.account_period_balance_period_range';
	end
	else 
	begin
		select 'no updates';
	end;

end;




select *
--into Archive.accounting_period_2022_03_21 -- 1,346
-- select count(*)
from Plex.accounting_period ap -- 1,418
where period_key = 45758
drop procedure Report.accounting_period
insert into Scratch.t1
exec Report.accounting_period 202201,202203

--I think this is for a power bi report
create procedure Report.accounting_period
@start_period int,
@end_period int
as 
select 
period,
period_display,
begin_date,
end_date,
case 
	when period_status = 1 then 'Active'
	else 'Closed'
end status,
update_date updated 
from Plex.accounting_period
where period between @start_period and @end_period
and pcn = 123681
order by pcn,period desc 



