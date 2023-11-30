-- Plex.accounting_period definition

select *
-- select count(*)
from Plex.accounting_period ap 
-- where ordinal =0 -- 1,418
where ordinal =1 -- 1,418

Set update_date of ordinal 1 to a more recent value for testing.
-- update Plex.accounting_period
set update_date = '2022-07-01'  -- 2021-12-13 11:49:00
-- set update_date = '2021-12-13 11:49:00'
where pcn = 123681
and period = 202111
and ordinal = 1;

-- update Plex.accounting_period
set update_date = '2022-07-27' -- 2022-07-21 15:14:00
-- set update_date = '2022-07-21 15:14:00'
where pcn = 123681
and period = 202206
and ordinal = 1

-- update Plex.accounting_period
set update_date = '2022-07-01'  -- 2021-11-05 15:42:00.000
-- set update_date = '2021-11-05 15:42:00.000'
where pcn = 300758
and period = 202110
and ordinal = 1


-- update Plex.accounting_period
set update_date = '2022-07-27'  -- 2022-07-18 10:01:00
-- set update_date = '2022-07-27'  -- 2022-07-18 10:01:00
where pcn = 300758
and period = 202206
and ordinal = 1

-- Original values
select update_date 
from Plex.accounting_period
where pcn = 123681
and period = 202111
and ordinal = 0  -- 2021-12-13 11:49:00
and ordinal = 1  -- 2022-07-18 10:01:00


select update_date 
from Plex.accounting_period
where pcn = 123681
and period = 202206
and ordinal = 0  -- 2022-07-21 15:14:00
and ordinal = 1  -- 2022-07-18 10:01:00

select update_date 
from Plex.accounting_period
where pcn = 300758
and period = 202110
and ordinal = 0  -- 2021-11-05 15:42:00.000
and ordinal = 1  

select update_date 
from Plex.accounting_period
where pcn = 300758
and period = 202206
and ordinal = 0  -- 2022-07-18 10:01:00
and ordinal = 1  


-- select count(*)
from Plex.accounting_period_2 p1 -- 2,836
-- call sp_calc_start_period()
select *
from Plex.accounting_balance_update_period_range 

-- Drop table

-- DROP TABLE Plex.accounting_period;

-- Plex.accounting_period definition

CREATE TABLE Plex.accounting_period (
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
show indexes from Plex.accounting_period

select COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_NAME
from information_schema.KEY_COLUMN_USAGE
where TABLE_NAME = 'accounting_period';

CREATE INDEX idx_accounting_period_pcn_period ON Plex.accounting_period(pcn,period);
select @start_period; 

if 
--into Archive.accounting_period_2022_03_21 -- 1,346
-- select count(*)
from Plex.accounting_period ap -- 1,418
where period_key = 45758
drop procedure Report.accounting_period
insert into Scratch.t1
exec Report.accounting_period 202201,202203
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



-- DROP TABLE Scratch.accounting_period

CREATE TABLE Scratch.accounting_period (
	period int NULL,
	period_display varchar(7) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	begin_date datetime NULL,
	end_date datetime NULL,
	status varchar(10) null,
	updated datetime null,
);
insert into Scratch.accounting_period
exec Report.accounting_period 202201,202203

insert into Scratch.accounting_period
exec Report.accounting_period 202201,202203
