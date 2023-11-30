
SELECT * 
FROM 
Plex.accounting_period
order by pcn,update_date desc

add ordinal column to Plex.accounting_period 
add ordinal column to primary key.

update accounting_period script 
delete records with newest = 0
select count(*) from Plex.accounting_period_2
where pcn in (123681,300758,310507,306766,300757)  -- 638
-- where pcn = 123681  -- 518
where pcn = 300758 -- 120
delete from Plex.accounting_period_2 where pcn in (1123681,300758,310507,306766,300757) and ordinal = 0

set records with ordinal = 1 to 0.

select count(*) 
from Plex.accounting_period_2 

-- update Plex.accounting_period_2 
set ordinal=0
where pcn in (123681,300758,310507,306766,300757) and ordinal = 1

select count(*) from Plex.accounting_period_2
where pcn in (123681,300758,310507,306766,300757)  -- 638
-- and ordinal = 1
and ordinal = 0 -- 1,418

compare newest period record's update_date to prev period record's value.
if the newest period record's update_date is greater than the prev period record's update_date then set 
  the views recalc column to 1 otherwise set it to 0.

Determine the oldest period that needs to be recalculated.
select min(period)
group by pcn
having recalc = 1

Update period_start to be the oldest_period that needs to be recalculate.

Change the order of Accounting_period and AccountingBalanceUpdatePeriodRange ETL script 
so that Plex.accounting_balance_update_period_range has been adjusted before 
we update it's period_start column with the oldest period to be recalculated.


select *  
from Plex.accounting_balance_update_period_range

add newest column to Plex.accounting_period 
add newest column to primary key.

-- Plex.accounting_period definition

-- Drop table

-- DROP TABLE Plex.accounting_period_2;

CREATE TABLE Plex.accounting_period_2 (
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
	ordinal int not null, 
	CONSTRAINT PK__accounting_period2 PRIMARY KEY clustered (pcn,period_key,ordinal),
	CONSTRAINT IX_accounting_period2_pcn_period_no_newest UNIQUE (pcn,period,ordinal)

);
insert into Plex.accounting_period_2 (pcn,period_key,period,fiscal_order,begin_date,end_date,period_display,quarter_group,period_status,add_date,update_date,ordinal)
select pcn,period_key,period,fiscal_order,begin_date,end_date,period_display,quarter_group,period_status,add_date,update_date,1 from Plex.accounting_period

EXEC sp_helpindex 'Plex.accounting_period_2';
select * from Plex.accounting_period_2
    -- This creates a primary key
    ,CONSTRAINT PK_MyTable PRIMARY KEY CLUSTERED (a)

    -- This creates a unique nonclustered index on columns b and c
    ,CONSTRAINT IX_MyTable1 UNIQUE (b, c)

