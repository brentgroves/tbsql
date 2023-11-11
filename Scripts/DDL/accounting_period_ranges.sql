-- populate from accounting_period_ranges_dw_import / sproc123681_11728751_2112421
-- drop table Plex.accounting_period_ranges
CREATE TABLE mgdw.Plex.accounting_period_ranges (
	id int IDENTITY(1,1) NOT NULL,
	pcn int NULL,
	start_period int NULL,
	end_period int NULL,
	start_open_period int NULL,
	end_open_period int NULL,
	no_update bit null,
	PRIMARY KEY (id)
);
EXEC sp_helpindex 'Plex.accounting_period_ranges'
select * from Plex.accounting_period_ranges
-- delete from Plex.accounting_period_ranges


--update Plex.accounting_period_ranges
set period_start = 202103,
period_end = 202202


