{'Base_No': 0, 'Account_No': 1, 'Abbreviated_Name': 2, 'Account_Name': 3, 
'Beginning_Balance': 4, 'Debit': 5, 'Credit': 6, 'Ending_Balance': 7, 'PCN_Name': 8, 
'Report_Currency': 9, 'Cost_Center_No': 10, 'Location_No': 11}
Account_Activity_Summary_xPCN_Get
select * 
-- select count(*)
from Plex.account_activity_summary
-- drop table Plex.account_activity_summary
create table Plex.account_activity_summary
( 
	pcn int not null,
	period int not null,
	account_no varchar(20) not null,
--	abbreviated_name varchar(10),
--	account_name varchar(110),
	beginning_balance decimal(19,5),
	debit decimal(19,5),
	credit decimal(19,5),
	balance decimal(19,5),
	ending_balance decimal(19,5),
	CONSTRAINT PK_account_activity_summary PRIMARY KEY (pcn,period,account_no)
)
-- https://www.mysqltutorial.org/mysql-index/mysql-create-index/
show indexes from Plex.account_activity_summary 
--CREATE INDEX idx_account_activity_summary ON Plex.accounting_account(pcn,account_no);
--CREATE INDEX idx_accounting_account_pcn_account_no ON Plex.accounting_account(pcn,account_no,peri);

-- delete from ETL.script where script_key = 8;

-- truncate TABLE ETL.script;
