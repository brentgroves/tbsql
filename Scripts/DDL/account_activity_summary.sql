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

EXEC sp_helpindex 'Plex.account_activity_summary'

  im2 ='''insert into Plex.account_activity_summary (pcn,period,account_no,beginning_balance,debit,credit,balance,ending_balance)
  values (?,?,?,?,?,?,?,?)'''
-- truncate table Plex.account_activity_summary  
  
insert into Plex.account_activity_summary (pcn,period,account_no,beginning_balance,debit,credit,balance,ending_balance)
values 
('123681', '202207', '63300-200-0000', '0', '0', '0', '0.0', '0')

