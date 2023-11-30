use Plex;
-- drop database Plex;
-- Plex.account_period_balance definition

-- Drop table

-- DROP TABLE Plex.account_period_balance;

CREATE TABLE Plex.account_period_balance (
	pcn int,
	account_no varchar(20),
	period int,
	period_display varchar(7) NULL,
	debit decimal(19,5) NULL,
	ytd_debit decimal(19,5) NULL,
	credit decimal(19,5) NULL,
	ytd_credit decimal(19,5) NULL,
	balance decimal(19,5) NULL,
	ytd_balance decimal(19,5) NULL,
	CONSTRAINT PK_account_period_balance PRIMARY KEY (pcn,account_no,period)
);

show indexes from Plex.account_period_balance; 
-- CREATE INDEX idx_accounting_account_pcn_account_no ON Plex.accounting_account(pcn,account_no);

/*
 * BACKUP
 */
select now()
select * 
-- into Archive.account_period_balance_06_01_2022 -- 132428
from Plex.account_period_balance
where account_no = '39100-000-0000'
ORDER BY pcn,period
--SELECT count(*) FROM Archive.account_period_balance_06_01_2022 
select *
from Plex.GL_Account_Activity_Summary s  --(),(221,202010)
where s.pcn = 123681 
and s.account_no = '39100-000-0000'
ORDER BY pcn,period
and s.period between 202101 and 202201  -- 2,462/2,718/2,975

select * from Plex.account_period_balance limit 10;
SELECT DISTINCT pcn, period 
FROM Plex.account_period_balance

ORDER BY pcn, period
/*
 * For Power BI report
 */
drop PROCEDURE Report.trial_balance
exec Report.trial_balance 202105,202205
CREATE PROCEDURE Report.trial_balance
@start_period int,
@end_period int 
AS 
begin

select 
--b.period,
b.period_display,
a.category_type,
-- don't use legacy category type even though it is on the real TB report. I think it will be less confusing 
-- for the Southfield PCN which hass missing accounts.
-- b.category_type_legacy category_type,  
/*
 * The Plex TB report uses the category type of the category linked to the account via the  category_account view. 
 * I believe Plex now mostly uses the account category located directly on the accounting_v_account view so I used 
 * this column instead of the one linked via the account_category view. 
 */
a.category_name_legacy category_name,
a.sub_category_name_legacy sub_category_name,
a.account_no,
--a.account_no [no],
a.account_name,
b.balance current_debit_credit,
b.ytd_balance ytd_debit_credit
--select count(*)
--select distinct pcn,period from Plex.account_period_balance b order by pcn,period -- 123,681 (202101 to 202111)
from Plex.account_period_balance b -- 43,620
--where b.pcn = @pcn  -- 50,545
inner join Plex.accounting_account a -- 43,620
on b.pcn=a.pcn 
and b.account_no=a.account_no 
where b.pcn = 123681  -- 50,545,55,140
AND b.period BETWEEN @start_period AND @end_period
order by b.period,a.account_no 
--where a.category_type != a.category_type_legacy 
--where b.period_display is not NULL -- 40,940
--where b.period_display is NULL -- 40,940
--where a.account_no = '10220-000-00000' 
END;

-- create schema Azure

insert into (pcn,account_no,period,period_display,debit,ytd_debit,credit,ytd_credit,balance,ytd_balance)    
values ()
select 
,pcn
,account_no
,period
,period_display
,debit
,ytd_debit
,credit
,ytd_credit
,balance
,ytd_balance
-- select count(*)
-- select distinct pcn,period  
from Plex.account_period_balance apb -- 148,998
where pcn in (123681,300758)
and period between 202101 and 202206
order by pcn, period 

select count(*)
from Plex.account_period_balance a -- 148,998

/*
 * compare Azure with MCP results
 * 148,845 out of 148,998
 */
select count(*)
from Azure.account_period_balance a -- 148,998
inner join Plex.account_period_balance m -- 148,998
on a.pcn=m.pcn
and a.account_no=m.account_no
and a.period=m.period
and a.period_display=m.period_display
and a.debit=m.debit
and a.ytd_debit=m.ytd_debit
and a.credit=m.credit
and a.ytd_credit=m.ytd_credit
and a.balance=m.balance
and a.ytd_balance=m.ytd_balance

-- where pcn in (123681,300758)
-- and period between 202101 and 202206
order by pcn, period 

