/*
 * Are there any accounts that have different values?
 */

declare @pcn int;
set @pcn= 123681;
-- declare @period int;
-- set @period = 202207;
 -- set @period = 202208;
declare @period_start int;
set @period_start = 202207;
declare @period_end int;
set @period_end = 202208;

-- select b.period,b.account_no, 
--b.debit b_debit, p.Current_Debit p_debit,
--b.credit b_credit, p.Current_Credit p_credit, 
-- b.balance b_balance, d.current_debit_credit d_balance
select count(*)
from Plex.account_activity_summary b 
join Plex.accounting_period ap
on b.pcn =ap.pcn 
and b.period = ap.period  -- 33,140
join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
on b.pcn=d.pcn
and ap.period_display = d.period_display 
and b.account_no = d.account_no  -- 16,816
join Plex.Account_Balances_by_Periods p 
on b.pcn = p.pcn 
and b.period = p.period 
and b.account_no = p.[No] -- 16,816
where b.pcn = @pcn
and b.period between @period_start and @period_end -- 8408
-- and b.balance  = d.current_debit_credit -- 8,408
and b.balance  != d.current_debit_credit -- 0 
-- and abs((b.balance - d.current_debit_credit)) >  0.01  -- 0 
-- and b.balance = p.Current_Debit - p.Current_Credit -- 8,408


declare @pcn int;
set @pcn= 123681;
-- declare @period int;
-- set @period = 202207;
 -- set @period = 202208;
declare @period_start int;
set @period_start = 202207;
declare @period_end int;
set @period_end = 202207;

-- select b.period,b.account_no, 
--b.debit b_debit, p.Current_Debit p_debit,
--b.credit b_credit, p.Current_Credit p_credit, 
-- b.balance b_balance, d.current_debit_credit d_balance
select count(*)
from Plex.account_activity_summary b 
join Plex.accounting_period ap
on b.pcn =ap.pcn 
and b.period = ap.period  -- 33,140
join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
on b.pcn=d.pcn
and ap.period_display = d.period_display 
and b.account_no = d.account_no  -- 16,816
join Plex.Account_Balances_by_Periods p 
on b.pcn = p.pcn 
and b.period = p.period 
and b.account_no = p.[No] -- 16,816
where b.pcn = @pcn
and b.period between @period_start and @period_end -- 8408
-- and b.balance  = d.current_debit_credit -- 8,404
-- and b.balance  != d.current_debit_credit -- 4
-- and abs((b.balance - d.current_debit_credit)) >  0.01  -- 0 
and b.balance = p.Current_Debit - p.Current_Credit -- 8,408

exec Plex.account_period_balance_delete_open_period_range 123681
exec Plex.account_period_balance_recreate_open_period_range 123681

/*
 * Abba willing see if open period range scripts 
 * give same values as trial balance report.
 * Make sure you click search just before you 
 * press the download button.
 */

select * 
-- select count(*)
from Plex.account_activity_summary  
where pcn=123681 
and period = 202208  -- 4,617

--exec Plex.account_period_balance_delete_period_range
select count(*) from Plex.account_period_balance apb -- 165,568/157,283/57,863,149,624,148,998/140,713/100,365 132,428,123,659,131,900, 123,615
where pcn=123681 -- 92,274
and period = 202208  -- 4,617

select distinct pcn,period from Plex.account_period_balance order by pcn,period  -- 41,293

declare @pcn int;
set @pcn= 123681;
-- declare @period int;
-- set @period = 202207;
 -- set @period = 202208;
declare @period_start int;
set @period_start = 202208;
declare @period_end int;
set @period_end = 202208;

 select b.period,b.account_no, 
b.debit b_debit, s.debit s_debit
--b.credit b_credit, p.Current_Credit p_credit, 
-- b.balance b_balance, d.current_debit_credit d_balance
-- select count(*)
from Plex.account_activity_summary b 
join Plex.accounting_period ap
on b.pcn =ap.pcn 
and b.period = ap.period  -- 33,140
join 
(
	select s.pcn,s.period, s.account_no,s.debit,s.credit,s.debit-s.credit balance
	--select count(*)
--select distinct pcn,period from Plex.GL_Account_Activity_Summary s order by pcn,period -- 123,681 (200812-202203)
--select * from Plex.GL_Account_Activity_Summary s where pcn=123681 and period = 202111  -- dont know when this was imported probably in early december
	from Plex.GL_Account_Activity_Summary s  --(),(221,202010)
--	where s.pcn = 123681 
--	and s.period between 202101 and 202201  -- 2,462/2,718/2,975
) s
on b.pcn=s.pcn 
and b.account_no=s.account_no
and b.period=s.period  -- 592
where b.pcn = @pcn
-- and b.debit = s.debit  -- 586
and b.debit != s.debit  -- 6

and b.credit = s.credit


declare @pcn int;
set @pcn= 123681;
-- declare @period int;
-- set @period = 202207;
 -- set @period = 202208;
declare @period_start int;
set @period_start = 202208;
declare @period_end int;
set @period_end = 202208;

-- select b.period,b.account_no, 
--b.debit b_debit, p.Current_Debit p_debit,
--b.credit b_credit, p.Current_Credit p_credit, 
-- b.balance b_balance, d.current_debit_credit d_balance
select count(*)
from Plex.account_activity_summary b 
join Plex.accounting_period ap
on b.pcn =ap.pcn 
and b.period = ap.period  -- 33,140
join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
on b.pcn=d.pcn
and ap.period_display = d.period_display 
and b.account_no = d.account_no  -- 16,816
join Plex.Account_Balances_by_Periods p 
on b.pcn = p.pcn 
and b.period = p.period 
and b.account_no = p.[No] -- 16,816
where b.pcn = @pcn
and b.period between @period_start and @period_end -- 8408
-- and b.balance  = d.current_debit_credit -- 8,404
 and abs((b.balance - d.current_debit_credit)) >  0.01  -- 0 
-- and b.balance = p.Current_Debit - p.Current_Credit -- 8,400
-- and d.current_debit_credit = p.Current_Debit - p.Current_Credit -- 8,400
--and d.current_debit_credit != p.Current_Debit - p.Current_Credit -- 8


