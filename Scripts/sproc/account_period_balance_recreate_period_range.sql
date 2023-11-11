select distinct pcn,period from Plex.account_period_balance order by pcn,period 

select * from Plex.account
select * from Plex.account_period_balance 

-- exec Plex.account_period_balance_delete_period_range 123681
select distinct pcn,period from Plex.account_period_balance apb order by pcn,period
select count(*) 
from Plex.account_period_balance apb -- 157,283/32,253
where pcn = 123681
and period between 202108 and 202206 -- 50,787/0

--and period = 202108 -- 22,136
select * from Plex.accounting_period_ranges apr 
-- update Plex.accounting_period_ranges 
set no_update = 0 
where pcn = 123681

-- exec Plex.account_period_balance_recreate_period_range 123681
-- drop procedure Plex.account_period_balance_recreate_period_range

-- drop procedure Plex.account_period_balance_recreate_period_range
create procedure Plex.account_period_balance_recreate_period_range
( 
	@pcn int
)
as 
begin

SET NOCOUNT ON;
--debug variable
--declare @pcn int;
--set @pcn = 123681;

declare @no_update int;
declare @start_period int;
declare @end_period int;
declare @period int;
declare @max_fiscal_period int;

declare @prev_period int;
declare @first_period int;
declare @anchor_period int;
declare @anchor_period_display varchar(7);

declare @cnt int

select @start_period=r.start_period, @period=r.start_period,@end_period=r.end_period,@no_update=r.no_update,
@max_fiscal_period=m.max_fiscal_period
--select * from Plex.accounting_balance_update_period_range r
from Plex.accounting_period_ranges r
inner join Plex.max_fiscal_period_view m 
on r.pcn=m.pcn
and (r.start_period/100) = m.[year]
where r.pcn = @pcn;


if (@no_update=1)
begin
--	select 'returning early', @no_update no_update;
	return 0;
end

if ((@start_period%100)!=1)
begin
	set @prev_period = @start_period - 1;
end
else
begin
	set @prev_period = (((@start_period/100)-1)*100)+12;
end;

set @anchor_period = @prev_period;

select @anchor_period_display=p.period_display 
from Plex.accounting_period p 
where p.pcn = @pcn
and p.period = @anchor_period
and p.ordinal = 1;

if @period%100 = 1 
begin
	set @first_period=1;
end 
else 
begin 
	set @first_period=0;
end;

--select @no_update no_update,@pcn pcn,@anchor_period anchor_period,@anchor_period_display anchor_period_display,
--@period period,
--@prev_period prev_period,@start_period start_period,
--@first_period first_period,@end_period end_period,@period period,@max_fiscal_period max_fiscal_period;

/*
 * Add new account records to Plex.accounting_account_year_category_type 
 * for the @anchor_period's year if not already added.
 */
with account_year_category_type
as
(
	select a.*
	-- select count(*)
	from Plex.accounting_account a  
	--where a.pcn=123681 -- 4,617
	inner join Plex.accounting_account_year_category_type y
	on a.pcn = y.pcn 
	and a.account_no =y.account_no
	where y.[year] = (@prev_period/100) 
	and a.pcn = @pcn
)
-- select count(*) from account_year_category_type  -- 4,595
,add_account_year_category_type
as 
( 	select a.*
	from Plex.accounting_account a  
	left outer join account_year_category_type y 
	on a.pcn = y.pcn 
	and a.account_no =y.account_no
	where y.pcn is null -- there is no account_year_category_type records for the @prev_period year so we must add them.
	and a.pcn = @pcn
)
-- select * from add_account_year_category_type	-- 22
INSERT INTO Plex.accounting_account_year_category_type (pcn,YEAR,category_type,revenue_or_expense)
	select y.pcn,y.[year],y.category_type,y.revenue_or_expense	
	from Plex.accounting_account_year_category_type y
	where y.[year] = (@end_period/100) -- there is no account_year_category_type records for the @prev_period year so we must add them.
	and y.pcn = @pcn
	and y.account_no in 
	( 
		select account_no from add_account_year_category_type
	)
	
/*
 * Update the anchor period. Add records for new accounts.
 */
insert into Plex.account_period_balance 
    select 
    @pcn pcn,
    a.account_no,
    @anchor_period period,
    @anchor_period_display period_display,
    0 debit,
    0 ytd_debit,
    0 credit,
    0 ytd_credit,
    0 balance,
    0 ytd_balance
    -- select count(*) from Plex.accounting_account where pcn = 123681  -- 4,617,4,363/4,595
    -- select distinct pcn,period from Plex.account_period_balance b order by pcn,period 
    -- select count(*) from Plex.account_period_balance b where pcn = 123681 and period = 202103  -- 4,595
	from Plex.accounting_account a   
	left outer join Plex.account_period_balance b 
	on a.pcn=b.pcn 
	and a.account_no=b.account_no 
	and b.period = @anchor_period
	where a.pcn = @pcn 
	and b.pcn is null;
--select count(*) account_period_balance_cnt from Plex.account_period_balance  where period = @anchor_period and pcn = @pcn -- 4,617


--while @period <= 202108
while @period <= @end_period
begin
	with period_balance(pcn,account_no,period,debit,credit,balance)
	as 
	(
	    select 
	    a.pcn,
	    a.account_no,
		@period period,
		case 
		when b.debit is null then 0 
		else b.debit 
		end debit,
		case 
		when b.credit is null then 0 
		else b.credit 
		end credit,
		case 
		when b.balance is null then 0 
		else b.balance 
		end balance
	    -- select count(*) from Plex.accounting_account where pcn = 123681  -- 4,595/4,363
		from Plex.accounting_account a   
		left outer join Plex.accounting_balance b 
		on a.pcn=b.pcn 
		and a.account_no=b.account_no 
		and b.period = @period
		where a.pcn = @pcn  
	),
	--select @cnt=count(*) from period_balance;
	--print '@cnt=' + cast(@cnt as varchar(4));
	account_period_balance(pcn,account_no,period,period_display,debit,ytd_debit,credit,ytd_credit,balance,ytd_balance)
	--,ending_period,ending_ytd_debit,ending_ytd_credit,ending_ytd_balance,next_period)
	as 
	(	
	--select * from Plex.accounting_period ap where pcn = 300758
		select b.pcn,b.account_no,b.period,ap.period_display,
		b.debit,
		cast(
		    case 
		    when (@first_period=0) then p.ytd_debit + b.debit 
		    when (@first_period=1) and (a.revenue_or_expense = 1) then b.debit 
		    when (@first_period=1) and (a.revenue_or_expense = 0) then p.ytd_debit + b.debit 
		    end as decimal(19,5) 
		) ytd_debit, 
		b.credit,
	  	cast(
		    case 
		    when (@first_period=0) then p.ytd_credit + b.credit 
		    when (@first_period=1) and (a.revenue_or_expense = 1) then b.credit 
		    when (@first_period=1) and (a.revenue_or_expense = 0) then p.ytd_credit + b.credit 
		    end as decimal(19,5) 
	  	) ytd_credit, 
		b.balance,
	  	cast(
		    case 
		    when (@first_period=0) then p.ytd_balance + b.balance 
		    when (@first_period=1) and (a.revenue_or_expense = 1) then b.balance 
		    when (@first_period=1) and (a.revenue_or_expense = 0) then p.ytd_balance + b.balance 
		    end as decimal(19,5) 
	  	) ytd_balance
		from period_balance b  -- will contain all the accounts labled with just one period
		inner join Plex.account_period_balance p
		on b.pcn = p.pcn 
		and b.account_no = p.account_no 
		AND b.period=@period
		and p.period=@prev_period
		inner join Plex.accounting_period ap 
		on b.pcn=ap.pcn 
		and b.period=ap.period 
		and ap.ordinal = 1
		inner join Plex.accounting_account_year_category_type a
		on p.pcn = a.pcn 
		and p.account_no =a.account_no
		and (p.period/100)=a.[year]
	
	)
	
--	select @period, count(*)  from account_period_balance;  -- 4,363
	
	insert into Plex.account_period_balance
	select pcn,account_no,period,period_display,debit,ytd_debit,credit,ytd_credit,balance,ytd_balance from account_period_balance;  -- 4,363

	set @prev_period = @period
	
    if @period < @max_fiscal_period 
    begin 
	    set @period=@period+1
	end 
	else 
	begin 
		set @period=((@period/100 + 1)*100) + 1 
	end 
		
	select @max_fiscal_period=m.max_fiscal_period
	from Plex.max_fiscal_period_view m 
	where m.pcn = @pcn 
	and m.year = @period/100

	if @period%100 = 1 
	begin
		set @first_period=1;
	end 
	else 
	begin 
		set @first_period=0;
	end
--	select @period period,@period_end period_end,@prev_period previous_period,@max_fiscal_period max_fiscal_period,@first_period first_period;
		
end 
	
end 

-- pcn,account_no,period,period_display,debit,ytd_debit,credit,ytd_credit,balance,ytd_balance
--select * from account_period_balance;  -- 4,363
select count(*) from Plex.account_period_balance b -- 4,595*12=     45950+9190
where b.pcn=123681 
--and b.period = 202101 -- 4,595
--and b.period = 202112 -- 4,595
and b.period = 202112 -- 4,595
-- 813-704-1772
-- 
select * from Plex.account_period_balance apb where period = 202111
-- select distinct pcn,period from Plex.account_period_balance order by pcn,period 

/*
 * Format to be like CSV download
 */
--select * from Plex.accounting_account a where a.account_no = '10220-000-00000' 
declare @pcn int;
set @pcn = 123681;

exec Report.trial_balance 202202,202202
SELECT @@ROWCOUNT;  -- 4,595
exec Report.trial_balance 202201,202202
SELECT @@ROWCOUNT;   -- 9,190
exec Report.trial_balance 202201,202203
SELECT @@ROWCOUNT;   -- 9,190 + 4,595 =13,785

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
--select *
--select count(*)
--select distinct pcn,period from Plex.account_period_balance b order by pcn,period -- 123,681 (202101 to 202111)
--Yinto Archive.account_period_balance_03_22_2022 -- 115,374
from Plex.account_period_balance b -- 43,620
--where b.pcn = @pcn  -- 50,545
inner join Plex.accounting_account a -- 43,620
on b.pcn=a.pcn 
and b.account_no=a.account_no 
where b.pcn = 123681  -- 50,545,55,140
AND b.period BETWEEN @start_period AND @end_period
order by b.period_display,a.account_no 
--where a.category_type != a.category_type_legacy 
--where b.period_display is not NULL -- 40,940
--where b.period_display is NULL -- 40,940?
--where a.account_no = '10220-000-00000' 
END 

select * from ssis.ScriptComplete sc 

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
where b.pcn = 123681  -- 50,545,55,140,64,330
--AND b.period = 202201  -- 4,595
--AND b.period = 202202  -- 4,595
and a.account_no = '12400-000-0000'
--and a.account_no='11010-000-0000'
--and a.account_no = '10220-000-00000' 
order by a.account_no,b.period 
--where a.category_type != a.category_type_legacy 
--where b.period_display is not NULL -- 40,940
--where b.period_display is NULL -- 40,940
