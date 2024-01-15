--Backup table before bug fix.
--select top 10 *
--select *
--into Archive.accounting_account_year_category_type_01_09_2024 -- 46,017
--from Plex.accounting_account_year_category_type y

create procedure Plex.account_period_balance_recreate_open_period_range
( 
	@pcn int
)
as 
begin

SET NOCOUNT ON;
--Debug
DECLARE @pcn int;
SET @pcn=123681;
declare @period int;
declare @start_open_period int;
declare @end_open_period int;
declare @max_fiscal_period int;

declare @prev_period int;
declare @first_period int;
declare @anchor_period int;
declare @anchor_period_display varchar(7);

declare @cnt int

select @start_open_period=r.start_open_period, @period=r.start_open_period,@end_open_period=r.end_open_period,
@max_fiscal_period=m.max_fiscal_period
from Plex.accounting_period_ranges r
inner join Plex.max_fiscal_period_view m 
on r.pcn=m.pcn
and (r.start_open_period/100) = m.[year]
where r.pcn = @pcn;

if ((@start_open_period%100)!=1)
begin
	set @prev_period = @start_open_period - 1;
end
else
begin
	set @prev_period = (((@start_open_period/100)-1)*100)+12;
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

--select @pcn pcn,@anchor_period anchor_period,@anchor_period_display anchor_period_display,
--@period period,@prev_period prev_period,@start_open_period start_open_period,
--@first_period first_period,@end_open_period end_open_period,@max_fiscal_period max_fiscal_period;

--pcn		anchor_period	anchor_period_display	period	prev_period	start_open_period	first_period	end_open_period	max_fiscal_period
--123681	202311			11-2023					202312	202311		202312				0				202401			202312
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
	where y.pcn is null -- if there is no account_year_category_type records for the @prev_period year so we must add them.
	and a.pcn = @pcn
)
-- select top 10 *
---- select *
-- from Plex.accounting_account_year_category_type y
---- where y.account_no = ''
-- where y.account_no is null
-- delete from Plex.accounting_account_year_category_type
-- where account_no is null
--select * 
-- from Plex.accounting_account_year_category_type y
--where y.account_no = '11055-000-9806'
--17372671	123681	11055-000-9806	2024	Asset	0
--17377293	123681	11055-000-9806	2025	Asset	0

-- Issue: There was 1 2024 record with a null account_no
-- Bug fix must insert the account_no column
-- change from: select y.pcn,y.[year],y.category_type,y.revenue_or_expense	
-- change to: select y.pcn,y.account_no,(@prev_period/100) [year] ,y.category_type,y.revenue_or_expense	 
-- INSERT INTO Plex.accounting_account_year_category_type (pcn,YEAR,category_type,revenue_or_expense)
 INSERT INTO Plex.accounting_account_year_category_type (pcn,account_no,YEAR,category_type,revenue_or_expense)
	select y.pcn,y.account_no,(@prev_period/100) [year] ,y.category_type,y.revenue_or_expense	
	from Plex.accounting_account_year_category_type y
	where y.[year] = (@end_open_period/100) -- if there is no account_year_category_type records for the @prev_period year so we must add them.
	and y.pcn = @pcn
	and y.account_no in 
	( 
		select account_no from add_account_year_category_type
	)

--pcn		anchor_period	anchor_period_display	period	prev_period	start_open_period	first_period	end_open_period	period
--123681	202311			11-2023					202312	202311		202312				0				202401			202312

--Why is 202212 present but not 202301-202310?	Work on account_period_balance_recreate_period_range to find answer.
--select *	
--from Plex.account_period_balance b 	
--where account_no = '11055-000-9806'
--order by period 
--pcn		account_no		period	period_display	debit	ytd_debit	credit	ytd_credit	balance	ytd_balance
--123681	11055-000-9806	202212	12-2022			0.00000	0.00000	0.00000	0.00000	0.00000	0.00000
--123681	11055-000-9806	202311	11-2023			0.00000	0.00000	0.00000	0.00000	0.00000	0.00000

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
--select * 
--from Plex.account_activity_summary
--where pcn = 123681 
--and account_no = '11055-000-9806'

--pcn		period	account_no		beginning_balance	debit		credit			balance			ending_balance
--123681	202311	11055-000-9806	0.00000				0.00000		0.00000			0.00000			0.00000
--123681	202312	11055-000-9806	0.00000				95934.43000	872657.96000	-776723.53000	-776723.53000
--123681	202401	11055-000-9806	-776723.53000		0.00000		95934.43000		-95934.43000	-872657.96000

while @period <= @end_open_period
begin
--	Make sure there is a summary record for every account and replace null values with 0
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
		left outer join Plex.account_activity_summary b 
		on a.pcn=b.pcn 
		and a.account_no=b.account_no 
		and b.period = @period
		where a.pcn = @pcn  
	),
	--select @cnt=count(*) from period_balance;
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
		and b.period = @period
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

--pcn		anchor_period	anchor_period_display	period	prev_period	start_open_period	first_period	end_open_period	max_fiscal_period
--123681	202311			11-2023					202312	202311		202312				0				202401			202312
	
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
	
--	select m.max_fiscal_period
--	from Plex.max_fiscal_period_view m 
--	where m.pcn = 123681 
--	and m.year = 2024 -- 202412

	if @period%100 = 1 
	begin
		set @first_period=1;
	end 
	else 
	begin 
		set @first_period=0;
	end
--	select @period period,@end_open_period end_open_period,@prev_period previous_period,@max_fiscal_period max_fiscal_period,@first_period first_period;
		
end 
	
end;