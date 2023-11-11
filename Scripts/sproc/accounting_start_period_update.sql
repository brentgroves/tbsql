

/*
 * Debug section
 */
-- update Plex.accounting_period_ranges
set start_period = 202109 
-- set no_update=0
where pcn = 123681

-- update Plex.accounting_period 
-- set update_date = '2021-11-06 13:19:00.000'
set update_date = '2021-11-05 13:19:00.000'
where pcn=123681 and period = 202110 and ordinal = 0

declare @start_period int;
-- change
declare @end_period int;
declare @pcn int;
-- change
declare @diff_count int;
-- change
declare @new_start_period int;
declare @id int;
declare @min_id int;
declare @max_id int;
select @min_id = min(id),@max_id=max(id) from Plex.accounting_period_ranges r 
set @id = @min_id;

-- Change
select @pcn = pcn,@start_period=start_period,@end_period=end_period  
from Plex.accounting_period_ranges r
where id = @id;

	select p1.pcn,p1.period,p1.update_date prev_update_date,p2.update_date cur_update_date 
	from Plex.accounting_period p1  
	join Plex.accounting_period p2
	on p1.pcn=p2.pcn 
	and p1.period_key=p2.period_key
	and p1.ordinal = 0
	and p2.ordinal = 1
--	where p1.pcn = @pcn 
--	and p1.period between @start_period and @end_period
	where p1.period between @start_period and @end_period
	order by pcn,period desc
	
with account_period_diff 
as 
(
	select p1.pcn,p1.period,p1.update_date prev_update_date,p2.update_date cur_update_date 
	from Plex.accounting_period p1  
	join Plex.accounting_period p2
	on p1.pcn=p2.pcn 
	and p1.period_key=p2.period_key
	and p1.ordinal = 0
	and p2.ordinal = 1
	where p1.pcn = @pcn 
	-- change
	and p1.period between @start_period and @end_period
	and p1.update_date <> p2.update_date 
)
-- select * from account_period_diff;
-- change
select @diff_count=count(*) from account_period_diff;
-- change
if @diff_count > 0 
begin 
	select @new_start_period=min(period) 
	from account_period_diff 
end
else 
begin
	set @new_start_period = 0
end
--change
select @diff_count diff_count,@new_start_period new_start_period

-- change
if @diff_count > 0 
begin 
	update Plex.accounting_period_ranges
	set no_update = 0,
	-- change 
	start_period = @new_start_period
	where pcn = @pcn
end
else 
begin
	update Plex.accounting_period_ranges
	set no_update = 1
	where pcn = @pcn
end;
	
--		select @pcn pcn,@start_period start_period,@id id, @min_id min_id, @max_id max_id;

-- update Plex.accounting_period_ranges
-- set start_period = 202109 
 set no_update=0
where pcn = 123681

-- update Plex.accounting_period 
-- set update_date = '2021-11-06 13:19:00.000'
set update_date = '2021-11-05 13:19:00.000'
where pcn=123681 and period = 202110 and ordinal = 0

-- update Plex.accounting_period 
-- set update_date = '2021-12-11 12:00:00.000'
 set update_date = '2021-12-10 12:00:00.000'
where pcn=300758 and period = 202111 and ordinal = 0

select pcn,period,update_date from Plex.accounting_period
where period between 202109 and 202207
order by pcn,period 

select * from Plex.accounting_period_ranges r

-- exec Plex.accounting_start_period_update;
-- drop procedure Plex.accounting_start_period_update;

create procedure Plex.accounting_start_period_update
as
begin 
	declare @start_period int;
	declare @end_period int;
	declare @pcn int;
	declare @diff_count int;
	declare @new_start_period int;
	declare @id int;
	declare @min_id int;
	declare @max_id int;
	select @min_id = min(id),@max_id=max(id) from Plex.accounting_period_ranges r 
	set @id = @min_id;

--	 select @id id, @min_id min_id, @max_id max_id;
	while @id <= @max_id
	BEGIN
		select @pcn = pcn,@start_period=start_period,@end_period=end_period  
		from Plex.accounting_period_ranges r
		where id = @id;
	
		with account_period_diff 
		as 
		(
			select p1.pcn,p1.period,p1.update_date prev_update_date,p2.update_date cur_update_date 
			from Plex.accounting_period p1  
			join Plex.accounting_period p2
			on p1.pcn=p2.pcn 
			and p1.period_key=p2.period_key
			and p1.ordinal = 0
			and p2.ordinal = 1
			where p1.pcn = @pcn 
			and p1.period between @start_period and @end_period
			and p1.update_date <> p2.update_date 
		)
--		 select * from account_period_diff;
		select @diff_count=count(*) from account_period_diff;

		if @diff_count > 0 
		begin 
			select @new_start_period=min(p1.period) 
			from Plex.accounting_period p1  
			join Plex.accounting_period p2
			on p1.pcn=p2.pcn 
			and p1.period_key=p2.period_key
			and p1.ordinal = 0
			and p2.ordinal = 1
			where p1.pcn = @pcn 
			and p1.period between @start_period and @end_period
			and p1.update_date <> p2.update_date 
		end
		else 
		begin
			set @new_start_period = 0
		end
		-- select @diff_count diff_count,@new_start_period new_start_period
		if @diff_count > 0 
		begin 
			update Plex.accounting_period_ranges
			set no_update = 0,
			-- change 
			start_period = @new_start_period
			where pcn = @pcn
		end
		else 
		begin
			update Plex.accounting_period_ranges
			set no_update = 1
			where pcn = @pcn
		end;
--		select @pcn pcn,@new_start_period new_start_period,@id id, @min_id min_id, @max_id max_id;
		set @id = @id + 1;
	end; 
end;


select * from Plex.accounting_balance_update_period_range
EXEC sp_helpindex 'Plex.accounting_period'

select *
from Plex.accounting_period_2 ap 
