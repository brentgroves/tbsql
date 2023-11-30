-- update Plex.accounting_period 
-- set update_date = '2021-11-06 13:19:00.000'
 set update_date = '2021-11-05 13:19:00.000'
where pcn=123681 and period = 202110 and ordinal = 0

-- update Plex.accounting_period 
-- set update_date = '2021-12-11 12:00:00.000'
 set update_date = '2021-12-10 12:00:00.000'
where pcn=300758 and period = 202111 and ordinal = 0

-- update Plex.accounting_period_ranges
 set start_period = 202109 
 set no_update=0
where pcn = 123681


select pcn,period,update_date from Plex.accounting_period
where period between 202109 and 202207
order by pcn,period 

select * from Plex.accounting_period_ranges r

-- call Plex.accounting_start_period_update()
-- drop procedure Plex.accounting_start_period_update

create procedure Plex.accounting_start_period_update() 
begin 
	declare v_start_period int;
	declare v_end_period int;
	declare v_pcn int;
	declare v_diff_count int;
	declare v_new_start_period int;

	declare v_id int;
	declare v_min_id int;
	declare v_max_id int;
	select min(id),max(id) into v_min_id,v_max_id from Plex.accounting_period_ranges; 
	set v_id := v_min_id;
-- 	select v_id id, v_min_id min_id, v_max_id max_id;
	while v_id <= v_max_id do
		select pcn,start_period,end_period into v_pcn,v_start_period,v_end_period 
		from Plex.accounting_period_ranges r
		where id = v_id;
	
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
			where p1.pcn = v_pcn 
			and p1.period between v_start_period and v_end_period
			and p1.update_date <> p2.update_date 
		)
		-- select * from account_period_diff;
		select count(*) INTO v_diff_count from account_period_diff;
		if v_diff_count > 0 THEN
			SELECT min(p1.period) INTO v_new_start_period
			from Plex.accounting_period p1  
			join Plex.accounting_period p2
			on p1.pcn=p2.pcn 
			and p1.period_key=p2.period_key
			and p1.ordinal = 0
			and p2.ordinal = 1
			where p1.pcn = v_pcn 
			and p1.period between v_start_period and v_end_period
			and p1.update_date <> p2.update_date; 
		ELSE 
			set v_new_start_period = 0;
		END IF;

		if v_diff_count > 0 then 
			update Plex.accounting_period_ranges
			set no_update = 0,
			start_period = v_new_start_period
			where pcn = v_pcn;
		else 
			update Plex.accounting_period_ranges
			set no_update = 1
			where pcn = v_pcn;
		end if;
	
-- 		select v_pcn pcn,v_start_period start_period,v_id id, v_min_id min_id, v_max_id max_id;
		set v_id = v_id + 1;
	end while; 
end;


select * from Plex.accounting_period_ranges



select *
from Plex.accounting_period_2 ap 
