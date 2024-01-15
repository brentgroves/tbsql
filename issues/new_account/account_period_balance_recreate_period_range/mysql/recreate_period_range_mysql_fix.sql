CREATE DEFINER=`root`@`%` PROCEDURE `Plex`.`account_period_balance_recreate_period_range`(
	in v_pcn int
)
proc_Exit:begin
	declare v_start_period int;
	declare v_end_period int;
	declare v_period int;
	declare v_max_fiscal_period int;
	declare v_no_update int;
	
	declare v_prev_period int;

	declare v_first_period int;
	declare v_anchor_period int;
	declare v_anchor_period_display varchar(7);
	
	declare v_cnt int;

	select r.start_period,r.start_period,r.end_period,r.no_update,m.max_fiscal_period 
	into v_start_period,v_period,v_end_period,v_no_update,v_max_fiscal_period
	from Plex.accounting_period_ranges r
	inner join Plex.max_fiscal_period_view m 
	on r.pcn=m.pcn
	and (r.start_period div 100) = m.`year`
	where r.pcn = v_pcn;

	if (v_no_update=1) then

 		LEAVE proc_Exit;
	end if;

	if ((v_start_period%100)!=1) then
		set v_prev_period = v_start_period - 1;
	else
		set v_prev_period = (((v_start_period div 100)-1)*100)+12;
	end if;


	set v_anchor_period=v_prev_period;

	select p.period_display into v_anchor_period_display
	from Plex.accounting_period p 
	where p.pcn = v_pcn
	and p.period = v_anchor_period
	and p.ordinal = 1;
	
	if v_period%100 = 1 then
		set v_first_period=1;
	else 
		set v_first_period=0;
	end if;


	
	INSERT INTO Plex.accounting_account_year_category_type ( pcn,account_no,`year`,category_type,revenue_or_expense)
	with account_year_category_type
	as
	(
		select a.*
		
		from Plex.accounting_account a  
		inner join Plex.accounting_account_year_category_type y
		on a.pcn = y.pcn 
		and a.account_no =y.account_no
		where y.`year` = (v_prev_period div 100) 
		and a.pcn = v_pcn
	)
	,add_account_year_category_type
	as 
	( 
		select a.*
		from Plex.accounting_account a  
		left outer join account_year_category_type y 
		on a.pcn = y.pcn 
		and a.account_no = y.account_no
		where y.pcn is null 
		and a.pcn = v_pcn
	)
	select y.pcn,y.account_no,v_prev_period div 100,y.category_type,y.revenue_or_expense
	from Plex.accounting_account_year_category_type y
	where y.year = (v_end_period div 100) 
	and y.pcn = v_pcn
	and y.account_no in 
	( 
		select account_no from add_account_year_category_type
	);
    

    insert into Plex.account_period_balance 
	    select 
	    v_pcn pcn,
	    a.account_no,
	    v_anchor_period period,
	    v_anchor_period_display period_display,
	    0 debit,
	    0 ytd_debit,
	    0 credit,
	    0 ytd_credit,
	    0 balance,
	    0 ytd_balance
	    
	    
	    
		from Plex.accounting_account a   
		left outer join Plex.account_period_balance b 
		on a.pcn=b.pcn 
		and a.account_no=b.account_no 
		and b.period = v_anchor_period  
		where a.pcn = v_pcn 
		and b.pcn is null;

   	while v_period <= v_end_period do
		
		insert into Plex.account_period_balance (pcn,account_no,period,period_display,debit,ytd_debit,credit,ytd_credit,balance,ytd_balance)
   		with period_balance(pcn,account_no,period,debit,credit,balance)
		as 
		(
		    select 
		    a.pcn,
		    a.account_no,
			v_period period,
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
			from Plex.accounting_account a   
			left outer join Plex.accounting_balance b 
			on a.pcn=b.pcn 
			and a.account_no=b.account_no 
			and b.period = v_period
			where a.pcn = v_pcn  
			order by a.pcn,a.account_no,b.period  
		)
		
		,account_period_balance(pcn,account_no,period,period_display,debit,ytd_debit,credit,ytd_credit,balance,ytd_balance)
		as 
		(	
			select b.pcn,b.account_no,b.period,ap.period_display, 
			b.debit,
			cast(
			    case 
			    when (v_first_period=0) then p.ytd_debit + b.debit 
			    when (v_first_period=1) and (a.revenue_or_expense = 1) then b.debit 
			    when (v_first_period=1) and (a.revenue_or_expense = 0) then p.ytd_debit + b.debit 
			    end as decimal(19,5) 
			) ytd_debit, 
			b.credit,
		  	cast(
			    case 
			    when (v_first_period=0) then p.ytd_credit + b.credit 
			    when (v_first_period=1) and (a.revenue_or_expense = 1) then b.credit 
			    when (v_first_period=1) and (a.revenue_or_expense = 0) then p.ytd_credit + b.credit 
			    end as decimal(19,5) 
		  	) ytd_credit, 
			b.balance,
			cast(
			    case 
			    when (v_first_period=0) then p.ytd_balance + b.balance 
			    when (v_first_period=1) and (a.revenue_or_expense = 1) then b.balance 
			    when (v_first_period=1) and (a.revenue_or_expense = 0) then p.ytd_balance + b.balance 
			    end as decimal(19,5) 
			) ytd_balance
			from period_balance b  
			inner join Plex.account_period_balance p
			on b.pcn = p.pcn 
			and b.account_no = p.account_no 
			and b.period = v_period
			and p.period = v_prev_period
			inner join Plex.accounting_account_year_category_type a
			on b.pcn = a.pcn 
			and b.account_no =a.account_no
			and a.`year`=(v_prev_period div 100)
			inner join Plex.accounting_period ap 
			on b.pcn=ap.pcn 
			and b.period=ap.period 
			and ap.ordinal = 1

		)
		select pcn,account_no,period,period_display,debit,ytd_debit,credit,ytd_credit,balance,ytd_balance 
		from account_period_balance;

		
		set v_prev_period = v_period;
		
	    if v_period < v_max_fiscal_period then
		    set v_period=v_period+1;
		else 
			set v_period=((v_period div 100 + 1)*100) + 1; 
		end if; 
		select m.max_fiscal_period into v_max_fiscal_period
		from Plex.max_fiscal_period_view m 
		where m.pcn = v_pcn 
		and m.`year` = v_period div 100;
	
		if v_period%100 = 1 then  
			set v_first_period=1;
		else 
			set v_first_period=0;
		end if;
    	
	
	end while;	
	
end;