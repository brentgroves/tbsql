
select * from Plex.accounting_period_ranges
set @pcn := 123681;
call Plex.accounting_get_period_ranges(@pcn,@start_period,@end_period,@start_open_period,@end_open_period,@no_update);
select @pcn pcn,@start_period start_period,@end_period end_period,@start_open_period,@end_open_period,@no_update;
-- drop procedure Plex.accounting_get_period_ranges
create procedure Plex.accounting_get_period_ranges
(	
	IN v_pcn int,
	OUT v_start_period int,
	OUT v_end_period int,
	OUT v_start_open_period int,
	OUT v_end_open_period int,
	out v_no_update bit
)
BEGIN   
	select start_period,end_period,start_open_period,end_open_period,no_update 
	into v_start_period,v_end_period,v_start_open_period,v_end_open_period,v_no_update 
	from Plex.accounting_period_ranges
	WHERE pcn=v_pcn;
	
END; 

    declare @period_start_out int; 
    declare @period_end_out int;
    EXEC Plex.accounting_balance_get_period_range @pcn = ?,@period_start = @period_start_out OUTPUT,@period_end = @period_end_out OUTPUT;
    SELECT @period_start_out AS period_start, @period_end_out as period_end;

   	set @pcn := 123681;
	call Plex.accounting_balance_get_period_range(@pcn,@period_start,@period_end);
	select @period_start period_start,@period_end period_end;

   
    plsql = """\
	call Plex.accounting_balance_get_period_range(@pcn = ?,@period_start,@period_end);
	select @period_start period_start,@period_end period_end;
    """
