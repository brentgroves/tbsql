
select * from Plex.accounting_balance_update_period_range

DECLARE @pcn INT;
declare @start_period int; 
declare @end_period int;
declare @start_open_period int;
declare @end_open_period int;
declare @no_update int;
set @pcn = 123681;

EXEC Plex.accounting_get_period_ranges @pcn,@start_period output,@end_period output,@start_open_period output,@end_open_period output,@no_update output

select @start_period start_period,@end_period end_period,@start_open_period start_open_period, @end_open_period end_open_period, @no_update no_update;

-- drop procedure Plex.accounting_get_period_ranges
create procedure Plex.accounting_get_period_ranges
(	
	@pcn int,
	@start_period int output,
	@end_period int output,
	@start_open_period int output,
	@end_open_period int output,
	@no_update bit output
)
as 
BEGIN   
	SET NOCOUNT ON; 
	select 
	@start_period = start_period,
	@end_period = end_period,
	@start_open_period = start_open_period,
	@end_open_period = end_open_period,
	@no_update = no_update
	from Plex.accounting_period_ranges
	WHERE pcn=@pcn;

END; 

select * from Plex.accounting_balance_update_period_range;
set @pcn = 123681;

declare @period_start_out int; 
declare @period_end_out int;
EXEC Plex.accounting_balance_get_period_range @pcn = 123681,@period_start = @period_start_out OUTPUT,@period_end = @period_end_out OUTPUT;
SELECT @period_start_out AS period_start, @period_end_out as period_end;

cursor = self.db.cursor()

sql = """\
declare @period_start_out int; 
declare @period_end_out int;
EXEC Plex.accounting_balance_get_period_range @pcn = ?,@period_start = @period_start_out OUTPUT,@period_end = @period_end_out OUTPUT;
SELECT @period_start_out AS period_start, @period_end_out as period_end;
"""
cursor.execute(sql, (123681))
row = cursor.fetchone()
print(row[0])

cursor = self.db.cursor()

sql = """\
DECLARE @out nvarchar(max);
EXEC [dbo].[storedProcedure] @x = ?, @y = ?, @z = ?,@param_out = @out OUTPUT;
SELECT @out AS the_output;
"""

cursor.execute(sql, (x, y, z))
row = cursor.fetchone()
print(row[0])
cursor.execute(sql, (x, y, z))
row = cursor.fetchone()
print(row[0])
