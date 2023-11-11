
select * from Plex.accounting_balance_update_period_range

DECLARE @pcn INT;
declare @period_start int; 
declare @period_end int;
set @pcn = 123681;

EXEC Plex.accounting_balance_get_period_range @pcn,@period_start output,@period_end output

select @period_start period_start,@period_end period_end;

-- drop procedure Plex.accounting_balance_get_period_range
create procedure Plex.accounting_balance_get_period_range
(	
	@pcn int,
	@period_start int output,
	@period_end int output
)
as 
BEGIN   
	SET NOCOUNT ON; 
	select 
	@period_start = period_start,
	@period_end = period_end 
	from Plex.accounting_balance_update_period_range
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
