/*
This is a modified version of GL_Account_Activity_Detail_Report that returns 1 record 
for each account that has any activitity for the specified period.
It is a modified version of Mobex Procedure: GL_Account_Activity_Summary_DW_Import.
The @Exclude_Period_13 and @Exclude_Period_Adjustments parameters are now hard coded to 0.
I did not fully understand there purpose and have always just used 0.
The @Offset parameter has been removed in favor of using the minimum open period from Mobex procedure: accounting_period_min_open.
*/


/*
Params
@PCNList varchar(max) = '123681,300758',
*/

declare @Exclude_Period_13 SMALLINT;
declare @Exclude_Period_Adjustments BIT;
set @Exclude_Period_13 = 0; 
set @Exclude_Period_Adjustments = 0;


-- select @period
-- CREATE NONCLUSTERED INDEX IX_Invoices ON #Invoices (Plexus_Customer_No, Part_Key, Ship_To_Address);
-- I dont thing we need to create an index if we put a primary key clustered in the table definition.
/*
Clustered indexes only sort tables. Therefore, they do not consume extra storage. 
Non-clustered indexes are stored in a separate place from the actual table claiming more storage space. 
Clustered indexes are faster than non-clustered indexes since they don't involve any extra lookup step.Aug 28, 2017
*/

/*
	PCN
	310507/Avilla
	300758/Albion
	295933/Franklin
	300757/Alabama
	306766/Edon
	312055/ BPG WorkHolding
	1	123681 / Southfield
2	295932 FruitPort
3	295933
4	300757
5	300758
6	306766
7	310507
8	312055
	*/

CREATE TABLE #accounting_balance_update_period_range (
	pcn int NULL,
	start_period int NULL,
	end_period int NULL,
	open_period int null
	-- CONSTRAINT PK__accounting_balance_update_period_range PRIMARY KEY (id)
);
insert into #accounting_balance_update_period_range(pcn,start_period,end_period,open_period)
exec sproc300758_11728751_1999565;
-- select * from #accounting_balance_update_period_range;

/*
create PCN table from param
*/
create table #list
(
  row_no int,
  tuple int
)
declare @row_no int
set @row_no = 0;
declare @max_row_no int;

declare @delimiter varchar(1)
set @delimiter = ','
declare @in_string varchar(max)
set @in_string = @PCNList
WHILE LEN(@in_string) > 0
BEGIN
    INSERT INTO #list
    SELECT @row_no,cast(left(@in_string, charindex(@delimiter, @in_string+',') -1) as int) as tuple
    SET @in_string = stuff(@in_string, 1, charindex(@delimiter, @in_string + @delimiter), '')
    set @row_no = @row_no+1;
end
select @max_row_no = max(row_no) from #list;
--select @max_row_no
-- select row_no,tuple from #list;


DECLARE @PCN_Currency_Code CHAR(3);
set @PCN_Currency_Code = 'USD'
-- select @PCN_Currency_Code
CREATE TABLE #Accounts
(
  pcn INT NOT NULL,
  account_no VARCHAR(20) NOT NULL,
  account_name varchar(110),
  category_type varchar(10),
  multiplier int, 
  --/*  -- 17
  PRIMARY KEY CLUSTERED
  (
    pcn,account_no
  )
  --*/
);
--CREATE NONCLUSTERED INDEX IX_Accounts ON #Accounts(pcn, account_no);  -- same time as primary key clustered with 2 pcn
INSERT #Accounts
(
  pcn,
  account_no,
  account_name,
  category_type
)
SELECT 
  a.plexus_customer_no pcn,
  Account_No,
  Account_Name,
  a.category_type
FROM accounting_v_Account_e a
join accounting_v_category_type t
on a.category_type=t.category_type -- 36,636
--WHERE Plexus_Customer_No = @PCN
where a.plexus_customer_no in
(
 select tuple from #list
)
--  AND (CHARINDEX( '|' + RTRIM(Account_No) + '|' , '|' + ISNULL(NULLIF(@Account_No,''),'') + '|' , 0) > 0  );
-- select count(*) accounts from #Accounts  -- 3413/Albion


CREATE TABLE #result
(
  pcn INT NOT NULL,
  period int not null,
  account_no VARCHAR(20) NOT NULL,
  account_name varchar(110),
  debit decimal(19,5),
  credit decimal(19,5),
  PRIMARY KEY CLUSTERED
  (
    PCN,period,account_no
  )
);
-- select * from #result;

declare @pcn int;
declare @period int;

set @row_no = 0;
WHILE @row_no <= @max_row_no
BEGIN
  select @pcn=tuple from #list where row_no = @row_no;
  
  select @period=open_period 
  from #accounting_balance_update_period_range
  where pcn=@pcn;
  -- select @row_no row_no,@pcn pcn,@period period,@max_row_no max_row_no

  set @row_no = @row_no + 1;
  insert into #result
  SELECT
    t1.pcn,
    t1.Period,
    a.Account_No,
    a.account_name,
    sum(t1.debit) debit,sum(t1.credit) credit  
  --  t1.[Description],
  FROM (
    SELECT
    i.plexus_customer_no pcn,i.period,D.Account_No,sum(d.debit) debit,sum(d.credit) credit
    FROM accounting_v_AP_Invoice_Dist_e AS D 
    JOIN accounting_v_AP_Invoice_e AS I 
      ON I.Plexus_Customer_No = D.Plexus_Customer_No 
      AND I.Invoice_Link = D.Invoice_Link
      AND I.Period BETWEEN @period AND @period -- faster than =
    JOIN #Accounts AS A
      ON A.PCN = D.Plexus_Customer_No
      AND A.Account_No = D.Account_No
  --  LEFT OUTER JOIN Common_v_Supplier_e AS S 
  --    ON S.Plexus_Customer_No = I.Plexus_Customer_No
  --    AND S.Supplier_No = I.Supplier_No
      group by i.plexus_customer_no,i.period,D.Account_No
      having i.plexus_customer_no = @pcn
--      having i.plexus_customer_no in
--      (
--       select tuple from #list
--      )
  
      --having i.plexus_customer_no = @PCN
  --  group by @PCN,I.Period,D.Account_No,S.Name,D.[Description]   
  --  having D.Plexus_Customer_No = @PCN
  
    UNION ALL
    --++--
    SELECT i.plexus_customer_no pcn,i.period,d.Account_No,sum(d.debit) debit,sum(d.credit) credit
  
    FROM accounting_v_AP_Check_Dist2_e AS D 
    JOIN accounting_v_AP_Check_e AS I 
      ON I.Plexus_Customer_No = D.Plexus_Customer_No 
      AND I.Check_Link = D.Check_Link
      AND I.Period BETWEEN @period AND @period -- faster than =
    JOIN #Accounts AS A2
      ON A2.PCN = D.Plexus_Customer_No
      AND A2.Account_No = D.Account_No
      group by i.plexus_customer_no,i.period,d.Account_No   
      having i.plexus_customer_no = @pcn
--      having i.plexus_customer_no in
--      (
--       select tuple from #list
--      )
    --++--
    UNION ALL  
    --++--
    SELECT     i.plexus_customer_no pcn,i.period,d.Account_No,sum(d.debit) debit,sum(d.credit) credit
    FROM accounting_v_AR_Invoice_Dist_e AS D 
    JOIN accounting_v_AR_Invoice_e AS I 
      ON I.Plexus_Customer_No = D.Plexus_Customer_No 
      AND I.Invoice_Link = D.Invoice_Link
      AND I.Void = 0
      AND I.Period BETWEEN @period AND @period -- faster than =
    JOIN #Accounts AS A3
      ON A3.PCN = D.Plexus_Customer_No
      AND A3.Account_No = D.Account_No
      group by i.plexus_customer_no,i.period,d.Account_No   
      having i.plexus_customer_no = @pcn
--      having i.plexus_customer_no in
--      (
--       select tuple from #list
--      )
    --++--
    UNION ALL
    --++--
    SELECT     i.plexus_customer_no pcn,i.period,d.Account_No,sum(d.debit) debit,sum(d.credit) credit
    FROM accounting_v_AR_Invoice_Applied_Dist2_e AS D 
    JOIN accounting_v_AR_Invoice_Applied_e AS A 
      ON A.Plexus_Customer_No = D.Plexus_Customer_No 
      AND A.Applied_Link = D.Applied_Link
      AND A.Period BETWEEN @period AND @period -- faster than =
    JOIN accounting_v_AR_Invoice_e AS I 
      ON I.Plexus_Customer_No = A.Plexus_Customer_No
      AND I.Invoice_Link = A.Invoice_Link
    JOIN #Accounts AS A4
      ON A4.PCN = D.Plexus_Customer_No
      AND A4.Account_No = D.Account_No
      group by i.plexus_customer_no,i.period,d.Account_No   
      having i.plexus_customer_no = @pcn
--      having i.plexus_customer_no in
--      (
--       select tuple from #list
--      )
   --++--
    UNION ALL
    --++--
    SELECT     i.plexus_customer_no pcn,i.period,d.Account_No,sum(d.debit) debit,sum(d.credit) credit
    FROM accounting_v_AR_Deposit_Dist_e AS D 
    JOIN accounting_v_AR_Deposit_e AS I 
      ON I.Plexus_Customer_No = D.Plexus_Customer_No 
      AND I.Deposit_Link = D.Deposit_Link
      AND I.Period BETWEEN @period AND @period -- faster than =
    JOIN #Accounts AS A5
      ON A5.PCN = D.Plexus_Customer_No
      AND A5.Account_No = D.Account_No
      group by i.plexus_customer_no,i.period,d.Account_No   
      having i.plexus_customer_no = @pcn
--      having i.plexus_customer_no in
--      (
--       select tuple from #list
--      )
  
    --++--
    UNION ALL
    --++--
    SELECT     i.plexus_customer_no pcn,i.period,d.Account_No,sum(d.debit) debit,sum(d.credit) credit
    FROM accounting_v_GL_Journal_Dist_e AS D 
    JOIN accounting_v_GL_Journal_e AS I 
      ON I.Plexus_Customer_No = D.Plexus_Customer_No 
      AND I.Journal_Link = D.Journal_Link
      AND ( @Exclude_Period_13 = 0 OR I.Period_13 = 0 )
      AND ( @Exclude_Period_Adjustments = 0 OR I.Period_Adjustment = 0 )
      AND I.Period BETWEEN @period AND @period -- faster than =
    JOIN #Accounts AS A6
      ON A6.PCN = D.Plexus_Customer_No
      AND A6.Account_No = D.Account_No
      group by i.plexus_customer_no,i.period,d.Account_No   
      having i.plexus_customer_no = @pcn
--      having i.plexus_customer_no in
--      (
--       select tuple from #list
--      )
  ) t1
  JOIN #Accounts AS A 
    ON A.PCN = t1.PCN
    AND A.Account_No = t1.Account_No
  JOIN accounting_v_Period_e AS P
    ON P.Plexus_Customer_No = t1.PCN 
    AND P.Period = t1.Period
  group by t1.pcn,t1.period,A.Account_No,A.account_name   
  ORDER BY  
    t1.pcn,t1.Period,a.account_no
end; -- end while  
-- select count(*) SouthfieldCnt from #result where pcn = 123681
-- select count(*) AlbionCnt from #result where pcn = 300758

select 
r.pcn,
@period period,
r.account_no,
--a.category_type,
r.debit,
r.credit,
r.debit-r.credit balance
from #result r
join #Accounts a
on r.pcn=a.pcn
and r.account_no=a.account_no

/*
select *    
FROM accounting_v_GL_Journal_Dist_e AS D 
where plexus_customer_no = 123681
and account_no in ('39100-000-0000')
*/
--where r.account_no in ('10220-000-00000','10250-000-00000','11900-000-0000','11010-000-0000','41100-000-0000','50100-200-0000','51450-200-0000')