--accounting_balance_period_add_date

/*
  select distinct plexus_customer_no pcn, period  from accounting_v_balance_e b
  where b.plexus_customer_no = 123681 
  order by period desc;
*/;

with account_balance_period_add_date
as 
(
  select  
  distinct plexus_customer_no pcn, period
  ,add_date -- for the periods I checked the add_date is the same for each period balance account
  --,update_date  -- for the periods I checked this value was null
  from accounting_v_balance_e b
  where b.plexus_customer_no = 123681 
  --and period = 202204
)
select * from account_balance_period_add_date order by pcn,period desc
/* THIS VIEW IS FOR VERIFICATION ONLY
--The only period to ever have a value in the update_date column for southfield was 202001
,account_balance_period_update_date
as 
(
  select  
  distinct plexus_customer_no pcn, period
  ,update_date -- for the periods I checked the add_date is the same for each period balance account
  --,update_date  -- for the periods I checked this value was null
  from accounting_v_balance_e b
  where b.plexus_customer_no = 123681 

)
--select * from account_balance_period_update_date order by pcn,period desc
*/
