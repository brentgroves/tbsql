SELECT 
    Datasource_Key,
    Datasource_Name,
    Datasource_Type,
    Note,
    Module,
    [Database_Name],
    [Procedure_Name],
    [Text]
  FROM Plex.Datasource_Text
  
select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc

select * from ETL.Script s 
-- delete from ETL.script_history where script_history_key = 4746

/*
AccountingYearCategoryType: Run this ETL Script in late December.   
It is used to add account category records for each year.  up 
This is needed in YTD calculations which rely on if an account  
is a revenue/expense to determine whether to reset YTD values to 0 for every year. 
*/

select top 10 * from Plex.accounting_account_year_category_type aayct 
where pcn = 123681 -- 23,063
and year between 2023 and 2024
order by account_no 
select count(*) from Plex.accounting_account_year_category_type aayct  --/41,381/33,096/24,811/16,526, 24,767, 24,723
where pcn = 123681 -- 23,063
and year between 2022 and 2023  -- 9,234 

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc


/*
Run the Accounting_account ETL script.  
Issue: This is used to generate records in account_period_balance. Since the previous 12 months account_period_balance gets  regenerated 
when a new period gets appended if the category type changes or an account somehow gets removed the previous 12 months worth of records get be affected.  
*/

select count(*) from Plex.accounting_account  -- 19,287/19,286,19,176
where pcn=123681 -- 4,617
select distinct(pcn) from Plex.accounting_account  
select top 10 * from Plex.accounting_account  

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc


/*
Accounting_period ETL script is used to refresh the DW accounting_period table containing 
start and end period dates, and add_date and update_date which well help us determine if  
there has been updates to the period since the last time these scripts were ran. 
*/
  select *
 -- select count(*)
 from Plex.accounting_period p  
 where pcn = 123681 -- 1084/1060/1036 
 and period > 202101 -- 718/694/670
and period between 202306 and 202308

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc


/* 
  AccountingPeriodRanges:
  accounting_period_ranges_dw_import,sproc123681_11728751_2112421
  The period range is min open period to year before it
  Pushed the min open period to one period back from the actual min open period
  in case the period was recently opened/closed, so the calculation is 
  based on the account activity summary web service which will have the 
  most up-to-date values.
 */
--TB-202205_to_202305_on_06-09_DM
select * from Plex.accounting_period_ranges -- 202105/202204
where pcn = 123681
-- update Plex.accounting_period_ranges 
-- set start_period = 202211,no_update = 0
-- set no_update=0
where pcn = 123681

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc

/*
The AccountingStartPeriodUpdate script calls the accounting_start_period_update python script 
to determine the date the Forever-to-date and YTD calculations need be started. This script
is not perfect.  It is bases its decision on the updated_date of the previous accounting_period
record being different than the current one. If the pipeline fails before the FTD and YTD balances
are calculated and then it is restarted then both the updated_dates will be the same. Then 
you must manually change the accounting_period_ranges start_period  to what the AccountingPeriodRanges
script set it to be.
 */

select * from Plex.accounting_period_ranges -- 202105/202204
where pcn = 123681
-- update Plex.accounting_period_ranges 
-- set start_period = 202211,no_update = 0
-- set no_update=0
where pcn = 123681

-- update Plex.accounting_period_ranges 
set start_period = 202201,no_update = 0
-- set start_period = 202208,no_update = 0
-- set no_update=0
where pcn = 123681

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc

/*
 * You can verify the AccountingStartPeriodUpdate script by running this select command.
 * The ending period should be the Plex.accounting_period_ranges.end_period value.
 */
select p1.pcn,p1.period,p1.update_date prev_update_date,p2.update_date cur_update_date 
from Plex.accounting_period p1  
join Plex.accounting_period p2
on p1.pcn=p2.pcn 
and p1.period_key=p2.period_key
and p1.ordinal = 0
and p2.ordinal = 1
where p1.pcn = 123681 
and p1.period between 202201 and 202306
and p1.update_date <> p2.update_date 
/*
 *  Or you can verify the AccountingStartPeriodUpdate script by running this select command.
 *  The ending period should be the Plex.accounting_period_ranges.end_period value.
 */
select o0.period_display,o0.add_date,o1.add_date,o0.update_date,o1.update_date
from 
(
 select * 
 from Plex.accounting_period o0 -- 1276/1418
 where pcn = 123681 -- 518
 and ordinal = 0
 and period between 202201 and 202302

) o0  
inner join 
(
 select * 
 from Plex.accounting_period o0 -- 1276/1418
 where pcn = 123681 -- 518
 and ordinal = 1
) o1  
on o0.pcn = o1.pcn 
and o0.period_key = o1.period_key
and o0.update_date!=o1.update_date

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc

/*
If no_update = 0 the AccountingBalanceAppendPeriodRange ETL script calls accounting_balance_append_period_range_dw_import
sproc300758_11728751_2000117 (pcn,start_period,end_period) to pull the most recent accounting_balance records from Plex.
It then runs Plex.accounting_balance_delete_period_range to delete the outdated accounting_balance records in the DW. 
*/
select * from Plex.accounting_period_ranges apr where pcn=123681
SELECT distinct pcn,period 
--TB-202205_to_202305_on_06-09_DM
-- select count(*)
-- select top 10 *
FROM Plex.accounting_balance 
where pcn = 123681 
-- and period = 202308  -- 232
and period between 202211 and 202308  -- 2417

and period between 202210 and 202308  -- 2651

and period between 202209 and 202307  -- 2654
and period = 202307  -- 234

 -- and period = 202301  -- 244
-- and period = 202302  -- 241
-- and period = 202303  -- 240
-- and period between 202206 and 202304  -- 2641
-- and period = 202304  -- 243
-- and period between 202207 and 202305  -- 2,646
-- and period = 202305  -- 248
 and period = 202306  -- 233
 and period = 202307  -- 0

order by pcn,period 

/*
 * Detected an error on 03/09/2023: 
 * no_update = 1 but there were no 2023-01 balance records at all.
 * The daily scripts were not running during this period.
 * Until the scripts are running again run the TrialBalance pipeline
 * with start_period_update = 0 so all the balance records will be
 * pulled from Plex.
 */
 select * 
 from Plex.accounting_period  -- 1276/1418
 where pcn = 123681 -- 518
 -- and ordinal = 0
 and period between 202212 and 202302
 order by pcn,period 

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc

/*
AccountActivitySummaryGetOpenPeriodRange
Takes the open period range from Plex.accounting_period_ranges and calls the Account_Activity_Summary_xPCN_Get web service
for each period. This gives the most up-to-date totals for each account. The Plex.account_activity_summary records for this
range are then deleted and added using the web service data.
*/

select * from Plex.accounting_period_ranges -- 202105/202204
where pcn = 123681
-- update Plex.accounting_period_ranges 
set start_period = 202208,no_update = 0
-- set no_update=0
where pcn = 123681

select distinct pcn,period 
-- select count(*)
from Plex.account_activity_summary  
where pcn=123681
and period = 202311  -- 4617
and period between 202309 and 202311 -- 13,851

--and period = 202310  -- 4617
and period between 202309 and 202310 -- 9,234

-- and period = 202309  -- 4617
and period between 202308 and 202309 -- 9,234

-- and period between 202303 and 202304 -- 9,234
--and period between 202304 and 202305 -- 9,234
--and period between 202305 and 202306 -- 9,234
-- and period between 202306 and 202307 -- 9,234
-- and period between 202307 and 202308 -- 9,234


order by pcn,period

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc

/*
AccountPeriodBalanceRecreatePeriodRange
The AccountPeriodBalanceRecreatePeriodRange ETL Script runs the Plex.account_period_balance_recreate_period_range procedure 
It calculates the Forever-to-date and YTD account totals by adding the accounting_balance totals for the period to the previous
periods values starting from the period previous to the start_period stored in the Plex.accounting_period_ranges table.
*/
/*
 * Backup
 */
-- select * 
into Archive.account_period_balance_10_18_2023 -- 244,878
--into Archive.account_period_balance_08_24_2023 -- 235,644
-- into Archive.account_period_balance_08_09_2023 -- 231,027
-- into Archive.account_period_balance_05_09_2023 -- 217,176
--into Archive.account_period_balance_04_11_2023 -- 212,559
from Plex.account_period_balance b -- 43,630
--select count(*) from Archive.account_period_balance_2022_06_04 -- 140,713
select distinct pcn,period 
--select count(*) 
from Archive.account_period_balance_05_09_2023 -- 140,713
-- where period between 202201 and 202303 -- -- 69,255
where period between 202201 and 202305 -- 69,255
and pcn = 123681
order by pcn,period 

select * from Plex.accounting_period_ranges -- 202105/202204
where pcn = 123681
-- update Plex.accounting_period_ranges 
set start_period = 202210,no_update = 0
-- set no_update=0
where pcn = 123681

--WATCH: THERE WAS AN ERROR IN THIS SCRIPT FOR MYSQL THERE WAS A 202213 PERIOD  
--AccountPeriodBalanceRecreateOpenPeriodRange"

select distinct pcn,period 
-- select count(*)
from Plex.account_period_balance 
--order by pcn,period desc
where pcn = 123681 
and period between 202211 and 202308  -- 46,170
--and period between 202210 and 202308  -- 50,787
--and period between 202209 and 202307  -- 50,787
-- and period between 202207 and 202306  -- 55,404
-- and period between 202208 and 202306  -- 50,787
-- and period between 202207 and 202307 -- 60,021
-- and period between 202206 and 202305 -- 55,404 
-- and period = 202308  -- 0
-- and period = 202307  -- 4,617
--and period = 202306  -- 4,617
-- and period = 202305  -- 4,617
--and period between 202205 and 202304 -- 55,404 
--and period = 202304  -- 4,617

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc


/*
AccountPeriodBalanceRecreateOpenPeriodRange
The AccountPeriodBalanceRecreateOpenPeriodRange ETL Script runs the Plex.account_period_balance_recreate_open_period_range procedure 
It calculates the Forever-to-date and YTD account totals by adding the accounting_balance totals for the period to the previous
periods values starting from the period previous to the end_period stored in the Plex.accounting_period_ranges table.
*/


select * from Plex.accounting_period_ranges -- 202303/202304
where pcn = 123681
-- update Plex.accounting_period_ranges 
set start_period = 202210,no_update = 0
-- set no_update=0
where pcn = 123681

select distinct pcn,period 
-- select *
-- delete from Plex.account_period_balance where period = 202213
-- select count(*)
from Plex.account_period_balance 
where pcn = 123681 
and period between 202309 and 202311  -- 13,851

and period between 202309 and 202310  -- 9234
--and period between 202308 and 202309  -- 9234
--and period between 202307 and 202308  -- 9234
--and period between 202207 and 202307  -- 60,021
--and period between 202206 and 202306 -- 60,021 
--and period  = 202306
--and period between 202205 and 202305 -- 60,021 
--and period between 202304 and 202306 -- 13,851 
--and period between 202201 and 202305 -- 78,489 
-- and period between 202201 and 202303 -- 69,255 
-- and period between 202303 and 202304 --  -- 9,234
-- and period between 202301 and 202303 -- 13,851
-- and period between 202210 and 202212 -- 13,851
--   and period = 202301  -- 4,617
-- limit 100
-- and period >= 202204
order by pcn,period desc

select s.Name,sh.*
from ETL.script_history sh 
join ETL.script s 
on sh.script_key=s.Script_Key 
where sh.script_key in (1,3,4,5,6,7,8,9,10,11,116,117)
and start_time between '2023-11-07 00:00:00' and '2023-11-08 00:00:00' 
order by script_history_key desc


TrialBalance ssis ETL script that takes as input the Plex Trial Balance CSV file.   
select distinct pcn,period from Plex.trial_balance_multi_level order by pcn,period desc

The CsvToTrialBalance python script is meant to replace the SSIS script
select distinct pcn,period from Plex.trial_balance_multi_level_py order by pcn,period desc
select * from Plex.trial_balance_multi_level_py order by pcn,period desc
-- delete from Plex.trial_balance_multi_level_py where period = 202212

GL_Account_Activity_Summary ETL script is used to validate accounts no longer showing in 
the Trial Balance Multi level report.

From Alb-utl Add or update Plex.trial_balance_multi_level records using the TrialBalance ETL script.  If you are sure there have been no changes 
to previous period values then just run the script for the current period. 

select * from Plex.accounting_period_ranges -- 202303/202304
where pcn = 123681

/*
 * Decide which TB periods to pull by  
 * each period has 2 records. ordinal 1 is the most recent
 * goto Main Compare: section and locate the last period to have any differences
 * and import all periods after and including that one for both balance and activity_summary records.
 * TB-202210_to_202310_on_11-07_DM_GP period 202310 not closed yet.No plex accessible account changes in 202309 so just import 202310 
 * TB-202209_to_202309_on_10-24_DM_GP found no diff between 202209 and 202308 from the trial_balance_multi_level so did not import 202308 again
 *  * TB-202209_to_202309_on_10-18_DM_GP found no diff between 202209 and 202308 from the trial_balance_multi_level so did not import 202308 again
 * TB-202209_to_202309_on_10-17_DM found 1 diff between 202209 and 202308 from the trial_balance_multi_level so i imported 202308 and 202309 again
 * |period |account_no    |b_balance|d_balance  |ytd_balance |ytd_debit_credit|
|-------|--------------|---------|-----------|------------|----------------|
|202,308|11010-000-0000|7,276.94 |-261,976.29|5,107,926.02|4,838,672.77    |

 * TB-202209_to_202309_on_10-10_DM found no diff between 202209 and 202308 from the trial_balance_multi_level so did not import 202308 again
 * TB-202208_to_202308_on_09-18_DM found no diff between 202209 and 202307 from the trial_balance_multi_level so did not import 202307 again
 * TB-202208_to_202308_on_09-13_DM found no diff between 202208 and 202307 from the trial_balance_multi_level so did not import 202307 again
 * TB-202208_to_202308_on_09-11_DM found no diff between 202208 and 202307 from the trial_balance_multi_level so did not import 202307 again
 * TB-202207_to_202307_on_08-24_DM found no diff between 202207 and 202307 from the trial_balance_multi_level so did not import 202307 again
 * TB-202207_to_202307_on_08-09_DM found no diff between 202207 and 202306 so just need to import 202307
 * TB-202206_to_202306_on_07-11_DM found no diff between 202206 and 202305 so just need to import 202306
 * TB-202205_to_202305_on_06-13_DM dan made minor adjustments to 202305
 * TB-202205_to_202305_on_06-09_DM found no diff between 202205 and 202304 so just need to import 202305
 * 05-09-2023: diff found in 202303 between trial_balance_multi-level and accounting_period_balance
 * 04-11-2023: diff found in 202303 between trial_balance_multi-level and accounting_period_balance
 * TB-202203_to_202303_on_04-11_DM
 * TB-202203_to_202303_on_04-11_GP
 * 03-09-2023: Pulled 202302 to 202303 since there where no updates on 202301 since 2023-02-27
 * 03-02-2023: Pulled 202212 to 202301
 */

/*
 * backup
 */

select * 
-- into Archive.trial_balance_multi_level_09_11_2023 -- 748,312
from Plex.trial_balance_multi_level -- 

/*
 * Main Compare
 * Are there any accounts that have different values?
 */
select * from ETL.script
select count(*) cnt 
from Plex.trial_balance_multi_level d
where pcn = 123681 and period = 202308  -- should be 0 for the last period because we have not ran the ETL script yet 
select * from Plex.accounting_period_ranges -- 202303/202304
where pcn = 123681


declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202210;
declare @period_end int;
set @period_end = 202310; -- TB-202210_to_202310_on_11-07_DM_GP no plex accessible account changes in 202309 
--set @period_end = 202309; -- TB-202210_to_202310_on_11-07_DM_GP no plex accessible account changes in 202309 
--set @period_end = 202309; -- TB-202209_to_202309_on_10-24_DM_GP  no account changes from 2209 to 202308;
-- set @period_end = 202309; -- TB-202209_to_202309_on_10-18_DM_GP  
--set @period_end = 202309; -- TB-202209_to_202309_on_10-17_DM there was 1 account that changed 
--set @period_end = 202308; -- TB-202209_to_202309_on_10-17_DM there was 1 account that changed 
--set @period_end = 202308; -- TB-202209_to_202309_on_10-10_DM 
--set @period_end = 202309; -- TB-202208_to_202308_on_09-18_DM 
--set @period_end = 202308; -- TB-202208_to_202308_on_09-13_DM
-- set @period_end = 202308; -- TB-202208_to_202308_on_09-11_DM
-- set @period_end = 202307; -- TB-202207_to_202307_on_08-24_DM
-- set @period_end = 202307; -- TB-202207_to_202307_on_08-09_DM
-- set @period_end = 202306; -- TB-202207_to_202307_on_08-09_DM
-- set @period_end = 202306; -- TB-202206_to_202306_on_07-11_DM


-- select b.period,b.account_no 
-- ,b.balance b_balance, d.current_debit_credit d_balance
-- ,b.ytd_balance, d.ytd_debit_credit 
select count(*)
-- select distinct b.period
from Plex.account_period_balance b 
-- where b.pcn = @pcn
-- and b.period between @period_start and @period_end -- 96,891/99,891/202101-202209/,234
join Plex.accounting_period ap
on b.pcn =ap.pcn 
and b.period = ap.period  
and ap.ordinal = 1
-- where b.pcn = @pcn
-- and b.period between @period_start and @period_end -- 96,891
join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
on b.pcn=d.pcn
and ap.period_display = d.period_display 
and b.account_no = d.account_no  -- 16,816
where b.pcn = @pcn
and b.period between @period_start and @period_end -- 54,652/202210-202310,54,652/202209-202309,54,652,50,448/202208-202308, 54,652/202207-202307, 54,652/202206-202306,54,652/202205-202305,117,712/202101-202304,109,304/202101-202302, 105,100/202101-202301,100,896/202101-202212,96,692/202101-202211,92,488/202101-202210/88,284/202101-202209/8,408/202208-202209,4,562/2021-01 to 2022-07,3,953/2021-01 to 2022-04
-- and b.balance  = d.current_debit_credit -- 54,614/202210-202310,54,611/202209-202309,54,614/202207-202307,54,617/202206-202306,54,618/202205-202305,117,117,645/202101-202304,109,242/202101-202302,/105,043/202101-202301,100,843/202101-202212,96,643/202101-202211,92,442/202101-202210,
-- and b.balance  != d.current_debit_credit -- 38/202210-202310,41/202209-202309,38/202207-202307,35/202206-202306,34/202205-202305,62/202101-202302,57/202101-202301,53/202101-202212,49/202101-202211,46//202101-202210
-- and abs(b.balance - d.current_debit_credit) >  0.01  -- 0 
-- and (b.ytd_balance = d.ytd_debit_credit)  -- 54,492/202210-202310 (7th),54,490/202209-202309 (17th),54,489/202209-202309(10th),54,491/202207-202307,54491/202206-202306,54,490/202205-202305,117,349/202101-202304,108,967/202101-202302,104,777/202101-202301,100,586/202101-202212,96,394/202101-202211,92,204/202101-202210
-- and (b.ytd_balance != d.ytd_debit_credit)  -- 160/202210-202310(7th),162/202209-202309(17th),163/202209-202309(10th),161/202207-202307,161/202206-202306,162/202205-202305,363/202101-202304,337/202101-202302,323/202101-202301,310/202101-202212,298/202101-202211,284/202101-202210
and abs(b.ytd_balance - d.ytd_debit_credit) > .01 -- 13/202210-202310,13/202209-202309,13/202207-202307,13/202206-202306,13/202205-202305,16/202101-202304,15/202101-202303,14/202101-202302,17/202101-202302,13/202101-202301,12/202101-202212,11/202101-202211,-- 10/202101-202210 (73100-000-0000)
and d.account_no != '73100-000-0000' -- 0/202207-202307,0/202206-202306,0/202205-202305,0/202101-202304,0/202101-202303,16,816
order by b.period asc 
-- ISSUE: 1 ACCOUNT IS NOT THE SAME
-- See issue section at the bottom of this procedure and the Mobex Plex procedure: accounting_year_category_type_issue 
-- 73100-000-0000 has different category_types in accounting_v_account it is an Expense and in accounting_v_category_type it is a liability
-- Conclusion: The Plex TB report and Plex authored procedure is wrong to not reset YTD values.

/* These values are not equal 
202309	16300-000-0000	119529.57000	1067950.09	44608858.79000	45557279.31
202309	16400-000-0000	0.00000	-41031.95	133727.56000	92695.61
202309	16500-000-0000	0.00000	47622.95	410031.68000	457654.63
202309	16900-000-0000	0.00000	-11025.01	1467061.87000	1456036.86
202309	17300-000-0000	-716221.16000	-903142.80	-25846324.82000	-26033246.46
202309	17400-000-0000	-419.61000	272850.45	-364663.89000	-91393.83
202309	17500-000-0000	0.00000	-280327.80	-169161.15000	-449488.95
202309	17900-000-0000	-3717.52000	7161.63	-1449662.04000	-1438782.89
202309	80300-200-0000	721570.54000	908492.18	4102809.46000	4289731.10
202309	80400-200-0000	159.19000	7216.92	5250.52000	12308.26
202309	80900-200-0000	0.00000	-10879.15	4952.50000	-5926.65
202309	90200-100-0000	264858.30000	-679128.21	264858.30000	-679128.21
 */

--From Plex download trial balance multi level report from the determined period range
--TrialBalance ssis ETL script that takes as input the Plex Trial Balance CSV file.   
--select distinct pcn,period from Plex.trial_balance_multi_level order by pcn,period desc


Optional: Add or update Plex.Account_Balances_by_Periods using the AccountBalancesByPeriod  ETL script.  If you are sure there have been no changes 
to previous period values then just run the script for the current period. 
select distinct pcn,period from Plex.Account_Balances_by_Periods order by pcn,period

Optional: Add or update Plex.GL_Account_Activity_Summary using the GLAccountActivitySummary  ETL script.  If you are sure there have been no changes 
to previous period values then just run the script for the current period. 
select distinct pcn,period from Plex.GL_Account_Activity_Summary order by pcn,period

Optional: Go to the Plex Account Activity by Period screen to check bookings that happened after the Plex snapshot.
Use the Plex AccountingAccountSummaryOpenPeriodDetail procedure to show the bookings for a single account.

/*
 * Backup
 */
--select * 
--into Archive.account_period_balance_04_11_2023 -- 212,559
from Plex.account_period_balance b -- 43,630
--select count(*) from Archive.account_period_balance_2022_06_04 -- 140,713

/*
 * Are there any accounts that we are not showing in our report?
 */
select * from Plex.accounting_period_ranges

declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202201;
declare @period_end int;
set @period_end = 202307;

select count(*)
from Plex.trial_balance_multi_level t -- 714,680,685,252
--left outer join Scratch.account_period_balance b -- 123,615
left outer join Plex.account_period_balance b -- 123,615
on b.pcn=t.pcn
and b.account_no = t.account_no
and b.period = t.period -- 688,665
where t.period between @period_start and @period_end -- 67,264/2021-01 to 2022-04
and b.pcn is null -- 0
--123681	202206	2022-07-11 18:17:30.473	2022-07-11 18:18:00.000

/*
 * Are there any accounts that we are not showing in our report?
 */
select * from Plex.accounting_period_ranges

declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202207;
declare @period_end int;
set @period_end = 202307;

select count(*)
from Plex.Account_Balances_by_Periods p -- 705,481/701,277
--left outer join Scratch.account_period_balance b -- 123,615
left outer join Plex.account_period_balance b -- 123,615
on b.pcn=p.pcn
and b.account_no = p.[no]
and b.period = p.period -- 688,665
where p.period between @period_start and @period_end -- 83,289/2021-01 to 2022-07,70,677/2021-01 to 2022-04
and b.pcn is null -- 0

/*
 * Are there any accounts that we are not showing in our report?
 */
select * from Plex.accounting_period_ranges

declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202101;
declare @period_end int;
set @period_end = 202209;

select count(*)
from Plex.GL_Account_Activity_Summary s -- 40,221
--left outer join Scratch.account_period_balance b -- 123,615
left outer join Plex.account_period_balance b -- 123,615
on b.pcn=s.pcn
and b.account_no = s.account_no
and b.period = s.period -- 39,612
where s.period between @period_start and @period_end -- 4,562/2021-01 to 2022-07,3,953/2021-01 to 2022-04
and b.pcn is null -- 0

/*
 * Are there any accounts that we are not showing in our report?
 */
select * from Plex.accounting_period_ranges

declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202208;
declare @period_end int;
set @period_end = 202209;

select count(*)
from Plex.GL_Account_Activity_Summary s -- 40,221
--left outer join Scratch.account_period_balance b -- 123,615
left outer join Plex.account_activity_summary b -- 123,615
on b.pcn=s.pcn
and b.account_no = s.account_no
and b.period = s.period -- 39,612
where s.period between @period_start and @period_end -- 4,562/2021-01 to 2022-07,3,953/2021-01 to 2022-04
and b.pcn is null -- 0

/*
 * Main Compare:
 * Are there any accounts that have different values?
 */
select * from Plex.accounting_period_ranges
select distinct period from Plex.trial_balance_multi_level where period >= 202204 order by period
select distinct period from Plex.account_activity_summary where period between 202307 and 202308 order by period 

select * from plex.accounting_period_ranges apr 
where pcn = 123681

/*
 * Are there any accounts that have different values?
 */

declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202101;
declare @period_end int;
set @period_end = 202303; -- plex.accounting_period_ranges.end_open_perid-1

-- select b.period,b.account_no 
 --,b.balance b_balance, d.current_debit_credit d_balance
 --,b.ytd_balance, d.ytd_debit_credit 
-- select count(*)
select distinct b.period
from Plex.account_period_balance b 
-- where b.pcn = @pcn
-- and b.period between @period_start and @period_end -- 96,891/99,891/202101-202209/,234
join Plex.accounting_period ap
on b.pcn =ap.pcn 
and b.period = ap.period  
and ap.ordinal = 1
-- where b.pcn = @pcn
-- and b.period between @period_start and @period_end -- 96,891
join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
on b.pcn=d.pcn
and ap.period_display = d.period_display 
and b.account_no = d.account_no  -- 16,816
where b.pcn = @pcn
and b.period between @period_start and @period_end -- 109,304/202101-202302, 105,100/202101-202301,100,896/202101-202212,96,692/202101-202211,92,488/202101-202210/88,284/202101-202209/8,408/202208-202209,4,562/2021-01 to 2022-07,3,953/2021-01 to 2022-04
-- and b.balance  = d.current_debit_credit -- 109,242/202101-202302,/105,043/202101-202301,100,843/202101-202212,96,643/202101-202211,92,442/202101-202210,
-- and b.balance  != d.current_debit_credit -- 62/202101-202302,57/202101-202301,53/202101-202212,49/202101-202211,46//202101-202210
-- and abs(b.balance - d.current_debit_credit) >  0.01  -- 0 
-- and (b.ytd_balance = d.ytd_debit_credit)  -- 108,967/202101-202302,104,777/202101-202301,100,586/202101-202212,96,394/202101-202211,92,204/202101-202210
-- and (b.ytd_balance != d.ytd_debit_credit)  -- 337/202101-202302,323/202101-202301,310/202101-202212,298/202101-202211,284/202101-202210
and abs(b.ytd_balance - d.ytd_debit_credit) > .01 -- 15/202101-202303,14/202101-202302,17/202101-202302,13/202101-202301,12/202101-202212,11/202101-202211,-- 10/202101-202210 (73100-000-0000)
and d.account_no != '73100-000-0000' -- 0/202101-202303,16,816
order by b.period asc 
-- ISSUE: 1 ACCOUNT IS NOT THE SAME
-- See issue section at the bottom of this procedure and the Mobex Plex procedure: accounting_year_category_type_issue 
-- 73100-000-0000 has different category_types in accounting_v_account it is an Expense and in accounting_v_category_type it is a liability
-- Conclusion: The Plex TB report and Plex authored procedure is wrong to not reset YTD values.


declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202304;  -- plex.accounting_period_ranges.start_open_perid
declare @period_end int;
set @period_end = 202305; -- plex.accounting_period_ranges.end_open_perid-1


--select b.period ,b.account_no
--,b.balance,d.current_debit_credit  
-- s.debit s_debit, b.debit b_debit
-- s.credit s_credit, b.credit b_credit 
-- s.debit - s.credit s_debit_credit, b.balance b_balance
-- select count(*)
select distinct b.period 
from Plex.account_activity_summary b -- 33,140
join Plex.accounting_period p
on b.pcn =p.pcn 
and b.period = p.period  -- 33,140
and p.ordinal = 1
join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
on b.pcn=d.pcn
and p.period_display = d.period_display 
and b.account_no = d.account_no  
where b.pcn = @pcn
and b.period between @period_start and @period_end -- 25,224,21,030,16,816
-- and b.balance  = d.current_debit_credit -- 25,203,25,207, 21,007, 16,806
-- and b.balance  != d.current_debit_credit -- 21/202101-202212,17/202101-202211,13/202101-202211,10/2021-10/2022 to 10/8/10/2/4
and abs(b.balance - d.current_debit_credit) > .01 -- 0
order by period asc 
-- 4 accounts have changed - but the not in the open period range 
202212	20100-000-0000	-1312618.79000	-1313468.79
202212	27800-000-9804	-4281131.52000	-4158537.09
202212	53100-200-0000	899954.25000	777359.82
202212	73100-200-0000	1415911.10000	1416761.10

these accounts changed since 12/19 so i redownloaded the TB from plex.
202211	12400-000-0000
202211	12500-000-0000
202211	12600-000-0000
202211	12900-000-0000
202211	53400-200-0000
202211	53700-200-0000

/*
 * Question about 202301 retained earnings account,39100-000-0000, not being rolled over from 202201
 * Is ok now. 
 */
select *
from Plex.account_period_balance b 
where b.pcn = 123681
and account_no = '39100-000-0000'
order by period desc
/*
 * Are there any accounts that have different values?
 */

declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202101;
declare @period_end int;
set @period_end = 202303; -- plex.accounting_period_ranges.end_open_perid-1

-- select b.period,b.account_no 
 --,b.balance b_balance, d.current_debit_credit d_balance
 --,b.ytd_balance, d.ytd_debit_credit 
-- select count(*)
select distinct b.period
from Plex.account_period_balance b 
-- where b.pcn = @pcn
-- and b.period between @period_start and @period_end -- 96,891/99,891/202101-202209/,234
join Plex.accounting_period ap
on b.pcn =ap.pcn 
and b.period = ap.period  
and ap.ordinal = 1
-- where b.pcn = @pcn
-- and b.period between @period_start and @period_end -- 96,891
join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
on b.pcn=d.pcn
and ap.period_display = d.period_display 
and b.account_no = d.account_no  -- 16,816
where b.pcn = @pcn
and b.period between @period_start and @period_end -- 109,304/202101-202302, 105,100/202101-202301,100,896/202101-202212,96,692/202101-202211,92,488/202101-202210/88,284/202101-202209/8,408/202208-202209,4,562/2021-01 to 2022-07,3,953/2021-01 to 2022-04
-- and b.balance  = d.current_debit_credit -- 109,242/202101-202302,/105,043/202101-202301,100,843/202101-202212,96,643/202101-202211,92,442/202101-202210,
-- and b.balance  != d.current_debit_credit -- 62/202101-202302,57/202101-202301,53/202101-202212,49/202101-202211,46//202101-202210
-- and abs(b.balance - d.current_debit_credit) >  0.01  -- 0 
-- and (b.ytd_balance = d.ytd_debit_credit)  -- 108,967/202101-202302,104,777/202101-202301,100,586/202101-202212,96,394/202101-202211,92,204/202101-202210
-- and (b.ytd_balance != d.ytd_debit_credit)  -- 337/202101-202302,323/202101-202301,310/202101-202212,298/202101-202211,284/202101-202210
and abs(b.ytd_balance - d.ytd_debit_credit) > .01 -- 15/202101-202303,14/202101-202302,17/202101-202302,13/202101-202301,12/202101-202212,11/202101-202211,-- 10/202101-202210 (73100-000-0000)
and d.account_no != '73100-000-0000' -- 0/202101-202303,16,816
order by b.period asc 
-- ISSUE: 1 ACCOUNT IS NOT THE SAME
-- See issue section at the bottom of this procedure and the Mobex Plex procedure: accounting_year_category_type_issue 
-- 73100-000-0000 has different category_types in accounting_v_account it is an Expense and in accounting_v_category_type it is a liability
-- Conclusion: The Plex TB report and Plex authored procedure is wrong to not reset YTD values.


/*
 * Full Validation includes the trial balance multi-level,
 * Plex.Account_Balances_by_Periods, and Plex.GL_Account_Activity_Summary s 
 * comparisons.
 */

declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202101;
declare @period_end int
set @period_end = 202207;

/*
SElect 
b.period,b.account_no
-- ,b.credit our_credit, p.current_credit pp_credit
 ,b.balance our_balance, p.Current_Debit - p.Current_Credit PP_balance, d.current_debit_credit TB_balance 
-- ,s.debit gl_debit, b.debit our_debit
-- ,b.ytd_credit our_ytd_credit,p.Ytd_Credit pp_ytd_credit
-- ,b.ytd_debit our_ytd_debit,p.Ytd_Debit  pp_ytd_debit
--,b.ytd_balance our_ytd_balance, p.Ytd_Debit - p.Ytd_Credit pp_ytd_balance, d.ytd_debit_credit tb_ytd_debit_credit 
*/
select count(*) 
from Plex.account_period_balance b -- 157,283,140,713/123,615
-- where b.period = 202206  -- 8,285
-- where b.period = 202205  -- 8,285
--where b.period between 202101 and 202203
--and b.pcn = 123681 -- 4595*15=45950+22975 = 68925
inner join Plex.accounting_account a 
on b.pcn=a.pcn 
and b.account_no=a.account_no -- 766,413
--LEFT OUTER JOIN Scratch.account_period_balance ab 
--ON b.pcn=ab.pcn 
--AND b.account_no=ab.account_no 
--AND b.period=ab.period
left outer join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
on b.pcn=d.pcn
and b.account_no = d.account_no
and b.period_display = d.period_display 
-- select * from Plex.Account_Balances_by_Periods p 
--select distinct pcn,period from Plex.Account_Balances_by_Periods p order by pcn,period -- 123,681 (200812-202110)
--select * from Plex.Account_Balances_by_Periods p where pcn=123681
-- select  
--select top(10) *
--into Scratch.Account_Balances_by_Periods
--select *
--into Archive.Account_Balances_by_Periods_2022_01_11  -- 667,645
--from Plex.Account_Balances_by_Periods p
left outer join Plex.Account_Balances_by_Periods p -- 43,620
on b.pcn=p.pcn
and b.account_no = p.[no]
and b.period = p.period 
--inner join 
left outer join 
(
	select s.pcn,s.period, s.account_no,s.debit,s.credit,s.debit-s.credit balance
	--select count(*)
--select distinct pcn,period from Plex.GL_Account_Activity_Summary s order by pcn,period -- 123,681 (200812-202203)
--select * from Plex.GL_Account_Activity_Summary s where pcn=123681 and period = 202111  -- dont know when this was imported probably in early december
	from Plex.GL_Account_Activity_Summary s  --(),(221,202010)
--	where s.pcn = 123681 
--	and s.period between 202101 and 202201  -- 2,462/2,718/2,975
) s
on b.pcn=s.pcn 
and b.account_no=s.account_no
and b.period=s.period  
-- where b.pcn=@pcn and b.period between @period_start and @period_end  --87,657/92,274/87,657/202207,83,040/2022-06/78,423/2021-01 to 2022-05, 73,806/2021-01 to 2022-04/68,925/2021-01 to 2022-03 -- 69,189
-- where b.pcn=@pcn and b.period=202201 and b.account_no = '73100-000-0000'
--DEBUG ONLY where b.pcn=@pcn and b.period between @period_start and @period_end and b.account_no like '4%' and b.period = 202201 and b.credit  > 0
-- where b.pcn=@pcn and b.period between @period_start and @period_end and p.pcn is not null -- 79,876/84,080/79,876/202207,75,672/2022-06,71,468/2021-01 to 2022-05, 67,264/2021-01 to 2022-04, 63,060/2021-01 to 2022-03
-- where b.pcn=@pcn and b.period between @period_start and @period_end and p.pcn is null and s.pcn is not null  --64/202207,60/2022-06,56/2021-01 to 2022-05, 53/2021-01 to 2022-04 --5/2021-01 to 2022-03 -- 47/2021-01 to 2022-03 --42/2021-01 to 2022-01 -- 38/2021-01 to 2021-12  account periods with activity not on the TB report.
-- where b.pcn=@pcn and b.period between @period_start and @period_end and s.pcn is not null  -- 4,666/202207/4,625/202207,4,562/202207,4,433/2022-06, 4,190/2021-01 to 2022-05-pulled-06-14,--4,186/2021-01 to 2022-05-pulled-06-04 --4,177/2021-01 to 2022-05--3,953/2021-01 to 2022-04 --3,696/2021-01 to 2022-03 --3,446/2021-01 to 2022-02-- 3,217/2021-01 to 2022-01 -- 2,975/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.debit=s.debit  --4,666/202207/4,625/202207,4,433/2022-06,4,190/2021-01 to 2022-05-pulled-06-14--4,186/2021-01 to 2022-05-pulled-06-04--4,177/2021-01 to 2022-05--3,953/2021-01 to 2022-04 --3,696/2021-01 to 2022-03 --3,446/2021-01 to 2022-02-- 3,217/2021-01 to 2022-01 -- 2,975/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and (s.debit != b.debit) -- 0/2021-01 to 2022-07 -- 0/2021-01 to 2022-03 -- 0/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.credit = s.credit  --4,666/202207/4,625/202207,4,433/2022-06,4,190/2021-01 to 2022-05-pulled-06-14--4,186/2021-01 to 2022-05-pulled-06-04--4,177/2021-01 to 2022-05--3,953/2021-01 to 2022-04 -- 3,696/2021-01 to 2022-03 -- 2,975/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.credit != s.credit -- 0/2022-06,0/2021-01 to 2022-05 -- 0/2021-01 to 2022-04 -- 0/2021-01 to 2022-03 
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.balance =s.balance  --4,666/202207/4,625/202207,4,433/2022-06,4,190/2021-01 to 2022-05-pulled-06-14--4,186/2021-01 to 2022-05-pulled-06-04--4,177/2021-01 to 2022-05--3,953/2021-01 to 2022-04 -- 3,696/2021-01 to 2022-03 -- 2,975/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.balance !=s.balance -- 0

-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.balance = d.current_debit_credit  --79,837/202207/79,840/79,838/202207,75,636/2022-6,71,432/2021-01 to 2022-05 --67,228/2021-01 to 2022-04 --63,028/2021-01 to 2022-03 --50,423/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.balance != d.current_debit_credit  -- 39/202207/36/202208,38/202207, 36/2022-06,36/2021-01 to 2022-05 -- 36/2021-01 to 2022-04 -- 32/2021-01 to 2022-03 -- 25/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and (b.balance - d.current_debit_credit) >  0.01 --0/202208, 0/202207,0/2022-06,0/2021-01 to 2022-05-- 0/2021-01 to 2022-04 -- 0/2021-01 to 2022-03 -- 0/2021-01 to 2021-12

-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.credit = p.current_credit  -- 79,876/202207,75,672/0,71,468/2021-01 to 2022-05-- 67,264/2021-01 to 2022-04 -- 63,060/2021-01 to 2022-03 --50,448/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.credit != p.current_credit  -- 0/2021-01 to 2022-05 -- 0/2021-01 to 2022-04 -- 0/2021-01 to 2022-03 --0/0 
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.debit = p.current_debit  -- 79,876/202207,75,672/2022-06,71,468/2021-01 to 2022-05-- 67,264/2021-01 to 2022-04-- 63,060/2021-01 to 2022-03 -- 50,448/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.debit != p.current_debit  -- 0/202207/ 0/2022-06,0/2021-01 to 2022-01 -- 0 
-- where b.pcn=@pcn and b.period between @period_start and @period_end and (b.balance = p.Current_Debit - p.Current_Credit)  -- 79,876/202207,75,672/2022-06,71,468/2021-01 to 2022-05-- 67,264/2021-01 to 2022-04 -- 63,056/2021-01 to 2022-03 -- 50,448/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and (b.balance != p.Current_Debit - p.Current_Credit)   -- 0/2021-01 to 2022-01 --0/2021-01 to 2021-12

-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.ytd_credit = p.ytd_credit  -- 79,869/202207, 75,666/2022-06,71,463/2021-01 to 2022-05-- 67,260/2021-01 to 2022-04-- 63,057/2021-01 to 2022-03 -- 50,448/2021-01 to 2021-12--
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.ytd_credit != p.ytd_credit  -- 7/202207, 6/2022-06
-- ISSUE: 1 ACCOUNT,73100-000-0000, IS NOT THE SAME
-- See issue section at the bottom of this procedure and the Mobex Plex procedure: accounting_year_category_type_issue 
-- 73100-000-0000 has different category_types in accounting_v_account it is an Expense and in accounting_v_category_type it is a liability
-- Conclusion: The Plex TB report and Plex authored procedure is wrong to not reset YTD values.

-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.ytd_debit = p.ytd_debit  -- 79,869/202207,75,666/2022-06,71,463/2021-01 to 2022-05-- 67,260/2021-01 to 2022-04--63,053/2021-01 to 2022-03 -- 50,448/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and b.ytd_debit != p.ytd_debit  -- 7/202207, 6/2022-06,5/2021-01 to 2022-05-- 4/2021-01 to 2022-04 -- 3/2021-01 to 2022-03 -- 0/2021-01 to 2021-12
-- ISSUE: 1 ACCOUNT IS NOT THE SAME
-- See issue section at the bottom of this procedure and the Mobex Plex procedure: accounting_year_category_type_issue 
-- 73100-000-0000 has different category_types in accounting_v_account it is an Expense and in accounting_v_category_type it is a liability
-- Conclusion: The Plex TB report and Plex authored procedure is wrong to not reset YTD values.

-- WHere b.pcn=@pcn and b.period between @period_start and @period_end and (d.ytd_debit_credit = (p.ytd_debit-p.ytd_credit))  --79,637/202207/79,638/202207,75,443/2022-06,71,248/2021-01 to 2022-05-pulled-06-14--71,250/2021-01 to 2022-05-pulled-06-04--71,249/2021-01 to 2022-05--67,056/2021-01 to 2022-04 --62,861/2021-01 to 2022-01 -- 50,286/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and (d.ytd_debit_credit != (p.ytd_debit-p.ytd_credit))  --239/202207/238/202207,229/2022-06,220/2021-01 to 2022-05-pulled-06-14--218/2021-01 to 2022-05-pulled-06-04--219/2021-01 to 2022-05--208/2021-01 to 2022-04 --199/2021-01 to 2022-01 -- 162/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and (((p.ytd_debit-p.ytd_credit) - d.ytd_debit_credit) > 0.01 or ((p.ytd_debit-p.ytd_credit) - d.ytd_debit_credit) < -0.01)   -- 0/202207/2/202207,0/2022-06,0/2021-01 to 2022-04 -- 0/2021-01 to 2022-03 

-- where b.pcn=@pcn and b.period between @period_start and @period_end and (b.ytd_balance = d.ytd_debit_credit) --79,630/202207/79,628/202208,79,631/202207,75,437/2022-06,71,243/2021-01 to 2022-05-pulled-06-14--71,245/2021-01 to 2022-05-pulled-06-04 --71,244/2021-01 to 2022-04--67,052/2021-01 to 2022-04 67,052/2021-01 to 2022-03-- 62,860/2021-01 to 2022-03 -- 50,286/2021-01 to 2021-12
-- where b.pcn=@pcn and b.period between @period_start and @period_end and (b.ytd_balance != d.ytd_debit_credit) --246/202207/246/248/202208,245/202207,235/2022-06,225/2021-01 to 2022-05-pulled-06-14--223/2021-01 to 2022-05-pulled-06-04-- 224/2021-01 to 2022-05-- 212/2021-01 to 2022-04-- 200/2021-01 to 2022-01 -- 162/2021-01 to 2021-12

 where b.pcn=@pcn and b.period between @period_start and @period_end and abs(b.ytd_balance - d.ytd_debit_credit) > 0.01 -- 7/202207/7/202208,7/202207,6/2022-06, 5/2021-01 to 2022-05 -- 4/2021-01 to 2022-04-- 3/2021-01 to 2022-03
 -- ISSUE: 1 ACCOUNT IS NOT THE SAME
-- See issue section at the bottom of this procedure and the Mobex Plex procedure: accounting_year_category_type_issue 
-- 73100-000-0000 has different category_types in accounting_v_account it is an Expense and in accounting_v_category_type it is a liability
-- Conclusion: The Plex TB report and Plex authored procedure is wrong to not reset YTD values.


/*
 * 'Revenue' or 'Expense' low accounts have no credit/debit values. 
 */
--where b.pcn=@pcn and b.period between @period_start and @period_end and a.category_type in ('Revenue','Expense') and left(b.account_no,1) < 4  -- 22*15=330/2021-01 to 2022-03, 22*13=286/2021-01 to 2022-01  -- 22*12=264/2021-01 to 2021-12
--and ((b.credit = 0) and (b.debit = 0) and (b.balance =0))  -- 22*15=330/2021-01 to 2022-03, 22*13=286/2021-01 to 2022-01  -- 264/2021-01 to 2021-12
--where b.pcn=@pcn and b.period between @period_start and @period_end and a.category_type in ('Revenue','Expense') and left(b.account_no,1) < 4  -- 22*13=286/2021-01 to 2022-01  -- 22*12/2021-01 to 2021-12
--and ((b.credit != 0) or (b.debit != 0) or (b.balance !=0))  -- 0/2021-01 to 2022-01  --0/2021-01 to 2021-12
-- See issue section at the bottom of this procedure and the Mobex Plex procedure: accounting_year_category_type_issue 
-- 73100-000-0000 has different category_types in accounting_v_account it is an Expense and in accounting_v_category_type it is a liability


-- reset all Plex.account_period_balance for this account
-- UPDATE Plex.account_period_balance !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!11
-- update Plex.account_period_balance set ytd_debit=18912.67,ytd_credit=18912.67 where account_no = '73100-000-0000'

/*
 * Do any new accounts show up on the TB report? Not as of Jan 7 for period 2021-11
 */
select * 
from Archive.accounting_new_accounts_01_07 a 
inner join Plex.trial_balance_multi_level d
on a.pcn=d.pcn 
and a.account_no = d.account_no 

/*
 * Validate the non debit/credit fields
 * Period display as Period
 * category type as 'Category Type'
 * category name as 'Category Name'
 * sub category name as 'Sub Category Name'
 * account no as 'No'
 * account name as 'Name'
 * balance as 'Current Debit/(Credit)'
 * ytd_balance as 'YTD Debit/(Credit)'
 */




declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202101;
declare @period_end int;
--set @period_end = 202101;
set @period_end = 202203;



select count(*)
--select distinct b.pcn,b.account_no,b.category_type,a.active,a.revenue_or_expense,a.category_type_legacy,a.sub_category_name_legacy 
--into Archive.tb_missing_accounts_after_new_accounts_added_01_2022
from 
(
	select b.pcn,b.period,b.period_display,b.account_no,a.account_name, 
	a.category_type,
	a.category_type_legacy, 
	a.category_name_legacy,
	a.sub_category_name_legacy,
	b.balance,
	b.ytd_balance
	-- select count(*)
	--from Archive.account_period_balance_01_03_2022 b -- 43,630 
	from Plex.account_period_balance b -- 4,595 
	--select * from Plex.accounting_account a
	inner join Plex.accounting_account a
	on b.pcn=a.pcn 
	and b.account_no=a.account_no 
--	where category_type = ''  -- 0
--	where category_type_legacy = ''  -- 1,590
--	where category_name_legacy = ''  -- 1,590
--	where sub_category_name_legacy = ''  -- 1,590
)b 
--order by b.pcn,b.period_display,b.account_no

-- select count(*) from Plex.trial_balance_multi_level d where pcn = 123681 and d.period between 202101 and 202110  -- 42,040
-- select * from Plex.trial_balance_multi_level d where pcn = 123681 and d.period between 202101 and 202110  -- 42,040
left outer join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
--inner join Plex.trial_balance_multi_level d -- TB download does not show the plex period for a multi period month, you must link to period_display
on b.pcn=d.pcn
and b.account_no = d.account_no
and b.period_display = d.period_display 
-- select * from Plex.accounting_account a where a.account_no = '73100-000-0000'
left outer join Plex.accounting_account a
on b.pcn = a.pcn 
and b.account_no=a.account_no 
--where b.account_no = '73100-000-0000'
-- select * from Plex.missing_accounts_2021_09  -- 158
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.category_type_legacy = ''  --5,865/2021-01 to 2022-03 -- 4,692/2021-01 to 2021-12
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.category_name_legacy = ''  --5,865/2021-01 to 2022-03 -- 4,692/2021-01 to 2021-12
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.category_type = ''  -- 0/2021-01 to 2022-03 -- 0/2021-01 to 2021-12

--where b.pcn=@pcn and b.period between @period_start and @period_end and b.period_display = d.period_display  -- 63,060/2021-01 to 2022-03 -- 50,448/2021-01 to 2021-12
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.period_display != d.period_display  --0/2021-01 to 2022-03 --  0/2021-01 to 2021-12
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.category_type != d.category_type  -- 60/2021-01 to 2022-03 -- 48/2021-01 to 2021-12  -- TB report uses the category type linked to the sub_category
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.category_type_legacy = d.category_type  -- 63,060/2021-01 to 2022-02 -- 50,448/2021-01 to 2021-12
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.category_type_legacy != d.category_type  -- 0/2021-01 to 2022-01 -- 0/2021-01 to 2021-12

--where b.pcn=@pcn and b.period between @period_start and @period_end and b.category_name_legacy = d.category_name  -- 63,060/2021-01 to 2022-03 -- 50,448/2021-01 to 2021-12
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.sub_category_name_legacy != d.sub_category_name  -- 0/2021-01 to 2022-03 -- 0/2021-01 to 2021-12
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.sub_category_name_legacy = d.sub_category_name  -- 63,060/2021-01 to 2022-01 -- 50,448/2021-01 to 2021-12
--where b.pcn=@pcn and b.period between @period_start and @period_end and b.account_name != d.account_name  -- 0/2021-01 to 2022-03 -- 0/2021-01 to 2021-12
where b.pcn=@pcn and b.period between @period_start and @period_end and b.account_name = d.account_name -- 63,060/2021-01 to 2022-03 -- 50,448/2021-01 to 2021-12

/*
 * What category_type is being used on the new chart of accounts multiple level?
 */
select * 
select count(*)
from Plex.accounting_account a 
where a.pcn = 123681 
and a.category_type != a.category_type_legacy -- 396  -- 73100-000-0000,40591-300-00000 (5 digit old account)
-- 73100-000-0000 category_type = Expense (ytd resets yearly), category_type_legacy=Liability
/*
 * How is the TB Itreport treating 73100-000-0000
 * In 2019 there were debits far exceeded credit values
 * In 2020 debit/credit values where equal.
 * TB is treating it as an Expense since it's YTD values match our procedures values.
 * So TB is using the accounting_v_account category_type YTD reset purposes.
 * But it seems to be using the category linked to the accounting_v_category_account view
 * for the category_type in the CSV file download.
 * Chart of Accounts plex screen lists this account as an Expense so it must
 * also be using the accounting_v_account category_type column.
 * The classic Chart of Accounts plex screen no longer works so I can't test 
 * its category type for that account.
 * So I decided to use the accounting_v_account category_type column for both the
 * YTD reset condition and the CSV category name since I thought that would be 
 * less confusing even though 40 account_period_balance records will have different category types 
 * shown on our report compared to the actual Plex TB CSV download.
 */

select * 
from Plex.accounting_balance b
--from Plex.account_period_balance_high b
where b.account_no = '73100-000-0000'
order by b.period 


select * from Plex.accounting_account a 
where a.pcn = 123681 
and left(a.account_no,1) < '4' 
and a.category_type in ('Revenue','Expense')  -- 22

select * from Plex.accounting_account a 
where a.pcn = 123681 
and left(a.account_no,1) > 3 
and a.category_type not in ('Revenue','Expense')  -- 0


/*
 * Format to be like CSV download
 */
--select * from Plex.accounting_account a where a.account_no = '10220-000-00000' 
declare @pcn int;
set @pcn= 123681;
declare @period_start int;
set @period_start = 202205;
declare @period_end int;
--set @period_end = 202101;
set @period_end = 202205;

select 
--b.period,
b.period_display,
a.category_type,
-- b.category_type_legacy category_type,  -- use legacy category type for the report.
/*
 * The Plex TB report uses the category type of the category linked to the account via the  category_account view. 
 * I believe Plex now mostly uses the account category located directly on the accounting_v_account view so I used 
 * this column instead of the one linked via the account_category view. 
 */
-- select category_name from Plex.accounting_account aa -- no category_name 
a.category_name_legacy category_name,
a.sub_category_name_legacy sub_category_name,
a.account_no,
a.account_name,
b.balance current_debit_credit,
b.ytd_balance ytd_debit_credit
--select count(*)
from Plex.account_period_balance b -- 43,620
inner join Plex.accounting_account a -- 43,620
on b.pcn=a.pcn 
and b.account_no=a.account_no 
--order by b.period_display,a.account_no 
--where a.category_type != a.category_type_legacy 
--where b.period_display is not NULL -- 40,940
--where b.period_display is NULL -- 40,940
where b.pcn = @pcn 
AND b.period BETWEEN @period_start AND @period_end 
order by b.period,b.account_no 
a.account_no = '10220-000-00000' 

/*
 * Backup
 */
select * 
-- select count(*) from Archive.account_period_balance_12_30  -- 43,630
--into Archive.account_period_balance_12_30
from Plex.account_period_balance b -- 43,630


/* ISSUE SECTION
 * 
Question: Why is Plex and Mobex Authored procedures differ in YTD. Credit/Debit/Balance values in 2022-01 for 73100-000-0000 only? 
Note: 73100-000-0000 changed to a 'Revenue' or 'Expense' but the Plex Authored procedure is still not resetting this value in 2022-01.  
It was a liability account before it changed. 
Account Details: Plexus_customer_no=123681/Southfield and account_no = '73100-000-0000' 
Name: Freight - In Machining-General-General 
Created: 1/16/2019 12:23:28 PM 
Update: 2/18/2020 11:53:42 AM 
Testing Details: Mobex authored procedure: accounting_year_category_type_issue 
Research: Shows that account 73100-000-0000 has different category_types. 
In accounting_v_account it is an Expense and in accounting_v_category_type it is a liability.  
There are 4 accounts with this same issue but none of the others had any activity.  
Conclusion: Since this account is an Expense account and not a liability account, we should go with the YTD calculation of the Mobex authored procedure 

 
select 
a.plexus_customer_no pcn,a.account_key,a.account_no,a.account_name,a.active,
a.category_type category_type,  --  This is new way of identifying the category type.  The old method used the following views category_account->category->category_type

case 
when c.plexus_customer_no is null then 0
else c.category_no
end category_no_legacy, -- legacy method of categorizing accounts
case
when c.plexus_customer_no is null then ''
else c.category_name
end category_name_legacy, -- legacy method of categorizing accounts
case
when t.category_type is null then ''
else t.category_type 
end category_type_legacy, -- legacy method of categorizing accounts
case
when sc.sub_category_no is null then 0
else sc.sub_category_no
end sub_category_no_legacy, -- legacy method of categorizing accounts
case
when sc.sub_category_name is null then ''
else sc.sub_category_name
end sub_category_name_legacy, -- legacy method of categorizing accounts

case
when t2.category_type is null then ''
else t2.category_type 
end sub_category_type_legacy, -- legacy method of categorizing accounts
-- select distinct [in] from accounting_v_category_type -- Credit/Debit
-- select count(*) from accounting_v_category_type where [in] = 'Credit' -- 3
-- select count(*) from accounting_v_category_type where [in] = 'Debit' -- 2
case
when a.category_type in ('Revenue','Expense') then 1
else 0
end revenue_or_expense

--ca.*,
--ca.category_name,
--b.*,
--sa.*,
--cc.*,
--t.*,
--a.*
-- select count(*)
-- select *
from accounting_v_account_e  a -- 36,636
--where a.plexus_customer_no=123681  -- 4362 
join accounting_v_category_type act -- This is the value used by the new method of configuring plex accounts. 
on a.category_type=act.category_type  -- 36,636

-- Category numbers linked to an account by the a category_account record will no longer be supported by Plex
left outer join accounting_v_category_account_e ca  --
on a.plexus_customer_no=ca.plexus_customer_no
and a.account_no=ca.account_no
--where a.plexus_customer_no=123681  -- 4204 

left outer join accounting_v_category_e c  --
on ca.plexus_customer_no=c.plexus_customer_no
and ca.category_no=c.category_no

left outer join accounting_v_category_type t -- This is the value used by the old method of configuring plex accounts. 
on c.category_type=t.category_type
--where a.plexus_customer_no=123681  -- 4204
--and t.[in] = 'Debit' -- 3998
--and t.[in] = 'Credit' -- 206

-- sub category numbers linked to an account by the sub category_account record will no longer be supported by Plex
left outer JOIN accounting_v_sub_category_account_e AS sca
--JOIN accounting_v_Sub_Category_Account_e AS SCA -- 4,204 for 123681
ON a.plexus_customer_no = sca.plexus_customer_no
and a.account_no = sca.account_no

left outer join accounting_v_sub_category_e sc  --
on sca.plexus_customer_no=sc.plexus_customer_no
and sca.sub_category_no=sc.sub_category_no

left outer join accounting_v_category_e c2  --
on sc.plexus_customer_no=c2.plexus_customer_no
and sc.category_no=c2.category_no

left outer join accounting_v_category_type t2 -- This is another value used by the old method of configuring plex accounts. 
on c2.category_type=t2.category_type


where a.plexus_customer_no = 123681
and a.account_no = '73100-000-0000'

select b.pcn,b.period,b.account_no,
d.category_type DL_category_type,p.category_type PP_category_type,a.category_type MP_category_type,
p.current_credit,b.credit, 
p.ytd_credit PP_ytd_credit,b.ytd_credit,
b.* 
from Plex.account_period_balance b 
join Plex.accounting_account a 
on b.pcn=a.pcn
and b.account_no=a.account_no
join Plex.Account_Balances_by_Periods p -- 43,620
on b.pcn=p.pcn
and b.account_no = p.[no]
and b.period = p.period 
join Plex.trial_balance_multi_level d 
on b.pcn=d.pcn
and b.account_no = d.account_no
and b.period_display = d.period_display 
where b.period in (202112,202201) and b.account_no = '73100-000-0000'
*/


/*
 * Archive
For 2022-01 PP current_credit is 0 but PP_YTD_credit is 18,912.67 
In this procedure the account is shown to be a revenue_or_expense type.
And in our procedure we would reset the YTD_credit to zero at the beginning of the year.
What is PP_YTD_credit and account_period_balance ytd_credit in 2021-12? 18,912.67
select d.*,p.current_credit,b.credit, 
p.ytd_credit PP_ytd_credit,b.ytd_credit,
b.* 
from Plex.account_period_balance b 
join Plex.Account_Balances_by_Periods p -- 43,620
on b.pcn=p.pcn
and b.account_no = p.[no]
and b.period = p.period 
join Plex.trial_balance_multi_level d 
on b.pcn=d.pcn
and b.account_no = d.account_no
and b.period_display = d.period_display 
where b.period = 202112 and b.account_no = '73100-000-0000'

The Plex Authored procedure treated this account as a non revenue_or_expense and did NOT reset the YTD_credit on 2022-01.
Why?
How does our Mobex authored procedure determine if an account is a revenue_or_expense? 
Looks at the account category_type value from the end of the previous year.

--From the Plex.accounting_account_year_category_type account record for the previous year. 

How does the Plex.accounting_account_year_category_type account determine if the account is a revenue_or_expense?
From the our Plex procedure accounting_year_category_type_dw_import?
How? a.category_type in ('Revenue','Expense') then 1

Did the category_type change from previous years? Yes. 73100-000-0000 changed to a 'Revenue' or 'Expense'
Created: 1/16/2019 12:23:28 PM
Update: 2/18/2020 11:53:42 AM - I believe this was when the category_type was changed to an Expense.

Plexus_customer_no=123681 and account_no = '73100-000-0000'
Created: 1/16/2019 12:23:28 PM
Update: 2/18/2020 11:53:42 AM

Are there any diffences between 73100-000-0000 and another expense category_type account that would cause 
Plex authored stored procedure to not treat it as other revenue_or_expense accounts and reset it's 
YTD values at the beginning of each year? Compare to an account which has a current credit 
value in 2022-01: 47100-000-0000	Chip and Scrap Sales
Differences: Price_Component	is True for 47100-000-0000 and false for 73100-000-0000
Conclusion: The price_component differenced does not appear to be significant.  
where plexus_customer_no = 123681
--and Price_Component = 0 and left(account_no,1) > '3'  -- 991
--and Price_Component = 0 and left(account_no,1) < '4'  -- 358

--and Price_Component = 1 and category_type in ('Revenue','Expense') -- 2942
--and Price_Component = 0 and category_type in ('Revenue','Expense') -- 1013
--and Price_Component = 0 and category_type in ('Revenue','Expense') and left(account_no,1) > '3'  -- 991
--and Price_Component = 1 and category_type in ('Revenue','Expense') and left(account_no,1) > '3'  -- 2,942
--and Price_Component = 0 and category_type in ('Revenue','Expense') and left(account_no,1) < '4'  -- 22
--and Price_Component = 1 and category_type in ('Revenue','Expense') and left(account_no,1) < '4'  -- 0

select * from Plex.accounting_account_year_category_type
where pcn=123681 and account_no = '73100-000-0000'/73100-000-0000
*/