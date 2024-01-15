CREATE PROCEDURE PLEX.ACCOUNT_PERIOD_BALANCE_RECREATE_PERIOD_RANGE (
    @PCN INT
) AS
BEGIN
    SET NOCOUNT ON;
 --debug variable
 --declare @pcn int;
 --set @pcn = 123681;
    DECLARE
        @NO_UPDATE INT;
        DECLARE   @START_PERIOD INT;
        DECLARE   @END_PERIOD INT;
 -- Bug fix need end_open_period in case account gets added in January and Dec of previous year is closed but January is open.
        DECLARE   @END_OPEN_PERIOD INT;
        DECLARE   @PERIOD INT;
        DECLARE   @MAX_FISCAL_PERIOD INT;
        DECLARE   @PREV_PERIOD INT;
        DECLARE   @FIRST_PERIOD INT;
        DECLARE   @ANCHOR_PERIOD INT;
        DECLARE   @ANCHOR_PERIOD_DISPLAY VARCHAR(7);
        DECLARE   @CNT INT
        SELECT
            @START_PERIOD=R.START_PERIOD,
            @PERIOD=R.START_PERIOD,
            @END_PERIOD=R.END_PERIOD,
            @END_OPEN_PERIOD=R.END_OPEN_PERIOD,
            @NO_UPDATE=R.NO_UPDATE,
            @MAX_FISCAL_PERIOD=M.MAX_FISCAL_PERIOD
 --select * from Plex.accounting_balance_update_period_range r
        FROM
            PLEX.ACCOUNTING_PERIOD_RANGES R
            INNER JOIN PLEX.MAX_FISCAL_PERIOD_VIEW M
            ON R.PCN=M.PCN
            AND (R.START_PERIOD/100) = M.[YEAR]
        WHERE
            R.PCN = @PCN;
 --Debug
        IF        (@NO_UPDATE=1) BEGIN
 --	select 'returning early', @no_update no_update;
        RETURN 0;
    END IF ((@START_PERIOD%100)!=1) BEGIN SET @PREV_PERIOD = @START_PERIOD - 1;
END ELSE BEGIN SET @PREV_PERIOD = (((@START_PERIOD/100)-1)*100)+12;
END;
SET       @ANCHOR_PERIOD = @PREV_PERIOD;
SELECT    @ANCHOR_PERIOD_DISPLAY=P.PERIOD_DISPLAY FROM PLEX.ACCOUNTING_PERIOD P WHERE P.PCN = @PCN AND P.PERIOD = @ANCHOR_PERIOD AND P.ORDINAL = 1;
IF        @PERIOD%100 = 1 BEGIN SET @FIRST_PERIOD=1;
END ELSE BEGIN SET @FIRST_PERIOD=0;
END;
 --select @no_update no_update,@pcn pcn,@anchor_period anchor_period,@anchor_period_display anchor_period_display,
 --@period period,
 --@prev_period prev_period,@start_period start_period,
 --@first_period first_period,@end_period end_period,@end_open_period end_open_period,@period period,@max_fiscal_period max_fiscal_period;
 --no_update	pcn		anchor_period	anchor_period_display	period	prev_period	start_period	first_period	end_period	period	max_fiscal_period
 --0			123681	202212			12-2022					202301	202212		202301			1				202311		202301	202312
 --select *
 ---- delete
 --from Plex.accounting_account_year_category_type
 --where account_no = '11055-000-9806' and [year] in (2022,2023)

 /*
 * Add new account records to Plex.accounting_account_year_category_type 
 * for the @anchor_period's year if not already added.
-- Bug fix 
-- 11055-000-9806 was not in Plex.accounting_account_year_category_type for 2023 but is for 2022, 2024 and 2025
-- To make sure an Plex.accounting_account_year_category_type records gets added for both 2022 and 2023
-- I added add_account_year_category_type_prev_period and add_account_year_category_type_end_period
-- to insert both records. Previously only the @prev_period record, 2022, was, added, but
-- now an @end_period year record is added.
-- Also we get Plex.accounting_account_year_category_type account data from @end_open_period instead of @end_period.
 */
 WITH      ACCOUNT_YEAR_CATEGORY_TYPE_PREV_PERIOD AS (
    SELECT
        A.*
 -- select count(*)
    FROM
        PLEX.ACCOUNTING_ACCOUNT A
 --where a.pcn=123681 -- 4,617
        INNER JOIN PLEX.ACCOUNTING_ACCOUNT_YEAR_CATEGORY_TYPE Y
        ON A.PCN = Y.PCN
        AND A.ACCOUNT_NO =Y.ACCOUNT_NO
    WHERE
        Y.[YEAR] = (@PREV_PERIOD/100)
        AND A.PCN = @PCN
)
 --select count(*) from account_year_category_type  -- 4,595
, ADD_ACCOUNT_YEAR_CATEGORY_TYPE_PREV_PERIOD AS (
    SELECT
        A.*
    FROM
        PLEX.ACCOUNTING_ACCOUNT A
        LEFT OUTER JOIN ACCOUNT_YEAR_CATEGORY_TYPE_PREV_PERIOD Y
        ON A.PCN = Y.PCN
        AND A.ACCOUNT_NO =Y.ACCOUNT_NO
    WHERE
        Y.PCN IS NULL -- there is no account_year_category_type records for the @prev_period year so we must add them.
        AND A.PCN = @PCN
) INSERT INTO PLEX.ACCOUNTING_ACCOUNT_YEAR_CATEGORY_TYPE (
    PCN,
    ACCOUNT_NO,
    YEAR,
    CATEGORY_TYPE,
    REVENUE_OR_EXPENSE
)
    SELECT
        Y.PCN,
        Y.ACCOUNT_NO,
        (@PREV_PERIOD/100) YEAR,
        Y.CATEGORY_TYPE,
        Y.REVENUE_OR_EXPENSE
    FROM
        PLEX.ACCOUNTING_ACCOUNT_YEAR_CATEGORY_TYPE Y
    WHERE
        Y.[YEAR] = (@END_OPEN_PERIOD/100) -- there is no account_year_category_type records for the @prev_period year so we must add them.
        AND Y.PCN = @PCN
        AND Y.ACCOUNT_NO IN (
            SELECT
                ACCOUNT_NO
            FROM
                ADD_ACCOUNT_YEAR_CATEGORY_TYPE_PREV_PERIOD
        );
WITH      ACCOUNT_YEAR_CATEGORY_TYPE_END_PERIOD AS (
    SELECT
        A.*
 -- select count(*)
    FROM
        PLEX.ACCOUNTING_ACCOUNT A
 --where a.pcn=123681 -- 4,617
        INNER JOIN PLEX.ACCOUNTING_ACCOUNT_YEAR_CATEGORY_TYPE Y
        ON A.PCN = Y.PCN
        AND A.ACCOUNT_NO =Y.ACCOUNT_NO
    WHERE
        Y.[YEAR] = (@END_PERIOD/100)
        AND A.PCN = @PCN
), ADD_ACCOUNT_YEAR_CATEGORY_TYPE_END_PERIOD AS (
    SELECT
        A.*
    FROM
        PLEX.ACCOUNTING_ACCOUNT A
        LEFT OUTER JOIN ACCOUNT_YEAR_CATEGORY_TYPE_END_PERIOD Y
        ON A.PCN = Y.PCN
        AND A.ACCOUNT_NO =Y.ACCOUNT_NO
    WHERE
        Y.PCN IS NULL -- there is no account_year_category_type records for the @prev_period year so we must add them.
        AND A.PCN = @PCN
)
 -- select *
 -- from Plex.accounting_account_year_category_type y
 -- where pcn=123681 and account_no = '11055-000-9806'
 -- 11055-000-9806
INSERT INTO PLEX.ACCOUNTING_ACCOUNT_YEAR_CATEGORY_TYPE (
    PCN,
    ACCOUNT_NO,
    YEAR,
    CATEGORY_TYPE,
    REVENUE_OR_EXPENSE
)
    SELECT
        Y.PCN,
        Y.ACCOUNT_NO,
        (@END_PERIOD/100) YEAR,
        Y.CATEGORY_TYPE,
        Y.REVENUE_OR_EXPENSE
    FROM
        PLEX.ACCOUNTING_ACCOUNT_YEAR_CATEGORY_TYPE Y
    WHERE
        Y.[YEAR] = (@END_OPEN_PERIOD/100) -- there is no account_year_category_type records for the @end_period year so we must add them.
        AND Y.PCN = @PCN
        AND Y.ACCOUNT_NO IN (
            SELECT
                ACCOUNT_NO
            FROM
                ADD_ACCOUNT_YEAR_CATEGORY_TYPE_END_PERIOD
        )
 /*
 * Update the anchor period. Add records for new accounts.
 */ INSERT INTO PLEX.ACCOUNT_PERIOD_BALANCE
        SELECT
            @PCN PCN,
            A.ACCOUNT_NO,
            @ANCHOR_PERIOD PERIOD,
            @ANCHOR_PERIOD_DISPLAY PERIOD_DISPLAY,
            0 DEBIT,
            0 YTD_DEBIT,
            0 CREDIT,
            0 YTD_CREDIT,
            0 BALANCE,
            0 YTD_BALANCE
 -- select count(*) from Plex.accounting_account where pcn = 123681  -- 4,617,4,363/4,595
 -- select distinct pcn,period from Plex.account_period_balance b order by pcn,period
 -- select count(*) from Plex.account_period_balance b where pcn = 123681 and period = 202103  -- 4,595
        FROM
            PLEX.ACCOUNTING_ACCOUNT A
            LEFT OUTER JOIN PLEX.ACCOUNT_PERIOD_BALANCE B
            ON A.PCN=B.PCN
            AND A.ACCOUNT_NO=B.ACCOUNT_NO
            AND B.PERIOD = @ANCHOR_PERIOD
        WHERE
            A.PCN = @PCN
            AND B.PCN IS NULL;
 --select count(*) account_period_balance_cnt from Plex.account_period_balance  where period = @anchor_period and pcn = @pcn -- 4,617
 --no_update	pcn		anchor_period	anchor_period_display	period	prev_period	start_period	first_period	end_period	period	max_fiscal_period
 --0			123681	202212			12-2022					202301	202212		202301			1				202311		202301	202312
WHILE     @PERIOD <= @END_PERIOD BEGIN WITH PERIOD_BALANCE(PCN, ACCOUNT_NO, PERIOD, DEBIT, CREDIT, BALANCE) AS (
    SELECT
        A.PCN,
        A.ACCOUNT_NO,
        @PERIOD PERIOD,
        CASE
            WHEN B.DEBIT IS NULL THEN
                0
            ELSE
                B.DEBIT
        END DEBIT,
        CASE
            WHEN B.CREDIT IS NULL THEN
                0
            ELSE
                B.CREDIT
        END CREDIT,
        CASE
            WHEN B.BALANCE IS NULL THEN
                0
            ELSE
                B.BALANCE
        END BALANCE
 -- select count(*) from Plex.accounting_account where pcn = 123681  -- 4,595/4,363
    FROM
        PLEX.ACCOUNTING_ACCOUNT A
        LEFT OUTER JOIN PLEX.ACCOUNTING_BALANCE B
        ON A.PCN=B.PCN
        AND A.ACCOUNT_NO=B.ACCOUNT_NO
        AND B.PERIOD = @PERIOD
    WHERE
        A.PCN = @PCN
),
 --select @cnt=count(*) from period_balance;
 --print '@cnt=' + cast(@cnt as varchar(4));
ACCOUNT_PERIOD_BALANCE(PCN, ACCOUNT_NO, PERIOD, PERIOD_DISPLAY, DEBIT, YTD_DEBIT, CREDIT, YTD_CREDIT, BALANCE, YTD_BALANCE)
 --,ending_period,ending_ytd_debit,ending_ytd_credit,ending_ytd_balance,next_period)
AS (
 --select * from Plex.accounting_period ap where pcn = 300758
    SELECT
        B.PCN,
        B.ACCOUNT_NO,
        B.PERIOD,
        AP.PERIOD_DISPLAY,
        B.DEBIT,
        CAST(
            CASE
                WHEN (@FIRST_PERIOD=0) THEN
                    P.YTD_DEBIT + B.DEBIT
                WHEN (@FIRST_PERIOD=1) AND (A.REVENUE_OR_EXPENSE = 1) THEN
                    B.DEBIT
                WHEN (@FIRST_PERIOD=1) AND (A.REVENUE_OR_EXPENSE = 0) THEN
                    P.YTD_DEBIT + B.DEBIT
            END AS DECIMAL(19,
        5) ) YTD_DEBIT,
        B.CREDIT,
        CAST(
            CASE
                WHEN (@FIRST_PERIOD=0) THEN
                    P.YTD_CREDIT + B.CREDIT
                WHEN (@FIRST_PERIOD=1) AND (A.REVENUE_OR_EXPENSE = 1) THEN
                    B.CREDIT
                WHEN (@FIRST_PERIOD=1) AND (A.REVENUE_OR_EXPENSE = 0) THEN
                    P.YTD_CREDIT + B.CREDIT
            END AS DECIMAL(19,
        5) ) YTD_CREDIT,
        B.BALANCE,
        CAST(
            CASE
                WHEN (@FIRST_PERIOD=0) THEN
                    P.YTD_BALANCE + B.BALANCE
                WHEN (@FIRST_PERIOD=1) AND (A.REVENUE_OR_EXPENSE = 1) THEN
                    B.BALANCE
                WHEN (@FIRST_PERIOD=1) AND (A.REVENUE_OR_EXPENSE = 0) THEN
                    P.YTD_BALANCE + B.BALANCE
            END AS DECIMAL(19,
        5) ) YTD_BALANCE
    FROM
        PERIOD_BALANCE B -- will contain all the accounts labled with just one period
        INNER JOIN PLEX.ACCOUNT_PERIOD_BALANCE P
        ON B.PCN = P.PCN
        AND B.ACCOUNT_NO = P.ACCOUNT_NO
        AND B.PERIOD=@PERIOD
        AND P.PERIOD=@PREV_PERIOD
        INNER JOIN PLEX.ACCOUNTING_PERIOD AP
        ON B.PCN=AP.PCN
        AND B.PERIOD=AP.PERIOD
        AND AP.ORDINAL = 1
 --no_update	pcn		anchor_period	anchor_period_display	period	prev_period	start_period	first_period	end_period	period	max_fiscal_period
 --0			123681	202212			12-2022					202301	202212		202301			1				202311		202301	202312
        INNER JOIN PLEX.ACCOUNTING_ACCOUNT_YEAR_CATEGORY_TYPE A
        ON P.PCN = A.PCN
        AND P.ACCOUNT_NO =A.ACCOUNT_NO
        AND (P.PERIOD/100)=A.[YEAR]
)
 --	select @period, count(*)  from account_period_balance;  -- 4,363
INSERT INTO PLEX.ACCOUNT_PERIOD_BALANCE
    SELECT
        PCN,
        ACCOUNT_NO,
        PERIOD,
        PERIOD_DISPLAY,
        DEBIT,
        YTD_DEBIT,
        CREDIT,
        YTD_CREDIT,
        BALANCE,
        YTD_BALANCE
    FROM
        ACCOUNT_PERIOD_BALANCE; -- 4,363
 --no_update	pcn		anchor_period	anchor_period_display	period	prev_period	start_period	first_period	end_period	period	max_fiscal_period
 --0			123681	202212			12-2022					202301	202212		202301			1				202311		202301	202312
SET       @PREV_PERIOD = @PERIOD IF @PERIOD < @MAX_FISCAL_PERIOD
BEGIN
    SET @PERIOD=@PERIOD+1 END ELSE BEGIN SET @PERIOD=((@PERIOD/100 + 1)*100) + 1 END SELECT @MAX_FISCAL_PERIOD=M.MAX_FISCAL_PERIOD FROM PLEX.MAX_FISCAL_PERIOD_VIEW M WHERE M.PCN = @PCN AND M.YEAR = @PERIOD/100 IF @PERIOD%100 = 1 BEGIN SET @FIRST_PERIOD=1;
END ELSE BEGIN SET @FIRST_PERIOD=0;
END
 --	select @period period,@period_end period_end,@prev_period previous_period,@max_fiscal_period max_fiscal_period,@first_period first_period;
END
END;