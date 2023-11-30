
-- mcpdw.Plex.accounting_balance definition

-- Drop table

-- DROP TABLE Plex.accounting_balance;

CREATE TABLE Plex.accounting_balance (
	pcn int NOT NULL,
	account_key int NOT NULL,
	account_no varchar(20) NULL,
	period int NOT NULL,
	debit decimal(19,5) NULL,
	credit decimal(19,5) NULL,
	balance decimal(19,5) NULL,
	CONSTRAINT PK__accounting_balance PRIMARY KEY (pcn,account_key,period)
);
show indexes from Plex.accounting_balance 
CREATE INDEX idx_accounting_balance_pcn_account_no_period ON Plex.accounting_balance(pcn,account_no,period);


select count(*)
from Plex.accounting_balance
--
select distinct pcn,period from Plex.accounting_balance ab order by pcn,period
select count(*) from Plex.accounting_balance ab -- 52,749

select count(*) from Plex.accounting_balance_ ab -- 52,749
select * 
--into Archive.accounting_balance_2022_01_24
from Plex.accounting_balance ab 
select count(*) from Archive.accounting_balance_2022_01_24 -- 52,749
select distinct pcn,period from Archive.accounting_balance_2022_01_24 order by pcn,period
