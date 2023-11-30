
-- Plex.accounting_period_ranges definition

-- Drop table

-- DROP TABLE Plex.accounting_period_ranges;

CREATE TABLE Plex.accounting_period_ranges (
	id int NOT NULL AUTO_INCREMENT,
	pcn int NULL,
	start_period int NULL,
	end_period int NULL,
	start_open_period int NULL,
	end_open_period int NULL,
	no_update bit null,
	CONSTRAINT PK_accounting_period_range PRIMARY KEY (id)
);
show indexes from Plex.accounting_period_ranges;
SHOW CREATE TABLE Plex.accounting_period_ranges;

select * from Plex.accounting_period_ranges 

-- show indexes from Plex.accounting_account 
-- CREATE INDEX idx_accounting_account_pcn_account_no ON Plex.accounting_account(pcn,account_no);
-- CREATE INDEX idx_accounting_account_pcn_account_no ON Plex.accounting_account(pcn,account_no,peri);

select * from Plex.accounting_balance_update_period_range
-- delete from Plex.accounting_balance_update_period_range

INSERT INTO Plex.accounting_balance_update_period_range (pcn,period_start,period_end)
VALUES
(123681,202106,202205)
,(300758,202106,202205)

-- update Plex.accounting_balance_update_period_range
set period_start = 202103,
period_end = 202202