drop database test;
create database mcpdw;
use mcpdw;
create schema Plex;

create database test;
use test;
create table mytable 
( 
	c char(20)
)
-- CREATE TABLE t (c CHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin);

INSERT INTO tbl_name (a,b,c)
    VALUES(1,2,3), (4,5,6), (7,8,9);
   
insert into mytable 
values 
('a')
,('b')
,('c')
select * from mytable


-- DROP TABLE test.accounting_account;

CREATE TABLE Plex.accounting_account (
	pcn int NOT NULL,
	account_key int NOT NULL,
	account_no varchar(20) NULL,
	account_name varchar(110) NULL,
	active bit NULL,
	category_type varchar(10) NULL,
	category_no_legacy int NULL,
	category_name_legacy varchar(50) NULL,
	category_type_legacy varchar(10) NULL,
	sub_category_no_legacy int NULL,
	sub_category_name_legacy varchar(50) NULL,
	sub_category_type_legacy varchar(10) NULL,
	revenue_or_expense bit NULL,
	start_period int NULL,
	PRIMARY KEY (pcn,account_key)
);
-- https://www.mysqltutorial.org/mysql-index/mysql-create-index/
show indexes from Plex.accounting_account 
CREATE INDEX idx_accounting_account_pcn_account_no ON Plex.accounting_account(pcn,account_no);
CREATE INDEX idx_accounting_account_pcn_account_no ON Plex.accounting_account(pcn,account_no,peri);
select * 
-- select count(*)
from Plex.accounting_account
--truncate table  Plex.accounting_account 
--delete from Plex.accounting_account where pcn in (123681)
INSERT INTO test.accounting_account 
VALUES
(123681, 629753, '10000-000-00000', 'Cash - Comerica General', 0, 'Asset', 0, '', '', 0, '', '', 0, 201604)
(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
