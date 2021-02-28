## The supported statements

minidb supports a set of sql statements:

### DDL

#### create:

Create Statement can be create table statement or create database statement.
* For create table statement, it supports:
    * `create table [if not exist] tb_name2 (
    Column_Def...
    ) [engine=value] [[Default | character set = value] | [Default | collate = value]];`

* For create database statement, if supports:
    * `create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];`

#### drop

Drop statement can be drop table statement or drop database statement.
* Drop database statement is like:
    * `drop {database | schema} [if exists] db_name;`
* Drop table statement is like:
    * `drop table [if exists] tb_name[,tb_name...];`

#### rename

Rename statement is like: 
* `rename table {tb1 To tb2...};`

#### truncate
Truncate table statement is like:
* `truncate [table] tb_name;`

### DML

#### insert
Insert statement is like:
* `insert into tb_name [( col_name... )] values (expression...);`

#### delete
Delete statement is like:
* `delete from tb_name [whereStm] [OrderByStm] [LimitStm];`
* `delete tb1,... from table_references [WhereStm];`

#### update
Update statement is like:
* `update table_reference set assignments... [WhereStm] [OrderByStm] [LimitStm];`
* `update table_reference... set assignments... [WhereStm];`

#### select
Select statement is like:
* `select select_expression... from table_reference... [WhereStm] [GroupByStm] [HavingStm] [OrderByStm] [LimitStm]`

where table_reference can be a single table or a table join another table(like inner join, left join, right join)

#### table name or database name rules

Only letters, _, or number are permitted and the first character must be a letter