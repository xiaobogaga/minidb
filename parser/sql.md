## The supported statements

MiniDB supports a subset of mysql statements and here we will talk about it.

### DDL

#### create:

Create Statement can be create table statement or create database statement.
* For create table statement, it supports:
    * `create table [if not exist] tb_name2 (
    Column_Def...
    ) [engine=value] [[Default | character set = value] | [Default | collate = value]];`e 

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
* `delete from tb_name [whereStm] [OrderByStm] [LimitStm]`;
* `delete tb1,... from table_references [WhereStm];`

#### update
Update statement is like:
* `update table_reference set assignments... [WhereStm] [OrderByStm] [LimitStm];`
* `update table_reference... set assignments... [WhereStm];`

#### select
Select statement is like:
* `select [all | distinct | distinctrow] select_expression... from table_reference... [WhereStm] [GroupByStm] [HavingStm]
[OrderByStm] [LimitStm] [for update | lock in share mode]`

#### others

* index_def: `{index|key} [index_name] (col_name, ...)`

* constraint_def: 
    * `[Constraint] primary key (col_name [,col_name...)`
    * `[Constraint] unique {index|key} [index_name] (col_name [,col_name...)`
    * `[Constraint] foreign key [index_name] (col_name [,col_name...) references tb_name (key...) [on delete reference_option] [on update reference_option]`
where reference_option is like: `{restrict | cascade | set null | no action | set default}`

* column_def: `col_name col_type [not null|null] [default default_value] [AUTO_INCREMENT] [unique [key]] [[primary] key]`

#### table name or database name rules

Only letters, _, or number are permitted and the first character must be a letter