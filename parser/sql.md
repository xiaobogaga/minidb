## The supported statements

MiniDB supports a subset of mysql statements and here we will talk about it. The more 
feature will be added in future to totally compatible with mysql.

### DDL

#### create:

Create Statement can be create table statement or create database statement.
* For create table statement, it supports:
    * `create table [if not exist] tb_name like orig_tab_name;`
    * `create table [if not exist] tb_name2 (
    Column_Def..., Index_Def..., Constraint_Def...
    ) [engine=value] [[Default | character set = value] | [Default | collate = value]];`
    * `create table [if not exist] as selectStatement;`

* For create database statement, if supports:
    * `create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];`

Diff with mysql, create table statement:
    * Doesn't support temporary table.
    * Doesn't support ignore or replace.
    * Doesn't support spatial or fulltext index.
    * Doesn't support to check
    * Doesn't support column definition.
    * For column format:
        * doesn't support comment.
        * doesn't support column format, collate, storage.
        * doesn't support reference.

#### drop

Drop statement can be drop table statement or drop database statement.
* Drop database statement is like:
    * `drop {database | schema} [if exists] db_name;`
* Drop table statement is like:
    * `drop table [if exists] tb_name[,tb_name...] [RESTRICT|CASCADE];`

#### alter

Alter statement can be alter table statement or alter database statement.
* Alter table statement is like:
```
alter [table] tb_name [
    add    [column] col_name col_def |
    drop   [column] col_name |
    modify [column] col_name col_def |
    change [column] old_col_name col_def |
    add {index|key} indexDef |
    add [constraint] primaryKeyDef |
    add [constraint] uniqueKeyDef |
    add [constraint] foreignKeyDef |
    drop {index|key} index_name |
    drop primary key |
    drop foreign key key_name |
    engine=value |
    [[default] | character set = value] |
    [[default] | collate = value]
]
```
* Alter database statement can be: `alter {database | schema} db_name [[Default | character set = value] | [Default | collate = value]]`

Diff with mysql: Too many, doesn't show here.

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