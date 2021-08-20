# minidb

A in-memory "database" can support basic sql processing.

Table of Contents
=================

* [minidb](#minidb)
  * [usage](#usage)
  * [The supported statements](#the-supported-statements)
    * [create:](#create)
    * [drop](#drop)
    * [rename](#rename)
    * [truncate](#truncate)
    * [insert](#insert)
    * [delete](#delete)
    * [update](#update)
    * [select](#select)

## usage

* Install

```shell script
go get -u github.com/xiaobogaga/minidb
go install github.com/xiaobogaga/minidb/minidb
```
you can find minidb in **$GOPATH/bin/** 

* Startup minidb

```shell script
minidb -d
```

* minidb queries

```shell script
minidb> show databases;
+-----------+
+ databases +
+-----------+
+       db1 +
+       db2 +
+-----------+
server:  OK
minidb> use db1;
server:  OK
minidb> show tables;
+--------+
+ tables +
+--------+
+  test1 +
+  test2 +
+--------+
server:  OK
minidb> select id, name, age from test1 where age > 0 order by age limit 3;
+----+--------------+------+
+ id +         name +  age +
+----+--------------+------+
+  8 + val!u@pu(g13 + 1.28 +
+  6 +            v + 3.28 +
+  2 +         v<%# + 3.86 +
+----+--------------+------+
server:  OK. no more data
minidb> select test1.id as id1, test2.id as id2, test1.name, test2.name from test1 left join test2 on test1.id = test2.id order by test1.age limit 10;
+-----+-----+---------------------+--------------------+
+ id1 + id2 +                name +               name +
+-----+-----+---------------------+--------------------+
+  11 +  11 +                   v +          vb0ulbcyv +
+  12 +  12 +    vxit0zoh;4<)0%1# +      v(30;^08s8c#k +
+  14 +  14 + v(%!#v35s~l)cc(vv8b +           v{<o)s8} +
+   9 +   9 +   vm4)$5i)4{~60i7d2 +   vq3>u~t7;s0&e)cj +
+   0 +   0 +               vjnr# +        v%4k~i>}nf& +
+   5 +   5 +             v8kgc}2 +    v74zp^0foxsdt)x +
+   4 +   4 +            v@y<nhp% +     vbb>2{{sh(1w2v +
+   8 +   8 +        val!u@pu(g13 +               v<%x +
+   6 +   6 +                   v +             v3!xju +
+   2 +   2 +                v<%# + vm>z*4e8jg6)tj237j +
+-----+-----+---------------------+--------------------+
server:  OK. no more data
minidb> select sex, count(id), sum(age) from test1 group by sex;
+-----+-----------+----------+
+ sex + COUNT(id) + SUM(age) +
+-----+-----------+----------+
+   0 +         8 + -7716.00 +
+   1 +         8 +  -324.00 +
+-----+-----------+----------+
server:  OK. no more data
```

or without `-d` to start with empty databases.

## The supported statements

minidb supports a set of sql statements:

### create:

* `create table [if not exist] tb_name2 (
    Column_Def...
    ) [engine=value] [[Default | character set = value] | [Default | collate = value]];`

* `create {database|schema} [if not exist] database_name [[Default | character set = value] | [Default | collate = value]];`

### drop

* `drop {database | schema} [if exists] db_name;`
* `drop table [if exists] tb_name[,tb_name...];`

### rename

* `rename table {tb1 To tb2...};`

### truncate

* `truncate [table] tb_name;`

### insert

* `insert into tb_name [( col_name... )] values (expression...);`

### delete

* `delete from tb_name [whereStm] [OrderByStm] [LimitStm];`
* `delete tb1,... from table_references [WhereStm];`

### update

* `update table_reference set assignments... [WhereStm] [OrderByStm] [LimitStm];`
* `update table_reference... set assignments... [WhereStm];`

### select

* `select select_expression... from table_reference... [WhereStm] [GroupByStm] [HavingStm] [OrderByStm] [LimitStm]`

where table_reference can be a single table or a table join another table(like inner join, left join, right join)