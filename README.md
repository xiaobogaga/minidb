# minidb

A in memory "database" can support basic sql processing.

# usage

```shell
go get -u github.com/xiaobogaga/minidb
go install github.com/xiaobogaga/minidb/minidb
minidb -d
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

# sql

For the supported sql statement, can refer [here](https://github.com/xiaobogaga/minidb/blob/master/parser/sql.md).

# demo

<img src="https://github.com/xiaobogaga/minidb/blob/master/demo/minidb-demo.gif" width="80%" height="80%" />
