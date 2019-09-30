This is a **global** deadlock detector for Greenplum 5.x which can detect deadlock across multiple segments.

# Install

```
$go build deadlock.go 

$./deadlock -h
Usage of ./deadlock:
The ./deadlock will exit 0 when there is no deadlock in current db. 
And will print the sql that can be break deadlock to the stdout, then exit 1.
  -c string
    	connection string. (default "sslmode=disable")
  -d string
    	If not empty, will generate lock wait-for graph in the dotfile with Graphviz.
```

# Usage

First, do some verification and initialization work:

```bash
# 3 segments.
gp5=> select count(1) from gp_segment_configuration where role = 'p' and content >= 0;
 count 
-------
     3
(1 row)

gp5=> create table gddtest(i int primary key, j varchar);
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "gddtest_pkey" for table "gddtest"
CREATE TABLE
```

In session one, we execute this follow SQL:

```bash
gp5=> BEGIN;
BEGIN
gp5=> insert into gddtest values(13, 'blog.hidva.com');
INSERT 0 1
gp5=> insert into gddtest values(43, 'blog.hidva.com');
# Blocking here
```

And in session two, we execute this follow SQL:

```bash
gp5=> begin;
BEGIN
gp5=> insert into gddtest values(43, 'blog.hidva.com');
INSERT 0 1
gp5=> insert into gddtest values(73, 'blog.hidva.com');
# Blocking here
```

In session three:

```bash
gp5=# BEGIN;
BEGIN
gp5=# insert into gddtest values(73, 'blog.hidva.com');
INSERT 0 1
gp5=# insert into gddtest values(13, 'blog.hidva.com');
# Blocking here
```

And we get the deadlock! (as for why the deadlock happens is not the content of the introduction here.)

Now, we can use this global deadlock detector tool to find the deadlock:

```bash
$./deadlock -d gdd.dot
Deadlock is found: 
Session 54 waits for ShareLock on seg:2;type:transactionid;xid:696; blocked by Session 56(granted ExclusiveLock);
Session 56 waits for ShareLock on seg:1;type:transactionid;xid:703; blocked by Session 55(granted ExclusiveLock);
Session 55 waits for ShareLock on seg:0;type:transactionid;xid:703; blocked by Session 54(granted ExclusiveLock);
The lock waits-for graph has been write to 'gdd.dot'
You can kill these session to break deadlock. sessions: 56
SELECT pg_cancel_backend(procpid) FROM pg_stat_activity WHERE sess_id IN (56);
```

We can use any of Graphviz tool to render 'gdd.dot' to a png:

![gdd.dot](https://blog.hidva.com/assets/gdd.dot.png)

Finally, this global deadlock detector and psql can be used in combination to break the deadlock.

```bash
$./deadlock -d gdd.dot | psql
Deadlock is found: 
Session 55 waits for ShareLock on seg:0;type:transactionid;xid:703; blocked by Session 54(granted ExclusiveLock);
Session 54 waits for ShareLock on seg:2;type:transactionid;xid:696; blocked by Session 56(granted ExclusiveLock);
Session 56 waits for ShareLock on seg:1;type:transactionid;xid:703; blocked by Session 55(granted ExclusiveLock);
The lock waits-for graph has been write to 'gdd.dot'
You can kill these session to break deadlock. sessions: 56
 pg_cancel_backend 
-------------------
 t
(1 row)
```
