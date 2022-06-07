-- PGSpider Extension does not have pg_spd_node_info table
-- --Testcase 1:
-- DELETE FROM pg_spd_node_info;
--SELECT pg_sleep(15);
--Testcase 183:
CREATE EXTENSION pgspider_ext;
--Testcase 184:
CREATE SERVER pgspider_svr FOREIGN DATA WRAPPER pgspider_ext;
--Testcase 185:
CREATE USER MAPPING FOR public SERVER pgspider_svr;
--Testcase 186:
CREATE TABLE test1 (i int,__spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 187:
CREATE EXTENSION postgres_fdw;
--Testcase 188:
CREATE EXTENSION file_fdw;
--Testcase 189:
CREATE EXTENSION sqlite_fdw;
--Testcase 190:
CREATE EXTENSION tinybrace_fdw;
--Testcase 191:
CREATE EXTENSION mysql_fdw;

-- Enable to pushdown aggregate
SET enable_partitionwise_aggregate TO on;

-- Turn off leader node participation to avoid duplicate data error when executing
-- parallel query
SET parallel_leader_participation TO off;

--Testcase 192:
CREATE SERVER file_svr FOREIGN DATA WRAPPER file_fdw;
--Testcase 193:
CREATE FOREIGN TABLE filetbl__file_svr__0 (i int) SERVER file_svr options(filename '/tmp/pgtest.csv');
--Testcase 194:
CREATE TABLE filetbl (i int,__spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 295:
CREATE FOREIGN TABLE filetbl_child1 PARTITION OF filetbl FOR VALUES IN ('/file_svr/') SERVER pgspider_svr OPTIONS(child_name 'filetbl__file_svr__0');
--Testcase 2:
SELECT * FROM filetbl;

-- PGSpider Extension does not support getting version
-- --get version
-- --Testcase 295:
-- \df pgspider_core*
-- --Testcase 296:
-- SELECT * FROM public.pgspider_core_fdw_version();
-- --Testcase 297:
-- SELECT pgspider_core_fdw_version();

--Testcase 195:
CREATE SERVER filesvr2 FOREIGN DATA WRAPPER file_fdw;
--Testcase 196:
CREATE FOREIGN TABLE test1__file_svr__0 (i int) SERVER file_svr options(filename '/tmp/pgtest.csv');
--Testcase 296:
CREATE FOREIGN TABLE test1_file_child1 PARTITION OF test1 FOR VALUES IN ('/file_svr/') SERVER pgspider_svr OPTIONS(child_name 'test1__file_svr__0');
--Testcase 3:
SELECT * FROM test1;
--Testcase 197:
CREATE FOREIGN TABLE test1__filesvr2__0 (i int) SERVER file_svr options(filename '/tmp/pgtest.csv');
--Testcase 297:
CREATE FOREIGN TABLE test1_file_child2 PARTITION OF test1 FOR VALUES IN ('/file_svr2/') SERVER pgspider_svr OPTIONS(child_name 'test1__filesvr2__0');
--Testcase 4:
SELECT * FROM test1 order by i,__spd_url;
-- PGSpider Extension does not support IN syntax
-- --Testcase 5:
-- SELECT * FROM test1 IN ('/file_svr/') ORDER BY i,__spd_url;
-- --Testcase 6:
-- SELECT * FROM test1 IN ('/file_svr/') where i = 1;

--Testcase 198:
CREATE SERVER tiny_svr FOREIGN DATA WRAPPER tinybrace_fdw OPTIONS (host '127.0.0.1',port '5100', dbname 'test.db');
--Testcase 199:
CREATE USER mapping for public server tiny_svr OPTIONS(username 'user',password 'testuser');
--Testcase 200:
CREATE FOREIGN TABLE test1__tiny_svr__0 (i int) SERVER tiny_svr OPTIONS(table_name 'test1');
--Testcase 298:
CREATE FOREIGN TABLE test1_tiny_child1 PARTITION OF test1 FOR VALUES IN ('/tiny_svr/') SERVER pgspider_svr OPTIONS(child_name 'test1__tiny_svr__0');
--Testcase 7:
SELECT * FROM test1__tiny_svr__0 ORDER BY i;
--Testcase 8:
SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 9:
-- SELECT * FROM test1 IN ('/tiny_svr/');
-- --Testcase 10:
-- SELECT * FROM test1 IN ('/tiny_svr/') where i = 1;
--Testcase 201:
CREATE SERVER post_svr FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1',port '15432');
--Testcase 202:
CREATE USER mapping for public server post_svr OPTIONS(user 'postgres',password 'postgres');
--Testcase 203:
CREATE FOREIGN TABLE test1__post_svr__0 (i int) SERVER post_svr OPTIONS(table_name 'test1');
--Testcase 299:
CREATE FOREIGN TABLE test1_post_child1 PARTITION OF test1 FOR VALUES IN ('/post_svr/') SERVER pgspider_svr OPTIONS(child_name 'test1__post_svr__0');
--Testcase 11:
SELECT * FROM test1__post_svr__0 ORDER BY i;
--Testcase 12:
SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 13:
-- SELECT * FROM test1 IN ('/post_svr/') ORDER BY i,__spd_url;
-- --Testcase 14:
-- SELECT * FROM test1 IN ('/post_svr/') where i = 1 ORDER BY i,__spd_url;
--Testcase 204:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER sqlite_fdw OPTIONS (database '/tmp/pgtest.db');
--Testcase 205:
CREATE FOREIGN TABLE test1__sqlite_svr__0 (i int) SERVER sqlite_svr OPTIONS(table 'test1');
--Testcase 300:
CREATE FOREIGN TABLE test1_sqlite_child1 PARTITION OF test1 FOR VALUES IN ('/sqlite_svr/') SERVER pgspider_svr OPTIONS(child_name 'test1__sqlite_svr__0');
--Testcase 15:
SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 16:
-- SELECT * FROM test1 IN ('/sqlite_svr/') ORDER BY i,__spd_url;
-- --Testcase 17:
-- SELECT * FROM test1 IN ('/sqlite_svr/') where i = 4 ORDER BY i,__spd_url;
--Testcase 206:
CREATE SERVER mysql_svr FOREIGN DATA WRAPPER mysql_fdw OPTIONS (host '127.0.0.1',port '3306');
--Testcase 207:
CREATE USER mapping for public server mysql_svr OPTIONS(username 'root',password 'Mysql_1234');
--Testcase 208:
CREATE FOREIGN TABLE test1__mysql_svr__0 (i int) SERVER mysql_svr OPTIONS(dbname 'test',table_name 'test1');
--Testcase 301:
CREATE FOREIGN TABLE test1_mysql_child1 PARTITION OF test1 FOR VALUES IN ('/mysql_svr/') SERVER pgspider_svr OPTIONS(child_name 'test1__mysql_svr__0');
--Testcase 18:
SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 19:
-- SELECT * FROM test1 IN ('/mysql_svr/') ORDER BY i,__spd_url;
--Testcase 20:
SELECT * FROM test1 where i = 1 ORDER BY i,__spd_url;
-- --Testcase 21:
-- SELECT * FROM test1 IN ('/mysql_svr/') where i = 5 ORDER BY i,__spd_url;
--Testcase 22:
SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 23:
-- SELECT * FROM test1 IN ('/test2/') ORDER BY i,__spd_url;
--Testcase 24:
SELECT * FROM test1 order by i,__spd_url;
-- --Testcase 25:
-- SELECT * FROM test1 IN ('/file_svr/') ORDER BY i,__spd_url;
-- --Testcase 26:
-- SELECT * FROM test1 IN ('/file_svr/') where i = 1 ORDER BY i,__spd_url;
--Testcase 27:
SELECT * FROM test1__tiny_svr__0 order by i;
--Testcase 28:
SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 29:
-- SELECT * FROM test1 IN ('/tiny_svr/');
-- --Testcase 30:
-- SELECT * FROM test1 IN ('/tiny_svr/') where i = 1;
--Testcase 31:
SELECT * FROM test1__post_svr__0 order by i;
--Testcase 32:
SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 33:
-- SELECT * FROM test1 IN ('/post_svr/') ORDER BY i,__spd_url;
-- --Testcase 34:
-- SELECT * FROM test1 IN ('/post_svr/') where i = 1 ORDER BY i,__spd_url;
--Testcase 35:
SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 36:
-- SELECT * FROM test1 IN ('/sqlite_svr/') ORDER BY i,__spd_url;
-- --Testcase 37:
-- SELECT * FROM test1 IN ('/sqlite_svr/') where i = 4 ORDER BY i,__spd_url;
--Testcase 38:
SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 39:
-- SELECT * FROM test1 IN ('/mysql_svr/') ORDER BY i,__spd_url;
--Testcase 40:
SELECT * FROM test1 where i = 1 ORDER BY i,__spd_url;
-- --Testcase 41:
-- SELECT * FROM test1 IN ('/mysql_svr/') where i = 5 ORDER BY i,__spd_url;
-- --Testcase 42:
-- SELECT * FROM test1 IN ('/mysql_svr/', '/sqlite_svr/') ORDER BY  i,__spd_url;

--Testcase 43:
SELECT * FROM test1 UNION ALL SELECT * FROM test1 ORDER BY i,__spd_url;
-- --Testcase 44:
-- SELECT * FROM test1 IN ('/mysql_svr/') UNION ALL SELECT * FROM test1 IN ('/mysql_svr/') ORDER BY i,__spd_url;
-- --Testcase 45:
-- SELECT * FROM test1 IN ('/mysql_svr/') UNION ALL SELECT * FROM test1 IN ('/sqlite_svr/') ORDER BY i,__spd_url;
-- --Testcase 46:
-- SELECT * FROM test1 IN ('/mysql_svr/', '/sqlite_svr/') UNION ALL SELECT * FROM test1 IN ('/mysql_svr/', '/sqlite_svr/') ORDER BY i,__spd_url;

--Testcase 209:
CREATE TABLE test1_1 (i int,__spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 210:
CREATE FOREIGN TABLE test1_1__tiny_svr__0 (i int) SERVER tiny_svr OPTIONS(table_name 'test1');
--Testcase 302:
CREATE FOREIGN TABLE test1_1_tiny_child1 PARTITION OF test1_1 FOR VALUES IN ('/tiny_svr/') SERVER pgspider_svr OPTIONS(child_name 'test1_1__tiny_svr__0');
--Testcase 211:
CREATE FOREIGN TABLE test1_1__post_svr__0 (i int) SERVER post_svr OPTIONS(table_name 'test1');
--Testcase 303:
CREATE FOREIGN TABLE test1_1_post_child1 PARTITION OF test1_1 FOR VALUES IN ('/post_svr/') SERVER pgspider_svr OPTIONS(child_name 'test1_1__post_svr__0');
--Testcase 212:
CREATE FOREIGN TABLE test1_1__sqlite_svr__0 (i int) SERVER sqlite_svr OPTIONS(table 'test1');
--Testcase 304:
CREATE FOREIGN TABLE test1_1_sqlite_child1 PARTITION OF test1_1 FOR VALUES IN ('/sqlite_svr/') SERVER pgspider_svr OPTIONS(child_name 'test1_1__sqlite_svr__0');
--Testcase 213:
CREATE FOREIGN TABLE test1_1__mysql_svr__0 (i int) SERVER mysql_svr OPTIONS(dbname 'test',table_name 'test1');
--Testcase 305:
CREATE FOREIGN TABLE test1_1_mysql_child1 PARTITION OF test1_1 FOR VALUES IN ('/mysql_svr/') SERVER pgspider_svr OPTIONS(child_name 'test1_1__mysql_svr__0');

-- --Testcase 47:
-- SELECT * FROM test1 IN ('/mysql_svr/'), test1_1 IN ('/sqlite_svr/') ORDER BY test1.i,test1.__spd_url,test1_1.i,test1_1.__spd_url;
-- --Testcase 48:
-- SELECT * FROM test1 IN ('/sqlite_svr/','/mysql_svr/'), test1_1 IN ('/mysql_svr/','/sqlite_svr/') ORDER BY test1.i,test1.__spd_url,test1_1.i,test1_1.__spd_url;
-- -- nothing case
-- --Testcase 49:
-- SELECT * FROM test1 IN ('/sqlite_svr/','/mysql_svrrrrrr/');
-- --Testcase 50:
-- SELECT * FROM test1 IN ('/mysql_svr/'), test1_1 IN ('/mysql_svr2/') ORDER BY test1.i,test1.__spd_url,test1_1.i,test1_1.__spd_url;
-- --Testcase 51:
-- SELECT * FROM test1 IN ('/mysql_svr2/'), test1_1 IN ('/mysql_svr/') ORDER BY test1.i,test1.__spd_url,test1_1.i,test1_1.__spd_url;
-- --Testcase 52:
-- SELECT * FROM test1 IN ('/mysql_svr2/'), test1_1 IN ('/mysql_svr2/') ORDER BY test1.i,test1.__spd_url,test1_1.i,test1_1.__spd_url;
-- --Testcase 53:
-- SELECT * FROM test1 IN ('/sqlite_svr/','/mysql_svr2/'), test1_1 IN ('/sqlite_svr2/','/mysql_svr/') ORDER BY test1.i,test1.__spd_url,test1_1.i,test1_1.__spd_url;

--Testcase 54:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM test1;
-- PGSpider Extension does not push down avg, so all targets are not pushed down
--Testcase 55:
EXPLAIN (VERBOSE, COSTS OFF) SELECT sum(i), avg(i) FROM test1;
-- -- only post_svr is alive
-- --Testcase 56:
-- EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM test1 IN ('/post_svr/');

-- only __spd_url target list is OK
--Testcase 57:
SELECT __spd_url FROM test1 ORDER BY __spd_url;

--Testcase 214:
EXPLAIN VERBOSE
SELECT i, __spd_url FROM test1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 215:
SELECT i, __spd_url FROM test1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 216:
EXPLAIN VERBOSE
SELECT i, __spd_url FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;
--Testcase 58:
SELECT i, __spd_url FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;

--Testcase 217:
EXPLAIN VERBOSE
SELECT __spd_url, i FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;
--Testcase 59:
SELECT __spd_url, i FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;

--Testcase 218:
EXPLAIN VERBOSE
SELECT avg(i), __spd_url FROM test1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 219:
SELECT avg(i), __spd_url FROM test1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 220:
EXPLAIN VERBOSE
SELECT avg(i), __spd_url FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;
--Testcase 60:
SELECT avg(i), __spd_url FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;

--Testcase 221:
EXPLAIN VERBOSE
SELECT __spd_url, avg(i) FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;
--Testcase 61:
SELECT __spd_url, avg(i) FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;

--Testcase 222:
EXPLAIN VERBOSE
SELECT __spd_url, sum(i) FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;
--Testcase 62:
SELECT __spd_url, sum(i) FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;

--Testcase 223:
EXPLAIN VERBOSE
SELECT __spd_url, avg(i), __spd_url FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;
--Testcase 63:
SELECT __spd_url, avg(i), __spd_url FROM test1 GROUP BY i, __spd_url ORDER BY i,__spd_url;

--Aggregate and function with __spd_url
--Testcase 276:
EXPLAIN VERBOSE
SELECT max(__spd_url), min(__spd_url) from test1;
--Testcase 277:
SELECT max(__spd_url), min(__spd_url) from test1;
--Testcase 278:
EXPLAIN VERBOSE
SELECT lower(__spd_url), upper(__spd_url) from test1 ORDER BY 1, 2;
--Testcase 279:
SELECT lower(__spd_url), upper(__spd_url) from test1  ORDER BY 1, 2;
--Testcase 280:
EXPLAIN VERBOSE
SELECT pg_typeof(max(i)), pg_typeof(count(*)), pg_typeof(max(__spd_url)) FROM test1;
--Testcase 281:
SELECT pg_typeof(max(i)), pg_typeof(count(*)), pg_typeof(max(__spd_url)) FROM test1;

--Testcase 64:
SELECT sum(i) FROM test1;
--Testcase 65:
SELECT avg(i) FROM test1;
--Testcase 66:
SELECT avg(i),i FROM test1 group by i order by i;
--Testcase 67:
SELECT sum(i),count(i),i FROM test1 group by i order by i;
--Testcase 68:
SELECT avg(i), count(i) FROM test1 group by i;
--Testcase 69:
SELECT SUM(i) as aa, avg(i) FROM test1 GROUP BY i;
--Testcase 70:
SELECT SUM(i) as aa, avg(i), i/2, SUM(i)/2 FROM test1 GROUP BY i;
--Testcase 71:
SELECT SUM(i) as aa, avg(i) FROM test1 GROUP BY i ORDER BY aa;
--Testcase 72:
SELECT sum(i), avg(i) FROM test1 GROUP BY i ORDER BY 1;
--Testcase 73:
SELECT i, avg(i) FROM test1 GROUP BY i ORDER BY 1;

--Test extract expression when target contains Var which exists in GROUP BY
--Testcase 270:
EXPLAIN VERBOSE
SELECT i/2, i/4 FROM test1 GROUP BY i ORDER BY 1;
--Testcase 271:
SELECT i/2, i/4 FROM test1 GROUP BY i ORDER BY 1;
--Testcase 272:
EXPLAIN VERBOSE
SELECT i/4, avg(i) FROM test1 GROUP BY i ORDER BY 1;
--Testcase 273:
SELECT i/4, avg(i) FROM test1 GROUP BY i ORDER BY 1;
--Testcase 274:
EXPLAIN VERBOSE
SELECT i, i*i FROM test1 GROUP BY i ORDER BY 1;
--Testcase 275:
SELECT i, i*i FROM test1 GROUP BY i ORDER BY 1;

-- allocate statement
--Testcase 74:
PREPARE stmt AS SELECT sum(i),count(i),i FROM test1 group by i order by i;
-- execute first time
--Testcase 75:
EXECUTE stmt;
-- performance test prepared statement
DO $$
BEGIN
   FOR counter IN 1..50 LOOP
--Testcase 224:
   EXECUTE 'EXECUTE stmt;';
   END LOOP;
END; $$;
-- deallocate statement
DEALLOCATE stmt;

--Testcase 225:
CREATE TABLE t1 (i int, t text,__spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 226:
CREATE FOREIGN TABLE t1__post_svr__0 (i int, t text) SERVER post_svr OPTIONS(table_name 't1');
--Testcase 306:
CREATE FOREIGN TABLE t1_post_child1 PARTITION OF t1 FOR VALUES IN ('/post_svr/') SERVER pgspider_svr OPTIONS(child_name 't1__post_svr__0');
--Testcase 76:
SELECT * FROM t1;
--Testcase 77:
SELECT * FROM t1 WHERE __spd_url='/post_svr/' and i = 1 and t = 'a';
--Testcase 78:
SELECT sum(i),t FROM t1 group by t;
--Testcase 79:
SELECT sum(i),t,count(i) FROM t1 group by t;

--Testcase 80:
SELECT * FROM t1 WHERE i = 1;
--Testcase 81:
SELECT sum(i),t FROM t1 group by t;
--Testcase 82:
SELECT avg(i) FROM t1;
--Testcase 83:
SELECT stddev(i) FROM t1;
--Testcase 84:
SELECT sum(i),t FROM t1 WHERE i = 1 group by t;
--Testcase 85:
SELECT avg(i),sum(i) FROM t1;
--Testcase 86:
SELECT sum(i),sum(i) FROM t1;
--Testcase 87:
SELECT avg(i),t FROM t1 group by t;
--Testcase 88:
SELECT avg(i) FROM t1 group by i;

--Testcase 89:
SELECT avg(i), count(i) FROM t1 GROUP BY i ORDER BY i;
--Testcase 90:
SELECT t, avg(i), t FROM t1 GROUP BY i, t ORDER BY i;

--Testcase 227:
EXPLAIN VERBOSE
SELECT t, __spd_url FROM t1 GROUP BY __spd_url, t ORDER BY t,__spd_url;
--Testcase 91:
SELECT t, __spd_url FROM t1 GROUP BY __spd_url, t ORDER BY t,__spd_url;

--Testcase 228:
EXPLAIN VERBOSE
SELECT i, __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 92:
SELECT i, __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 229:
EXPLAIN VERBOSE
SELECT __spd_url, i FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 93:
SELECT __spd_url, i FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 230:
EXPLAIN VERBOSE
SELECT avg(i), __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 94:
SELECT avg(i), __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 231:
EXPLAIN VERBOSE
SELECT __spd_url, avg(i) FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 95:
SELECT __spd_url, avg(i) FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 232:
EXPLAIN VERBOSE
SELECT __spd_url, avg(i), __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 96:
SELECT __spd_url, avg(i), __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 233:
EXPLAIN VERBOSE
SELECT __spd_url, sum(i) FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 97:
SELECT __spd_url, sum(i) FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 234:
EXPLAIN VERBOSE
SELECT __spd_url, avg(i), __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 98:
SELECT __spd_url, avg(i), __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 235:
EXPLAIN VERBOSE
SELECT __spd_url, avg(i), sum(i), __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;
--Testcase 99:
SELECT __spd_url, avg(i), sum(i), __spd_url FROM t1 GROUP BY __spd_url, i ORDER BY i,__spd_url;

--Testcase 100:
SELECT * FROM (SELECT sum(i) FROM t1) A,(SELECT count(i) FROM t1) B;

--Testcase 101:
SELECT SUM(i) as aa, avg(i) FROM t1 GROUP BY i;
--Testcase 102:
SELECT SUM(i) as aa, avg(i) FROM t1 GROUP BY t;
--Testcase 103:
SELECT SUM(i) as aa, avg(i), i/2, SUM(i)/2 FROM t1 GROUP BY i, t;
--Testcase 104:
SELECT SUM(i) as aa, avg(i) FROM t1 GROUP BY i ORDER BY aa;

-- query contains all constant
--Testcase 236:
SELECT 1, 2, 'asd$@' FROM t1 group by 1, 3, 2;

-- allocate statement
--Testcase 105:
PREPARE stmt AS SELECT * FROM t1;
-- execute first time
--Testcase 106:
EXECUTE stmt;
-- performance test prepared statement
DO $$
BEGIN
   FOR counter IN 1..50 LOOP
--Testcase 237:
   EXECUTE 'EXECUTE stmt;';
   END LOOP;
END; $$;
-- deallocate statement
DEALLOCATE stmt;

--Testcase 107:
EXPLAIN (VERBOSE, COSTS OFF) SELECT STDDEV(i) FROM t1;

--Testcase 238:
CREATE TABLE t3 (t text, t2 text, i int,__spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 239:
CREATE FOREIGN TABLE t3__mysql_svr__0 (t text,t2 text,i int) SERVER mysql_svr OPTIONS(dbname 'test',table_name 'test3');
--Testcase 307:
CREATE FOREIGN TABLE t3_mysql_child1 PARTITION OF t3 FOR VALUES IN ('/mysql_svr/') SERVER pgspider_svr OPTIONS(child_name 't3__mysql_svr__0');

--Testcase 108:
SELECT count(t) FROM t3;
--Testcase 109:
SELECT count(t2) FROM t3;
--Testcase 110:
SELECT count(i) FROM t3;

--Testcase 111:
SELECT * FROM t3;
-- test target list push down for mysql fdw
-- push down abs(-i*2) and i+1
--Testcase 112:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT abs(-i*2), i+1, i, i FROM t3;
--Testcase 113:
SELECT abs(-i*2), i+1, i, i FROM t3;

-- can't push down abs(A.i) in join case
--Testcase 114:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT abs(A.i) FROM t3 A, t3 B LIMIT 3;
--Testcase 115:
SELECT abs(A.i) FROM t3 A, t3 B LIMIT 3;

--Testcase 116:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT abs(i) c1 FROM t3 UNION SELECT abs(i+1) FROM t3 ORDER BY c1;
--Testcase 117:
SELECT abs(i) c1 FROM t3 UNION SELECT abs(i+1) FROM t3 ORDER BY c1;

--Testcase 118:
SELECT i+1, __spd_url FROM t3;
--Testcase 119:
SELECT i, __spd_url FROM t3 ORDER BY i, __spd_url;
--Testcase 120:
SELECT i FROM t3 ORDER BY __spd_url;

-- can't push down i+1 because test1 includes fdws other than mysql fdw
--Testcase 121:
EXPLAIN (VERBOSE, COSTS OFF) 
SELECT i+1,__spd_url FROM test1 ORDER BY __spd_url, i;
--Testcase 122:
SELECT i+1,__spd_url FROM test1  ORDER BY __spd_url, i;
--Testcase 123:
SELECT __spd_url,i FROM test1 ORDER BY __spd_url, i;

-- t is not included in target list, but is pushed down, it is OK
--Testcase 124:
select t from t3 where i  = 1;

-- t is not included and cannot be pushed down, so it is error
-- select i from t3 where t COLLATE "ja_JP.utf8" = 'aa';

-- error stack test
-- PGSpider Extension does not have this variable
-- Set pgspider_core_fdw.throw_error_ifdead to false;
--Testcase 240:
CREATE SERVER mysql_svr2 FOREIGN DATA WRAPPER mysql_fdw OPTIONS (host '127.0.0.1',port '3306');
--Testcase 241:
CREATE USER mapping for public server mysql_svr2 OPTIONS(username 'root',password 'wrongpass');
--Testcase 242:
CREATE FOREIGN TABLE t3__mysql_svr2__0 (t text,t2 text,i int) SERVER mysql_svr2 OPTIONS(dbname 'test',table_name 'test3');
--Testcase 308:
CREATE FOREIGN TABLE t3_mysql_child2 PARTITION OF t3 FOR VALUES IN ('/mysql_svr2/') SERVER pgspider_svr OPTIONS(child_name 't3__mysql_svr2__0');
-- PGSpider Extension displays error and stop processing immediately when one child node has error
--Testcase 125:
SELECT count(t) FROM t3;
--Testcase 126:
SELECT count(t) FROM t3;
--Testcase 127:
SELECT count(t) FROM t3;
--Testcase 128:
SELECT count(t) FROM t3;
--Testcase 129:
SELECT count(t) FROM t3;
--Testcase 130:
SELECT count(t) FROM t3;
--Testcase 131:
SELECT count(t) FROM t3;
--Testcase 132:
SELECT count(t) FROM t3;
--Testcase 133:
SELECT count(t) FROM t3;
--Testcase 134:
SELECT count(t) FROM t3;
--Testcase 135:
SELECT count(t) FROM t3;
--Testcase 136:
SELECT count(t) FROM t3;
--Testcase 137:
SELECT count(t) FROM t3;
--Testcase 138:
SELECT count(t) FROM t3;
--Testcase 139:
SELECT count(t) FROM t3;
--Testcase 140:
SELECT count(t) FROM t3;
--Testcase 141:
SELECT count(t) FROM t3;
--Testcase 142:
SELECT count(t) FROM t3;
--Testcase 143:
SELECT count(t) FROM t3;
--Testcase 144:
SELECT count(t) FROM t3;
--Testcase 145:
SELECT count(t) FROM t3;
--Testcase 146:
SELECT count(t) FROM t3;
--Testcase 147:
SELECT count(t) FROM t3;
--Testcase 148:
SELECT count(t) FROM t3;
--Testcase 149:
SELECT count(t) FROM t3;
--Testcase 150:
SELECT count(t) FROM t3;
--Testcase 151:
SELECT count(t) FROM t3;
--Testcase 152:
SELECT count(t) FROM t3;
--Testcase 153:
SELECT count(t) FROM t3;
--Testcase 154:
SELECT count(t) FROM t3;
--Testcase 155:
SELECT count(t) FROM t3;
--Testcase 156:
SELECT count(t) FROM t3;

-- Set pgspider_core_fdw.throw_error_ifdead to true;

--Testcase 243:
DROP TABLE t3;
--Testcase 244:
DROP FOREIGN TABLE t3__mysql_svr__0;
--Testcase 245:
DROP FOREIGN TABLE t3__mysql_svr2__0;
-- wrong result:
-- SELECT sum(i),t  FROM t1 group by t having sum(i) > 2;
--  sum | t 
-- -----+---
--    1 | a
--    5 | b
--    4 | c
-- (3 rows)

-- stress test for finding multithread error
DO $$
BEGIN
   FOR counter IN 1..50 LOOP
   PERFORM sum(i) FROM test1;
   END LOOP;
END; $$;

--Testcase 246:
CREATE TABLE mysqlt (t text, t2 text, i int,__spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 247:
CREATE FOREIGN TABLE mysqlt__mysql_svr__0 (t text,t2 text,i int) SERVER mysql_svr OPTIONS(dbname 'test',table_name 'test3');
--Testcase 309:
CREATE FOREIGN TABLE mysqlt_mysql_child0 PARTITION OF mysqlt FOR VALUES IN ('/mysql_svr0/') SERVER pgspider_svr OPTIONS(child_name 'mysqlt__mysql_svr__0');
--Testcase 248:
CREATE FOREIGN TABLE mysqlt__mysql_svr__1 (t text,t2 text,i int) SERVER mysql_svr OPTIONS(dbname 'test',table_name 'test3');
--Testcase 310:
CREATE FOREIGN TABLE mysqlt_mysql_child1 PARTITION OF mysqlt FOR VALUES IN ('/mysql_svr1/') SERVER pgspider_svr OPTIONS(child_name 'mysqlt__mysql_svr__1');
--Testcase 249:
CREATE FOREIGN TABLE mysqlt__mysql_svqr__2 (t text,t2 text,i int) SERVER mysql_svr OPTIONS(dbname 'test',table_name 'test3');
--Testcase 311:
CREATE FOREIGN TABLE mysqlt_mysql_child2 PARTITION OF mysqlt FOR VALUES IN ('/mysql_svr2/') SERVER pgspider_svr OPTIONS(child_name 'mysqlt__mysql_svqr__2');

DO $$
BEGIN
   FOR counter IN 1..50 LOOP
   PERFORM sum(i) FROM mysqlt;
   END LOOP;
END; $$;

--Testcase 250:
CREATE TABLE post_large (i int, t text,__spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 251:
CREATE FOREIGN TABLE post_large__post_svr__1 (i int, t text) SERVER post_svr OPTIONS(table_name 'large_t');
--Testcase 312:
CREATE FOREIGN TABLE post_large_post_child1 PARTITION OF post_large FOR VALUES IN ('/post_svr_1/') SERVER pgspider_svr OPTIONS(child_name 'post_large__post_svr__1');
--Testcase 252:
CREATE FOREIGN TABLE post_large__post_svr__2 (i int, t text) SERVER post_svr OPTIONS(table_name 'large_t');
--Testcase 313:
CREATE FOREIGN TABLE post_large_post_child2 PARTITION OF post_large FOR VALUES IN ('/post_svr_2/') SERVER pgspider_svr OPTIONS(child_name 'post_large__post_svr__2');
--Testcase 253:
CREATE FOREIGN TABLE post_large__post_svr__3 (i int, t text) SERVER post_svr OPTIONS(table_name 'large_t');
--Testcase 314:
CREATE FOREIGN TABLE post_large_post_child3 PARTITION OF post_large FOR VALUES IN ('/post_svr_3/') SERVER pgspider_svr OPTIONS(child_name 'post_large__post_svr__3');

--Testcase 157:
SELECT i,t FROM post_large WHERE i < 3 ORDER BY i,t;
DO $$
BEGIN
   FOR counter IN 1..10 LOOP
   PERFORM i,t FROM post_large WHERE i < 3 ORDER BY i,t;
   END LOOP;
END; $$;

--Testcase 158:
SELECT count(*) FROM post_large;

DO $$
BEGIN
   FOR counter IN 1..10 LOOP
   PERFORM sum(i) FROM post_large;
   END LOOP;
END; $$;

--Testcase 254:
CREATE TABLE t2 (i int, t text, a text,__spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 255:
CREATE FOREIGN TABLE t2__post_svr__0 (i int, t text,a text) SERVER post_svr OPTIONS(table_name 't2');
--Testcase 315:
CREATE FOREIGN TABLE t2_post_child1 PARTITION OF t2 FOR VALUES IN ('/post_svr_0/') SERVER pgspider_svr OPTIONS(child_name 't2__post_svr__0');
--Testcase 159:
SELECT i,t,a FROM t2 ORDER BY i,__spd_url;
--Testcase 256:
CREATE FOREIGN TABLE t2__post_svr__1 (i int, t text,a text) SERVER post_svr OPTIONS(table_name 't2');
--Testcase 316:
CREATE FOREIGN TABLE t2_post_child2 PARTITION OF t2 FOR VALUES IN ('/post_svr_1/') SERVER pgspider_svr OPTIONS(child_name 't2__post_svr__1');
--Testcase 257:
CREATE FOREIGN TABLE t2__post_svr__2 (i int, t text,a text) SERVER post_svr OPTIONS(table_name 't2');
--Testcase 317:
CREATE FOREIGN TABLE t2_post_child3 PARTITION OF t2 FOR VALUES IN ('/post_svr_2/') SERVER pgspider_svr OPTIONS(child_name 't2__post_svr__2');
--Testcase 258:
CREATE FOREIGN TABLE t2__post_svr__3 (i int, t text,a text) SERVER post_svr OPTIONS(table_name 't2');
--Testcase 318:
CREATE FOREIGN TABLE t2_post_child4 PARTITION OF t2 FOR VALUES IN ('/post_svr_3/') SERVER pgspider_svr OPTIONS(child_name 't2__post_svr__3');

-- random cannot be pushed down and i=2 is pushed down
--Testcase 160:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM t2 WHERE i=2 AND random() < 2.0;
--Testcase 161:
SELECT * FROM t2 WHERE i=2 AND random() < 2.0;

--Testcase 162:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;
--Testcase 163:
SELECT a,i, __spd_url, t FROM t2 ORDER BY i,t,a,__spd_url;

--Testcase 164:
SELECT __spd_url,i FROM t2 WHERE __spd_url='/post_svr_0/' ORDER BY i LIMIT 1;

-- Keep alive test
--Testcase 259:
CREATE SERVER post_svr2 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '127.0.0.1',port '49503');
--Testcase 260:
CREATE USER mapping for public server post_svr2 OPTIONS(user 'postgres',password 'postgres');
--Testcase 261:
CREATE FOREIGN TABLE t2__post_svr2__0 (i int, t text,a text) SERVER post_svr2 OPTIONS(table_name 't2');
--Testcase 319:
CREATE FOREIGN TABLE t2_post_child5 PARTITION OF t2 FOR VALUES IN ('/post_svr2_0/') SERVER pgspider_svr OPTIONS(child_name 't2__post_svr2__0');
-- --Testcase 165:
-- INSERT INTO pg_spd_node_info VALUES(0,'post_svr','postgres_fdw','127.0.0.1');
-- --Testcase 166:
-- INSERT INTO pg_spd_node_info VALUES(0,'post_svr2','postgres_fdw','127.0.0.1');
--Testcase 167:
SELECT pg_sleep(2);
-- PGSpider Extension displays error and stop processing immediately when one child node has error
-- Set pgspider_core_fdw.throw_error_ifdead to false;
--Testcase 168:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;;
-- SET pgspider_core_fdw.throw_error_ifdead to true;
--Testcase 169:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;;
-- SET pgspider_core_fdw.throw_error_ifdead to false;
--Testcase 170:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;;
-- SET pgspider_core_fdw.print_error_nodes to true;
--Testcase 171:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;;
-- SET pgspider_core_fdw.print_error_nodes to false;
--Testcase 172:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;;
--Testcase 262:
CREATE SERVER post_svr3 FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '192.168.11.12',port '15432');
--Testcase 263:
CREATE USER mapping for public server post_svr3 OPTIONS(user 'postgres',password 'postgres');
--Testcase 264:
CREATE FOREIGN TABLE t2__post_svr3__0 (i int, t text,a text) SERVER post_svr3 OPTIONS(table_name 't2');
--Testcase 320:
CREATE FOREIGN TABLE t2_post_child6 PARTITION OF t2 FOR VALUES IN ('/post_svr3_0/') SERVER pgspider_svr OPTIONS(child_name 't2__post_svr3__0');
-- --Testcase 173:
-- INSERT INTO pg_spd_node_info VALUES(0,'post_svr3','postgres_fdw','192.168.11.12');
--Testcase 174:
SELECT pg_sleep(2);

/*
--Testcase 175:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;
SET pgspider_core_fdw.throw_error_ifdead to true;
--Testcase 176:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;
SET pgspider_core_fdw.throw_error_ifdead to false;
--Testcase 177:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;
SET pgspider_core_fdw.print_error_nodes to true;
--Testcase 178:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;
SET pgspider_core_fdw.print_error_nodes to false;
--Testcase 179:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;
DROP FOREIGN TABLE t2__post_svr3__0;
--Testcase 180:
DELETE FROM pg_spd_node_info WHERE servername = 't2__post_svr3__0';
--Testcase 181:
SELECT pg_sleep(2);
--Testcase 182:
SELECT i,t,a FROM t2 ORDER BY i,t,a,__spd_url;
*/

-- Test CoerceViaIO type
--Testcase 282:
CREATE TABLE tbl01 (c1 timestamp without time zone, c2 timestamp with time zone, __spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 283:
CREATE FOREIGN TABLE tbl01__sqlite_svr__0 (c1 timestamp without time zone, c2 timestamp with time zone) SERVER sqlite_svr OPTIONS(table 'tbl01');
--Testcase 321:
CREATE FOREIGN TABLE tbl01_sqlite_child1 PARTITION OF tbl01 FOR VALUES IN ('/sqlite_svr/') SERVER pgspider_svr OPTIONS(child_name 'tbl01__sqlite_svr__0');
--Testcase 284:
SELECT * FROM tbl01;
--Testcase 285:
SELECT c1 || 'time1', c2 || 'time2' FROM tbl01 GROUP BY c1, c2;
--Testcase 286:
DROP FOREIGN TABLE tbl01__sqlite_svr__0;
--Testcase 287:
DROP TABLE tbl01;

-- Test select operator expressions which contain different data type, with WHERE clause contains __spd_url
--Testcase 288:
CREATE TABLE tbl02 (c1 double precision, c2 integer, c3 real, c4 smallint, c5 bigint, c6 numeric,__spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 289:
CREATE FOREIGN TABLE tbl02__sqlite_svr__0 (c1 double precision, c2 integer, c3 real, c4 smallint, c5 bigint, c6 numeric) SERVER sqlite_svr OPTIONS(table 'tbl02');
--Testcase 322:
CREATE FOREIGN TABLE tbl02_sqlite_child1 PARTITION OF tbl02 FOR VALUES IN ('/sqlite_svr/') SERVER pgspider_svr OPTIONS(child_name 'tbl02__sqlite_svr__0');
--Testcase 290:
SELECT * FROM tbl02;
--Testcase 291:
EXPLAIN VERBOSE
SELECT c1-c2, c2-c3, c3-c4, c3-c5, c5-c6 FROM tbl02 WHERE __spd_url != '$';
--Testcase 292:
SELECT c1-c2, c2-c3, c3-c4, c3-c5, c5-c6 FROM tbl02 WHERE __spd_url != '$';
--Testcase 293:
DROP FOREIGN TABLE tbl02__sqlite_svr__0;
--Testcase 294:
DROP TABLE tbl02;
--Testcase 265:
DROP TABLE test1;
--Testcase 266:
DROP TABLE t1;
--Testcase 267:
DROP TABLE t2;
--Testcase 268:
DROP SERVER pgspider_svr CASCADE;
--Testcase 269:
DROP EXTENSION pgspider_ext CASCADE;

--Clean
--Testcase 270:
DROP EXTENSION postgres_fdw CASCADE;
--Testcase 271:
DROP EXTENSION file_fdw CASCADE;
--Testcase 272:
DROP EXTENSION sqlite_fdw CASCADE;
--Testcase 273:
DROP EXTENSION tinybrace_fdw CASCADE;
--Testcase 274:
DROP EXTENSION mysql_fdw CASCADE;
