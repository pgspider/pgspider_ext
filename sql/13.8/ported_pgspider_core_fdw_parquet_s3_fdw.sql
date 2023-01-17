-- PGSpider Extension does not have pg_spd_node_info table
-- --Testcase 1:
-- DELETE FROM pg_spd_node_info;
--Testcase 2:
CREATE EXTENSION pgspider_ext;
--Testcase 3:
CREATE SERVER pgspider_svr FOREIGN DATA WRAPPER pgspider_ext;
--Testcase 4:
CREATE USER MAPPING FOR public SERVER pgspider_svr;
--Testcase 5:
CREATE TABLE test1 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8,
    __spd_url text) 
 PARTITION BY LIST (__spd_url);

-- create parquet_s3_fdw extension
SET datestyle = 'ISO';
SET client_min_messages = WARNING;
SET log_statement TO 'none';
--Testcase 6:
CREATE EXTENSION parquet_s3_fdw;
--Testcase 7:
CREATE SERVER parquet_s3_svr FOREIGN DATA WRAPPER parquet_s3_fdw OPTIONS (use_minio 'true');
--Testcase 8:
CREATE USER MAPPING FOR public SERVER parquet_s3_svr OPTIONS (user 'minioadmin', password 'minioadmin');

-- Enable to pushdown aggregate
SET enable_partitionwise_aggregate TO on;

-- Turn off leader node participation to avoid duplicate data error when executing
-- parallel query
SET parallel_leader_participation TO off;

-- create multi-tenant
--Testcase 9:
CREATE FOREIGN TABLE test1__parquet_s3_svr__0 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_1.parquet', sorted 'one');

CREATE FOREIGN TABLE test1_parquet_s3_child0 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_0/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__0');

--Testcase 10:
SELECT * FROM test1 ORDER BY one;

--Testcase 11:
CREATE FOREIGN TABLE test1__parquet_s3_svr__1 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_2.parquet', sorted 'one');

CREATE FOREIGN TABLE test1_parquet_s3_child1 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_1/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__1');

--Testcase 12:
SELECT * FROM test1 ORDER BY one;

--Testcase 13:
CREATE FOREIGN TABLE test1__parquet_s3_svr__2 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_3.parquet', sorted 'one');

CREATE FOREIGN TABLE test1_parquet_s3_child2 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_2/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__2');

--Testcase 14:
SELECT * FROM test1 ORDER BY one;

-- PGSpider Extension does not support IN syntax
-- --Testcase 15:
-- SELECT * FROM test1 IN ('/parquet_s3_svr/') ORDER BY one,seven,__spd_url;

-- no explicit columns mentions
--Testcase 16:
SELECT 1 as x FROM test1;
--Testcase 17:
SELECT count(*) as count FROM test1;

-- sorting
--Testcase 18:
EXPLAIN (COSTS OFF) SELECT * FROM test1 ORDER BY one;
--Testcase 19:
EXPLAIN (COSTS OFF) SELECT * FROM test1 ORDER BY three;

-- filtering
SET client_min_messages = DEBUG1;
--Testcase 20:
SELECT * FROM test1 WHERE one < 1 ORDER BY one;
--Testcase 21:
SELECT * FROM test1 WHERE one <= 1 ORDER BY one;
--Testcase 22:
SELECT * FROM test1 WHERE one > 6 ORDER BY one;
--Testcase 23:
SELECT * FROM test1 WHERE one >= 6 ORDER BY one;
--Testcase 24:
SELECT * FROM test1 WHERE one = 2 ORDER BY one;
--Testcase 25:
SELECT * FROM test1 WHERE one = 7 ORDER BY one;
--Testcase 26:
SELECT * FROM test1 WHERE six = true ORDER BY one;
--Testcase 27:
SELECT * FROM test1 WHERE six = false ORDER BY one;
--Testcase 28:
SELECT * FROM test1 WHERE seven < 0.9 ORDER BY one;
--Testcase 29:
SELECT * FROM test1 WHERE seven IS NULL ORDER BY one;

-- prepared statements
--Testcase 30:
prepare prep(date) as select * from test1 where five < $1;
--Testcase 31:
execute prep('2018-01-03');
--Testcase 32:
execute prep('2018-01-01');

-- invalid options
-- So, Don't need create child's foreign table.
SET client_min_messages = WARNING;
--Testcase 33:
CREATE FOREIGN TABLE test1__parquet_s3_svr__3 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr;
--Testcase 34:
CREATE FOREIGN TABLE test1__parquet_s3_svr__3 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 'nonexistent.parquet', some_option '123');
--Testcase 35:
CREATE FOREIGN TABLE test1__parquet_s3_svr__3 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_2.parquet', some_option '123');

-- type mismatch
-- So, Don't need create child's foreign table.
--Testcase 36:
CREATE FOREIGN TABLE test1__parquet_s3_svr__4 (one INT8[], two INT8, three TEXT)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_2.parquet', sorted 'one');
--Testcase 37:
SELECT one FROM test1__parquet_s3_svr__4;
--Testcase 38:
SELECT two FROM test1__parquet_s3_svr__4;

-- sequential multifile reader
--Testcase 39:
CREATE TABLE test2 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8,
    __spd_url text) 
 PARTITION BY LIST (__spd_url);

--Testcase 40:
CREATE FOREIGN TABLE test2__parquet_s3_svr__0 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_3.parquet s3://parquets3/ported_2.parquet s3://parquets3/ported_1.parquet', sorted 'one');

CREATE FOREIGN TABLE test2_parquet_s3_child0 PARTITION OF test2 FOR VALUES IN ('/parquet_s3_svr/') SERVER pgspider_svr OPTIONS(child_name 'test2__parquet_s3_svr__0');

--Testcase 41:
EXPLAIN SELECT * FROM test2;
--Testcase 42:
SELECT * FROM test2;

-- multifile merge reader
--Testcase 43:
CREATE TABLE test3 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8,
    __spd_url text) 
 PARTITION BY LIST (__spd_url);

--Testcase 44:
CREATE FOREIGN TABLE test3__parquet_s3_svr__0 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_3.parquet s3://parquets3/ported_2.parquet s3://parquets3/ported_1.parquet', sorted 'one');

CREATE FOREIGN TABLE test3_parquet_s3_child0 PARTITION OF test3 FOR VALUES IN ('/parquet_s3_svr/') SERVER pgspider_svr OPTIONS(child_name 'test3__parquet_s3_svr__0');

--Testcase 45:
EXPLAIN (COSTS OFF) SELECT * FROM test3 ORDER BY one;
--Testcase 46:
SELECT * FROM test3 ORDER BY one;

--These test cases are not suitable for PGSpider
---- parallel execution
--SET parallel_setup_cost = 0;
--SET parallel_tuple_cost = 0.001;
--SET cpu_operator_cost = 0.000025;
--ANALYZE test2;
--ANALYZE test3;
--EXPLAIN (COSTS OFF) SELECT * FROM test2;
--EXPLAIN (COSTS OFF) SELECT * FROM test2 ORDER BY one;
--EXPLAIN (COSTS OFF) SELECT * FROM test2 ORDER BY two;
--EXPLAIN (COSTS OFF) SELECT * FROM test3;
--EXPLAIN (COSTS OFF) SELECT * FROM test3 ORDER BY one;
--EXPLAIN (COSTS OFF) SELECT * FROM test3 ORDER BY two;

-- multiple sorting keys
--Testcase 47:
CREATE TABLE test4 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8,
    __spd_url text) 
 PARTITION BY LIST (__spd_url);

--Testcase 48:
CREATE FOREIGN TABLE test4__parquet_s3_svr__0 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_1.parquet', sorted 'one five');

CREATE FOREIGN TABLE test4_parquet_s3_child0 PARTITION OF test4 FOR VALUES IN ('/parquet_s3_svr_0/') SERVER pgspider_svr OPTIONS(child_name 'test4__parquet_s3_svr__0');

--Testcase 49:
CREATE FOREIGN TABLE test4__parquet_s3_svr__1 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_2.parquet', sorted 'one five');

CREATE FOREIGN TABLE test4_parquet_s3_child1 PARTITION OF test4 FOR VALUES IN ('/parquet_s3_svr_1/') SERVER pgspider_svr OPTIONS(child_name 'test4__parquet_s3_svr__1');

--Testcase 50:
CREATE FOREIGN TABLE test4__parquet_s3_svr__2 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8)
SERVER parquet_s3_svr
OPTIONS (filename 's3://parquets3/ported_3.parquet', sorted 'one five');

CREATE FOREIGN TABLE test4_parquet_s3_child2 PARTITION OF test4 FOR VALUES IN ('/parquet_s3_svr_2/') SERVER pgspider_svr OPTIONS(child_name 'test4__parquet_s3_svr__2');

--Testcase 51:
EXPLAIN (COSTS OFF) SELECT * FROM test4 ORDER BY one, five;
--Testcase 52:
SELECT * FROM test4 ORDER BY one, five;

--Testcase 53:
DROP EXTENSION parquet_s3_fdw CASCADE;

--Testcase 54:
DROP TABLE test1;
--Testcase 55:
DROP TABLE test2;
--Testcase 56:
DROP TABLE test3;
--Testcase 57:
DROP TABLE test4;
--Testcase 58:
DROP SERVER pgspider_svr CASCADE;
--Testcase 59:
DROP EXTENSION pgspider_ext CASCADE;
