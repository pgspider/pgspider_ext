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

-- import foreign schema
IMPORT FOREIGN SCHEMA "s3://parquets3"
FROM SERVER parquet_s3_svr
INTO public
OPTIONS (sorted 'one');
--Testcase 9:
\d

--Testcase 10:
SELECT * FROM ported_1;
--Testcase 11:
SELECT * FROM ported_2;
--Testcase 12:
SELECT * FROM ported_3;

--Testcase 13:
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

--Testcase 14:
SELECT * FROM test1 ORDER BY one;

--Testcase 15:
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

--Testcase 16:
SELECT * FROM test1 ORDER BY one;

--Testcase 17:
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

--Testcase 18:
SELECT * FROM test1 ORDER BY one;

-- PGSpider Extension does not support IN syntax
-- --Testcase 19:
-- SELECT * FROM test1 IN ('/parquet_s3_svr/') ORDER BY three,__spd_url;

-- import_parquet
--Testcase 20:
create function list_parquet_s3_files(args jsonb)
returns text[] as
$$
    select array[args->>'dir' || '/ported_1.parquet', args->>'dir' || '/ported_3.parquet']::text[];
$$
language sql;

--Testcase 21:
select import_parquet_s3('test1__parquet_s3_svr__3', 'public', 'parquet_s3_svr', 'list_parquet_s3_files', '{"dir": "s3://parquets3"}', '{"sorted": "one"}');

CREATE FOREIGN TABLE test1_parquet_s3_child3 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_3/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__3');

--Testcase 22:
SELECT * FROM test1__parquet_s3_svr__3 ORDER BY one, three;
--Testcase 23:
SELECT * FROM test1 ORDER BY one;

--Testcase 24:
select import_parquet_s3_explicit('test1__parquet_s3_svr__4', 'public', 'parquet_s3_svr', array['one', 'three', 'six'], array['int8', 'text', 'bool']::regtype[], 'list_parquet_s3_files', '{"dir": "s3://parquets3"}', '{"sorted": "one"}');

CREATE FOREIGN TABLE test1_parquet_s3_child4 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_4/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__4');

--Testcase 25:
SELECT * FROM test1__parquet_s3_svr__4;
--PGSpider Extension requires the schema of tables of pgspider_ext and data source fdw must be the same.
--Therefore, this test case returns error message.
--Testcase 26:
SELECT * FROM test1 ORDER BY one;

--Testcase 27:
CREATE FOREIGN TABLE parquets3tbl__parquet_s3_svr__0 (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8) 
SERVER parquet_s3_svr options(filename 's3://parquets3/ported_3.parquet', sorted 'one');
--Testcase 28:
SELECT * FROM parquets3tbl__parquet_s3_svr__0;

--Testcase 29:
CREATE TABLE parquets3tbl (
    one     INT8,
    two     INT8[],
    three   TEXT,
    four    TIMESTAMP,
    five    DATE,
    six     BOOL,
    seven   FLOAT8,
    __spd_url text) 
 PARTITION BY LIST (__spd_url);

CREATE FOREIGN TABLE parquets3tbl_parquet_s3_child PARTITION OF parquets3tbl FOR VALUES IN ('/parquet_s3_svr/') SERVER pgspider_svr OPTIONS(child_name 'parquets3tbl__parquet_s3_svr__0');

--Testcase 30:
SELECT * FROM parquets3tbl;

-- PGSpider Extension does not support IN syntax
-- --Testcase 31:
-- SELECT * FROM test1 IN ('/parquet_s3_svr/') where one < 1 ORDER BY one;

--Testcase 32:
DROP FUNCTION list_parquet_s3_files;
--Testcase 33:
DROP EXTENSION parquet_s3_fdw CASCADE;

--Testcase 34:
DROP TABLE test1;
DROP TABLE parquets3tbl;
--Testcase 35:
DROP SERVER pgspider_svr CASCADE;
--Testcase 36:
DROP EXTENSION pgspider_ext CASCADE;
