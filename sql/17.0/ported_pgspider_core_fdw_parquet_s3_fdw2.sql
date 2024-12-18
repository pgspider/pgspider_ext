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
    timestamp timestamp, 
    col1 text, 
    col2 bigint, 
    col3 double precision, 
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

-- **********************************************
-- Foreign table using 'filename' option
-- **********************************************
-- File under bucket
--Testcase 9:
CREATE FOREIGN TABLE test1__parquet_s3_svr__0 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (filename 's3://test-bucket/file0.parquet');

CREATE FOREIGN TABLE test1_parquet_s3_child0 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_0/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__0');

--Testcase 10:
SELECT * FROM test1__parquet_s3_svr__0;
--Testcase 11:
SELECT * FROM test1 ORDER BY timestamp, col3;

-- File in directory
--Testcase 12:
CREATE FOREIGN TABLE test1__parquet_s3_svr__1 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (filename 's3://test-bucket/file0.parquet');

CREATE FOREIGN TABLE test1_parquet_s3_child1 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_1/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__1');

--Testcase 13:
SELECT * FROM test1__parquet_s3_svr__0;
--Testcase 14:
SELECT * FROM test1 ORDER BY timestamp, col3;

-- File in sub directory
--Testcase 15:
CREATE FOREIGN TABLE test1__parquet_s3_svr__2 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (filename 's3://test-bucket/dir1/dir11/file111.parquet');

CREATE FOREIGN TABLE test1_parquet_s3_child2 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_2/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__2');

--Testcase 16:
SELECT * FROM test1__parquet_s3_svr__2;
--Testcase 17:
SELECT * FROM test1 ORDER BY timestamp, col3;

-- Multiple files in the same directory
--Testcase 18:
CREATE FOREIGN TABLE test1__parquet_s3_svr__3 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (filename 's3://test-bucket/dir2/file21.parquet s3://test-bucket/dir2/file22.parquet s3://test-bucket/dir2/file23.parquet');

CREATE FOREIGN TABLE test1_parquet_s3_child3 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_3/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__3');

--Testcase 19:
SELECT * FROM test1__parquet_s3_svr__3;
--Testcase 20:
SELECT * FROM test1 ORDER BY timestamp, col3;

-- Multiple files in some directories
--Testcase 21:
CREATE FOREIGN TABLE test1__parquet_s3_svr__4 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (filename 's3://test-bucket/file0.parquet s3://test-bucket/dir1/dir12/file121.parquet');

CREATE FOREIGN TABLE test1_parquet_s3_child4 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_4/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__4');

--Testcase 22:
SELECT * FROM test1__parquet_s3_svr__4;
--Testcase 23:
SELECT * FROM test1 ORDER BY timestamp, col3;


-- **********************************************
-- Foreign table using 'dirname' option
-- **********************************************
-- Only bucket name
--Testcase 24:
CREATE FOREIGN TABLE test1__parquet_s3_svr__5 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (dirname 's3://test-bucket');

CREATE FOREIGN TABLE test1_parquet_s3_child5 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_5/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__5');

--Testcase 25:
SELECT * FROM test1__parquet_s3_svr__5;
--Testcase 26:
SELECT * FROM test1 ORDER BY timestamp, col3;

-- Directory
--Testcase 27:
CREATE FOREIGN TABLE test1__parquet_s3_svr__6 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (dirname 's3://test-bucket/dir1');

CREATE FOREIGN TABLE test1_parquet_s3_child6 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_6/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__6');

--Testcase 28:
SELECT * FROM test1__parquet_s3_svr__6;
--Testcase 29:
SELECT * FROM test1 ORDER BY timestamp, col3;

-- Sub directory
--Testcase 30:
CREATE FOREIGN TABLE test1__parquet_s3_svr__7 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (dirname 's3://test-bucket/dir1/dir11');

CREATE FOREIGN TABLE test1_parquet_s3_child7 PARTITION OF test1 FOR VALUES IN ('/parquet_s3_svr_7/') SERVER pgspider_svr OPTIONS(child_name 'test1__parquet_s3_svr__7');

--Testcase 31:
SELECT * FROM test1__parquet_s3_svr__7;
--Testcase 32:
SELECT * FROM test1 ORDER BY timestamp, col3;


-- **********************************************
-- Error cases
-- So, Don't need create child's foreign table.
-- **********************************************
-- File does not exist
--Testcase 33:
CREATE FOREIGN TABLE test1__parquet_s3_svr__8 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (filename 's3://test-bucket/dummy-file.parquet');
--Testcase 34:
SELECT * FROM test1__parquet_s3_svr__8;

-- Bucket does not exist
--Testcase 35:
CREATE FOREIGN TABLE test1__parquet_s3_svr__9 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (dirname 's3://dummy-bucket');
--Testcase 36:
SELECT * FROM test1__parquet_s3_svr__8;

-- Directory does not exist
--Testcase 37:
CREATE FOREIGN TABLE test1__parquet_s3_svr__10 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (dirname 's3://test-bucket/dummy-dir');
--Testcase 38:
SELECT * FROM test1__parquet_s3_svr__8;

-- Use both options 'filename' and 'dirname'
--Testcase 39:
CREATE FOREIGN TABLE test1__parquet_s3_svr__11 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (filename 's3://test-bucket/dir1/file1.parquet', dirname 's3://test-bucket/dir1');

-- Specify both local file and S3 file
--Testcase 40:
CREATE FOREIGN TABLE test1__parquet_s3_svr__12 (timestamp timestamp, col1 text, col2 bigint, col3 double precision) SERVER parquet_s3_svr OPTIONS (filename 's3://test-bucket/dir1/file1.parquet /tmp/file2.parquet');


-- **********************************************
-- Cleanup
-- **********************************************
--Testcase 41:
DROP EXTENSION parquet_s3_fdw CASCADE;

--Testcase 42:
DROP TABLE test1;
--Testcase 43:
DROP SERVER pgspider_svr CASCADE;
--Testcase 44:
DROP EXTENSION pgspider_ext CASCADE;
