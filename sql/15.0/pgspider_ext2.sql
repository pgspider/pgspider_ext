-- ===================================================================
-- create FDW objects
-- ===================================================================
CREATE EXTENSION postgres_fdw;
-- Create 4 data sources
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER node1 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER node2 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER node3 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER node4 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;

CREATE USER MAPPING FOR CURRENT_USER SERVER node1;
CREATE USER MAPPING FOR CURRENT_USER SERVER node2;
CREATE USER MAPPING FOR CURRENT_USER SERVER node3;
CREATE USER MAPPING FOR CURRENT_USER SERVER node4;

CREATE EXTENSION pgspider_ext;
CREATE SERVER spdsrv FOREIGN DATA WRAPPER pgspider_ext;
CREATE USER MAPPING FOR CURRENT_USER SERVER spdsrv;

SET enable_partitionwise_aggregate TO on;

-- *******************************************************************
-- Test data 1
-- *******************************************************************
-- ===================================================================
-- Create objects used through FDW loopback server
-- ===================================================================
CREATE TABLE test1_node1_src(i integer, t text);
CREATE TABLE test1_node2_src(i integer, t text);
CREATE TABLE test1_node3_src(i integer, t text);
CREATE TABLE test1_node4_src(i integer, t text);

-- ===================================================================
-- Insert test data
-- ===================================================================
-- node1
INSERT INTO test1_node1_src VALUES(1, 'a');
-- node2
INSERT INTO test1_node2_src VALUES(1111, 'b');
-- node3
INSERT INTO test1_node3_src VALUES(1, 'a');
INSERT INTO test1_node3_src VALUES(22222, 'a');
-- node4
INSERT INTO test1_node4_src VALUES(1, 'a');
INSERT INTO test1_node4_src VALUES(777, 'a');
INSERT INTO test1_node4_src VALUES(777, 'b');
INSERT INTO test1_node4_src VALUES(777, 'c');
INSERT INTO test1_node4_src VALUES(777, 'd');

-- ===================================================================
-- Create foreign tables
-- ===================================================================
CREATE FOREIGN TABLE test1_node1(i integer, t text) SERVER node1 OPTIONS(table_name 'test1_node1_src');
CREATE FOREIGN TABLE test1_node2(i integer, t text) SERVER node2 OPTIONS(table_name 'test1_node2_src');
CREATE FOREIGN TABLE test1_node3(i integer, t text) SERVER node3 OPTIONS(table_name 'test1_node3_src');
CREATE FOREIGN TABLE test1_node4(i integer, t text) SERVER node4 OPTIONS(table_name 'test1_node4_src');

-- ===================================================================
-- Create a partition parent table
-- ===================================================================
CREATE TABLE test1(i integer, t text, __spd_url text) PARTITION BY LIST (__spd_url);

-- ===================================================================
-- Add nodes and select tables
-- ===================================================================
CREATE FOREIGN TABLE test1_node1_part PARTITION OF test1 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'test1_node1');
SELECT * FROM test1 ORDER BY i, __spd_url;

CREATE FOREIGN TABLE test1_node2_part PARTITION OF test1 FOR VALUES IN ('/node2/') SERVER spdsrv OPTIONS(child_name 'test1_node2');
SELECT * FROM test1 ORDER BY i, __spd_url;

CREATE FOREIGN TABLE test1_node3_part PARTITION OF test1 FOR VALUES IN ('/node3/') SERVER spdsrv OPTIONS(child_name 'test1_node3');
SELECT * FROM test1 ORDER BY i, __spd_url;

CREATE FOREIGN TABLE test1_node4_part PARTITION OF test1 FOR VALUES IN ('/node4/') SERVER spdsrv OPTIONS(child_name 'test1_node4');
SELECT * FROM test1 ORDER BY i, __spd_url;

-- ===================================================================
-- Execute queries
-- ===================================================================
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM test1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT sum(i), count(i) FROM test1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT stddev(i) FROM test1;

SELECT * FROM test1;
SELECT i FROM test1;
SELECT i + 1 FROM test1;
SELECT t FROM test1;
SELECT * FROM test1 WHERE i = 1;
SELECT * FROM test1 WHERE __spd_url LIKE '/node1/' and i = 1 and t = 'a';
SELECT * FROM test1 WHERE i = 1 ORDER BY i,__spd_url;
SELECT * FROM test1 UNION ALL SELECT * FROM test1 ORDER BY i,__spd_url;
SELECT __spd_url FROM test1 ORDER BY __spd_url;
SELECT i, __spd_url FROM test1 GROUP BY i, __spd_url ORDER BY i;
SELECT t, __spd_url FROM test1 GROUP BY __spd_url, t ORDER BY t;
SELECT __spd_url, i FROM test1 GROUP BY i, __spd_url ORDER BY i;
-- Aggregate function
SELECT sum(i) FROM test1;
SELECT avg(i) FROM test1;
SELECT stddev(i) FROM test1;
SELECT sum(i), t FROM test1 GROUP BY t;
SELECT sum(i), t, count(i) FROM test1 GROUP BY t;
SELECT sum(i), i FROM test1 GROUP BY i ORDER BY i;
SELECT sum(i), count(i), i FROM test1 GROUP BY i ORDER BY i;
SELECT t, sum(i), t FROM test1 GROUP BY i, t ORDER BY i;

SELECT sum(i) AS aa, count(i) FROM test1 GROUP BY i;
SELECT sum(i) AS aa, count(i), i/2, sum(i)/2 FROM test1 GROUP BY i;
SELECT sum(i) AS aa, count(i) FROM test1 GROUP BY i ORDER BY aa;
SELECT sum(i), count(i) FROM test1 GROUP BY i ORDER BY 1;
SELECT i, sum(i) FROM test1 GROUP BY i ORDER BY 1;
-- Aggregate function with partition key
SELECT sum(i), __spd_url FROM test1 GROUP BY i, __spd_url ORDER BY i;
SELECT __spd_url, sum(i) FROM test1 GROUP BY i, __spd_url ORDER BY i;
SELECT __spd_url, sum(i), __spd_url FROM test1 GROUP BY i, __spd_url ORDER BY i;
SELECT __spd_url, count(i), sum(i), __spd_url FROM test1 GROUP BY __spd_url, i ORDER BY i;
-- Prepared statement
PREPARE stmt AS SELECT sum(i),count(i),i FROM test1 GROUP BY i ORDER BY i;
EXECUTE stmt;
DO $$
BEGIN
   FOR counter IN 1..50 LOOP
   EXECUTE 'EXECUTE stmt;';
   END LOOP;
END; $$;
DEALLOCATE stmt;
SELECT * FROM (SELECT sum(i) FROM test1) A,(SELECT count(i) FROM test1) B;
-- ===================================================================
-- Clean up
-- ===================================================================
DROP FOREIGN TABLE test1_node1_part;
DROP FOREIGN TABLE test1_node2_part;
DROP FOREIGN TABLE test1_node3_part;
DROP FOREIGN TABLE test1_node4_part;
DROP TABLE test1;
DROP FOREIGN TABLE test1_node1;
DROP FOREIGN TABLE test1_node2;
DROP FOREIGN TABLE test1_node3;
DROP FOREIGN TABLE test1_node4;
DROP TABLE test1_node1_src;
DROP TABLE test1_node2_src;
DROP TABLE test1_node3_src;
DROP TABLE test1_node4_src;
DROP USER MAPPING FOR CURRENT_USER SERVER spdsrv;
DROP USER MAPPING FOR CURRENT_USER SERVER node1;
DROP USER MAPPING FOR CURRENT_USER SERVER node2;
DROP USER MAPPING FOR CURRENT_USER SERVER node3;
DROP USER MAPPING FOR CURRENT_USER SERVER node4;
DROP SERVER spdsrv;
DROP SERVER node1;
DROP SERVER node2;
DROP SERVER node3;
DROP SERVER node4;
DROP EXTENSION pgspider_ext;
DROP EXTENSION postgres_fdw;
-- End

