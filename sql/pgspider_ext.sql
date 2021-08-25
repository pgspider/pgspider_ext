-- ===================================================================
-- Create FDW objects
-- ===================================================================
CREATE EXTENSION postgres_fdw;

DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;

CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;


CREATE EXTENSION pgspider_ext;
CREATE SERVER spdsrv FOREIGN DATA WRAPPER pgspider_ext;
CREATE USER MAPPING FOR CURRENT_USER SERVER spdsrv;

-- ===================================================================
-- Create objects used through FDW loopback server
-- ===================================================================
CREATE TABLE tbl1_a_src(col1 integer primary key, col2 integer, col3 text);
CREATE TABLE tbl1_b_src(col1 integer primary key, col2 integer, col3 text);
CREATE TABLE tbl1_c_src(col1 integer primary key, col2 integer, col3 text);

-- ===================================================================
-- Insert test data
-- ===================================================================
INSERT INTO tbl1_a_src VALUES(1, 11, 'valA1');
INSERT INTO tbl1_a_src VALUES(2, 12, 'valA2');
INSERT INTO tbl1_a_src VALUES(3, 12, 'valA3');
INSERT INTO tbl1_a_src VALUES(4, 13, 'valA4');
INSERT INTO tbl1_a_src VALUES(5, 13, 'valA5');
INSERT INTO tbl1_a_src VALUES(6, 13, 'valA6');

INSERT INTO tbl1_b_src VALUES(1, 21, 'valB1');
INSERT INTO tbl1_b_src VALUES(2, 22, 'valB2');
INSERT INTO tbl1_b_src VALUES(3, 22, 'valB3');
INSERT INTO tbl1_b_src VALUES(4, 23, 'valB4');
INSERT INTO tbl1_b_src VALUES(5, 23, 'valB5');
INSERT INTO tbl1_b_src VALUES(6, 23, 'valB6');
INSERT INTO tbl1_b_src VALUES(7, 24, 'valB7');

INSERT INTO tbl1_c_src VALUES(1, 31, 'valC1');
INSERT INTO tbl1_c_src VALUES(2, 32, 'valC2');
INSERT INTO tbl1_c_src VALUES(3, 32, 'valC3');
INSERT INTO tbl1_c_src VALUES(4, 33, 'valC4');
INSERT INTO tbl1_c_src VALUES(5, 33, 'valC5');
INSERT INTO tbl1_c_src VALUES(6, 33, 'valC6');
INSERT INTO tbl1_c_src VALUES(7, 34, 'valC7');
INSERT INTO tbl1_c_src VALUES(8, 35, 'valC8');
INSERT INTO tbl1_c_src VALUES(9, 35, 'valC8');

-- ===================================================================
-- Create foreign tables
-- ===================================================================
CREATE FOREIGN TABLE tbl1_a_child(col1 integer, col2 integer, col3 text) SERVER loopback OPTIONS(table_name 'tbl1_a_src');
CREATE FOREIGN TABLE tbl1_b_child(col1 integer, col2 integer, col3 text) SERVER loopback OPTIONS(table_name 'tbl1_b_src');
CREATE FOREIGN TABLE tbl1_c_cld(col1 integer, col2 integer, col3 text) SERVER loopback OPTIONS(table_name 'tbl1_c_src');

CREATE TABLE tbl1(col1 int, col2 integer, col3 text, spdurl text) PARTITION BY LIST (spdurl);
CREATE FOREIGN TABLE tbl1_a PARTITION OF tbl1 FOR VALUES IN ('/node1/') SERVER spdsrv;
CREATE FOREIGN TABLE tbl1_b PARTITION OF tbl1 FOR VALUES IN ('/node2/') SERVER spdsrv;
CREATE FOREIGN TABLE tbl1_c PARTITION OF tbl1 FOR VALUES IN ('/node3/') SERVER spdsrv OPTIONS(child_name 'tbl1_c_cld');

-- ===================================================================
-- Execute queries
-- ===================================================================
SELECT col1 FROM tbl1;
SELECT col3 FROM tbl1 WHERE col2 > 22;
SELECT col2, col3 FROM tbl1 WHERE col1 % 2 = 0;
SELECT col3, col2 * col1 FROM tbl1 WHERE col1 % 2 = 1 AND col2 % 2 = 1;
SELECT col2, col3 FROM tbl1 WHERE col3 LIKE 'val%_2' OR col3 LIKE 'val_4' ORDER BY col2 DESC;
SELECT col3 FROM tbl1 WHERE col1 IN (SELECT DISTINCT col2 % 30 FROM tbl1);
SELECT col1, spdurl FROM tbl1;
SELECT spdurl || col3 FROM tbl1;
SELECT spdurl, col2 FROM tbl1 WHERE spdurl LIKE '/node1/';
SELECT col1, spdurl FROM tbl1 WHERE spdurl || col3 = '/node1/valA1';
SELECT col3, spdurl FROM tbl1 WHERE spdurl || col3 = '/node2/valB2';

-- Disable to pushdown aggregate
SET enable_partitionwise_aggregate TO off;

SELECT count(col2) FROM tbl1;
SELECT col2, sum(col1) FROM tbl1 GROUP BY col2;
SELECT col2, sum(col1) FROM tbl1 GROUP BY col2 HAVING col2 % 2 = 1;
SELECT sum(col1) FROM tbl1 GROUP BY spdurl;
SELECT spdurl, sum(col1) FROM tbl1 GROUP BY spdurl;
SELECT col2, sum(col1) FROM tbl1 GROUP BY spdurl, col2;
SELECT spdurl, col2, sum(col1) FROM tbl1 GROUP BY spdurl, col2;

-- Enable to pushdown aggregate
SET enable_partitionwise_aggregate TO on;

SELECT count(col2) FROM tbl1;
SELECT col2, sum(col1) FROM tbl1 GROUP BY col2;
SELECT col2, sum(col1) FROM tbl1 GROUP BY col2 HAVING col2 % 2 = 1;
SELECT sum(col1) FROM tbl1 GROUP BY spdurl;
SELECT spdurl, sum(col1) FROM tbl1 GROUP BY spdurl;
SELECT col2, sum(col1) FROM tbl1 GROUP BY spdurl, col2;
SELECT spdurl, col2, sum(col1) FROM tbl1 GROUP BY spdurl, col2;

-- ===================================================================
-- Clean up
-- ===================================================================
DROP FOREIGN TABLE tbl1_a;
DROP FOREIGN TABLE tbl1_b;
DROP FOREIGN TABLE tbl1_c;
DROP TABLE tbl1;
DROP FOREIGN TABLE tbl1_a_child;
DROP FOREIGN TABLE tbl1_b_child;
DROP FOREIGN TABLE tbl1_c_cld;
DROP TABLE tbl1_a_src;
DROP TABLE tbl1_b_src;
DROP TABLE tbl1_c_src;
DROP USER MAPPING FOR CURRENT_USER SERVER spdsrv;
DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
DROP SERVER spdsrv;
DROP SERVER loopback;
DROP EXTENSION pgspider_ext;
DROP EXTENSION postgres_fdw;
-- End
