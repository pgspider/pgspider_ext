-- ===================================================================
-- create FDW objects
-- ===================================================================
\set ECHO none
\ir sql/parameters/mongodb_parameters.conf
\set ECHO all

--Testcase 1:
CREATE EXTENSION mongo_fdw;
--Testcase 2:
CREATE SERVER testserver1 FOREIGN DATA WRAPPER mongo_fdw;

--Testcase 3:
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);

--Testcase 4:
CREATE SERVER mongo_server2 FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);

--Testcase 814:
CREATE SERVER mongo_server3 FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);

--Testcase 5:
CREATE USER MAPPING FOR public SERVER testserver1
	OPTIONS (username 'value', password 'value');

--Testcase 6:
CREATE USER MAPPING FOR CURRENT_USER SERVER mongo_server;
--Testcase 7:
CREATE USER MAPPING FOR CURRENT_USER SERVER mongo_server2;
--Testcase 815:
CREATE USER MAPPING FOR public SERVER mongo_server3;

--Testcase 8:
CREATE EXTENSION pgspider_ext;
--Testcase 9:
CREATE SERVER spdsrv FOREIGN DATA WRAPPER pgspider_ext;
--Testcase 10:
CREATE USER MAPPING FOR CURRENT_USER SERVER spdsrv;

-- ===================================================================
-- create objects used through FDW loopback server
-- ===================================================================
--Testcase 11:
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
--Testcase 12:
CREATE SCHEMA "S 1";
-- IMPORT FOREIGN SCHEMA public FROM SERVER mongo_server1 INTO "S 1";

--Testcase 13:
CREATE FOREIGN TABLE "S 1"."T 1" (
	_id name,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10),
	c8 text
) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'T1');

--Testcase 14:
INSERT INTO "S 1"."T 1"
	SELECT id,
		   id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
	       '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
	       id % 10,
	       id % 10,
	       'foo'
	FROM generate_series(1, 1000) id;

--Testcase 15:
CREATE FOREIGN TABLE "S 1"."T 0" (
	_id name,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10),
	c8 text
) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'T0');

--Testcase 16:
INSERT INTO "S 1"."T 0"
	SELECT id,
	       id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
	       '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
	       id % 10,
	       id % 10,
	       'foo'
	FROM generate_series(1, 1000) id;

--Testcase 17:
CREATE FOREIGN TABLE "S 1"."T 2" (
	_id name,
	c1 int NOT NULL,
	c2 text
) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'T2');

--Testcase 18:
INSERT INTO "S 1"."T 2"
	SELECT id,
	       id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;

--Testcase 19:
CREATE FOREIGN TABLE "S 1"."T 3" (
	_id name,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'T3');

--Testcase 20:
INSERT INTO "S 1"."T 3"
	SELECT id,
	       id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;

--Testcase 21:
DELETE FROM "S 1"."T 3" WHERE c1 % 2 != 0;	-- delete for outer join tests

--Testcase 22:
CREATE FOREIGN TABLE "S 1"."T 4" (
	_id name,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'T4');

--Testcase 23:
INSERT INTO "S 1"."T 4"
	SELECT id,
	       id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;

--Testcase 24:
DELETE FROM "S 1"."T 4" WHERE c1 % 3 != 0;	-- delete for outer join tests

-- -- Disable autovacuum for these tables to avoid unexpected effects of that
-- ALTER TABLE "S 1"."T 1" SET (autovacuum_enabled = 'false');
-- ALTER TABLE "S 1"."T 2" SET (autovacuum_enabled = 'false');
-- ALTER TABLE "S 1"."T 3" SET (autovacuum_enabled = 'false');
-- ALTER TABLE "S 1"."T 4" SET (autovacuum_enabled = 'false');

ANALYZE "S 1"."T 1";
ANALYZE "S 1"."T 2";
ANALYZE "S 1"."T 3";
ANALYZE "S 1"."T 4";

-- ===================================================================
-- create foreign tables
-- ===================================================================
--Testcase 25:
CREATE FOREIGN TABLE ft1_a_child (
	_id name,
	c0 int,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 text
) SERVER mongo_server;
--Testcase 26:
ALTER FOREIGN TABLE ft1_a_child DROP COLUMN c0;

--Testcase 27:
CREATE FOREIGN TABLE ft2_a_child (
	_id name,
	c1 int NOT NULL,
	c2 int NOT NULL,
	cx int,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 text
) SERVER mongo_server;
--Testcase 28:
ALTER FOREIGN TABLE ft2_a_child DROP COLUMN cx;

--Testcase 29:
CREATE FOREIGN TABLE ft4_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'T3');

--Testcase 30:
CREATE FOREIGN TABLE ft5_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'T4');

--Testcase 31:
CREATE FOREIGN TABLE ft6_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'T4');

--Testcase 816:
CREATE FOREIGN TABLE ft7_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'T4');

-- ===================================================================
-- tests for validator
-- ===================================================================
-- requiressl and some other parameters are omitted because
-- valid values for them depend on configure options
-- ALTER SERVER testserver1 OPTIONS (
-- 	use_remote_estimate 'false',
-- 	updatable 'true',
-- 	fdw_startup_cost '123.456',
-- 	fdw_tuple_cost '0.123',
-- 	service 'value',
-- 	connect_timeout 'value',
-- 	dbname 'value',
-- 	host 'value',
-- 	hostaddr 'value',
-- 	port 'value',
-- 	--client_encoding 'value',
-- 	application_name 'value',
-- 	--fallback_application_name 'value',
-- 	keepalives 'value',
-- 	keepalives_idle 'value',
-- 	keepalives_interval 'value',
-- 	tcp_user_timeout 'value',
-- 	-- requiressl 'value',
-- 	sslcompression 'value',
-- 	sslmode 'value',
-- 	sslcert 'value',
-- 	sslkey 'value',
-- 	sslrootcert 'value',
-- 	sslcrl 'value',
-- 	--requirepeer 'value',
-- 	krbsrvname 'value',
-- 	gsslib 'value'
-- 	--replication 'value'
-- );

-- MongoDB FDW does not support extensions option.
-- Error, invalid list syntax
-- ALTER SERVER mongo_server1 OPTIONS (ADD extensions 'foo; bar');

-- -- OK but gets a warning
-- ALTER SERVER mongo_server1 OPTIONS (ADD extensions 'foo, bar');
-- ALTER SERVER mongo_server1 OPTIONS (DROP extensions);

--Testcase 32:
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (DROP username, DROP password);

-- Attempt to add a valid option that's not allowed in a user mapping
-- ALTER USER MAPPING FOR public SERVER mongo_server1
-- 	OPTIONS (ADD sslmode 'require');

-- But we can add valid ones fine
-- ALTER USER MAPPING FOR public SERVER mongo_server1
-- 	OPTIONS (ADD sslpassword 'dummy');

-- Ensure valid options we haven't used in a user mapping yet are
-- permitted to check validation.
-- ALTER USER MAPPING FOR public SERVER mongo_server1
-- 	OPTIONS (ADD sslkey 'value', ADD sslcert 'value');

--Testcase 33:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (database 'mongo_fdw_post_regress', collection 'T1');
--Testcase 34:
ALTER FOREIGN TABLE ft2_a_child OPTIONS (database 'mongo_fdw_post_regress', collection 'T1');
-- MongoDB FDW does not support column_name option
-- ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c1 OPTIONS (column_name 'C 1');
-- ALTER FOREIGN TABLE ft2_a_child ALTER COLUMN c1 OPTIONS (column_name 'C 1');
--Testcase 35:
\det+

--Testcase 36:
CREATE TABLE ft1(
	_id name,
	c1 int,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 text,
	spdurl text) PARTITION BY LIST (spdurl);
--Testcase 37:
CREATE FOREIGN TABLE ft1_a PARTITION OF ft1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 38:
CREATE TABLE ft2(
	_id name,
	c1 int,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 text,
	spdurl text) PARTITION BY LIST (spdurl);
--Testcase 39:
CREATE FOREIGN TABLE ft2_a PARTITION OF ft2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 40:
CREATE TABLE ft4 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text) PARTITION BY LIST (spdurl);

--Testcase 41:
CREATE FOREIGN TABLE ft4_a PARTITION OF ft4 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 42:
CREATE TABLE ft5 (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	spdurl text) PARTITION BY LIST (spdurl);

--Testcase 43:
CREATE FOREIGN TABLE ft5_a PARTITION OF ft5 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 44:
CREATE TABLE ft6 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text) PARTITION BY LIST (spdurl);

--Testcase 45:
CREATE FOREIGN TABLE ft6_a PARTITION OF ft6 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 817:
CREATE TABLE ft7 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text) PARTITION BY LIST (spdurl);

--Testcase 851:
CREATE FOREIGN TABLE ft7_a PARTITION OF ft7 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- Enable to pushdown aggregate
--Testcase 46:
SET enable_partitionwise_aggregate TO on;
--Testcase 47:
SET parallel_leader_participation = 'off';

-- Test that alteration of server options causes reconnection
-- Remote's errors might be non-English, so hide them to ensure stable results
\set VERBOSITY terse
--Testcase 48:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
--Testcase 49:
ALTER TABLE ft1_a_child OPTIONS (SET database 'no such database');
--Testcase 50:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
--Testcase 51:
ALTER TABLE ft1_a_child OPTIONS (SET database 'mongo_fdw_post_regress');
--Testcase 52:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again

-- -- Test that alteration of user mapping options causes reconnection
--Testcase 53:
DROP USER MAPPING FOR CURRENT_USER SERVER mongo_server;
--Testcase 54:
CREATE USER MAPPING FOR CURRENT_USER SERVER mongo_server
  OPTIONS (username 'wrong', password 'wrong');
--Testcase 55:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
--Testcase 56:
ALTER USER MAPPING FOR CURRENT_USER SERVER mongo_server
  OPTIONS (DROP username);
--Testcase 57:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
\set VERBOSITY default

-- -- Now we should be able to run ANALYZE.
-- -- To exercise multiple code paths, we use local stats on ft1
-- -- and remote-estimate mode on ft2.
-- ANALYZE ft1;
-- ALTER FOREIGN TABLE ft2 OPTIONS (use_remote_estimate 'true');
-- ===================================================================
-- test error case for create publication on foreign table
-- ===================================================================
--Testcase 818:
CREATE PUBLICATION testpub_ftbl FOR TABLE ft1_a_child;  -- should fail
-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias
--Testcase 58:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
--Testcase 59:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
-- single table with alias - also test that tableoid sort is not pushed to remote side
-- EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference
--Testcase 60:
EXPLAIN (VERBOSE, COSTS OFF) SELECT (t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8) FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 61:
SELECT (t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8) FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result
--Testcase 62:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft1 WHERE false;
-- with WHERE clause
--Testcase 63:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
--Testcase 64:
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
-- with FOR UPDATE/SHARE
--Testcase 65:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c1 = 101;
--Testcase 66:
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c1 = 101;
--Testcase 67:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c1 = 102;
--Testcase 68:
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c1 = 102;
-- aggregate
--Testcase 69:
SELECT COUNT(*) FROM ft1 t1;
-- subquery
--Testcase 70:
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX
--Testcase 71:
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;
-- used in CTE
--Testcase 72:
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values
--Testcase 73:
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.
--Testcase 74:
SET enable_hashjoin TO false;
--Testcase 75:
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 76:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2.c1 FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2.c1) OFFSET 100 LIMIT 10;
--Testcase 77:
SELECT t1.c1, t2.c1 FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2.c1) OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 78:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2.c1 FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2.c1) OFFSET 100 LIMIT 10;
--Testcase 79:
SELECT t1.c1, t2.c1 FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2.c1) OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
--Testcase 80:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1 FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1.c1) OFFSET 100 LIMIT 10;
--Testcase 81:
SELECT t1.c1 FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1.c1) OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
--Testcase 82:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1.c1) OFFSET 100 LIMIT 10;
--Testcase 83:
SELECT t1.c1, t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1.c1) OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins
--Testcase 84:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1.c1) OFFSET 100 LIMIT 10;
--Testcase 85:
SELECT t1.c1, t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1.c1) OFFSET 100 LIMIT 10;
--Testcase 86:
RESET enable_hashjoin;
--Testcase 87:
RESET enable_nestloop;

-- Test executing assertion in estimate_path_cost_size() that makes sure that
-- retrieved_rows for foreign rel re-used to cost pre-sorted foreign paths is
-- a sensible value even when the rel has tuples=0
-- CREATE TABLE loct_empty (c1 int NOT NULL, c2 text);
--Testcase 88:
CREATE FOREIGN TABLE ft_empty_a_child (_id name, c1 int NOT NULL, c2 text)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'loct_empty');
--Testcase 89:
CREATE TABLE ft_empty (_id name, c1 int NOT NULL, c2 text, spdurl text)
   PARTITION BY LIST (spdurl);
--Testcase 90:
CREATE FOREIGN TABLE ft_empty_a PARTITION OF ft_empty FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 91:
INSERT INTO ft_empty_a_child
  SELECT id, id, 'AAA' || to_char(id, 'FM000') FROM generate_series(1, 100) id;
--Testcase 92:
DELETE FROM ft_empty_a_child;
ANALYZE ft_empty;
--Testcase 93:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_empty ORDER BY c1;

-- test restriction on non-system foreign tables.
SET restrict_nonsystem_relation_kind TO 'foreign-table';
--Testcase 936:
SELECT * from ft1 where c1 < 1; -- ERROR
--Testcase 937:
INSERT INTO ft1 (c1) VALUES (1); -- ERROR due to the missing spdurl column during the insert.
--Testcase 938:
INSERT INTO ft1 (c1, spdurl) VALUES (1, '/node1/'); -- ERROR due to not supporting foreign insert.
--Testcase 939:
DELETE FROM ft1 WHERE c1 = 1; -- ERROR
TRUNCATE ft1; -- ERROR
RESET restrict_nonsystem_relation_kind;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
--Testcase 94:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
--Testcase 95:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
--Testcase 96:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c3 IS NULL;        -- NullTest
--Testcase 97:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c3 IS NOT NULL;    -- NullTest
--Testcase 98:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
--Testcase 99:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
--Testcase 101:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
--Testcase 102:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
--Testcase 103:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef
--Testcase 104:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
--Testcase 105:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote
-- parameterized remote path for foreign table
--Testcase 106:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT a.c1, a.c2, a.c3, a.c4, a.c5, a.c6, a.c7, a.c8, b.c1, b.c2, b.c3, b.c4, b.c5, b.c6, b.c7, b.c8 FROM "S 1"."T 1" a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;
--Testcase 107:
SELECT a.c1, a.c2, a.c3, a.c4, a.c5, a.c6, a.c7, a.c8, b.c1, b.c2, b.c3, b.c4, b.c5, b.c6, b.c7, b.c8 FROM "S 1"."T 1" a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;

-- check both safe and unsafe join conditions
--Testcase 108:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT a.c1, a.c2, a.c3, a.c4, a.c5, a.c6, a.c7, a.c8, b.c1, b.c2, b.c3, b.c4, b.c5, b.c6, b.c7, b.c8 FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
--Testcase 109:
SELECT a.c1, a.c2, a.c3, a.c4, a.c5, a.c6, a.c7, a.c8, b.c1, b.c2, b.c3, b.c4, b.c5, b.c6, b.c7, b.c8 FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
--Testcase 110:
SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5));
--Testcase 111:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8 FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5));

-- user-defined operator/function
--Testcase 114:
CREATE FUNCTION mongo_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
--Testcase 115:
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- built-in operators and functions can be shipped for remote execution
--Testcase 116:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 117:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 118:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 119:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot
--Testcase 120:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = mongo_fdw_abs(t1.c2);
--Testcase 121:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = mongo_fdw_abs(t1.c2);
--Testcase 122:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 123:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ORDER BY can be shipped, though
--Testcase 124:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 125:
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- MongoDB FDW not support extensions option
-- but let's put them in an extension ...
--Testcase 126:
ALTER EXTENSION mongo_fdw ADD FUNCTION mongo_fdw_abs(int);
--Testcase 127:
ALTER EXTENSION mongo_fdw ADD OPERATOR === (int, int);
-- ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

-- ... now they can be shipped
--Testcase 128:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = mongo_fdw_abs(t1.c2);
--Testcase 129:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = mongo_fdw_abs(t1.c2);
--Testcase 130:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 131:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- and both ORDER BY and LIMIT can be shipped
--Testcase 132:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 133:
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- Ensure we don't ship FETCH FIRST .. WITH TIES
--Testcase 934:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c2 FROM ft1 t1 WHERE t1.c1 > 960 ORDER BY t1.c2 FETCH FIRST 2 ROWS WITH TIES;
--Testcase 935:
SELECT t1.c2 FROM ft1 t1 WHERE t1.c1 > 960 ORDER BY t1.c2 FETCH FIRST 2 ROWS WITH TIES;

-- Test CASE pushdown
-- MongoDB not support CASE expressions
--Testcase 819:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;
--Testcase 820:
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;

-- Nested CASE
--Testcase 821:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;
--Testcase 822:
SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;

-- CASE arg WHEN
--Testcase 823:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE mod(c1, 4) WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- CASE cannot be pushed down because of unshippable arg clause
--Testcase 824:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE random()::integer WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- these are shippable
--Testcase 825:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c6 WHEN 'foo' THEN true ELSE c3 < 'bar' END;
--Testcase 826:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 WHEN c6 THEN true ELSE c3 < 'bar' END;

-- but this is not because of collation
--Testcase 827:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 COLLATE "C" WHEN c6 THEN true ELSE c3 < 'bar' END;

-- a regconfig constant referring to this text search configuration
-- is initially unshippable
--Testcase 845:
CREATE TEXT SEARCH CONFIGURATION public.custom_search
  (COPY = pg_catalog.english);
--Testcase 846:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
--Testcase 847:
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
-- but if it's in a shippable extension, it can be shipped
ALTER EXTENSION mongo_fdw ADD TEXT SEARCH CONFIGURATION public.custom_search;
-- however, that doesn't flush the shippability cache, so do a quick reconnect
\c -
-- Enable to pushdown aggregate
SET enable_partitionwise_aggregate TO on;
SET parallel_leader_participation = 'off';
--Testcase 852:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
--Testcase 853:
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
ALTER EXTENSION mongo_fdw DROP TEXT SEARCH CONFIGURATION public.custom_search;
--Testcase 848:
DROP TEXT SEARCH CONFIGURATION public.custom_search;

-- ===================================================================
-- ORDER BY queries
-- ===================================================================
-- we should not push order by clause with volatile expressions or unsafe
-- collations
--Testcase 889:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, random();
--Testcase 890:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- Ensure we don't push ORDER BY expressions which are Consts at the UNION
-- child level to the foreign server.
--Testcase 891:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM (
    SELECT 1 AS type,c1 FROM ft1
    UNION ALL
    SELECT 2 AS type,c1 FROM ft2
) a ORDER BY type,c1;

--Testcase 892:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM (
    SELECT 1 AS type,c1 FROM ft1
    UNION ALL
    SELECT 2 AS type,c1 FROM ft2
) a ORDER BY type;

-- ===================================================================
-- JOIN queries
-- ===================================================================
-- Analyze ft4 and ft5 so that we have better statistics. These tables do not
-- have use_remote_estimate set.
-- ANALYZE ft4;
-- ANALYZE ft5;

-- join two tables
--Testcase 134:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 135:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join three tables
--Testcase 136:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 137:
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join
--Testcase 138:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 139:
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables
--Testcase 140:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 141:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
--Testcase 142:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
--Testcase 143:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
--Testcase 144:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
--Testcase 145:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
-- right outer join
--Testcase 146:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 147:
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
-- right outer join three tables
--Testcase 148:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 149:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join
--Testcase 150:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
--Testcase 151:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
--Testcase 152:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 153:
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 154:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
--Testcase 155:
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
-- b. one of the joining relations is a base relation and the other is a join
-- relation
--Testcase 156:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 157:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries
--Testcase 158:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 159:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries
--Testcase 160:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
--Testcase 161:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join
--Testcase 162:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
--Testcase 163:
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
-- full outer join three tables
--Testcase 164:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 165:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + right outer join
--Testcase 166:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 167:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + full outer join
--Testcase 168:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 169:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + left outer join
--Testcase 170:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 171:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + full outer join
--Testcase 172:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 173:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + left outer join
--Testcase 174:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 175:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + right outer join
--Testcase 176:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 177:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows
--Testcase 178:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 179:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set
--Testcase 180:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE mongo_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
-- ALTER SERVER mongo_server OPTIONS (DROP extensions);
-- full outer join + WHERE clause with shippable extensions not set
--Testcase 181:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE mongo_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
-- ALTER SERVER mongo_server OPTIONS (ADD extensions 'mongo_fdw');
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
--Testcase 182:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 183:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 184:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
--Testcase 185:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause
--Testcase 186:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 187:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 188:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
--Testcase 189:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE
--Testcase 190:
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
--Testcase 191:
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference
--Testcase 192:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- SEMI JOIN, not pushed down
--Testcase 193:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 194:
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down
--Testcase 195:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 196:
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- CROSS JOIN can be pushed down
--Testcase 197:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 198:
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.
--Testcase 199:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 200:
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
--Testcase 201:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 202:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
--Testcase 203:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 204:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
--Testcase 205:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 206:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs
--Testcase 207:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
--Testcase 208:
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference
--Testcase 209:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 210:
SELECT t1.c1 FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1.c1 OFFSET 10 LIMIT 10;

-- join with pseudoconstant quals, not pushed down.
--Testcase 875:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1 AND CURRENT_USER = SESSION_USER) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
--Testcase 211:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;
--Testcase 212:
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
--Testcase 213:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;
--Testcase 214:
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;

-- join with nullable side with some columns with null values
--Testcase 215:
UPDATE ft5_a_child SET c3 = null where c1 % 9 = 0;
--Testcase 216:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;
--Testcase 217:
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
--Testcase 218:
CREATE TABLE local_tbl (c1 int NOT NULL, c2 int NOT NULL, c3 text, CONSTRAINT local_tbl_pkey PRIMARY KEY (c1));
--Testcase 219:
INSERT INTO local_tbl SELECT id, id % 10, to_char(id, 'FM0000') FROM generate_series(1, 1000) id;
ANALYZE local_tbl;
--Testcase 220:
SET enable_nestloop TO false;
--Testcase 221:
SET enable_hashjoin TO false;
--Testcase 222:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8, ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft4.c1, ft4.c2, ft4.c3, ft5.c1, ft5.c2, ft5.c3, local_tbl FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;
--Testcase 223:
SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8, ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft4.c1, ft4.c2, ft4.c3, ft5.c1, ft5.c2, ft5.c3, local_tbl FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 ORDER BY ft1.c1 FOR UPDATE;
--Testcase 224:
RESET enable_nestloop;
--Testcase 225:
RESET enable_hashjoin;

-- test that add_paths_with_pathkeys_for_rel() arranges for the epq_path to
-- return columns needed by the parent ForeignScan node
--Testcase 849:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.*, COALESCE(ft1.c3 || ft2.c3, 'foobar') FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100)) ss ON (local_tbl.c1 = ss.c1) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;

-- ALTER SERVER loopback OPTIONS (DROP extensions);
-- ALTER SERVER loopback OPTIONS (ADD fdw_startup_cost '10000.0');
--Testcase 850:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.* FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100 AND (ft1.c1 - mongo_fdw_abs(ft2.c2)) = 0)) ss ON (local_tbl.c3 = ss.c3) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;
-- ALTER SERVER loopback OPTIONS (DROP fdw_startup_cost);
-- ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

--Testcase 226:
DROP TABLE local_tbl;

-- check join pushdown in situations where multiple userids are involved
--Testcase 227:
CREATE ROLE regress_view_owner SUPERUSER;
--Testcase 228:
CREATE USER MAPPING FOR regress_view_owner SERVER mongo_server;
--Testcase 893:
CREATE USER MAPPING FOR regress_view_owner SERVER spdsrv;
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;

--Testcase 229:
CREATE VIEW v4 AS SELECT * FROM ft4;
--Testcase 230:
CREATE VIEW v5 AS SELECT * FROM ft5;
--Testcase 231:
ALTER VIEW v5 OWNER TO regress_view_owner;
--Testcase 232:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
--Testcase 233:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 234:
ALTER VIEW v4 OWNER TO regress_view_owner;
--Testcase 235:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 236:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 237:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
--Testcase 238:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 239:
ALTER VIEW v4 OWNER TO CURRENT_USER;
--Testcase 240:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 241:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 242:
ALTER VIEW v4 OWNER TO regress_view_owner;

-- ====================================================================
-- Check that userid to use when querying the remote table is correctly
-- propagated into foreign rels present in subqueries under an UNION ALL
-- ====================================================================
--Testcase 876:
CREATE ROLE regress_view_owner_another;
--Testcase 877:
ALTER VIEW v4 OWNER TO regress_view_owner_another;
--Testcase 878:
GRANT SELECT ON ft4 TO regress_view_owner_another;
-- ALTER FOREIGN TABLE ft4_a_child OPTIONS (use_remote_estimate 'true');
-- The following should query the remote backing table of ft4 as user
-- regress_view_owner_another, the view owner, though it fails as expected
-- due to the lack of a user mapping for that user.
--Testcase 879:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;
-- Likewise, but with the query under an UNION ALL
--Testcase 880:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT * FROM v4 UNION ALL SELECT * FROM v4);
-- Should not get that error once a user mapping is created
--Testcase 881:
CREATE USER MAPPING FOR regress_view_owner_another SERVER mongo_server;
--Testcase 882:
CREATE USER MAPPING FOR regress_view_owner_another SERVER spdsrv;
--Testcase 883:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;
--Testcase 884:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT * FROM v4 UNION ALL SELECT * FROM v4);
--Testcase 885:
DROP USER MAPPING FOR regress_view_owner_another SERVER mongo_server;
--Testcase 886:
DROP USER MAPPING FOR regress_view_owner_another SERVER spdsrv;
--Testcase 887:
DROP OWNED BY regress_view_owner_another;
--Testcase 888:
DROP ROLE regress_view_owner_another;
-- ALTER FOREIGN TABLE ft4_a_child OPTIONS (use_remote_estimate 'false');

-- cleanup
--Testcase 243:
DROP OWNED BY regress_view_owner;
--Testcase 244:
DROP ROLE regress_view_owner;


-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================

-- Simple aggregates
--Testcase 245:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;
--Testcase 246:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

--Testcase 247:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;
--Testcase 248:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()
--Testcase 249:
explain (verbose, costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query
--Testcase 250:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;
--Testcase 251:
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel
--Testcase 252:
explain (verbose, costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions
--Testcase 253:
explain (verbose, costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;
--Testcase 254:
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are pushed down.
set enable_incremental_sort = off;
--Testcase 255:
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
--Testcase 256:
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
reset enable_incremental_sort;

-- Aggregate is still pushed down by taking unshippable expression out
--Testcase 257:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;
--Testcase 258:
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
--Testcase 259:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
--Testcase 260:
explain (verbose, costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
--Testcase 261:
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
--Testcase 262:
explain (verbose, costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);
--Testcase 263:
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability
--Testcase 264:
explain (verbose, costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;
--Testcase 265:
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
--Testcase 266:
explain (verbose, costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
--Testcase 267:
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
--Testcase 268:
explain (verbose, costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
--Testcase 269:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1;
--Testcase 270:
select exists(select 1 from pg_enum), sum(c1) from ft1;

--Testcase 271:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;
--Testcase 272:
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;


-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order
--Testcase 273:
explain (verbose, costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
--Testcase 274:
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC
--Testcase 275:
explain (verbose, costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;
--Testcase 276:
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate
--Testcase 277:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 278:
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
--Testcase 279:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 280:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 281:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 282:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- FILTER within aggregate
--Testcase 283:
explain (verbose, costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
--Testcase 284:
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
--Testcase 285:
explain (verbose, costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;
--Testcase 286:
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query
--Testcase 287:
explain (verbose, costs off)
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 288:
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
-- Inner query is aggregation query
--Testcase 289:
explain (verbose, costs off)
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 290:
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
--Testcase 291:
explain (verbose, costs off)
select sum(c1) filter (where (c1 / c1) * random() <= 1) from ft1 group by c2 order by 1;
--Testcase 292:
explain (verbose, costs off)
select sum(c2) filter (where c2 in (select c2 from ft1 where c2 < 5)) from ft1;

-- Ordered-sets within aggregate
--Testcase 293:
explain (verbose, costs off)
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;
--Testcase 294:
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

-- Using multiple arguments within aggregates
--Testcase 295:
explain (verbose, costs off)
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;
--Testcase 296:
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC
--Testcase 297:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
--Testcase 298:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.
--Testcase 299:
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate
--Testcase 300:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension
--Testcase 301:
alter extension mongo_fdw add function least_accum(anyelement, variadic anyarray);
--Testcase 302:
alter extension mongo_fdw add aggregate least_agg(variadic items anyarray);
-- alter server loopback options (set extensions 'postgres_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.
--Testcase 303:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;
--Testcase 304:
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension
--Testcase 305:
alter extension mongo_fdw drop function least_accum(anyelement, variadic anyarray);
--Testcase 306:
alter extension mongo_fdw drop aggregate least_agg(variadic items anyarray);
-- alter server loopback options (set extensions 'postgres_fdw');

-- Not pushed down as we have dropped objects from extension.
--Testcase 307:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup
--Testcase 308:
reset enable_hashagg;
--Testcase 309:
drop aggregate least_agg(variadic items anyarray);
--Testcase 310:
drop function least_accum(anyelement, variadic anyarray);


-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.
--Testcase 311:
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

--Testcase 312:
create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

--Testcase 313:
create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

--Testcase 314:
create operator family my_op_family using btree;

--Testcase 315:
create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

--Testcase 316:
create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.
--Testcase 317:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- This should not be pushed either.
--Testcase 828:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Update local stats on ft2
-- ANALYZE ft2;

-- Add into extension
--Testcase 318:
alter extension mongo_fdw add operator class my_op_class using btree;
--Testcase 319:
alter extension mongo_fdw add function my_op_cmp(a int, b int);
--Testcase 320:
alter extension mongo_fdw add operator family my_op_family using btree;
--Testcase 321:
alter extension mongo_fdw add operator public.<^(int, int);
--Testcase 322:
alter extension mongo_fdw add operator public.=^(int, int);
--Testcase 323:
alter extension mongo_fdw add operator public.>^(int, int);
-- alter server loopback options (set extensions 'postgres_fdw');

-- Now this will be pushed as sort operator is part of the extension.
-- alter server loopback options (add fdw_tuple_cost '0.5');
--Testcase 324:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
--Testcase 325:
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
-- alter server loopback options (add fdw_tuple_cost '0.5');

-- This should be pushed too.
-- MongoDB not support user defined operator.
--Testcase 829:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Remove from extension
--Testcase 326:
alter extension mongo_fdw drop operator class my_op_class using btree;
--Testcase 327:
alter extension mongo_fdw drop function my_op_cmp(a int, b int);
--Testcase 328:
alter extension mongo_fdw drop operator family my_op_family using btree;
--Testcase 329:
alter extension mongo_fdw drop operator public.<^(int, int);
--Testcase 330:
alter extension mongo_fdw drop operator public.=^(int, int);
--Testcase 331:
alter extension mongo_fdw drop operator public.>^(int, int);
-- alter server loopback options (set extensions 'postgres_fdw');

-- This will not be pushed as sort operator is now removed from the extension.
--Testcase 332:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup
--Testcase 333:
drop operator class my_op_class using btree;
--Testcase 334:
drop function my_op_cmp(a int, b int);
--Testcase 335:
drop operator family my_op_family using btree;
--Testcase 336:
drop operator public.>^(int, int);
--Testcase 337:
drop operator public.=^(int, int);
--Testcase 338:
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
--Testcase 339:
explain (verbose, costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate
--Testcase 340:
explain (verbose, costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;
--Testcase 341:
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
--Testcase 342:
explain (verbose, costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;
--Testcase 343:
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
--Testcase 344:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);
--Testcase 345:
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
--Testcase 346:
explain (verbose, costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;
--Testcase 347:
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization
--Testcase 348:
set enable_hashagg to false;
--Testcase 349:
explain (verbose, costs off)
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1.c1) sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1.c1 < 100 order by 1;
--Testcase 350:
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1.c1) sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1.c1 < 100 order by 1;
--Testcase 351:
reset enable_hashagg;

-- bug #15613: bad plan for foreign table scan with lateral reference
--Testcase 352:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."T 1" AS ref_0,
    LATERAL (
        SELECT ref_0.c1 c1, subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0.c1 < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0.c1;

--Testcase 353:
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."T 1" AS ref_0,
    LATERAL (
        SELECT ref_0.c1 c1, subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0.c1 < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0.c1;

-- Check with placeHolderVars
--Testcase 354:
explain (verbose, costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);
--Testcase 355:
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);


-- Not supported cases
-- Grouping sets
--Testcase 356:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 357:
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 358:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 359:
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 360:
explain (verbose, costs off)
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 361:
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 362:
explain (verbose, costs off)
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;
--Testcase 363:
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
--Testcase 364:
explain (verbose, costs off)
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;
--Testcase 365:
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

-- WindowAgg
--Testcase 366:
explain (verbose, costs off)
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 367:
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 368:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 369:
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 370:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 371:
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;


-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join
--Testcase 372:
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
--Testcase 373:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);
--Testcase 374:
EXECUTE st1(1, 1);
--Testcase 375:
EXECUTE st1(101, 101);
SET enable_hashjoin TO off;
SET enable_sort TO off;
-- subquery using stable function (can't be sent to remote)
--Testcase 376:
PREPARE st2(int) AS SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;
--Testcase 377:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);
--Testcase 378:
EXECUTE st2(10, 20);
--Testcase 379:
EXECUTE st2(101, 121);
RESET enable_hashjoin;
RESET enable_sort;
-- subquery using immutable function (can be sent to remote)
--Testcase 380:
PREPARE st3(int) AS SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;
--Testcase 381:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);
--Testcase 382:
EXECUTE st3(10, 20);
--Testcase 383:
EXECUTE st3(20, 30);
-- custom plan should be chosen initially
--Testcase 384:
PREPARE st4(int) AS SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 = $1;
--Testcase 385:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 386:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 387:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 388:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 389:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
--Testcase 390:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
--Testcase 391:
PREPARE st5(text,int) AS SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
--Testcase 392:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 393:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 394:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 395:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 396:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 397:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 398:
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
--Testcase 399:
PREPARE st6 AS SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 400:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 401:
PREPARE st7 AS INSERT INTO ft1 (c1,c2,c3) VALUES (1001,101,'foo');
--Testcase 402:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
-- ALTER TABLE "S 1"."T 1" RENAME TO "T 0";
--Testcase 403:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (SET collection 'T0');
--Testcase 404:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 405:
EXECUTE st6;
--Testcase 406:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
-- ALTER TABLE "S 1"."T 0" RENAME TO "T 1";
--Testcase 407:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (SET collection 'T1');

--Testcase 408:
PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 409:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
-- ALTER SERVER loopback OPTIONS (DROP extensions);
--Testcase 410:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--Testcase 411:
EXECUTE st8;
-- ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

-- cleanup
DEALLOCATE st1;
DEALLOCATE st2;
DEALLOCATE st3;
DEALLOCATE st4;
DEALLOCATE st5;
DEALLOCATE st6;
DEALLOCATE st7;
DEALLOCATE st8;

-- System columns, except ctid and oid, should not be sent to remote
--Testcase 412:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;
--Testcase 413:
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.tableoid = 'ft1_a'::regclass LIMIT 1;
--Testcase 414:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 LIMIT 1;
--Testcase 415:
SELECT tableoid::regclass, t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 LIMIT 1;
--Testcase 416:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 417:
SELECT t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 418:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 LIMIT 1;
--Testcase 419:
SELECT ctid, t1.c1, t1.c2, t1.c3, t1.c4, t1.c5, t1.c6, t1.c7, t1.c8 FROM ft1 t1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================
--Testcase 420:
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
--Testcase 421:
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
--Testcase 422:
SELECT f_test(100);
--Testcase 423:
DROP FUNCTION f_test(int);

-- ===================================================================
-- REINDEX
-- ===================================================================
-- -- remote table is not created here
-- CREATE FOREIGN TABLE reindex_foreign (c1 int, c2 int)
--   SERVER loopback2 OPTIONS (table_name 'reindex_local');
-- REINDEX TABLE reindex_foreign; -- error
-- REINDEX TABLE CONCURRENTLY reindex_foreign; -- error
-- DROP FOREIGN TABLE reindex_foreign;
-- -- partitions and foreign tables
-- CREATE TABLE reind_fdw_parent (c1 int) PARTITION BY RANGE (c1);
-- CREATE TABLE reind_fdw_0_10 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (0) TO (10);
-- CREATE FOREIGN TABLE reind_fdw_10_20 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (10) TO (20)
--   SERVER loopback OPTIONS (table_name 'reind_local_10_20');
-- REINDEX TABLE reind_fdw_parent; -- ok
-- REINDEX TABLE CONCURRENTLY reind_fdw_parent; -- ok
-- DROP TABLE reind_fdw_parent;

-- ===================================================================
-- conversion error
-- ===================================================================
--Testcase 424:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE int;
--Testcase 425:
SELECT * FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8,x9) WHERE x2 = 1;  -- ERROR
--Testcase 426:
SELECT ftx.x2, ft2.c2, ftx.x9 FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8,x9), ft2
  WHERE ftx.x2 = ft2.c1 AND ftx.x2 = 1; -- ERROR
--Testcase 427:
SELECT ftx.x2, ft2.c2, ftx FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8,x9), ft2
  WHERE ftx.x2 = ft2.c1 AND ftx.x2 = 1; -- ERROR
--Testcase 428:
SELECT sum(c2), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR
-- ANALYZE ft1; -- ERROR
--Testcase 429:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE text;

-- ===================================================================
-- local type can be different from remote type in some cases,
-- in particular if similarly-named operators do equivalent things
-- ===================================================================
--Testcase 830:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft1 WHERE c8 = 'foo' LIMIT 1;
--Testcase 831:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft1 WHERE c8 = 'foo' LIMIT 1;
--Testcase 832:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft1 WHERE 'foo' = c8 LIMIT 1;
--Testcase 833:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft1 WHERE 'foo' = c8 LIMIT 1;
-- we declared c8 to be text locally, but it's still the same type on
-- the remote which will balk if we try to do anything incompatible
-- with that remote type
-- Can not create user define type in MongoDB.
-- Type c8 of foreign table ft1 and remote table T1 are 
-- match. These case below not error with mongo_fdw. 
--Testcase 834:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft1 WHERE c8 LIKE 'foo' LIMIT 1; -- ERROR
--Testcase 835:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft1 WHERE c8::text LIKE 'foo' LIMIT 1; -- ERROR; cast not pushed down

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 ORDER BY c1;
--Testcase 430:
FETCH c;
SAVEPOINT s;
ERROR OUT;          -- ERROR
ROLLBACK TO s;
--Testcase 431:
FETCH c;
SAVEPOINT s;
--Testcase 432:
SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK TO s;
--Testcase 433:
FETCH c;
--Testcase 434:
SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 ORDER BY c1 LIMIT 1;
COMMIT;

-- ===================================================================
-- test handling of collations
-- ===================================================================
--Testcase 435:
create foreign table loct3_a_child (_id name, f1 text collate "C", f2 text, f3 varchar(10)) server mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'loct3');
--Testcase 436:
create table loct3 (_id name, f1 text collate "C", f2 text, f3 varchar(10), spdurl text) PARTITION BY LIST (spdurl);
--Testcase 437:
CREATE FOREIGN TABLE loct3_a PARTITION OF loct3 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 438:
create foreign table ft3_a_child (_id name, f1 text collate "C", f2 text, f3 varchar(10))
  server mongo_server options (database 'mongo_fdw_post_regress', collection 'loct3');
--Testcase 439:
create table ft3 (_id name, f1 text collate "C", f2 text, f3 varchar(10), spdurl text) PARTITION BY LIST (spdurl);
--Testcase 440:
create foreign table ft3_a PARTITION OF ft3 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- can be sent to remote
--Testcase 441:
explain (verbose, costs off) select ft3.f1, ft3.f2, ft3.f3 from ft3 where f1 = 'foo';
--Testcase 442:
explain (verbose, costs off) select ft3.f1, ft3.f2, ft3.f3 from ft3 where f1 COLLATE "C" = 'foo';
--Testcase 443:
explain (verbose, costs off) select ft3.f1, ft3.f2, ft3.f3 from ft3 where f2 = 'foo';
--Testcase 444:
explain (verbose, costs off) select ft3.f1, ft3.f2, ft3.f3 from ft3 where f3 = 'foo';
--Testcase 445:
explain (verbose, costs off) select f.f1, f.f2, f.f3, l.f1, l.f2, l.f3 from ft3 f, loct3 l
  where f.f3 = l.f3 and l.f1 = 'foo';
-- can't be sent to remote
--Testcase 446:
explain (verbose, costs off) select ft3.f1, ft3.f2, ft3.f3 from ft3 where f1 COLLATE "POSIX" = 'foo';
--Testcase 447:
explain (verbose, costs off) select ft3.f1, ft3.f2, ft3.f3 from ft3 where f1 = 'foo' COLLATE "C";
--Testcase 448:
explain (verbose, costs off) select ft3.f1, ft3.f2, ft3.f3 from ft3 where f2 COLLATE "C" = 'foo';
--Testcase 449:
explain (verbose, costs off) select ft3.f1, ft3.f2, ft3.f3 from ft3 where f2 = 'foo' COLLATE "C";
--Testcase 450:
explain (verbose, costs off) select f.f1, f.f2, f.f3, l.f1, l.f2, l.f3 from ft3 f, loct3 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

-- ===================================================================
-- test SEMI-JOIN pushdown
-- ===================================================================
--Testcase 896:
EXPLAIN (verbose, costs off)
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 INNER JOIN ft4 ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  AND EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)
  ORDER BY ft2.c1;
--Testcase 897:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 INNER JOIN ft4 ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  AND EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)
  ORDER BY ft2.c1;

-- The same query, different join order
--Testcase 898:
EXPLAIN (verbose, costs off)
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 INNER JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1;
--Testcase 899:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 INNER JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1;

-- Left join
--Testcase 900:
EXPLAIN (verbose, costs off)
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 LEFT JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;
--Testcase 901:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 LEFT JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;

-- Several semi-joins per upper level join
--Testcase 902:
EXPLAIN (verbose, costs off)
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 INNER JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  INNER JOIN (SELECT * FROM ft5 WHERE
  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c1 = ft5.c1)) ft5
  ON ft2.c2 <= ft5.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;
--Testcase 903:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 INNER JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  INNER JOIN (SELECT * FROM ft5 WHERE
  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c1 = ft5.c1)) ft5
  ON ft2.c2 <= ft5.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;

-- Semi-join below Semi-join
--Testcase 904:
EXPLAIN (verbose, costs off)
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl FROM ft2 WHERE
  c1 = ANY (
	SELECT c1 FROM ft2 WHERE
	  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c2 = ft2.c2))
  AND ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;
--Testcase 905:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl FROM ft2 WHERE
  c1 = ANY (
	SELECT c1 FROM ft2 WHERE
	  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c2 = ft2.c2))
  AND ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;

-- Upper level relations shouldn't refer EXISTS() subqueries
--Testcase 906:
EXPLAIN (verbose, costs off)
SELECT ftupper.c1, ftupper.c2, ftupper.c3, ftupper.c4, ftupper.c5, ftupper.c6, ftupper.c7, ftupper.c8, ftupper.spdurl FROM ft2 ftupper WHERE
   EXISTS (
	SELECT c1 FROM ft2 WHERE
	  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c2 = ft2.c2) AND c1 = ftupper.c1 )
  AND ftupper.c1 > 900
  ORDER BY ftupper.c1 LIMIT 10;
--Testcase 907:
SELECT ftupper.c1, ftupper.c2, ftupper.c3, ftupper.c4, ftupper.c5, ftupper.c6, ftupper.c7, ftupper.c8, ftupper.spdurl FROM ft2 ftupper WHERE
   EXISTS (
	SELECT c1 FROM ft2 WHERE
	  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c2 = ft2.c2) AND c1 = ftupper.c1 )
  AND ftupper.c1 > 900
  ORDER BY ftupper.c1 LIMIT 10;

-- EXISTS should be propagated to the highest upper inner join
--Testcase 908:
EXPLAIN (verbose, costs off)
	SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 INNER JOIN
	(SELECT * FROM ft4 WHERE EXISTS (
		SELECT 1 FROM ft2 WHERE ft2.c2 = ft4.c2)) ft4
	ON ft2.c2 = ft4.c1
	INNER JOIN
	(SELECT * FROM ft2 WHERE EXISTS (
		SELECT 1 FROM ft4 WHERE ft2.c2 = ft4.c2)) ft21
	ON ft2.c2 = ft21.c2
	WHERE ft2.c1 > 900
	ORDER BY ft2.c1 LIMIT 10;
--Testcase 909:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8, ft2.spdurl, ft4.c1, ft4.c2, ft4.c3, ft4.spdurl FROM ft2 INNER JOIN
	(SELECT * FROM ft4 WHERE EXISTS (
		SELECT 1 FROM ft2 WHERE ft2.c2 = ft4.c2)) ft4
	ON ft2.c2 = ft4.c1
	INNER JOIN
	(SELECT * FROM ft2 WHERE EXISTS (
		SELECT 1 FROM ft4 WHERE ft2.c2 = ft4.c2)) ft21
	ON ft2.c2 = ft21.c2
	WHERE ft2.c1 > 900
	ORDER BY ft2.c1 LIMIT 10;

-- Can't push down semi-join with inner rel vars in targetlist
--Testcase 910:
EXPLAIN (verbose, costs off)
SELECT ft1.c1 FROM ft1 JOIN ft2 on ft1.c1 = ft2.c1 WHERE
	ft1.c1 IN (
		SELECT ft2.c1 FROM ft2 JOIN ft4 ON ft2.c1 = ft4.c1)
	ORDER BY ft1.c1 LIMIT 5;

-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================
--Testcase 451:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
--Testcase 452:
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
--Testcase 453:
INSERT INTO ft2_a_child (c1,c2,c3)
  VALUES (1101,201,'aaa'), (1102,202,'bbb'), (1103,203,'ccc');
--Testcase 454:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft2 WHERE c1 >= 1101 and c1 <= 1103;
--Testcase 455:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1104,204,'ddd'), (1105,205,'eee');
--Testcase 456:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;              -- can be pushed down
--Testcase 457:
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;
--Testcase 458:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;  -- can be pushed down
--Testcase 459:
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;
--Testcase 460:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft2 WHERE c1 % 10 = 7;
--Testcase 461:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;                               -- can be pushed down
--Testcase 462:
UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;
--Testcase 463:
EXPLAIN (verbose, costs off)
  DELETE FROM ft2_a_child WHERE c1 % 10 = 5;                               -- can be pushed down
-- SELECT c1, c4 FROM ft2 WHERE c1 % 10 = 5;
--Testcase 464:
DELETE FROM ft2_a_child WHERE c1 % 10 = 5;
--Testcase 465:
SELECT c1, c4 FROM ft2 WHERE c1 % 10 = 5;
--Testcase 466:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child USING ft1_a_child WHERE ft1_a_child.c1 = ft2_a_child.c2 AND ft1_a_child.c1 % 10 = 2;                -- can be pushed down
--Testcase 467:
DELETE FROM ft2_a_child USING ft1_a_child WHERE ft1_a_child.c1 = ft2_a_child.c2 AND ft1_a_child.c1 % 10 = 2;
--Testcase 468:
SELECT c1,c2,c3,c4 FROM ft2 ORDER BY c1;
--Testcase 469:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 470:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 471:
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;
--Testcase 472:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200;             -- can be pushed down
--Testcase 473:
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200;
--Testcase 474:
SELECT tableoid::regclass FROM ft2 LIMIT 1;
--Testcase 475:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child WHERE c1 = 1200;                       -- can be pushed down
--Testcase 476:
DELETE FROM ft2_a_child WHERE c1 = 1200;
--Testcase 477:
SELECT tableoid::regclass FROM ft2 LIMIT 1;

-- Test UPDATE/DELETE with RETURNING on a three-table join
-- MongoDB FDW does not support returning and direct modify
--Testcase 478:
INSERT INTO ft2_a_child (c1,c2,c3)
  SELECT id, id - 1200, to_char(id, 'FM00000') FROM generate_series(1201, 1300) id;
--Testcase 479:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c2 = ft4.c1;       -- can be pushed down
--Testcase 480:
UPDATE ft2_a_child SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c2 = ft4.c1;
-- --Testcase 481:
-- SELECT ft2, ft2.*, ft4, ft4.* FROM ft2, ft4, ft5 WHERE (ft4.c1 = ft5.c1) AND (ft2.c1 > 1200) AND (ft2.c2 = ft4.c1);
--Testcase 482:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c1 % 10 = 0 AND ft2_a_child.c2 = ft4.c1;                          -- can be pushed down
--Testcase 483:
DELETE FROM ft2_a_child
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c1 % 10 = 0 AND ft2_a_child.c2 = ft4.c1;
--Testcase 484:
DELETE FROM ft2_a_child WHERE ft2_a_child.c1 > 1200;

-- Test UPDATE with a MULTIEXPR sub-select
-- (maybe someday this'll be remotely executable, but not today)
--Testcase 485:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;
--Testcase 486:
UPDATE ft2_a_child AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

--Testcase 487:
UPDATE ft2_a_child AS target SET (c2) = (
    SELECT c2 / 10
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

-- Test UPDATE involving a join that can be pushed down,
-- but a SET clause that can't be
--Testcase 836:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE ft2_a_child d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;
--Testcase 837:
UPDATE ft2_a_child d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;

-- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- user-defined operators/functions
-- ALTER SERVER mongo_server1 OPTIONS (DROP extensions);
--Testcase 488:
INSERT INTO ft2_a_child (c1,c2,c3)
  SELECT id, id % 10, to_char(id, 'FM00000') FROM generate_series(2001, 2010) id;
--Testcase 489:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE mongo_fdw_abs(c1) > 2000;            -- can't be pushed down
--Testcase 490:
UPDATE ft2_a_child SET c3 = 'bar' WHERE mongo_fdw_abs(c1) > 2000;
--Testcase 491:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM ft2 WHERE mongo_fdw_abs(c1) > 2000;
--Testcase 492:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1;                                                    -- can't be pushed down
--Testcase 493:
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1;
--Testcase 494:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1;       -- can't be pushed down
--Testcase 495:
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1;
--Testcase 496:
DELETE FROM ft2_a_child WHERE ft2_a_child.c1 > 2000;
-- ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

-- Test that trigger on remote table works as expected
--Testcase 820:
CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.c3 = NEW.c3 || '_trig_update';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--Testcase 821:
CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
    ON ft2_a_child FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

--Testcase 822:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1208, 818, 'fff');
--Testcase 823:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8 FROM ft2 WHERE c1 = 1208;
--Testcase 824:
INSERT INTO ft2_a_child (c1,c2,c3,c6) VALUES (1218, 818, 'ggg', '(--;');
--Testcase 825:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8 FROM ft2 WHERE c1 = 1218;
--Testcase 826:
UPDATE ft2_a_child SET c2 = c2 + 600 WHERE c1 % 10 = 8 AND c1 < 1200;
--Testcase 827:
SELECT ft2.c1, ft2.c2, ft2.c3, ft2.c4, ft2.c5, ft2.c6, ft2.c7, ft2.c8 FROM ft2 WHERE c1 % 10 = 8 AND c1 < 1200;

-- MongoDB not support transaction
-- Test errors thrown on remote side during update
--Testcase 828:
ALTER TABLE "S 1"."T 1" ADD CONSTRAINT c2positive CHECK (c2 >= 0);
-- MongoDB automatically generates value for key column (_id), so can not test duplicate key
-- INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12);  -- duplicate key
-- INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT DO NOTHING; -- works
-- INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO NOTHING; -- unsupported
-- INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO UPDATE SET c3 = 'ffg'; -- unsupported
-- Mongo does not support constraints
-- INSERT INTO ft1_a_child(c1, c2) VALUES(1111, -2);  -- c2positive
-- UPDATE ft1_a_child SET c2 = -c2 WHERE c1 = 1;  -- c2positive

-- Test savepoint/rollback behavior
-- not supprort transaction
--Testcase 829:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 830:
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
begin;
--Testcase 831:
update ft2_a_child set c2 = 42 where c2 = 0;
--Testcase 832:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s1;
--Testcase 833:
update ft2_a_child set c2 = 44 where c2 = 4;
--Testcase 834:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s1;
--Testcase 835:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s2;
--Testcase 836:
update ft2_a_child set c2 = 46 where c2 = 6;
--Testcase 837:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
rollback to savepoint s2;
--Testcase 838:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s2;
--Testcase 839:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s3;
-- MongoDB not support constraints
-- update ft2_a_child set c2 = -2 where c2 = 42 and c1 = 10; -- fail on remote side
rollback to savepoint s3;
--Testcase 840:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s3;
--Testcase 841:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- none of the above is committed yet remotely
--Testcase 842:
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
commit;
--Testcase 843:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 844:
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;

-- VACUUM ANALYZE "S 1"."T 1";

-- Above DMLs add data with c6 as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- FIRST behavior here.
-- ORDER BY DESC NULLS LAST options
--Testcase 505:
EXPLAIN (VERBOSE, COSTS OFF) SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;
--Testcase 506:
SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795  LIMIT 10;
-- ORDER BY DESC NULLS FIRST options
--Testcase 507:
EXPLAIN (VERBOSE, COSTS OFF) SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 508:
SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- ORDER BY ASC NULLS FIRST options
--Testcase 509:
EXPLAIN (VERBOSE, COSTS OFF) SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 510:
SELECT ft1.c1, ft1.c2, ft1.c3, ft1.c4, ft1.c5, ft1.c6, ft1.c7, ft1.c8 FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

-- Test ReScan code path that recreates the cursor even when no parameters
-- change (bug #17889)
--Testcase 939:
CREATE FOREIGN TABLE loct1_a_child (_id name, c1 int) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'loct1_rescan');
--Testcase 940:
CREATE TABLE loct1 (_id name, c1 int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 941:
CREATE FOREIGN TABLE loct1_a PARTITION OF loct1 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 942:
CREATE FOREIGN TABLE loct2_a_child (_id name, c1 int, c2 text) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'loct2_rescan');
--Testcase 943:
CREATE TABLE loct2 (_id name, c1 int, c2 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 944:
CREATE FOREIGN TABLE loct2_a PARTITION OF loct2 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 945:
INSERT INTO loct1 (c1, spdurl) VALUES (1001, '/node1/');
--Testcase 946:
INSERT INTO loct1 (c1, spdurl) VALUES (1001, '/node1/');
--Testcase 947:
INSERT INTO loct2 (c1, c2, spdurl) SELECT id, to_char(id, 'FM0000'), '/node1/' FROM generate_series(1, 1000) id;
--Testcase 948:
INSERT INTO loct2 (c1, c2, spdurl) VALUES (1001, 'foo', '/node1/');
--Testcase 949:
INSERT INTO loct2 (c1, c2, spdurl) VALUES (1002, 'bar', '/node1/');
--Testcase 950:
CREATE FOREIGN TABLE remt2_rescan_a_child (_id name, c1 int, c2 text) SERVER mongo_server options(database 'mongo_fdw_post_regress', collection 'loct2_rescan');
--Testcase 951:
CREATE TABLE remt2_rescan (_id name, c1 int, c2 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 952:
CREATE FOREIGN TABLE remt2_rescan_a PARTITION OF remt2_rescan FOR VALUES IN ('/node1/') SERVER spdsrv;
-- ANALYZE loct1;
-- ANALYZE remt2_rescan;
SET enable_mergejoin TO false;
SET enable_hashjoin TO false;
SET enable_material TO false;
--Testcase 953:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE remt2_rescan SET c2 = remt2_rescan.c2 || remt2_rescan.c2 FROM loct1 WHERE loct1.c1 = remt2_rescan.c1;
--Testcase 954:
UPDATE remt2_rescan SET c2 = remt2_rescan.c2 || remt2_rescan.c2 FROM loct1 WHERE loct1.c1 = remt2_rescan.c1;
--Testcase 955:
SELECT remt2_rescan.c1, remt2_rescan.c2 FROM loct1, remt2_rescan WHERE loct1.c1 = remt2_rescan.c1;
RESET enable_mergejoin;
RESET enable_hashjoin;
RESET enable_material;
--Testcase 956:
DROP FOREIGN TABLE remt2_rescan_a_child;
--Testcase 957:
DROP TABLE loct1;
--Testcase 958:
DROP TABLE loct2;
--Testcase 959:
DROP TABLE remt2_rescan;

/*
-- MongoDB does not support constraint
-- ===================================================================
-- test check constraints
-- ===================================================================

-- Consistent check constraints provide consistent results
--Testcase 511:
ALTER TABLE ft1 ADD CONSTRAINT ft1_c2positive CHECK (c2 >= 0);
SET constraint_exclusion = 'off';
--Testcase 512:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 513:
SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 514:
SET constraint_exclusion = 'on';
--Testcase 515:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 516:
SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 517:
RESET constraint_exclusion;
-- check constraint is enforced on the remote side, not locally
--Testcase 518:
INSERT INTO ft1_a_child(c1, c2) VALUES(1111, -2);  -- c2positive
--Testcase 519:
UPDATE ft1_a_child SET c2 = -c2 WHERE c1 = 1;  -- c2positive
--Testcase 520:
ALTER TABLE ft1 DROP CONSTRAINT ft1_c2positive;

-- But inconsistent check constraints provide inconsistent results
--Testcase 521:
ALTER TABLE ft1 ADD CONSTRAINT ft1_c2negative CHECK (c2 < 0);
SET constraint_exclusion = 'off';
--Testcase 522:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 523:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 524:
SET constraint_exclusion = 'on';
--Testcase 525:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 526:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 527:
RESET constraint_exclusion;
-- local check constraint is not actually enforced
--Testcase 528:
INSERT INTO ft1_dml_a_child(c1, c2) VALUES(1111, 2);
--Testcase 529:
UPDATE ft1_dml_a_child SET c2 = c2 + 1 WHERE c1 = 1;
--Testcase 530:
ALTER TABLE ft1 DROP CONSTRAINT ft1_c2negative;
*/
/*
-- MongoDB FDW does not support WITH CHECK OPTION
-- ===================================================================
-- test WITH CHECK OPTION constraints
-- mongo_fdw does not support transaction
-- ===================================================================

--Testcase 531:
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;

--Testcase 532:
CREATE FOREIGN TABLE foreign_tbl_a_child (_id name, a int, b int) SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'base_tbl');
--Testcase 533:
CREATE TABLE foreign_tbl (_id name, a int, b int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 534:
CREATE FOREIGN TABLE foreign_tbl_a PARTITION OF foreign_tbl FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 817:
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl_a_child FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

--Testcase 535:
CREATE VIEW rw_view AS SELECT * FROM foreign_tbl_a_child
  WHERE a < b WITH CHECK OPTION;
--Testcase 536:
\d+ rw_view

--Testcase 537:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view(a, b) VALUES (0, 5);
--Testcase 538:
INSERT INTO rw_view(a, b) VALUES (0, 5); -- should fail
--Testcase 539:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view(a, b) VALUES (0, 15);
--Testcase 540:
INSERT INTO rw_view(a, b) VALUES (0, 15); -- ok
--Testcase 541:
SELECT a, b FROM foreign_tbl;

--Testcase 542:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 5;
--Testcase 543:
UPDATE rw_view SET b = b + 5; -- should fail
--Testcase 544:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 15;
SELECT a, b FROM foreign_tbl;
--Testcase 545:
UPDATE rw_view SET b = b + 15; -- ok
SELECT a, b FROM foreign_tbl_a_child;
SELECT a, b FROM foreign_tbl;
--Testcase 546:
SELECT a, b FROM foreign_tbl;

--Testcase 547:
DROP TRIGGER row_before_insupd_trigger ON foreign_tbl_a_child;
--Testcase 548:
DROP FOREIGN TABLE foreign_tbl_a_child CASCADE;

-- We don't allow batch insert when there are any WCO constraints
ALTER SERVER loopback OPTIONS (ADD batch_size '10');
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 15), (0, 5);
INSERT INTO rw_view VALUES (0, 15), (0, 5); -- should fail
SELECT * FROM foreign_tbl;
ALTER SERVER loopback OPTIONS (DROP batch_size);

--Testcase 549:
DROP TABLE foreign_tbl CASCADE;

-- test WCO for partitions
Skip test partitions
CREATE TABLE child_tbl (a int, b int);
ALTER TABLE child_tbl SET (autovacuum_enabled = 'false');
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON child_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();
CREATE FOREIGN TABLE foreign_tbl (a int, b int)
  SERVER loopback OPTIONS (table_name 'child_tbl');

CREATE TABLE parent_tbl (a int, b int) PARTITION BY RANGE(a);
ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);
-- Detach and re-attach once, to stress the concurrent detach case.
ALTER TABLE parent_tbl DETACH PARTITION foreign_tbl CONCURRENTLY;
ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);

CREATE VIEW rw_view AS SELECT * FROM parent_tbl
  WHERE a < b WITH CHECK OPTION;
\d+ rw_view

EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 5);
INSERT INTO rw_view VALUES (0, 5); -- should fail
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 15);
INSERT INTO rw_view VALUES (0, 15); -- ok
SELECT * FROM foreign_tbl;

EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 5;
UPDATE rw_view SET b = b + 5; -- should fail
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 15;
UPDATE rw_view SET b = b + 15; -- ok
SELECT * FROM foreign_tbl;

-- We don't allow batch insert when there are any WCO constraints
ALTER SERVER loopback OPTIONS (ADD batch_size '10');
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 15), (0, 5);
INSERT INTO rw_view VALUES (0, 15), (0, 5); -- should fail
SELECT * FROM foreign_tbl;
ALTER SERVER loopback OPTIONS (DROP batch_size);

DROP FOREIGN TABLE foreign_tbl CASCADE;
DROP TRIGGER row_before_insupd_trigger ON child_tbl;
DROP TABLE parent_tbl CASCADE;

DROP FUNCTION row_before_insupd_trigfunc;
*/

-- Try a more complex permutation of WCO where there are multiple levels of
-- partitioned tables with columns not all in the same order
--Testcase 854:
CREATE TABLE parent_tbl (a int, b text, c numeric) PARTITION BY RANGE(a);
--Testcase 855:
CREATE TABLE sub_parent (c numeric, a int, b text) PARTITION BY RANGE(a);
ALTER TABLE parent_tbl ATTACH PARTITION sub_parent FOR VALUES FROM (1) TO (10);
--Testcase 856:
CREATE FOREIGN TABLE child_foreign (b text, c numeric, a int)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'child_local');
ALTER TABLE sub_parent ATTACH PARTITION child_foreign FOR VALUES FROM (1) TO (10);
--Testcase 857:
CREATE VIEW rw_view AS SELECT * FROM parent_tbl WHERE a < 5 WITH CHECK OPTION;

--Testcase 858:
INSERT INTO parent_tbl (a) VALUES(1),(5);
--Testcase 859:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = 'text', c = 123.456;
--Testcase 860:
UPDATE rw_view SET b = 'text', c = 123.456;
--Testcase 861:
SELECT * FROM parent_tbl ORDER BY a;

--Testcase 862:
DROP VIEW rw_view;
--Testcase 863:
DROP FOREIGN TABLE child_foreign;
--Testcase 864:
DROP TABLE sub_parent;
--Testcase 865:
DROP TABLE parent_tbl;

-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- ===================================================================
--Testcase 550:
create foreign table loc1_a_child (_id name, f1 serial, f2 text) server mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'loc1');
--Testcase 551:
create table loc1 (_id name, f1 serial, f2 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 552:
create foreign table loc1_a PARTITION OF loc1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 553:
create foreign table rem1_a_child (_id name, f1 serial, f2 text)
  server mongo_server options(database 'mongo_fdw_post_regress', collection 'loc1');
--Testcase 554:
create table rem1 (_id name, f1 serial, f2 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 555:
create foreign table rem1_a PARTITION OF rem1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 556:
select pg_catalog.setval('rem1_a_child_f1_seq', 10, false);
--Testcase 557:
insert into loc1_a_child(f2) values('hi');
--Testcase 558:
insert into rem1_a_child(f2) values('hi remote');
--Testcase 559:
insert into loc1_a_child(f2) values('bye');
--Testcase 560:
insert into rem1_a_child(f2) values('bye remote');
--Testcase 561:
select f1, f2 from loc1;
--Testcase 562:
select f1, f2 from rem1;

-- ===================================================================
-- test generated columns
-- ===================================================================
--Testcase 563:
create foreign table grem1_a_child (
  _id name,
  a int,
  b int generated always as (a * 2) stored)
  server mongo_server options(database 'mongo_fdw_post_regress', collection 'gloc1');
--Testcase 564:
create table grem1 (
  _id name,
  a int,
  b int generated always as (a * 2) stored,
  spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 565:
CREATE FOREIGN TABLE grem1_a PARTITION OF grem1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 566:
explain (verbose, costs off)
insert into grem1_a_child (a) values (1), (2);
--Testcase 567:
insert into grem1_a_child (a) values (1), (2);
--Testcase 568:
explain (verbose, costs off)
update grem1_a_child set a = 22 where a = 2;
--Testcase 569:
update grem1_a_child set a = 22 where a = 2;
--Testcase 570:
select a, b from grem1;
--Testcase 571:
delete from grem1_a_child;

-- -- test copy from
-- copy grem1 from stdin;
-- 1
-- 2
-- \.
-- select * from gloc1;
-- select * from grem1;
-- delete from grem1;

-- -- test batch insert
-- alter server loopback options (add batch_size '10');
-- explain (verbose, costs off)
-- insert into grem1 (a) values (1), (2);
-- insert into grem1 (a) values (1), (2);
-- select * from gloc1;
-- select * from grem1;
-- delete from grem1;
-- alter server loopback options (drop batch_size);

-- batch insert with foreign partitions.
-- This schema uses two partitions, one local and one remote with a modulo
-- to loop across all of them in batches.
--Testcase 866:
create table tab_batch_local (id int, data text);
--Testcase 867:
insert into tab_batch_local select i, 'test'|| i from generate_series(1, 45) i;
--Testcase 868:
create table tab_batch_sharded (id int, data text) partition by hash(id);
--Testcase 869:
create table tab_batch_sharded_p0 partition of tab_batch_sharded
  for values with (modulus 2, remainder 0);

--Testcase 870:
create foreign table tab_batch_sharded_p1 partition of tab_batch_sharded
  for values with (modulus 2, remainder 1)
  server mongo_server options (database 'mongo_fdw_post_regress', collection 'tab_batch_sharded_p1_remote');
--Testcase 871:
insert into tab_batch_sharded select * from tab_batch_local;
--Testcase 872:
select count(*) from tab_batch_sharded;
--Testcase 873:
drop table tab_batch_local;
--Testcase 874:
drop table tab_batch_sharded;

-- ===================================================================
-- test local triggers
-- ===================================================================

-- Trigger functions "borrowed" from triggers regress test.
--Testcase 572:
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

--Testcase 573:
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE OR TRUNCATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 574:
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE OR TRUNCATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 575:
CREATE OR REPLACE FUNCTION trigger_data()  RETURNS trigger
LANGUAGE plpgsql AS $$

declare
	oldnew text[];
	relid text;
    argstr text;
	id_tmp text;
begin

	relid := TG_relid::regclass;
	argstr := '';
	for i in 0 .. TG_nargs - 1 loop
		if i > 0 then
			argstr := argstr || ', ';
		end if;
		argstr := argstr || TG_argv[i];
	end loop;

    RAISE NOTICE '%(%) % % % ON %',
		tg_name, argstr, TG_when, TG_level, TG_OP, relid;
    oldnew := '{}'::text[];
	id_tmp := OLD._id;
	OLD._id := 0;
	if TG_OP != 'INSERT' then
		oldnew := array_append(oldnew, format('OLD: %s', OLD));
	end if;
	OLD._id := id_tmp;

	id_tmp := NEW._id;
	NEW._id := NULL;
	if TG_OP != 'DELETE' then
		oldnew := array_append(oldnew, format('NEW: %s', NEW));
	end if;
	NEW._id := id_tmp;

    RAISE NOTICE '%', array_to_string(oldnew, ',');

	if TG_OP = 'DELETE' then
		return OLD;
	else
		return NEW;
	end if;
end;
$$;

-- Test basic functionality
--Testcase 576:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 577:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 578:
delete from rem1_a_child;
--Testcase 579:
insert into rem1_a_child(f1, f2) values(1,'insert');
--Testcase 580:
update rem1_a_child set f2  = 'update' where f1 = 1;
--Testcase 581:
update rem1_a_child set f2 = f2 || f2;
-- mongo_fdw does not support truncate foreign table
truncate rem1_a_child;


-- cleanup
--Testcase 582:
DROP TRIGGER trig_row_before ON rem1_a_child;
--Testcase 583:
DROP TRIGGER trig_row_after ON rem1_a_child;
--Testcase 584:
DROP TRIGGER trig_stmt_before ON rem1_a_child;
--Testcase 585:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

--Testcase 586:
DELETE from rem1_a_child;

-- Test multiple AFTER ROW triggers on a foreign table
--Testcase 587:
CREATE TRIGGER trig_row_after1
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 588:
CREATE TRIGGER trig_row_after2
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 589:
insert into rem1_a_child(f1, f2) values(1,'insert');
--Testcase 590:
update rem1_a_child set f2  = 'update' where f1 = 1;
--Testcase 591:
update rem1_a_child set f2 = f2 || f2;
--Testcase 592:
delete from rem1_a_child;

-- cleanup
--Testcase 593:
DROP TRIGGER trig_row_after1 ON rem1_a_child;
--Testcase 594:
DROP TRIGGER trig_row_after2 ON rem1_a_child;

-- Test WHEN conditions

--Testcase 595:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 596:
CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens
--Testcase 597:
INSERT INTO rem1_a_child(f1, f2) values(1, 'insert');
--Testcase 598:
UPDATE rem1_a_child set f2 = 'test';

-- Insert or update matching: triggers are fired
--Testcase 599:
INSERT INTO rem1_a_child(f1, f2) values(2, 'update');
--Testcase 600:
UPDATE rem1_a_child set f2 = 'update update' where f1 = '2';

--Testcase 601:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 602:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1
--Testcase 603:
DELETE FROM rem1_a_child;

-- cleanup
--Testcase 604:
DROP TRIGGER trig_row_before_insupd ON rem1_a_child;
--Testcase 605:
DROP TRIGGER trig_row_after_insupd ON rem1_a_child;
--Testcase 606:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;
--Testcase 607:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;


-- Test various RETURN statements in BEFORE triggers.

--Testcase 608:
CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

--Testcase 609:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- The new values should have 'triggered' appended
--Testcase 610:
INSERT INTO rem1_a_child(f1, f2) values(1, 'insert');
--Testcase 611:
SELECT f1, f2 from rem1;
--Testcase 612:
INSERT INTO rem1_a_child(f1, f2) values(2, 'insert');
--Testcase 613:
SELECT f1, f2 from rem1;
--Testcase 614:
UPDATE rem1_a_child set f2 = '';
--Testcase 615:
SELECT f1, f2 from rem1;
--Testcase 616:
UPDATE rem1_a_child set f2 = 'skidoo';
--Testcase 617:
SELECT f1, f2 from rem1;

--Testcase 618:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f1 = 10;          -- all columns should be transmitted
--Testcase 619:
UPDATE rem1_a_child set f1 = 10;
--Testcase 620:
SELECT f1, f2 from rem1;

--Testcase 621:
DELETE FROM rem1_a_child;

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger
--Testcase 622:
CREATE TRIGGER trig_row_before_insupd2
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 623:
INSERT INTO rem1_a_child(f1, f2) values(1, 'insert');
--Testcase 624:
SELECT f1, f2 from rem1;
--Testcase 625:
INSERT INTO rem1_a_child(f1, f2) values(2, 'insert');
--Testcase 626:
SELECT f1, f2 from rem1;
--Testcase 627:
UPDATE rem1_a_child set f2 = '';
--Testcase 628:
SELECT f1, f2 from rem1;
--Testcase 629:
UPDATE rem1_a_child set f2 = 'skidoo';
--Testcase 630:
SELECT f1, f2 from rem1;

--Testcase 631:
DROP TRIGGER trig_row_before_insupd ON rem1_a_child;
--Testcase 632:
DROP TRIGGER trig_row_before_insupd2 ON rem1_a_child;

--Testcase 633:
DELETE from rem1_a_child;

--Testcase 634:
INSERT INTO rem1_a_child(f1, f2) VALUES (1, 'test');

-- Test with a trigger returning NULL
--Testcase 635:
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

--Testcase 636:
CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.
--Testcase 637:
INSERT INTO rem1_a_child(f1, f2) VALUES (2, 'test2');

--Testcase 638:
SELECT f1, f2 from rem1;

--Testcase 639:
UPDATE rem1_a_child SET f2 = 'test2';

--Testcase 640:
SELECT f1, f2 from rem1;

--Testcase 641:
DELETE from rem1_a_child;

--Testcase 642:
SELECT f1, f2 from rem1;

--Testcase 643:
DROP TRIGGER trig_null ON rem1_a_child;
--Testcase 644:
DELETE from rem1_a_child;

-- Test a combination of local and remote triggers
--Testcase 645:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 646:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 647:
CREATE TRIGGER trig_local_before BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 648:
INSERT INTO rem1_a_child(f2) VALUES ('test');
--Testcase 649:
UPDATE rem1_a_child SET f2 = 'testo';

-- Test returning a system attribute
--Testcase 650:
INSERT INTO rem1_a_child(f2) VALUES ('test');

-- cleanup
--Testcase 651:
DROP TRIGGER trig_row_before ON rem1_a_child;
--Testcase 652:
DROP TRIGGER trig_row_after ON rem1_a_child;
--Testcase 653:
DROP TRIGGER trig_local_before ON rem1_a_child;


-- Test direct foreign table modification functionality
--Testcase 838:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 839:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child WHERE false;     -- currently can't be pushed down

-- Test with statement-level triggers
--Testcase 654:
CREATE TRIGGER trig_stmt_before
	BEFORE DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 655:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 656:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 657:
DROP TRIGGER trig_stmt_before ON rem1_a_child;

--Testcase 658:
CREATE TRIGGER trig_stmt_after
	AFTER DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 659:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 660:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 661:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

-- Test with row-level ON INSERT triggers
--Testcase 662:
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 663:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 664:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 665:
DROP TRIGGER trig_row_before_insert ON rem1_a_child;

--Testcase 666:
CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 667:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 668:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 669:
DROP TRIGGER trig_row_after_insert ON rem1_a_child;

-- Test with row-level ON UPDATE triggers
--Testcase 670:
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 671:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down
--Testcase 672:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 673:
DROP TRIGGER trig_row_before_update ON rem1_a_child;

--Testcase 674:
CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 675:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down
--Testcase 676:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 677:
DROP TRIGGER trig_row_after_update ON rem1_a_child;

-- Test with row-level ON DELETE triggers
--Testcase 678:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 679:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 680:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down
--Testcase 681:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;

--Testcase 682:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 683:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 684:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down
--Testcase 685:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;

-- ===================================================================
-- test inheritance features
-- ===================================================================

--Testcase 686:
CREATE TABLE a (_id name, aa TEXT);
--Testcase 687:
ALTER TABLE a SET (autovacuum_enabled = 'false');
--Testcase 688:
CREATE FOREIGN TABLE b_b_child (bb TEXT) INHERITS (a)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_post_regress', collection 'loct');
--Testcase 689:
CREATE TABLE b (_id name, aa TEXT, bb TEXT, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 690:
CREATE FOREIGN TABLE b_b PARTITION OF b FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 691:
INSERT INTO a(aa) VALUES('aaa');
--Testcase 692:
INSERT INTO a(aa) VALUES('aaaa');
--Testcase 693:
INSERT INTO a(aa) VALUES('aaaaa');

--Testcase 694:
INSERT INTO b_b_child(aa) VALUES('bbb');
--Testcase 695:
INSERT INTO b_b_child(aa) VALUES('bbbb');
--Testcase 696:
INSERT INTO b_b_child(aa) VALUES('bbbbb');

--Testcase 697:
SELECT tableoid::regclass, aa FROM a;
--Testcase 698:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 699:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 700:
UPDATE a SET aa = 'zzzzzz' WHERE aa LIKE 'aaaa%';

--Testcase 701:
SELECT tableoid::regclass, aa FROM a;
--Testcase 702:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 703:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 704:
UPDATE b_b_child SET aa = 'new';

--Testcase 705:
SELECT tableoid::regclass, aa FROM a;
--Testcase 706:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 707:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 708:
UPDATE a SET aa = 'newtoo';

--Testcase 709:
SELECT tableoid::regclass, aa FROM a;
--Testcase 710:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 711:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 712:
DELETE FROM a;

--Testcase 713:
SELECT tableoid::regclass, aa FROM a;
--Testcase 714:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 715:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 716:
DROP TABLE a CASCADE;

-- Check SELECT FOR UPDATE/SHARE with an inherited source table
--Testcase 717:
create table foo (_id name, f1 int, f2 int);
--Testcase 718:
create foreign table foo2_a_child (f3 int) inherits (foo)
  server mongo_server options (database 'mongo_fdw_post_regress', collection 'loct1');
--Testcase 719:
create table foo2 (_id name, f1 int, f2 int, f3 int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 720:
create foreign table foo2_a PARTITION OF foo2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 721:
create table bar (_id name, f1 int, f2 int);
--Testcase 722:
create foreign table bar2_a_child (f3 int) inherits (bar)
  server mongo_server options (database 'mongo_fdw_post_regress', collection 'loct2');
--Testcase 723:
create table bar2 (_id name, f1 int, f2 int, f3 int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 724:
create foreign table bar2_a PARTITION OF bar2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 725:
alter table foo set (autovacuum_enabled = 'false');
--Testcase 726:
alter table bar set (autovacuum_enabled = 'false');

--Testcase 727:
insert into foo(f1, f2) values(1,1);
--Testcase 728:
insert into foo(f1, f2) values(3,3);
--Testcase 729:
insert into foo2_a_child(f1, f2, f3) values(2,2,2);
--Testcase 730:
insert into foo2_a_child(f1, f2, f3) values(4,4,4);
--Testcase 731:
insert into bar(f1, f2) values(1,11);
--Testcase 732:
insert into bar(f1, f2) values(2,22);
--Testcase 733:
insert into bar(f1, f2) values(6,66);
--Testcase 734:
insert into bar2_a_child(f1, f2, f3) values(3,33,33);
--Testcase 735:
insert into bar2_a_child(f1, f2, f3) values(4,44,44);
--Testcase 736:
insert into bar2_a_child(f1, f2, f3) values(7,77,77);

--Testcase 737:
explain (verbose, costs off)
select f1, f2 from bar where f1 in (select f1 from foo) for update;
--Testcase 738:
select f1, f2 from bar where f1 in (select f1 from foo) for update;

--Testcase 739:
explain (verbose, costs off)
select f1, f2 from bar where f1 in (select f1 from foo) for share;
--Testcase 740:
select f1, f2 from bar where f1 in (select f1 from foo) for share;

-- Now check SELECT FOR UPDATE/SHARE with an inherited source table,
-- where the parent is itself a foreign table
--Testcase 741:
create foreign table foo2child_a_child (f3 int) inherits (foo2_a_child)
  server mongo_server options (database 'mongo_fdw_post_regress', collection 'loct4');
--Testcase 742:
create table foo2child (f3 int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 743:
create foreign table foo2child_a PARTITION OF foo2child FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 744:
explain (verbose, costs off)
select f1, f2 from bar where f1 in (select f1 from foo2) for share;
--Testcase 745:
select f1, f2 from bar where f1 in (select f1 from foo2) for share;

--Testcase 746:
drop foreign table foo2child_a_child;
--Testcase 747:
drop table foo2child;

-- And with a local child relation of the foreign table parent
--Testcase 748:
create table foo2child (f3 int) inherits (foo2_a_child);

--Testcase 749:
explain (verbose, costs off)
select f1, f2 from bar where f1 in (select f1 from foo2) for share;
--Testcase 750:
select f1, f2 from bar where f1 in (select f1 from foo2) for share;

--Testcase 751:
drop table foo2child;

-- Check UPDATE with inherited target and an inherited source table
--Testcase 752:
explain (verbose, costs off)
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);
--Testcase 753:
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

--Testcase 754:
select tableoid::regclass, f1, f2 from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery
--Testcase 755:
explain (verbose, costs off)
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;
--Testcase 756:
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

--Testcase 757:
select tableoid::regclass, f1, f2 from bar order by 1,2;

-- Test forcing the remote server to produce sorted data for a merge join,
-- but the foreign table is an inheritance child.
-- truncate table loct1;
--Testcase 758:
delete from foo2_a_child;
truncate table only foo;
\set num_rows_foo 2000
--Testcase 759:
insert into foo2_a_child(f1, f2, f3) select generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2);
--Testcase 760:
insert into foo(f1, f2) select generate_series(1, :num_rows_foo, 2), generate_series(1, :num_rows_foo, 2);
--Testcase 761:
SET enable_hashjoin to false;
--Testcase 762:
SET enable_nestloop to false;
-- alter foreign table foo2 options (use_remote_estimate 'true');
-- create index i_loct1_f1 on loct1(f1);
--Testcase 763:
create index i_foo_f1 on foo(f1);
analyze foo;
-- analyze loct1;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 764:
explain (verbose, costs off)
	select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 765:
select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 766:
explain (verbose, costs off)
	select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 767:
select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 768:
RESET enable_hashjoin;
--Testcase 769:
RESET enable_nestloop;

-- Test that WHERE CURRENT OF is not supported
begin;
declare c cursor for select f1, f2 from bar where f1 = 7;
--Testcase 770:
fetch from c;
--Testcase 771:
update bar set f2 = null where current of c;
rollback;

--Testcase 772:
explain (verbose, costs off)
delete from foo where f1 < 5;
--Testcase 773:
delete from foo where f1 < 5;
--Testcase 774:
explain (verbose, costs off)
update bar set f2 = f2 + 100;
--Testcase 775:
update bar set f2 = f2 + 100;
--Testcase 776:
select f1, f2 from bar;

-- Test that UPDATE/DELETE with inherited target works with row-level triggers
--Testcase 777:
CREATE TRIGGER trig_row_before
BEFORE UPDATE OR DELETE ON bar2_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 778:
CREATE TRIGGER trig_row_after
AFTER UPDATE OR DELETE ON bar2_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 779:
explain (verbose, costs off)
update bar set f2 = f2 + 100;
--Testcase 780:
update bar set f2 = f2 + 100;

--Testcase 781:
explain (verbose, costs off)
delete from bar where f2 < 400;
--Testcase 782:
delete from bar where f2 < 400;

-- cleanup
--Testcase 783:
drop table foo cascade;
--Testcase 784:
drop table bar cascade;

-- Test pushing down UPDATE/DELETE joins to the remote server
--Testcase 785:
create table parent (_id name, a int, b text);
--Testcase 786:
create foreign table remt1_a_child (_id name, a int, b text)
  server mongo_server options (database 'mongo_fdw_post_regress', collection 'loct12');
--Testcase 787:
create table remt1 (_id name, a int, b text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 788:
create foreign table remt1_a PARTITION OF remt1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 789:
create foreign table remt2_a_child (_id name, a int, b text)
  server mongo_server options (database 'mongo_fdw_post_regress', collection 'loct22');
--Testcase 790:
create table remt2 (_id name, a int, b text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 791:
create foreign table remt2_a PARTITION OF remt2 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 792:
alter foreign table remt1_a_child inherit parent;

--Testcase 793:
insert into remt1_a_child(a, b) values (1, 'foo');
--Testcase 794:
insert into remt1_a_child(a, b) values (2, 'bar');
--Testcase 795:
insert into remt2_a_child(a, b) values (1, 'foo');
--Testcase 796:
insert into remt2_a_child(a, b) values (2, 'bar');

--Testcase 797:
explain (verbose, costs off)
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;
--Testcase 798:
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;
--Testcase 799:
select parent.a, parent.b, remt2.a, remt2.b from parent inner join remt2 on (parent.a = remt2.a);
--Testcase 800:
explain (verbose, costs off)
delete from parent using remt2 where parent.a = remt2.a;
--Testcase 801:
delete from parent using remt2 where parent.a = remt2.a;

-- cleanup
--Testcase 802:
drop foreign table remt1_a_child;
--Testcase 803:
drop foreign table remt2_a_child;
--Testcase 804:
drop table parent;

-- PGSpider Extension does not support INSERT/UPDATE/DELETE directly on
-- parent table, so we skip these test cases.
-- -- ===================================================================
-- -- test tuple routing for foreign-table partitions
-- -- ===================================================================

-- -- Test insert tuple routing
-- create table itrtest (a int, b text) partition by list (a);
-- create table loct1 (a int check (a in (1)), b text);
-- create foreign table remp1 (a int check (a in (1)), b text) server loopback options (table_name 'loct1');
-- create table loct2 (a int check (a in (2)), b text);
-- create foreign table remp2 (b text, a int check (a in (2))) server loopback options (table_name 'loct2');
-- alter table itrtest attach partition remp1 for values in (1);
-- alter table itrtest attach partition remp2 for values in (2);

-- insert into itrtest values (1, 'foo');
-- insert into itrtest values (1, 'bar') returning *;
-- insert into itrtest values (2, 'baz');
-- insert into itrtest values (2, 'qux') returning *;
-- insert into itrtest values (1, 'test1'), (2, 'test2') returning *;

-- select tableoid::regclass, * FROM itrtest;
-- select tableoid::regclass, * FROM remp1;
-- select tableoid::regclass, * FROM remp2;

-- delete from itrtest;

-- MERGE ought to fail cleanly
-- merge into itrtest using (select 1, 'foo') as source on (true)
--   when matched then do nothing;

-- create unique index loct1_idx on loct1 (a);

-- -- DO NOTHING without an inference specification is supported
-- insert into itrtest values (1, 'foo') on conflict do nothing returning *;
-- insert into itrtest values (1, 'foo') on conflict do nothing returning *;

-- -- But other cases are not supported
-- insert into itrtest values (1, 'bar') on conflict (a) do nothing;
-- insert into itrtest values (1, 'bar') on conflict (a) do update set b = excluded.b;

-- select tableoid::regclass, * FROM itrtest;

-- delete from itrtest;

-- drop index loct1_idx;

-- -- Test that remote triggers work with insert tuple routing
-- create function br_insert_trigfunc() returns trigger as $$
-- begin
-- 	new.b := new.b || ' triggered !';
-- 	return new;
-- end
-- $$ language plpgsql;
-- create trigger loct1_br_insert_trigger before insert on loct1
-- 	for each row execute procedure br_insert_trigfunc();
-- create trigger loct2_br_insert_trigger before insert on loct2
-- 	for each row execute procedure br_insert_trigfunc();

-- -- The new values are concatenated with ' triggered !'
-- insert into itrtest values (1, 'foo') returning *;
-- insert into itrtest values (2, 'qux') returning *;
-- insert into itrtest values (1, 'test1'), (2, 'test2') returning *;
-- with result as (insert into itrtest values (1, 'test1'), (2, 'test2') returning *) select * from result;

-- drop trigger loct1_br_insert_trigger on loct1;
-- drop trigger loct2_br_insert_trigger on loct2;

-- drop table itrtest;
-- drop table loct1;
-- drop table loct2;

-- -- Test update tuple routing
-- create table utrtest (a int, b text) partition by list (a);
-- create table loct (a int check (a in (1)), b text);
-- create foreign table remp (a int check (a in (1)), b text) server loopback options (table_name 'loct');
-- create table locp (a int check (a in (2)), b text);
-- alter table utrtest attach partition remp for values in (1);
-- alter table utrtest attach partition locp for values in (2);

-- insert into utrtest values (1, 'foo');
-- insert into utrtest values (2, 'qux');

-- select tableoid::regclass, * FROM utrtest;
-- select tableoid::regclass, * FROM remp;
-- select tableoid::regclass, * FROM locp;

-- -- It's not allowed to move a row from a partition that is foreign to another
-- update utrtest set a = 2 where b = 'foo' returning *;

-- -- But the reverse is allowed
-- update utrtest set a = 1 where b = 'qux' returning *;

-- select tableoid::regclass, * FROM utrtest;
-- select tableoid::regclass, * FROM remp;
-- select tableoid::regclass, * FROM locp;

-- -- The executor should not let unexercised FDWs shut down
-- update utrtest set a = 1 where b = 'foo';

-- -- Test that remote triggers work with update tuple routing
-- create trigger loct_br_insert_trigger before insert on loct
-- 	for each row execute procedure br_insert_trigfunc();

-- delete from utrtest;
-- insert into utrtest values (2, 'qux');

-- -- Check case where the foreign partition is a subplan target rel
-- explain (verbose, costs off)
-- update utrtest set a = 1 where a = 1 or a = 2 returning *;
-- -- The new values are concatenated with ' triggered !'
-- update utrtest set a = 1 where a = 1 or a = 2 returning *;

-- delete from utrtest;
-- insert into utrtest values (2, 'qux');

-- -- Check case where the foreign partition isn't a subplan target rel
-- explain (verbose, costs off)
-- update utrtest set a = 1 where a = 2 returning *;
-- -- The new values are concatenated with ' triggered !'
-- update utrtest set a = 1 where a = 2 returning *;

-- drop trigger loct_br_insert_trigger on loct;

-- -- We can move rows to a foreign partition that has been updated already,
-- -- but can't move rows to a foreign partition that hasn't been updated yet

-- delete from utrtest;
-- insert into utrtest values (1, 'foo');
-- insert into utrtest values (2, 'qux');

-- -- Test the former case:
-- -- with a direct modification plan
-- explain (verbose, costs off)
-- update utrtest set a = 1 returning *;
-- update utrtest set a = 1 returning *;

-- delete from utrtest;
-- insert into utrtest values (1, 'foo');
-- insert into utrtest values (2, 'qux');

-- -- with a non-direct modification plan
-- explain (verbose, costs off)
-- update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x returning *;
-- update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x returning *;

-- -- Change the definition of utrtest so that the foreign partition get updated
-- -- after the local partition
-- delete from utrtest;
-- alter table utrtest detach partition remp;
-- drop foreign table remp;
-- alter table loct drop constraint loct_a_check;
-- alter table loct add check (a in (3));
-- create foreign table remp (a int check (a in (3)), b text) server loopback options (table_name 'loct');
-- alter table utrtest attach partition remp for values in (3);
-- insert into utrtest values (2, 'qux');
-- insert into utrtest values (3, 'xyzzy');

-- -- Test the latter case:
-- -- with a direct modification plan
-- explain (verbose, costs off)
-- update utrtest set a = 3 returning *;
-- update utrtest set a = 3 returning *; -- ERROR

-- -- with a non-direct modification plan
-- explain (verbose, costs off)
-- update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x returning *;
-- update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x returning *; -- ERROR

-- drop table utrtest;
-- drop table loct;

-- -- Test copy tuple routing
-- create table ctrtest (a int, b text) partition by list (a);
-- create table loct1 (a int check (a in (1)), b text);
-- create foreign table remp1 (a int check (a in (1)), b text) server loopback options (table_name 'loct1');
-- create table loct2 (a int check (a in (2)), b text);
-- create foreign table remp2 (b text, a int check (a in (2))) server loopback options (table_name 'loct2');
-- alter table ctrtest attach partition remp1 for values in (1);
-- alter table ctrtest attach partition remp2 for values in (2);

-- copy ctrtest from stdin;
-- 1	foo
-- 2	qux
-- \.

-- select tableoid::regclass, * FROM ctrtest;
-- select tableoid::regclass, * FROM remp1;
-- select tableoid::regclass, * FROM remp2;

-- -- Copying into foreign partitions directly should work as well
-- copy remp1 from stdin;
-- 1	bar
-- \.

-- select tableoid::regclass, * FROM remp1;

-- delete from ctrtest;

-- -- Test copy tuple routing with the batch_size option enabled
-- alter server loopback options (add batch_size '2');

-- copy ctrtest from stdin;
-- 1	foo
-- 1	bar
-- 2	baz
-- 2	qux
-- 1	test1
-- 2	test2
-- \.

-- select tableoid::regclass, * FROM ctrtest;
-- select tableoid::regclass, * FROM remp1;
-- select tableoid::regclass, * FROM remp2;

-- delete from ctrtest;

-- alter server loopback options (drop batch_size);

-- drop table ctrtest;
-- drop table loct1;
-- drop table loct2;

-- MongoDB FDW does not support COPY FROM
-- -- ===================================================================
-- -- test COPY FROM
-- -- ===================================================================

-- create table loc2 (f1 int, f2 text);
-- alter table loc2 set (autovacuum_enabled = 'false');
-- create foreign table rem2 (f1 int, f2 text) server loopback options(table_name 'loc2');

-- -- Test basic functionality
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- delete from rem2;

-- -- Test check constraints
-- alter table loc2 add constraint loc2_f1positive check (f1 >= 0);
-- alter foreign table rem2 add constraint rem2_f1positive check (f1 >= 0);

-- -- check constraint is enforced on the remote side, not locally
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- copy rem2 from stdin; -- ERROR
-- -1	xyzzy
-- \.
-- select * from rem2;

-- alter foreign table rem2 drop constraint rem2_f1positive;
-- alter table loc2 drop constraint loc2_f1positive;

-- delete from rem2;

-- -- Test local triggers
-- create trigger trig_stmt_before before insert on rem2
-- 	for each statement execute procedure trigger_func();
-- create trigger trig_stmt_after after insert on rem2
-- 	for each statement execute procedure trigger_func();
-- create trigger trig_row_before before insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- create trigger trig_row_after after insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');

-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_row_before on rem2;
-- drop trigger trig_row_after on rem2;
-- drop trigger trig_stmt_before on rem2;
-- drop trigger trig_stmt_after on rem2;

-- delete from rem2;

-- create trigger trig_row_before_insert before insert on rem2
-- 	for each row execute procedure trig_row_before_insupdate();

-- -- The new values are concatenated with ' triggered !'
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_row_before_insert on rem2;

-- delete from rem2;

-- create trigger trig_null before insert on rem2
-- 	for each row execute procedure trig_null();

-- -- Nothing happens
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_null on rem2;

-- delete from rem2;

-- -- Test remote triggers
-- create trigger trig_row_before_insert before insert on loc2
-- 	for each row execute procedure trig_row_before_insupdate();

-- -- The new values are concatenated with ' triggered !'
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_row_before_insert on loc2;

-- delete from rem2;

-- create trigger trig_null before insert on loc2
-- 	for each row execute procedure trig_null();

-- -- Nothing happens
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_null on loc2;

-- delete from rem2;

-- -- Test a combination of local and remote triggers
-- create trigger rem2_trig_row_before before insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- create trigger rem2_trig_row_after after insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- create trigger loc2_trig_row_before_insert before insert on loc2
-- 	for each row execute procedure trig_row_before_insupdate();

-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- select * from rem2;

-- drop trigger rem2_trig_row_before on rem2;
-- drop trigger rem2_trig_row_after on rem2;
-- drop trigger loc2_trig_row_before_insert on loc2;

-- delete from rem2;

-- -- test COPY FROM with foreign table created in the same transaction
-- create table loc3 (f1 int, f2 text);
-- begin;
-- create foreign table rem3 (f1 int, f2 text)
-- 	server loopback options(table_name 'loc3');
-- copy rem3 from stdin;
-- 1	foo
-- 2	bar
-- \.
-- commit;
-- select * from rem3;
-- drop foreign table rem3;
-- drop table loc3;

-- -- Test COPY FROM with the batch_size option enabled
-- alter server loopback options (add batch_size '2');

-- -- Test basic functionality
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- 3	baz
-- \.
-- select * from rem2;

-- delete from rem2;

-- -- Test check constraints
-- alter table loc2 add constraint loc2_f1positive check (f1 >= 0);
-- alter foreign table rem2 add constraint rem2_f1positive check (f1 >= 0);

-- -- check constraint is enforced on the remote side, not locally
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- 3	baz
-- \.
-- copy rem2 from stdin; -- ERROR
-- -1	xyzzy
-- \.
-- select * from rem2;

-- alter foreign table rem2 drop constraint rem2_f1positive;
-- alter table loc2 drop constraint loc2_f1positive;

-- delete from rem2;

-- -- Test remote triggers
-- create trigger trig_row_before_insert before insert on loc2
-- 	for each row execute procedure trig_row_before_insupdate();

-- -- The new values are concatenated with ' triggered !'
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- 3	baz
-- \.
-- select * from rem2;

-- drop trigger trig_row_before_insert on loc2;

-- delete from rem2;

-- create trigger trig_null before insert on loc2
-- 	for each row execute procedure trig_null();

-- -- Nothing happens
-- copy rem2 from stdin;
-- 1	foo
-- 2	bar
-- 3	baz
-- \.
-- select * from rem2;

-- drop trigger trig_null on loc2;

-- delete from rem2;

-- -- Check with zero-column foreign table; batch insert will be disabled
-- alter table loc2 drop column f1;
-- alter table loc2 drop column f2;
-- alter table rem2 drop column f1;
-- alter table rem2 drop column f2;
-- copy rem2 from stdin;



-- \.
-- select * from rem2;

-- delete from rem2;

-- alter server loopback options (drop batch_size);

-- -- ===================================================================
-- -- test for TRUNCATE
-- -- ===================================================================
-- CREATE TABLE tru_rtable0 (id int primary key);
-- CREATE FOREIGN TABLE tru_ftable (id int)
--        SERVER loopback OPTIONS (table_name 'tru_rtable0');
-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(1,10) x);

-- CREATE TABLE tru_ptable (id int) PARTITION BY HASH(id);
-- CREATE TABLE tru_ptable__p0 PARTITION OF tru_ptable
--                             FOR VALUES WITH (MODULUS 2, REMAINDER 0);
-- CREATE TABLE tru_rtable1 (id int primary key);
-- CREATE FOREIGN TABLE tru_ftable__p1 PARTITION OF tru_ptable
--                                     FOR VALUES WITH (MODULUS 2, REMAINDER 1)
--        SERVER loopback OPTIONS (table_name 'tru_rtable1');
-- INSERT INTO tru_ptable (SELECT x FROM generate_series(11,20) x);

-- CREATE TABLE tru_pk_table(id int primary key);
-- CREATE TABLE tru_fk_table(fkey int references tru_pk_table(id));
-- INSERT INTO tru_pk_table (SELECT x FROM generate_series(1,10) x);
-- INSERT INTO tru_fk_table (SELECT x % 10 + 1 FROM generate_series(5,25) x);
-- CREATE FOREIGN TABLE tru_pk_ftable (id int)
--        SERVER loopback OPTIONS (table_name 'tru_pk_table');

-- CREATE TABLE tru_rtable_parent (id int);
-- CREATE TABLE tru_rtable_child (id int);
-- CREATE FOREIGN TABLE tru_ftable_parent (id int)
--        SERVER loopback OPTIONS (table_name 'tru_rtable_parent');
-- CREATE FOREIGN TABLE tru_ftable_child () INHERITS (tru_ftable_parent)
--        SERVER loopback OPTIONS (table_name 'tru_rtable_child');
-- INSERT INTO tru_rtable_parent (SELECT x FROM generate_series(1,8) x);
-- INSERT INTO tru_rtable_child  (SELECT x FROM generate_series(10, 18) x);

-- -- normal truncate
-- SELECT sum(id) FROM tru_ftable;        -- 55
-- TRUNCATE tru_ftable;
-- SELECT count(*) FROM tru_rtable0;		-- 0
-- SELECT count(*) FROM tru_ftable;		-- 0

-- -- 'truncatable' option
-- ALTER SERVER loopback OPTIONS (ADD truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (ADD truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER SERVER loopback OPTIONS (DROP truncatable);
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted

-- -- partitioned table with both local and foreign tables as partitions
-- SELECT sum(id) FROM tru_ptable;        -- 155
-- TRUNCATE tru_ptable;
-- SELECT count(*) FROM tru_ptable;		-- 0
-- SELECT count(*) FROM tru_ptable__p0;	-- 0
-- SELECT count(*) FROM tru_ftable__p1;	-- 0
-- SELECT count(*) FROM tru_rtable1;		-- 0

-- -- 'CASCADE' option
-- SELECT sum(id) FROM tru_pk_ftable;      -- 55
-- TRUNCATE tru_pk_ftable;	-- failed by FK reference
-- TRUNCATE tru_pk_ftable CASCADE;
-- SELECT count(*) FROM tru_pk_ftable;    -- 0
-- SELECT count(*) FROM tru_fk_table;		-- also truncated,0

-- -- truncate two tables at a command
-- INSERT INTO tru_ftable (SELECT x FROM generate_series(1,8) x);
-- INSERT INTO tru_pk_ftable (SELECT x FROM generate_series(3,10) x);
-- SELECT count(*) from tru_ftable; -- 8
-- SELECT count(*) from tru_pk_ftable; -- 8
-- TRUNCATE tru_ftable, tru_pk_ftable CASCADE;
-- SELECT count(*) from tru_ftable; -- 0
-- SELECT count(*) from tru_pk_ftable; -- 0

-- -- truncate with ONLY clause
-- -- Since ONLY is specified, the table tru_ftable_child that inherits
-- -- tru_ftable_parent locally is not truncated.
-- TRUNCATE ONLY tru_ftable_parent;
-- SELECT sum(id) FROM tru_ftable_parent;  -- 126
-- TRUNCATE tru_ftable_parent;
-- SELECT count(*) FROM tru_ftable_parent; -- 0

-- -- in case when remote table has inherited children
-- CREATE TABLE tru_rtable0_child () INHERITS (tru_rtable0);
-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(5,9) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(10,14) x);
-- SELECT sum(id) FROM tru_ftable;   -- 95

-- -- Both parent and child tables in the foreign server are truncated
-- -- even though ONLY is specified because ONLY has no effect
-- -- when truncating a foreign table.
-- TRUNCATE ONLY tru_ftable;
-- SELECT count(*) FROM tru_ftable;   -- 0

-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(21,25) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(26,30) x);
-- SELECT sum(id) FROM tru_ftable;		-- 255
-- TRUNCATE tru_ftable;			-- truncate both of parent and child
-- SELECT count(*) FROM tru_ftable;    -- 0

-- -- cleanup
-- DROP FOREIGN TABLE tru_ftable_parent, tru_ftable_child, tru_pk_ftable,tru_ftable__p1,tru_ftable;
-- DROP TABLE tru_rtable0, tru_rtable1, tru_ptable, tru_ptable__p0, tru_pk_table, tru_fk_table,
-- tru_rtable_parent,tru_rtable_child, tru_rtable0_child;

-- MongoDB FDW does not support IMPORT FOREIGN SCHEMA
-- -- ===================================================================
-- -- test IMPORT FOREIGN SCHEMA
-- -- ===================================================================

-- CREATE SCHEMA import_source;
-- CREATE TABLE import_source.t1 (c1 int, c2 varchar NOT NULL);
-- CREATE TABLE import_source.t2 (c1 int default 42, c2 varchar NULL, c3 text collate "POSIX");
-- CREATE TYPE typ1 AS (m1 int, m2 varchar);
-- CREATE TABLE import_source.t3 (c1 timestamptz default now(), c2 typ1);
-- CREATE TABLE import_source."x 4" (c1 float8, "C 2" text, c3 varchar(42));
-- CREATE TABLE import_source."x 5" (c1 float8);
-- ALTER TABLE import_source."x 5" DROP COLUMN c1;
-- CREATE TABLE import_source."x 6" (c1 int, c2 int generated always as (c1 * 2) stored);
-- CREATE TABLE import_source.t4 (c1 int) PARTITION BY RANGE (c1);
-- CREATE TABLE import_source.t4_part PARTITION OF import_source.t4
--   FOR VALUES FROM (1) TO (100);
-- CREATE TABLE import_source.t4_part2 PARTITION OF import_source.t4
--   FOR VALUES FROM (100) TO (200);

-- CREATE SCHEMA import_dest1;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER loopback INTO import_dest1;
-- \det+ import_dest1.*
-- \d import_dest1.*

-- -- Options
-- CREATE SCHEMA import_dest2;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER loopback INTO import_dest2
--   OPTIONS (import_default 'true');
-- \det+ import_dest2.*
-- \d import_dest2.*
-- CREATE SCHEMA import_dest3;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER loopback INTO import_dest3
--   OPTIONS (import_collate 'false', import_generated 'false', import_not_null 'false');
-- \det+ import_dest3.*
-- \d import_dest3.*

-- -- Check LIMIT TO and EXCEPT
-- CREATE SCHEMA import_dest4;
-- IMPORT FOREIGN SCHEMA import_source LIMIT TO (t1, nonesuch)
--   FROM SERVER loopback INTO import_dest4;
-- \det+ import_dest4.*
-- IMPORT FOREIGN SCHEMA import_source EXCEPT (t1, "x 4", nonesuch)
--   FROM SERVER loopback INTO import_dest4;
-- \det+ import_dest4.*

-- -- Assorted error cases
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER loopback INTO import_dest4;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER loopback INTO import_dest4;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER loopback INTO notthere;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER nowhere INTO notthere;

-- -- Check case of a type present only on the remote server.
-- -- We can fake this by dropping the type locally in our transaction.
-- CREATE TYPE "Colors" AS ENUM ('red', 'green', 'blue');
-- CREATE TABLE import_source.t5 (c1 int, c2 text collate "C", "Col" "Colors");

-- CREATE SCHEMA import_dest5;
-- BEGIN;
-- DROP TYPE "Colors" CASCADE;
-- IMPORT FOREIGN SCHEMA import_source LIMIT TO (t5)
--   FROM SERVER loopback INTO import_dest5;  -- ERROR

-- ROLLBACK;

-- BEGIN;


-- CREATE SERVER fetch101 FOREIGN DATA WRAPPER postgres_fdw OPTIONS( fetch_size '101' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'fetch101'
-- AND srvoptions @> array['fetch_size=101'];

-- ALTER SERVER fetch101 OPTIONS( SET fetch_size '202' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'fetch101'
-- AND srvoptions @> array['fetch_size=101'];

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'fetch101'
-- AND srvoptions @> array['fetch_size=202'];

-- CREATE FOREIGN TABLE table30000 ( x int ) SERVER fetch101 OPTIONS ( fetch_size '30000' );

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30000'::regclass
-- AND ftoptions @> array['fetch_size=30000'];

-- ALTER FOREIGN TABLE table30000 OPTIONS ( SET fetch_size '60000');

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30000'::regclass
-- AND ftoptions @> array['fetch_size=30000'];

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30000'::regclass
-- AND ftoptions @> array['fetch_size=60000'];

-- ROLLBACK;

-- PGSpider Extension only support Partition by List. This test is not
-- suitable.
-- -- ===================================================================
-- -- test partitionwise joins
-- -- ===================================================================
-- SET enable_partitionwise_join=on;

-- CREATE TABLE fprt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
-- CREATE TABLE fprt1_p1 (LIKE fprt1);
-- CREATE TABLE fprt1_p2 (LIKE fprt1);
-- ALTER TABLE fprt1_p1 SET (autovacuum_enabled = 'false');
-- ALTER TABLE fprt1_p2 SET (autovacuum_enabled = 'false');
-- INSERT INTO fprt1_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 2) i;
-- INSERT INTO fprt1_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 2) i;
-- CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (0) TO (250)
-- 	SERVER loopback OPTIONS (table_name 'fprt1_p1', use_remote_estimate 'true');
-- CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (250) TO (500)
-- 	SERVER loopback OPTIONS (TABLE_NAME 'fprt1_p2');
-- ANALYZE fprt1;
-- ANALYZE fprt1_p1;
-- ANALYZE fprt1_p2;

-- CREATE TABLE fprt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
-- CREATE TABLE fprt2_p1 (LIKE fprt2);
-- CREATE TABLE fprt2_p2 (LIKE fprt2);
-- ALTER TABLE fprt2_p1 SET (autovacuum_enabled = 'false');
-- ALTER TABLE fprt2_p2 SET (autovacuum_enabled = 'false');
-- INSERT INTO fprt2_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 3) i;
-- INSERT INTO fprt2_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 3) i;
-- CREATE FOREIGN TABLE ftprt2_p1 (b int, c varchar, a int)
-- 	SERVER loopback OPTIONS (table_name 'fprt2_p1', use_remote_estimate 'true');
-- ALTER TABLE fprt2 ATTACH PARTITION ftprt2_p1 FOR VALUES FROM (0) TO (250);
-- CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (250) TO (500)
-- 	SERVER loopback OPTIONS (table_name 'fprt2_p2', use_remote_estimate 'true');
-- ANALYZE fprt2;
-- ANALYZE fprt2_p1;
-- ANALYZE fprt2_p2;

-- -- inner join three tables
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;
-- SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;

-- -- left outer join + nullable clause
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;
-- SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;

-- -- with whole-row reference; partitionwise join does not apply
-- EXPLAIN (COSTS OFF)
-- SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;
-- SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;

-- -- join with lateral reference
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;
-- SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;

-- -- with PHVs, partitionwise join selected but no join pushdown
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
-- SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- -- test FOR UPDATE; partitionwise join does not apply
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;
-- SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;

-- RESET enable_partitionwise_join;


-- -- ===================================================================
-- -- test partitionwise aggregates
-- -- ===================================================================

-- CREATE TABLE pagg_tab (a int, b int, c text) PARTITION BY RANGE(a);

-- CREATE TABLE pagg_tab_p1 (LIKE pagg_tab);
-- CREATE TABLE pagg_tab_p2 (LIKE pagg_tab);
-- CREATE TABLE pagg_tab_p3 (LIKE pagg_tab);

-- INSERT INTO pagg_tab_p1 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 10;
-- INSERT INTO pagg_tab_p2 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 20 and (i % 30) >= 10;
-- INSERT INTO pagg_tab_p3 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 30 and (i % 30) >= 20;

-- -- Create foreign partitions
-- CREATE FOREIGN TABLE fpagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0) TO (10) SERVER loopback OPTIONS (table_name 'pagg_tab_p1');
-- CREATE FOREIGN TABLE fpagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10) TO (20) SERVER loopback OPTIONS (table_name 'pagg_tab_p2');;
-- CREATE FOREIGN TABLE fpagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20) TO (30) SERVER loopback OPTIONS (table_name 'pagg_tab_p3');;

-- ANALYZE pagg_tab;
-- ANALYZE fpagg_tab_p1;
-- ANALYZE fpagg_tab_p2;
-- ANALYZE fpagg_tab_p3;

-- -- When GROUP BY clause matches with PARTITION KEY.
-- -- Plan with partitionwise aggregates is disabled
-- SET enable_partitionwise_aggregate TO false;
-- EXPLAIN (COSTS OFF)
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- -- Plan with partitionwise aggregates is enabled
-- SET enable_partitionwise_aggregate TO true;
-- EXPLAIN (COSTS OFF)
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- -- Check with whole-row reference
-- -- Should have all the columns in the target list for the given relation
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
-- SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- -- When GROUP BY clause does not match with PARTITION KEY.
-- EXPLAIN (COSTS OFF)
-- SELECT b, avg(a), max(a), count(*) FROM pagg_tab GROUP BY b HAVING sum(a) < 700 ORDER BY 1;

-- MongoDB does not support superuser
-- -- ===================================================================
-- -- access rights and superuser
-- -- ===================================================================

-- -- Non-superuser cannot create a FDW without a password in the connstr
-- CREATE ROLE regress_nosuper NOSUPERUSER;

-- GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO regress_nosuper;

-- SET ROLE regress_nosuper;

-- SHOW is_superuser;

-- -- This will be OK, we can create the FDW
-- DO $d$
--     BEGIN
--         EXECUTE $$CREATE SERVER loopback_nopw FOREIGN DATA WRAPPER postgres_fdw
--             OPTIONS (dbname '$$||current_database()||$$',
--                      port '$$||current_setting('port')||$$'
--             )$$;
--     END;
-- $d$;

-- -- But creation of user mappings for non-superusers should fail
-- CREATE USER MAPPING FOR public SERVER loopback_nopw;
-- CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

-- CREATE FOREIGN TABLE ft1_nopw (
-- 	c1 int NOT NULL,
-- 	c2 int NOT NULL,
-- 	c3 text,
-- 	c4 timestamptz,
-- 	c5 timestamp,
-- 	c6 varchar(10),
-- 	c7 char(10) default 'ft1',
-- 	c8 user_enum
-- ) SERVER loopback_nopw OPTIONS (schema_name 'public', table_name 'ft1');

-- SELECT * FROM ft1_nopw LIMIT 1;

-- -- If we add a password to the connstr it'll fail, because we don't allow passwords
-- -- in connstrs only in user mappings.

-- ALTER SERVER loopback_nopw OPTIONS (ADD password 'dummypw');

-- -- If we add a password for our user mapping instead, we should get a different
-- -- error because the password wasn't actually *used* when we run with trust auth.
-- --
-- -- This won't work with installcheck, but neither will most of the FDW checks.

-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password 'dummypw');

-- SELECT * FROM ft1_nopw LIMIT 1;

-- -- Unpriv user cannot make the mapping passwordless
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password_required 'false');


-- SELECT * FROM ft1_nopw LIMIT 1;

-- RESET ROLE;

-- -- But the superuser can
-- ALTER USER MAPPING FOR regress_nosuper SERVER loopback_nopw OPTIONS (ADD password_required 'false');

-- SET ROLE regress_nosuper;

-- -- Should finally work now
-- SELECT * FROM ft1_nopw LIMIT 1;

-- -- unpriv user also cannot set sslcert / sslkey on the user mapping
-- -- first set password_required so we see the right error messages
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (SET password_required 'true');
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslcert 'foo.crt');
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslkey 'foo.key');

-- -- We're done with the role named after a specific user and need to check the
-- -- changes to the public mapping.
-- DROP USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

-- -- This will fail again as it'll resolve the user mapping for public, which
-- -- lacks password_required=false
-- SELECT * FROM ft1_nopw LIMIT 1;

-- RESET ROLE;

-- -- The user mapping for public is passwordless and lacks the password_required=false
-- -- mapping option, but will work because the current user is a superuser.
-- SELECT * FROM ft1_nopw LIMIT 1;

-- -- cleanup
-- DROP USER MAPPING FOR public SERVER loopback_nopw;
-- DROP OWNED BY regress_nosuper;
-- DROP ROLE regress_nosuper;

-- -- Clean-up
-- RESET enable_partitionwise_aggregate;

-- -- Two-phase transactions are not supported.
-- BEGIN;
-- SELECT count(*) FROM ft1;
-- -- error here
-- PREPARE TRANSACTION 'fdw_tpc';
-- ROLLBACK;

-- -- ===================================================================
-- reestablish new connection
-- ===================================================================

-- -- Change application_name of remote connection to special one
-- -- so that we can easily terminate the connection later.
-- ALTER SERVER loopback OPTIONS (application_name 'fdw_retry_check');

-- -- Make sure we have a remote connection.
-- SELECT 1 FROM ft1 LIMIT 1;

-- -- Terminate the remote connection and wait for the termination to complete.
-- -- (If a cache flush happens, the remote connection might have already been
-- -- dropped; so code this step in a way that doesn't fail if no connection.)
-- DO $$ BEGIN
-- PERFORM pg_terminate_backend(pid, 180000) FROM pg_stat_activity
-- 	WHERE application_name = 'fdw_retry_check';
-- END $$;

-- -- This query should detect the broken connection when starting new remote
-- -- transaction, reestablish new connection, and then succeed.
-- BEGIN;
-- SELECT 1 FROM ft1 LIMIT 1;

-- -- If we detect the broken connection when starting a new remote
-- -- subtransaction, we should fail instead of establishing a new connection.
-- -- Terminate the remote connection and wait for the termination to complete.
-- DO $$ BEGIN
-- PERFORM pg_terminate_backend(pid, 180000) FROM pg_stat_activity
-- 	WHERE application_name = 'fdw_retry_check';
-- END $$;
-- SAVEPOINT s;
-- -- The text of the error might vary across platforms, so only show SQLSTATE.
-- \set VERBOSITY sqlstate
-- SELECT 1 FROM ft1 LIMIT 1;    -- should fail
-- \set VERBOSITY default
-- COMMIT;

-- -- =============================================================================
-- -- test connection invalidation cases and postgres_fdw_get_connections function
-- -- =============================================================================
-- -- Let's ensure to close all the existing cached connections.
-- SELECT 1 FROM postgres_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- This test case is for closing the connection in pgfdw_xact_callback
BEGIN;
-- Connection xact depth becomes 1 i.e. the connection is in midst of the xact.
--Testcase 805:
SELECT 1 FROM ft1 LIMIT 1;
-- -- List all the existing cached connections. loopback and loopback3 should be
-- -- output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- -- Connections are not closed at the end of the alter and drop statements.
-- -- That's because the connections are in midst of this xact,
-- -- they are just marked as invalid in pgfdw_inval_callback.
-- ALTER SERVER loopback OPTIONS (ADD use_remote_estimate 'off');
-- DROP SERVER loopback3 CASCADE;
-- -- List all the existing cached connections. loopback and loopback3
-- -- should be output as invalid connections. Also the server name for
-- -- loopback3 should be NULL because the server was dropped.
-- SELECT * FROM postgres_fdw_get_connections() ORDER BY 1;
-- -- The invalid connections get closed in pgfdw_xact_callback during commit.
COMMIT;
-- -- All cached connections were closed while committing above xact, so no
-- -- records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;

-- -- =======================================================================
-- -- test postgres_fdw_disconnect and postgres_fdw_disconnect_all functions
-- -- =======================================================================
-- BEGIN;
-- -- Ensure to cache loopback connection.
-- SELECT 1 FROM ft1 LIMIT 1;
-- -- Ensure to cache loopback2 connection.
-- SELECT 1 FROM ft6 LIMIT 1;
-- -- List all the existing cached connections. loopback and loopback2 should be
-- -- output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- -- Issue a warning and return false as loopback connection is still in use and
-- -- can not be closed.
-- SELECT postgres_fdw_disconnect('loopback');
-- -- List all the existing cached connections. loopback and loopback2 should be
-- -- output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- -- Return false as connections are still in use, warnings are issued.
-- -- But disable warnings temporarily because the order of them is not stable.
-- SET client_min_messages = 'ERROR';
-- SELECT postgres_fdw_disconnect_all();
-- RESET client_min_messages;
-- COMMIT;
-- -- Ensure that loopback2 connection is closed.
-- SELECT 1 FROM postgres_fdw_disconnect('loopback2');
-- SELECT server_name FROM postgres_fdw_get_connections() WHERE server_name = 'loopback2';
-- -- Return false as loopback2 connection is closed already.
-- SELECT postgres_fdw_disconnect('loopback2');
-- -- Return an error as there is no foreign server with given name.
-- SELECT postgres_fdw_disconnect('unknownserver');
-- -- Let's ensure to close all the existing cached connections.
-- SELECT 1 FROM postgres_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;

-- -- =============================================================================
-- -- test case for having multiple cached connections for a foreign server
-- -- =============================================================================
-- CREATE ROLE regress_multi_conn_user1 SUPERUSER;
-- CREATE ROLE regress_multi_conn_user2 SUPERUSER;
-- CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER loopback;
-- CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER loopback;

-- BEGIN;
-- -- Will cache loopback connection with user mapping for regress_multi_conn_user1
-- SET ROLE regress_multi_conn_user1;
-- SELECT 1 FROM ft1 LIMIT 1;
-- RESET ROLE;

-- -- Will cache loopback connection with user mapping for regress_multi_conn_user2
-- SET ROLE regress_multi_conn_user2;
-- SELECT 1 FROM ft1 LIMIT 1;
-- RESET ROLE;

-- -- Should output two connections for loopback server
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- COMMIT;
-- -- Let's ensure to close all the existing cached connections.
-- SELECT 1 FROM postgres_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;

-- -- Clean up
-- DROP USER MAPPING FOR regress_multi_conn_user1 SERVER loopback;
-- DROP USER MAPPING FOR regress_multi_conn_user2 SERVER loopback;
-- DROP ROLE regress_multi_conn_user1;
-- DROP ROLE regress_multi_conn_user2;

-- -- ===================================================================
-- -- Test foreign server level option keep_connections
-- -- ===================================================================
-- -- By default, the connections associated with foreign server are cached i.e.
-- -- keep_connections option is on. Set it to off.
-- ALTER SERVER loopback OPTIONS (keep_connections 'off');
-- -- connection to loopback server is closed at the end of xact
-- -- as keep_connections was set to off.
-- SELECT 1 FROM ft1 LIMIT 1;
-- -- No cached connections, so no records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- ALTER SERVER loopback OPTIONS (SET keep_connections 'on');

-- -- ===================================================================
-- -- batch insert
-- -- ===================================================================

-- BEGIN;

-- CREATE SERVER batch10 FOREIGN DATA WRAPPER postgres_fdw OPTIONS( batch_size '10' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=10'];

-- ALTER SERVER batch10 OPTIONS( SET batch_size '20' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=10'];

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=20'];

-- CREATE FOREIGN TABLE table30 ( x int ) SERVER batch10 OPTIONS ( batch_size '30' );

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=30'];

-- ALTER FOREIGN TABLE table30 OPTIONS ( SET batch_size '40');

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=30'];

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=40'];

-- ROLLBACK;

-- CREATE TABLE batch_table ( x int );

-- CREATE FOREIGN TABLE ftable ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table', batch_size '10' );
-- EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;
-- INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;
-- INSERT INTO ftable SELECT * FROM generate_series(11, 31) i;
-- INSERT INTO ftable VALUES (32);
-- INSERT INTO ftable VALUES (33), (34);
-- SELECT COUNT(*) FROM ftable;
-- TRUNCATE batch_table;
-- DROP FOREIGN TABLE ftable;

-- -- Disable batch insert
-- CREATE FOREIGN TABLE ftable ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table', batch_size '1' );
-- EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable VALUES (1), (2);
-- INSERT INTO ftable VALUES (1), (2);
-- SELECT COUNT(*) FROM ftable;

-- -- Disable batch inserting into foreign tables with BEFORE ROW INSERT triggers
-- -- even if the batch_size option is enabled.
-- ALTER FOREIGN TABLE ftable OPTIONS ( SET batch_size '10' );
-- CREATE TRIGGER trig_row_before BEFORE INSERT ON ftable
-- FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
-- EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable VALUES (3), (4);
-- INSERT INTO ftable VALUES (3), (4);
-- SELECT COUNT(*) FROM ftable;

-- -- Clean up
-- DROP TRIGGER trig_row_before ON ftable;
-- DROP FOREIGN TABLE ftable;
-- DROP TABLE batch_table;

-- -- Use partitioning
-- CREATE TABLE batch_table ( x int ) PARTITION BY HASH (x);

-- CREATE TABLE batch_table_p0 (LIKE batch_table);
-- CREATE FOREIGN TABLE batch_table_p0f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 0)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table_p0', batch_size '10');

-- CREATE TABLE batch_table_p1 (LIKE batch_table);
-- CREATE FOREIGN TABLE batch_table_p1f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 1)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table_p1', batch_size '1');

-- CREATE TABLE batch_table_p2
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 2);

-- INSERT INTO batch_table SELECT * FROM generate_series(1, 66) i;
-- SELECT COUNT(*) FROM batch_table;

-- -- Clean up
-- DROP TABLE batch_table;
-- DROP TABLE batch_table_p0;
-- DROP TABLE batch_table_p1;

-- -- Check that batched mode also works for some inserts made during
-- -- cross-partition updates
-- CREATE TABLE batch_cp_upd_test (a int) PARTITION BY LIST (a);
-- CREATE TABLE batch_cp_upd_test1 (LIKE batch_cp_upd_test);
-- CREATE FOREIGN TABLE batch_cp_upd_test1_f
-- 	PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (1)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_cp_upd_test1', batch_size '10');
-- CREATE TABLE batch_cp_upd_test2 PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (2);
-- CREATE TABLE batch_cp_upd_test3 (LIKE batch_cp_upd_test);
-- CREATE FOREIGN TABLE batch_cp_upd_test3_f
-- 	PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (3)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_cp_upd_test3', batch_size '1');

-- -- Create statement triggers on remote tables that "log" any INSERTs
-- -- performed on them.
-- CREATE TABLE cmdlog (cmd text);
-- CREATE FUNCTION log_stmt() RETURNS TRIGGER LANGUAGE plpgsql AS $$
-- 	BEGIN INSERT INTO public.cmdlog VALUES (TG_OP || ' on ' || TG_RELNAME); RETURN NULL; END;
-- $$;
-- CREATE TRIGGER stmt_trig AFTER INSERT ON batch_cp_upd_test1
-- 	FOR EACH STATEMENT EXECUTE FUNCTION log_stmt();
-- CREATE TRIGGER stmt_trig AFTER INSERT ON batch_cp_upd_test3
-- 	FOR EACH STATEMENT EXECUTE FUNCTION log_stmt();

-- -- This update moves rows from the local partition 'batch_cp_upd_test2' to the
-- -- foreign partition 'batch_cp_upd_test1', one that has insert batching
-- -- enabled, so a single INSERT for both rows.
-- INSERT INTO batch_cp_upd_test VALUES (2), (2);
-- UPDATE batch_cp_upd_test t SET a = 1 FROM (VALUES (1), (2)) s(a) WHERE t.a = s.a AND s.a = 2;

-- -- This one moves rows from the local partition 'batch_cp_upd_test2' to the
-- -- foreign partition 'batch_cp_upd_test2', one that has insert batching
-- -- disabled, so separate INSERTs for the two rows.
-- INSERT INTO batch_cp_upd_test VALUES (2), (2);
-- UPDATE batch_cp_upd_test t SET a = 3 FROM (VALUES (1), (2)) s(a) WHERE t.a = s.a AND s.a = 2;

-- SELECT tableoid::regclass, * FROM batch_cp_upd_test ORDER BY 1;

-- -- Should see 1 INSERT on batch_cp_upd_test1 and 2 on batch_cp_upd_test3 as
-- -- described above.
-- SELECT * FROM cmdlog ORDER BY 1;

-- -- Clean up
-- DROP TABLE batch_cp_upd_test;
-- DROP TABLE batch_cp_upd_test1;
-- DROP TABLE batch_cp_upd_test3;
-- DROP TABLE cmdlog;
-- DROP FUNCTION log_stmt();

-- -- Use partitioning
-- ALTER SERVER loopback OPTIONS (ADD batch_size '10');

-- CREATE TABLE batch_table ( x int, field1 text, field2 text) PARTITION BY HASH (x);

-- CREATE TABLE batch_table_p0 (LIKE batch_table);
-- ALTER TABLE batch_table_p0 ADD CONSTRAINT p0_pkey PRIMARY KEY (x);
-- CREATE FOREIGN TABLE batch_table_p0f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 2, REMAINDER 0)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table_p0');

-- CREATE TABLE batch_table_p1 (LIKE batch_table);
-- ALTER TABLE batch_table_p1 ADD CONSTRAINT p1_pkey PRIMARY KEY (x);
-- CREATE FOREIGN TABLE batch_table_p1f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 2, REMAINDER 1)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table_p1');

-- INSERT INTO batch_table SELECT i, 'test'||i, 'test'|| i FROM generate_series(1, 50) i;
-- SELECT COUNT(*) FROM batch_table;
-- SELECT * FROM batch_table ORDER BY x;

-- -- Clean up
-- DROP TABLE batch_table;
-- DROP TABLE batch_table_p0;
-- DROP TABLE batch_table_p1;

-- ALTER SERVER loopback OPTIONS (DROP batch_size);

-- -- Test that pending inserts are handled properly when needed
-- CREATE TABLE batch_table (a text, b int);
-- CREATE FOREIGN TABLE ftable (a text, b int)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table', batch_size '2');
-- CREATE TABLE ltable (a text, b int);
-- CREATE FUNCTION ftable_rowcount_trigf() RETURNS trigger LANGUAGE plpgsql AS
-- $$
-- begin
-- 	raise notice '%: there are % rows in ftable',
-- 		TG_NAME, (SELECT count(*) FROM ftable);
-- 	if TG_OP = 'DELETE' then
-- 		return OLD;
-- 	else
-- 		return NEW;
-- 	end if;
-- end;
-- $$;
-- CREATE TRIGGER ftable_rowcount_trigger
-- BEFORE INSERT OR UPDATE OR DELETE ON ltable
-- FOR EACH ROW EXECUTE PROCEDURE ftable_rowcount_trigf();

-- WITH t AS (
-- 	INSERT INTO ltable VALUES ('AAA', 42), ('BBB', 42) RETURNING *
-- )
-- INSERT INTO ftable SELECT * FROM t;

-- SELECT * FROM ltable;
-- SELECT * FROM ftable;
-- DELETE FROM ftable;

-- WITH t AS (
-- 	UPDATE ltable SET b = b + 100 RETURNING *
-- )
-- INSERT INTO ftable SELECT * FROM t;

-- SELECT * FROM ltable;
-- SELECT * FROM ftable;
-- DELETE FROM ftable;

-- WITH t AS (
-- 	DELETE FROM ltable RETURNING *
-- )
-- INSERT INTO ftable SELECT * FROM t;

-- SELECT * FROM ltable;
-- SELECT * FROM ftable;
-- DELETE FROM ftable;

-- -- Clean up
-- DROP FOREIGN TABLE ftable;
-- DROP TABLE batch_table;
-- DROP TRIGGER ftable_rowcount_trigger ON ltable;
-- DROP TABLE ltable;

-- CREATE TABLE parent (a text, b int) PARTITION BY LIST (a);
-- CREATE TABLE batch_table (a text, b int);
-- CREATE FOREIGN TABLE ftable
-- 	PARTITION OF parent
-- 	FOR VALUES IN ('AAA')
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_table', batch_size '2');
-- CREATE TABLE ltable
-- 	PARTITION OF parent
-- 	FOR VALUES IN ('BBB');
-- CREATE TRIGGER ftable_rowcount_trigger
-- BEFORE INSERT ON ltable
-- FOR EACH ROW EXECUTE PROCEDURE ftable_rowcount_trigf();

-- INSERT INTO parent VALUES ('AAA', 42), ('BBB', 42), ('AAA', 42), ('BBB', 42);

-- SELECT tableoid::regclass, * FROM parent;

-- -- Clean up
-- DROP FOREIGN TABLE ftable;
-- DROP TABLE batch_table;
-- DROP TRIGGER ftable_rowcount_trigger ON ltable;
-- DROP TABLE ltable;
-- DROP TABLE parent;
-- DROP FUNCTION ftable_rowcount_trigf;

-- -- ===================================================================
-- -- test asynchronous execution
-- -- ===================================================================

-- ALTER SERVER loopback OPTIONS (DROP extensions);
-- ALTER SERVER loopback OPTIONS (ADD async_capable 'true');
-- ALTER SERVER loopback2 OPTIONS (ADD async_capable 'true');

-- CREATE TABLE async_pt (a int, b int, c text) PARTITION BY RANGE (a);
-- CREATE TABLE base_tbl1 (a int, b int, c text);
-- CREATE TABLE base_tbl2 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p1 PARTITION OF async_pt FOR VALUES FROM (1000) TO (2000)
--   SERVER loopback OPTIONS (table_name 'base_tbl1');
-- CREATE FOREIGN TABLE async_p2 PARTITION OF async_pt FOR VALUES FROM (2000) TO (3000)
--   SERVER loopback2 OPTIONS (table_name 'base_tbl2');
-- INSERT INTO async_p1 SELECT 1000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- INSERT INTO async_p2 SELECT 2000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- -- simple queries
-- CREATE TABLE result_tbl (a int, b int, c text);

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b % 100 = 0;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b % 100 = 0;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT a, b, 'AAA' || c FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT a, b, 'AAA' || c FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- -- Test error handling, if accessing one of the foreign partitions errors out
-- CREATE FOREIGN TABLE async_p_broken PARTITION OF async_pt FOR VALUES FROM (10000) TO (10001)
--   SERVER loopback OPTIONS (table_name 'non_existent_table');
-- SELECT * FROM async_pt;
-- DROP FOREIGN TABLE async_p_broken;

-- -- Check case where multiple partitions use the same connection
-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p3 PARTITION OF async_pt FOR VALUES FROM (3000) TO (4000)
--   SERVER loopback2 OPTIONS (table_name 'base_tbl3');
-- INSERT INTO async_p3 SELECT 3000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- DROP FOREIGN TABLE async_p3;
-- DROP TABLE base_tbl3;

-- -- Check case where the partitioned table has local/remote partitions
-- CREATE TABLE async_p3 PARTITION OF async_pt FOR VALUES FROM (3000) TO (4000);
-- INSERT INTO async_p3 SELECT 3000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- -- partitionwise joins
-- SET enable_partitionwise_join TO true;

-- CREATE TABLE join_tbl (a1 int, b1 int, c1 text, a2 int, b2 int, c2 text);

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO join_tbl SELECT * FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;
-- INSERT INTO join_tbl SELECT * FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO join_tbl SELECT t1.a, t1.b, 'AAA' || t1.c, t2.a, t2.b, 'AAA' || t2.c FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;
-- INSERT INTO join_tbl SELECT t1.a, t1.b, 'AAA' || t1.c, t2.a, t2.b, 'AAA' || t2.c FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- RESET enable_partitionwise_join;

-- -- Test rescan of an async Append node with do_exec_prune=false
-- SET enable_hashjoin TO false;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO join_tbl SELECT * FROM async_p1 t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;
-- INSERT INTO join_tbl SELECT * FROM async_p1 t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- RESET enable_hashjoin;

-- -- Test interaction of async execution with plan-time partition pruning
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt WHERE a < 3000;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt WHERE a < 2000;

-- -- Test interaction of async execution with run-time partition pruning
-- SET plan_cache_mode TO force_generic_plan;

-- PREPARE async_pt_query (int, int) AS
--   INSERT INTO result_tbl SELECT * FROM async_pt WHERE a < $1 AND b === $2;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- EXECUTE async_pt_query (3000, 505);
-- EXECUTE async_pt_query (3000, 505);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- EXECUTE async_pt_query (2000, 505);
-- EXECUTE async_pt_query (2000, 505);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- RESET plan_cache_mode;

-- CREATE TABLE local_tbl(a int, b int, c text);
-- INSERT INTO local_tbl VALUES (1505, 505, 'foo'), (2505, 505, 'bar');
-- ANALYZE local_tbl;

-- CREATE INDEX base_tbl1_idx ON base_tbl1 (a);
-- CREATE INDEX base_tbl2_idx ON base_tbl2 (a);
-- CREATE INDEX async_p3_idx ON async_p3 (a);
-- ANALYZE base_tbl1;
-- ANALYZE base_tbl2;
-- ANALYZE async_p3;

-- ALTER FOREIGN TABLE async_p1 OPTIONS (use_remote_estimate 'true');
-- ALTER FOREIGN TABLE async_p2 OPTIONS (use_remote_estimate 'true');

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM local_tbl, async_pt WHERE local_tbl.a = async_pt.a AND local_tbl.c = 'bar';
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM local_tbl, async_pt WHERE local_tbl.a = async_pt.a AND local_tbl.c = 'bar';
-- SELECT * FROM local_tbl, async_pt WHERE local_tbl.a = async_pt.a AND local_tbl.c = 'bar';

-- ALTER FOREIGN TABLE async_p1 OPTIONS (DROP use_remote_estimate);
-- ALTER FOREIGN TABLE async_p2 OPTIONS (DROP use_remote_estimate);

-- DROP TABLE local_tbl;
-- DROP INDEX base_tbl1_idx;
-- DROP INDEX base_tbl2_idx;
-- DROP INDEX async_p3_idx;

-- -- UNION queries
-- SET enable_sort TO off;
-- SET enable_incremental_sort TO off;
-- -- Adjust fdw_startup_cost so that we get an unordered path in the Append.
-- ALTER SERVER loopback2 OPTIONS (ADD fdw_startup_cost '0.00');

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl
-- (SELECT a, b, 'AAA' || c FROM async_p1 ORDER BY a LIMIT 10)
-- UNION
-- (SELECT a, b, 'AAA' || c FROM async_p2 WHERE b < 10);
-- INSERT INTO result_tbl
-- (SELECT a, b, 'AAA' || c FROM async_p1 ORDER BY a LIMIT 10)
-- UNION
-- (SELECT a, b, 'AAA' || c FROM async_p2 WHERE b < 10);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl
-- (SELECT a, b, 'AAA' || c FROM async_p1 ORDER BY a LIMIT 10)
-- UNION ALL
-- (SELECT a, b, 'AAA' || c FROM async_p2 WHERE b < 10);
-- INSERT INTO result_tbl
-- (SELECT a, b, 'AAA' || c FROM async_p1 ORDER BY a LIMIT 10)
-- UNION ALL
-- (SELECT a, b, 'AAA' || c FROM async_p2 WHERE b < 10);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- RESET enable_incremental_sort;
-- RESET enable_sort;
-- ALTER SERVER loopback2 OPTIONS (DROP fdw_startup_cost);

-- -- Disable async execution if we use gating Result nodes for pseudoconstant
-- -- quals
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt WHERE CURRENT_USER = SESSION_USER;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- (SELECT * FROM async_p1 WHERE CURRENT_USER = SESSION_USER)
-- UNION ALL
-- (SELECT * FROM async_p2 WHERE CURRENT_USER = SESSION_USER);

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM ((SELECT * FROM async_p1 WHERE b < 10) UNION ALL (SELECT * FROM async_p2 WHERE b < 10)) s WHERE CURRENT_USER = SESSION_USER;

-- -- Test that pending requests are processed properly
-- SET enable_mergejoin TO false;
-- SET enable_hashjoin TO false;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt t1, async_p2 t2 WHERE t1.a = t2.a AND t1.b === 505;
-- SELECT * FROM async_pt t1, async_p2 t2 WHERE t1.a = t2.a AND t1.b === 505;

-- CREATE TABLE local_tbl (a int, b int, c text);
-- INSERT INTO local_tbl VALUES (1505, 505, 'foo');
-- ANALYZE local_tbl;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM local_tbl t1 LEFT JOIN (SELECT *, (SELECT count(*) FROM async_pt WHERE a < 3000) FROM async_pt WHERE a < 3000) t2 ON t1.a = t2.a;
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM local_tbl t1 LEFT JOIN (SELECT *, (SELECT count(*) FROM async_pt WHERE a < 3000) FROM async_pt WHERE a < 3000) t2 ON t1.a = t2.a;
-- SELECT * FROM local_tbl t1 LEFT JOIN (SELECT *, (SELECT count(*) FROM async_pt WHERE a < 3000) FROM async_pt WHERE a < 3000) t2 ON t1.a = t2.a;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;

-- -- Check with foreign modify
-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE remote_tbl (a int, b int, c text)
--   SERVER loopback OPTIONS (table_name 'base_tbl3');
-- INSERT INTO remote_tbl VALUES (2505, 505, 'bar');

-- CREATE TABLE base_tbl4 (a int, b int, c text);
-- CREATE FOREIGN TABLE insert_tbl (a int, b int, c text)
--   SERVER loopback OPTIONS (table_name 'base_tbl4');

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO insert_tbl (SELECT * FROM local_tbl UNION ALL SELECT * FROM remote_tbl);
-- INSERT INTO insert_tbl (SELECT * FROM local_tbl UNION ALL SELECT * FROM remote_tbl);

-- SELECT * FROM insert_tbl ORDER BY a;

-- -- Check with direct modify
-- EXPLAIN (VERBOSE, COSTS OFF)
-- WITH t AS (UPDATE remote_tbl SET c = c || c RETURNING *)
-- INSERT INTO join_tbl SELECT * FROM async_pt LEFT JOIN t ON (async_pt.a = t.a AND async_pt.b = t.b) WHERE async_pt.b === 505;
-- WITH t AS (UPDATE remote_tbl SET c = c || c RETURNING *)
-- INSERT INTO join_tbl SELECT * FROM async_pt LEFT JOIN t ON (async_pt.a = t.a AND async_pt.b = t.b) WHERE async_pt.b === 505;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- DROP TABLE local_tbl;
-- DROP FOREIGN TABLE remote_tbl;
-- DROP FOREIGN TABLE insert_tbl;
-- DROP TABLE base_tbl3;
-- DROP TABLE base_tbl4;

-- RESET enable_mergejoin;
-- RESET enable_hashjoin;

-- -- Test that UPDATE/DELETE with inherited target works with async_capable enabled
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE async_pt SET c = c || c WHERE b = 0 RETURNING *;
-- UPDATE async_pt SET c = c || c WHERE b = 0 RETURNING *;
-- EXPLAIN (VERBOSE, COSTS OFF)
-- DELETE FROM async_pt WHERE b = 0 RETURNING *;
-- DELETE FROM async_pt WHERE b = 0 RETURNING *;

-- -- Check EXPLAIN ANALYZE for a query that scans empty partitions asynchronously
-- DELETE FROM async_p1;
-- DELETE FROM async_p2;
-- DELETE FROM async_p3;

-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM async_pt;

-- -- Clean up
-- DROP TABLE async_pt;
-- DROP TABLE base_tbl1;
-- DROP TABLE base_tbl2;
-- DROP TABLE result_tbl;
-- DROP TABLE join_tbl;

-- -- Test that an asynchronous fetch is processed before restarting the scan in
-- -- ReScanForeignScan
-- CREATE TABLE base_tbl (a int, b int);
-- INSERT INTO base_tbl VALUES (1, 11), (2, 22), (3, 33);
-- CREATE FOREIGN TABLE foreign_tbl (b int)
--   SERVER loopback OPTIONS (table_name 'base_tbl');
-- CREATE FOREIGN TABLE foreign_tbl2 () INHERITS (foreign_tbl)
--   SERVER loopback OPTIONS (table_name 'base_tbl');

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT a FROM base_tbl WHERE (a, random() > 0) IN (SELECT a, random() > 0 FROM foreign_tbl);
-- SELECT a FROM base_tbl WHERE (a, random() > 0) IN (SELECT a, random() > 0 FROM foreign_tbl);

-- -- Clean up
-- DROP FOREIGN TABLE foreign_tbl CASCADE;
-- DROP TABLE base_tbl;

-- ALTER SERVER loopback OPTIONS (DROP async_capable);
-- ALTER SERVER loopback2 OPTIONS (DROP async_capable);

-- -- ===================================================================
-- -- test invalid server, foreign table and foreign data wrapper options
-- -- ===================================================================
-- -- Invalid fdw_startup_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
-- 	OPTIONS(fdw_startup_cost '100$%$#$#');
-- -- Invalid fdw_tuple_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
-- 	OPTIONS(fdw_tuple_cost '100$%$#$#');
-- -- Invalid fetch_size option
-- CREATE FOREIGN TABLE inv_fsz (c1 int )
-- 	SERVER loopback OPTIONS (fetch_size '100$%$#$#');
-- -- Invalid batch_size option
-- CREATE FOREIGN TABLE inv_bsz (c1 int )
-- 	SERVER loopback OPTIONS (batch_size '100$%$#$#');

-- -- No option is allowed to be specified at foreign data wrapper level
-- ALTER FOREIGN DATA WRAPPER postgres_fdw OPTIONS (nonexistent 'fdw');

-- -- ===================================================================
-- -- test postgres_fdw.application_name GUC
-- -- ===================================================================
-- -- To avoid race conditions in checking the remote session's application_name,
-- -- use this view to make the remote session itself read its application_name.
-- CREATE VIEW my_application_name AS
--   SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid();

-- CREATE FOREIGN TABLE remote_application_name (application_name text)
--   SERVER loopback2
--   OPTIONS (schema_name 'public', table_name 'my_application_name');

-- SELECT count(*) FROM remote_application_name;

-- -- Specify escape sequences in application_name option of a server
-- -- object so as to test that they are replaced with status information
-- -- expectedly.  Note that we are also relying on ALTER SERVER to force
-- -- the remote session to be restarted with its new application name.
-- --
-- -- Since pg_stat_activity.application_name may be truncated to less than
-- -- NAMEDATALEN characters, note that substring() needs to be used
-- -- at the condition of test query to make sure that the string consisting
-- -- of database name and process ID is also less than that.
-- ALTER SERVER loopback2 OPTIONS (application_name 'fdw_%d%p');
-- SELECT count(*) FROM remote_application_name
--   WHERE application_name =
--     substring('fdw_' || current_database() || pg_backend_pid() for
--       current_setting('max_identifier_length')::int);

-- -- postgres_fdw.application_name overrides application_name option
-- -- of a server object if both settings are present.
-- ALTER SERVER loopback2 OPTIONS (SET application_name 'fdw_wrong');
-- SET postgres_fdw.application_name TO 'fdw_%a%u%%';
-- SELECT count(*) FROM remote_application_name
--   WHERE application_name =
--     substring('fdw_' || current_setting('application_name') ||
--       CURRENT_USER || '%' for current_setting('max_identifier_length')::int);
-- RESET postgres_fdw.application_name;

-- -- Test %c (session ID) and %C (cluster name) escape sequences.
-- ALTER SERVER loopback2 OPTIONS (SET application_name 'fdw_%C%c');
-- SELECT count(*) FROM remote_application_name
--   WHERE application_name =
--     substring('fdw_' || current_setting('cluster_name') ||
--       to_hex(trunc(EXTRACT(EPOCH FROM (SELECT backend_start FROM
--       pg_stat_get_activity(pg_backend_pid()))))::integer) || '.' ||
--       to_hex(pg_backend_pid())
--       for current_setting('max_identifier_length')::int);

-- -- Clean up.
-- DROP FOREIGN TABLE remote_application_name;
-- DROP VIEW my_application_name;

-- -- ===================================================================
-- -- test parallel commit and parallel abort
-- -- ===================================================================
-- ALTER SERVER loopback OPTIONS (ADD parallel_commit 'true');
-- ALTER SERVER loopback OPTIONS (ADD parallel_abort 'true');
-- ALTER SERVER loopback2 OPTIONS (ADD parallel_commit 'true');
-- ALTER SERVER loopback2 OPTIONS (ADD parallel_abort 'true');

-- CREATE TABLE ploc1 (f1 int, f2 text);
-- CREATE FOREIGN TABLE prem1 (f1 int, f2 text)
--   SERVER loopback OPTIONS (table_name 'ploc1');
-- CREATE TABLE ploc2 (f1 int, f2 text);
-- CREATE FOREIGN TABLE prem2 (f1 int, f2 text)
--   SERVER loopback2 OPTIONS (table_name 'ploc2');

-- BEGIN;
-- INSERT INTO prem1 VALUES (101, 'foo');
-- INSERT INTO prem2 VALUES (201, 'bar');
-- COMMIT;
-- SELECT * FROM prem1;
-- SELECT * FROM prem2;

-- BEGIN;
-- SAVEPOINT s;
-- INSERT INTO prem1 VALUES (102, 'foofoo');
-- INSERT INTO prem2 VALUES (202, 'barbar');
-- RELEASE SAVEPOINT s;
-- COMMIT;
-- SELECT * FROM prem1;
-- SELECT * FROM prem2;

-- -- This tests executing DEALLOCATE ALL against foreign servers in parallel
-- -- during pre-commit
-- BEGIN;
-- SAVEPOINT s;
-- INSERT INTO prem1 VALUES (103, 'baz');
-- INSERT INTO prem2 VALUES (203, 'qux');
-- ROLLBACK TO SAVEPOINT s;
-- RELEASE SAVEPOINT s;
-- INSERT INTO prem1 VALUES (104, 'bazbaz');
-- INSERT INTO prem2 VALUES (204, 'quxqux');
-- COMMIT;
-- SELECT * FROM prem1;
-- SELECT * FROM prem2;

-- BEGIN;
-- INSERT INTO prem1 VALUES (105, 'test1');
-- INSERT INTO prem2 VALUES (205, 'test2');
-- ABORT;
-- SELECT * FROM prem1;
-- SELECT * FROM prem2;

-- -- This tests executing DEALLOCATE ALL against foreign servers in parallel
-- -- during post-abort
-- BEGIN;
-- SAVEPOINT s;
-- INSERT INTO prem1 VALUES (105, 'test1');
-- INSERT INTO prem2 VALUES (205, 'test2');
-- ROLLBACK TO SAVEPOINT s;
-- RELEASE SAVEPOINT s;
-- INSERT INTO prem1 VALUES (105, 'test1');
-- INSERT INTO prem2 VALUES (205, 'test2');
-- ABORT;
-- SELECT * FROM prem1;
-- SELECT * FROM prem2;

-- ALTER SERVER loopback OPTIONS (DROP parallel_commit);
-- ALTER SERVER loopback OPTIONS (DROP parallel_abort);
-- ALTER SERVER loopback2 OPTIONS (DROP parallel_commit);
-- ALTER SERVER loopback2 OPTIONS (DROP parallel_abort);

-- -- ===================================================================
-- -- test for ANALYZE sampling
-- -- ===================================================================

-- CREATE TABLE analyze_table (id int, a text, b bigint);

-- CREATE FOREIGN TABLE analyze_ftable (id int, a text, b bigint)
--        SERVER loopback OPTIONS (table_name 'analyze_rtable1');

-- INSERT INTO analyze_table (SELECT x FROM generate_series(1,1000) x);
-- ANALYZE analyze_table;

-- SET default_statistics_target = 10;
-- ANALYZE analyze_table;

-- ALTER SERVER loopback OPTIONS (analyze_sampling 'invalid');

-- ALTER SERVER loopback OPTIONS (analyze_sampling 'auto');
-- ANALYZE analyze_table;

-- ALTER SERVER loopback OPTIONS (SET analyze_sampling 'system');
-- ANALYZE analyze_table;

-- ALTER SERVER loopback OPTIONS (SET analyze_sampling 'bernoulli');
-- ANALYZE analyze_table;

-- ALTER SERVER loopback OPTIONS (SET analyze_sampling 'random');
-- ANALYZE analyze_table;

-- ALTER SERVER loopback OPTIONS (SET analyze_sampling 'off');
-- ANALYZE analyze_table;

-- MongoDB FDW does not support query cancel feature
-- SELECT version() ~ 'cygwin' AS skip_test \gset
-- \if :skip_test
-- \quit
-- \endif

-- -- Let's test canceling a remote query.  Use a table that does not have
-- -- remote_estimate enabled, else there will be multiple queries to the
-- -- remote and we might unluckily send the cancel in between two of them.
-- -- First let's confirm that the query is actually pushed down.
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT count(*) FROM ft1 a CROSS JOIN ft1 b CROSS JOIN ft1 c CROSS JOIN ft1 d;

-- BEGIN;
-- -- Make sure that connection is open and set up.
-- SELECT count(*) FROM ft1 a;
-- -- Timeout needs to be long enough to be sure that we've sent the slow query.
-- SET LOCAL statement_timeout = '100ms';
-- -- This would take very long if not canceled:
-- SELECT count(*) FROM ft1 a CROSS JOIN ft1 b CROSS JOIN ft1 c CROSS JOIN ft1 d;
-- COMMIT;

-- -- cleanup
-- DROP FOREIGN TABLE analyze_ftable;
-- DROP TABLE analyze_table;

--Testcase 818:
DROP USER MAPPING FOR public SERVER testserver1;
--Testcase 806:
DROP USER MAPPING FOR CURRENT_USER SERVER mongo_server;
--Testcase 807:
DROP USER MAPPING FOR CURRENT_USER SERVER mongo_server2;
--Testcase 808:
DROP USER MAPPING FOR CURRENT_USER SERVER spdsrv;
--Testcase 819:
DROP SERVER testserver1 CASCADE;
--Testcase 809:
DROP SERVER mongo_server CASCADE;
--Testcase 810:
DROP SERVER mongo_server2 CASCADE;
--Testcase 811:
DROP SERVER spdsrv CASCADE;
--Testcase 812:
DROP EXTENSION mongo_fdw CASCADE;
--Testcase 813:
DROP EXTENSION pgspider_ext CASCADE;
--Testcase 911:
DROP SCHEMA "S 1" CASCADE;
--Testcase 912:
DROP TABLE ft1;
--Testcase 913:
DROP TABLE ft2;
--Testcase 914:
DROP TABLE ft3;
--Testcase 915:
DROP TABLE ft4;
--Testcase 916:
DROP TABLE ft5;
--Testcase 917:
DROP TABLE ft6;
--Testcase 918:
DROP TABLE ft7;
--Testcase 919:
DROP TABLE loct3;
--Testcase 920:
DROP TYPE user_enum;
--Testcase 921:
DROP TABLE ft_empty;
--Testcase 922:
DROP TABLE loc1;
--Testcase 923:
DROP TABLE rem1;
--Testcase 924:
DROP TABLE grem1;
--Testcase 925:
DROP TABLE foo2;
--Testcase 926:
DROP TABLE bar2;
--Testcase 927:
DROP TABLE b;
--Testcase 928:
DROP TABLE remt1;
--Testcase 929:
DROP TABLE remt2;
--Testcase 930:
DROP FUNCTION trigger_func CASCADE;
--Testcase 931:
DROP FUNCTION trig_row_before_insupdate CASCADE;
--Testcase 932:
DROP FUNCTION trig_null CASCADE;
--Testcase 933:
DROP FUNCTION trigger_data CASCADE;
