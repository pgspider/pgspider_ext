\set ECHO none
\ir sql/parameters/mysql_parameters.conf
\set ECHO all

-- Before running this file User must create database mysql_fdw_post on
-- mysql with all permission for MYSQL_USER_NAME user with MYSQL_PWD password

--Testcase 717:
CREATE EXTENSION pgspider_ext;

--Testcase 718:
CREATE SERVER spdsrv FOREIGN DATA WRAPPER pgspider_ext;

--Testcase 719:
CREATE USER MAPPING FOR CURRENT_USER SERVER spdsrv;

--Testcase 1:
CREATE EXTENSION mysql_fdw;

--Testcase 2:
CREATE SERVER mysql_svr FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT);

--Testcase 3:
CREATE SERVER mysql_svr2 FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT);

--Testcase 720:
CREATE SERVER mysql_svr3 FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT);

--Testcase 4:
CREATE USER MAPPING FOR PUBLIC SERVER mysql_svr
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);
--Testcase 5:
CREATE USER MAPPING FOR PUBLIC SERVER mysql_svr2
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);

--Testcase 721:
CREATE USER MAPPING FOR PUBLIC SERVER mysql_svr3
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);

-- ===================================================================
-- create objects used through FDW mysql_svr server
-- ===================================================================
--Testcase 6:
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
--Testcase 7:
CREATE SCHEMA "S 1";
IMPORT FOREIGN SCHEMA mysql_fdw_post FROM SERVER mysql_svr INTO "S 1";
--Testcase 8:
INSERT INTO "S 1"."T 1"
	SELECT id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
	       '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
	       id % 10,
	       id % 10,
	       'foo'::user_enum
	FROM generate_series(1, 1000) id;
--Testcase 9:
INSERT INTO "S 1"."T 2"
	SELECT id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 10:
INSERT INTO "S 1"."T 3"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 11:
DELETE FROM "S 1"."T 3" WHERE c1 % 2 != 0;	-- delete for outer join tests
--Testcase 12:
INSERT INTO "S 1"."T 4"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;

--Testcase 13:
DELETE FROM "S 1"."T 4" WHERE c1 % 3 != 0;	-- delete for outer join tests

-- ANALYZE "S 1"."T 1";
-- ANALYZE "S 1"."T 2";
-- ANALYZE "S 1"."T 3";
-- ANALYZE "S 1"."T 4";

-- ===================================================================
-- create foreign tables
-- ===================================================================
--Testcase 14:
CREATE FOREIGN TABLE ft1_a_child (
	c0 int,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 user_enum
) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post');

--Testcase 15:
ALTER FOREIGN TABLE ft1_a_child DROP COLUMN c0;

--Testcase 722:
CREATE TABLE ft1 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 user_enum,
    spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 723:
CREATE FOREIGN TABLE ft1_a PARTITION OF ft1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 724:
CREATE FOREIGN TABLE ft2_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
    cx int,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 user_enum
) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post');

--Testcase 725:
ALTER FOREIGN TABLE ft2_a_child DROP COLUMN cx;

--Testcase 16:
CREATE TABLE ft2 (
    c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 user_enum,
    spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 726:
CREATE FOREIGN TABLE ft2_a PARTITION OF ft2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 727:
CREATE FOREIGN TABLE ft4_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'T 3');

--Testcase 18:
CREATE TABLE ft4 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 728:
CREATE FOREIGN TABLE ft4_a PARTITION OF ft4 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 729:
CREATE FOREIGN TABLE ft5_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'T 4');

--Testcase 19:
CREATE TABLE ft5 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 730:
CREATE FOREIGN TABLE ft5_a PARTITION OF ft5 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 731:
CREATE FOREIGN TABLE ft6_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mysql_svr2 OPTIONS (dbname 'mysql_fdw_post', table_name 'T 4');

--Testcase 20:
CREATE TABLE ft6 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 732:
CREATE FOREIGN TABLE ft6_a PARTITION OF ft6 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 733:
CREATE FOREIGN TABLE ft7_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER mysql_svr3 OPTIONS (dbname 'mysql_fdw_post', table_name 'T 4');

--Testcase 734:
CREATE TABLE ft7 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 735:
CREATE FOREIGN TABLE ft7_a PARTITION OF ft7 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- Enable to pushdown aggregate
--Testcase 736:
SET enable_partitionwise_aggregate TO on;

-- Turn off leader node participation to avoid duplicate data error when executing
-- parallel query
--Testcase 737:
SET parallel_leader_participation TO off;

-- -- ===================================================================
-- -- tests for validator
-- -- ===================================================================
-- -- requiressl and some other parameters are omitted because
-- -- valid values for them depend on configure options
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

-- -- Error, invalid list syntax
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo; bar');

-- -- OK but gets a warning
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo, bar');
-- ALTER SERVER testserver1 OPTIONS (DROP extensions);

-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (DROP user, DROP password);

-- -- Attempt to add a valid option that's not allowed in a user mapping
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslmode 'require');

-- -- But we can add valid ones fine
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslpassword 'dummy');

-- -- Ensure valid options we haven't used in a user mapping yet are
-- -- permitted to check validation.
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslkey 'value', ADD sslcert 'value');

--Testcase 21:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (table_name 'T 1');
--Testcase 22:
ALTER FOREIGN TABLE ft2_a_child OPTIONS (table_name 'T 1');
--Testcase 23:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c1 OPTIONS (column_name 'C 1');
--Testcase 24:
ALTER FOREIGN TABLE ft2_a_child ALTER COLUMN c1 OPTIONS (column_name 'C 1');
--Testcase 25:
\det+

-- Test that alteration of server options causes reconnection
-- Remote's errors might be non-English, so hide them to ensure stable results
\set VERBOSITY terse
--Testcase 26:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
--Testcase 27:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (SET dbname 'no such database');
--Testcase 28:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
--Testcase 29:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (SET dbname 'mysql_fdw_post');
--Testcase 30:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again

-- Test that alteration of user mapping options causes reconnection
-- ALTER USER MAPPING FOR CURRENT_USER SERVER mysql_svr
--   OPTIONS (ADD user 'no such user');
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
-- ALTER USER MAPPING FOR CURRENT_USER SERVER mysql_svr
--   OPTIONS (DROP user);
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
-- \set VERBOSITY default

-- Now we should be able to run ANALYZE.
-- To exercise multiple code paths, we use local stats on ft1
-- and remote-estimate mode on ft2.
-- ANALYZE ft1;
--Testcase 31:
ALTER FOREIGN TABLE ft2_a_child OPTIONS (use_remote_estimate 'true');
-- ===================================================================
-- test error case for create publication on foreign table
-- ===================================================================
--Testcase 908:
CREATE PUBLICATION testpub_ftbl FOR TABLE ft1_a_child;  -- should fail

-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias
--Testcase 32:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
--Testcase 33:
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
-- single table with alias - also test that tableoid sort is not pushed to remote side
--Testcase 738:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
--Testcase 739:
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference
--Testcase 34:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 35:
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result
--Testcase 36:
SELECT * FROM ft1 WHERE false;
-- with WHERE clause
--Testcase 37:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
--Testcase 38:
SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
-- with FOR UPDATE/SHARE
--Testcase 39:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 40:
SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 41:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
--Testcase 42:
SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
-- aggregate
--Testcase 43:
SELECT COUNT(*) FROM ft1 t1;
-- subquery
--Testcase 44:
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX
--Testcase 45:
SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;
-- used in CTE
--Testcase 46:
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values
--Testcase 47:
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.
--Testcase 48:
SET enable_hashjoin TO false;
--Testcase 49:
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 50:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
--Testcase 51:
SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 52:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
--Testcase 53:
SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
--Testcase 54:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 55:
SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
--Testcase 56:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 57:
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins
--Testcase 58:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 59:
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 60:
RESET enable_hashjoin;
--Testcase 61:
RESET enable_nestloop;
-- Test executing assertion in estimate_path_cost_size() that makes sure that
-- retrieved_rows for foreign rel re-used to cost pre-sorted foreign paths is
-- a sensible value even when the rel has tuples=0
--Testcase 740:
CREATE FOREIGN TABLE ft_empty_a_child (c1 int NOT NULL, c2 text)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'loct_empty');

--Testcase 741:
CREATE TABLE ft_empty (c1 int NOT NULL, c2 text, spdurl text)
   PARTITION BY LIST (spdurl);

--Testcase 742:
CREATE FOREIGN TABLE ft_empty_a PARTITION OF ft_empty FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 743:
INSERT INTO ft_empty_a_child
  SELECT id, 'AAA' || to_char(id, 'FM000') FROM generate_series(1, 100) id;
--Testcase 744:
DELETE FROM ft_empty_a_child;
-- ANALYZE ft_empty;
--Testcase 745:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_empty ORDER BY c1;

-- test restriction on non-system foreign tables.
SET restrict_nonsystem_relation_kind TO 'foreign-table';
--Testcase 1136:
SELECT * from ft1 where c1 < 1; -- ERROR
--Testcase 1137:
INSERT INTO ft1 (c1) VALUES (1); -- ERROR due to the missing spdurl column during the insert.
--Testcase 1138:
INSERT INTO ft1 (c1, spdurl) VALUES (1, '/node1/'); -- ERROR due to not supporting foreign insert.
--Testcase 1139:
DELETE FROM ft1 WHERE c1 = 1; -- ERROR
TRUNCATE ft1; -- ERROR
RESET restrict_nonsystem_relation_kind;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
--Testcase 62:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
--Testcase 63:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
--Testcase 64:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c3 IS NULL;        -- NullTest
--Testcase 65:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c3 IS NOT NULL;    -- NullTest
--Testcase 66:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
--Testcase 67:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
--Testcase 69:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
--Testcase 70:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
--Testcase 71:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef
--Testcase 72:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
--Testcase 73:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote
-- parameterized remote path for foreign table
--Testcase 74:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM "S 1"."T 1" a, ft2 b WHERE a."C 1" = 47 AND b.c1 = a.c2;
--Testcase 75:
SELECT * FROM "S 1"."T 1" a, ft2 b WHERE a."C 1" = 47 AND b.c1 = a.c2;

-- check both safe and unsafe join conditions
--Testcase 76:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
--Testcase 77:
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
--Testcase 78:
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5));
--Testcase 79:
SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5));

-- user-defined operator/function
--Testcase 82:
CREATE FUNCTION mysql_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
--Testcase 83:
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- built-in operators and functions can be shipped for remote execution
--Testcase 84:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 85:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 86:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 87:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot
--Testcase 88:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = mysql_fdw_abs(t1.c2);
--Testcase 89:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = mysql_fdw_abs(t1.c2);
--Testcase 90:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 91:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ORDER BY can be shipped, though
--Testcase 92:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 93:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- but let's put them in an extension ...
--Testcase 94:
ALTER EXTENSION mysql_fdw ADD FUNCTION mysql_fdw_abs(int);
--Testcase 95:
ALTER EXTENSION mysql_fdw ADD OPERATOR === (int, int);
--Testcase 96:
ALTER SERVER mysql_svr OPTIONS (ADD extensions 'mysql_fdw');

-- ... now they can be shipped
--Testcase 97:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = mysql_fdw_abs(t1.c2);
--Testcase 98:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = mysql_fdw_abs(t1.c2);
--Testcase 99:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 100:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- and both ORDER BY and LIMIT can be shipped
--Testcase 101:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 102:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- Ensure we don't ship FETCH FIRST .. WITH TIES
--Testcase 1052:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c2 FROM ft1 t1 WHERE t1.c1 > 960 ORDER BY t1.c2 FETCH FIRST 2 ROWS WITH TIES;
--Testcase 1053:
SELECT t1.c2 FROM ft1 t1 WHERE t1.c1 > 960 ORDER BY t1.c2 FETCH FIRST 2 ROWS WITH TIES;

-- Test CASE pushdown
-- Mysql_fdw does not support CASE expression pushdown.
--Testcase 870:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;
--Testcase 871:
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;

-- Nested CASE
--Testcase 872:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;

--Testcase 873:
SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;

-- CASE arg WHEN
--Testcase 874:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE mod(c1, 4) WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- CASE cannot be pushed down because of unshippable arg clause
--Testcase 875:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE random()::integer WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- these are shippable
--Testcase 876:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c6 WHEN 'foo' THEN true ELSE c3 < 'bar' END;
--Testcase 877:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 WHEN c6 THEN true ELSE c3 < 'bar' END;

-- but this is not because of collation
--Testcase 878:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 COLLATE "C" WHEN c6 THEN true ELSE c3 < 'bar' END;

-- a regconfig constant referring to this text search configuration
-- is initially unshippable
--Testcase 909:
CREATE TEXT SEARCH CONFIGURATION public.custom_search
  (COPY = pg_catalog.english);
--Testcase 910:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
--Testcase 911:
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
-- but if it's in a shippable extension, it can be shipped
ALTER EXTENSION mysql_fdw ADD TEXT SEARCH CONFIGURATION public.custom_search;
-- however, that doesn't flush the shippability cache, so do a quick reconnect
\c -
-- Enable to pushdown aggregate
SET enable_partitionwise_aggregate TO on;
SET parallel_leader_participation = 'off';
--Testcase 933:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
--Testcase 934:
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
ALTER EXTENSION mysql_fdw DROP TEXT SEARCH CONFIGURATION public.custom_search;
--Testcase 935:
DROP TEXT SEARCH CONFIGURATION public.custom_search;

-- ===================================================================
-- ORDER BY queries
-- ===================================================================
-- we should not push order by clause with volatile expressions or unsafe
-- collations
--Testcase 80:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, random();
--Testcase 81:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- Ensure we don't push ORDER BY expressions which are Consts at the UNION
-- child level to the foreign server.
--Testcase 1054:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM (
    SELECT 1 AS type,c1 FROM ft1
    UNION ALL
    SELECT 2 AS type,c1 FROM ft2
) a ORDER BY type,c1;

--Testcase 1055:
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
--Testcase 103:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 104:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join three tables
--Testcase 105:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 106:
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join
--Testcase 107:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 108:
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables
--Testcase 109:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 110:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
--Testcase 111:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
--Testcase 112:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
--Testcase 113:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
--Testcase 114:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
-- right outer join
--Testcase 115:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 116:
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
-- right outer join three tables
--Testcase 117:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 118:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join
--Testcase 119:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
--Testcase 120:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
--Testcase 121:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 122:
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 123:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
--Testcase 124:
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
-- b. one of the joining relations is a base relation and the other is a join
-- relation
--Testcase 125:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 126:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries
--Testcase 127:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 128:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries
--Testcase 129:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
--Testcase 130:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join
--Testcase 131:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
--Testcase 132:
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
-- full outer join three tables
--Testcase 133:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 134:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + right outer join
--Testcase 135:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 136:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- right outer join + full outer join
--Testcase 137:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 138:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + left outer join
--Testcase 139:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 140:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join + full outer join
--Testcase 141:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 142:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 746:
SET enable_memoize TO off;
-- right outer join + left outer join
--Testcase 143:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 144:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 747:
RESET enable_memoize;
-- left outer join + right outer join
--Testcase 145:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 146:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows
--Testcase 147:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 148:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set
--Testcase 149:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE mysql_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--Testcase 150:
ALTER SERVER mysql_svr OPTIONS (DROP extensions);
-- full outer join + WHERE clause with shippable extensions not set
--Testcase 151:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE mysql_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--Testcase 152:
ALTER SERVER mysql_svr OPTIONS (ADD extensions 'mysql_fdw');
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
--Testcase 153:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 154:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 155:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
--Testcase 156:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause
--Testcase 157:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 158:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 159:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
--Testcase 160:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE
--Testcase 161:
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
--Testcase 162:
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference
--Testcase 748:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- SEMI JOIN
--Testcase 163:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 164:
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down
--Testcase 165:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 166:
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- CROSS JOIN can be pushed down
--Testcase 167:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 168:
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.
--Testcase 169:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 170:
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
--Testcase 171:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 172:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
--Testcase 173:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 174:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
--Testcase 175:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 176:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs
--Testcase 177:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
--Testcase 178:
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference
--Testcase 179:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;
--Testcase 180:
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;

-- join with pseudoconstant quals
--Testcase 1036:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1 AND CURRENT_USER = SESSION_USER) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
--Testcase 181:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;
--Testcase 182:
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
--Testcase 183:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;
--Testcase 184:
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;

-- join with nullable side with some columns with null values
--Testcase 185:
UPDATE ft5_a_child SET c3 = null where c1 % 9 = 0;
--Testcase 186:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;
--Testcase 187:
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
--Testcase 188:
CREATE TABLE local_tbl (c1 int NOT NULL, c2 int NOT NULL, c3 text, CONSTRAINT local_tbl_pkey PRIMARY KEY (c1));
--Testcase 189:
INSERT INTO local_tbl SELECT id, id % 10, to_char(id, 'FM0000') FROM generate_series(1, 1000) id;
-- ANALYZE local_tbl;
--Testcase 190:
SET enable_nestloop TO false;
--Testcase 191:
SET enable_hashjoin TO false;
--Testcase 192:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 ORDER BY ft1.c1 FOR UPDATE;
--Testcase 193:
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 ORDER BY ft1.c1 FOR UPDATE;
--Testcase 194:
RESET enable_nestloop;
--Testcase 195:
RESET enable_hashjoin;

-- test that add_paths_with_pathkeys_for_rel() arranges for the epq_path to
-- return columns needed by the parent ForeignScan node
--Testcase 912:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.*, COALESCE(ft1.c3 || ft2.c3, 'foobar') FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100)) ss ON (local_tbl.c1 = ss.c1) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;
ALTER SERVER mysql_svr OPTIONS (DROP extensions);
-- ALTER SERVER loopback OPTIONS (ADD fdw_startup_cost '10000.0');
--Testcase 913:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.* FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100 AND (ft1.c1 - mysql_fdw_abs(ft2.c2)) = 0)) ss ON (local_tbl.c3 = ss.c3) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;
-- ALTER SERVER loopback OPTIONS (DROP fdw_startup_cost);
ALTER SERVER mysql_svr OPTIONS (ADD extensions 'mysql_fdw');

--Testcase 196:
DROP TABLE local_tbl;

-- check join pushdown in situations where multiple userids are involved
--Testcase 197:
CREATE ROLE regress_view_owner SUPERUSER;
--Testcase 198:
CREATE USER MAPPING FOR regress_view_owner SERVER mysql_svr;
--Testcase 1056:
CREATE USER MAPPING FOR regress_view_owner SERVER spdsrv;
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;

--Testcase 199:
CREATE VIEW v4 AS SELECT * FROM ft4;
--Testcase 200:
CREATE VIEW v5 AS SELECT * FROM ft5;
--Testcase 201:
ALTER VIEW v5 OWNER TO regress_view_owner;
--Testcase 202:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
--Testcase 203:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 204:
ALTER VIEW v4 OWNER TO regress_view_owner;
--Testcase 205:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 206:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 207:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
--Testcase 208:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 209:
ALTER VIEW v4 OWNER TO CURRENT_USER;
--Testcase 210:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 211:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 212:
ALTER VIEW v4 OWNER TO regress_view_owner;

-- ====================================================================
-- Check that userid to use when querying the remote table is correctly
-- propagated into foreign rels present in subqueries under an UNION ALL
-- ====================================================================
--Testcase 1037:
CREATE ROLE regress_view_owner_another;
--Testcase 1038:
ALTER VIEW v4 OWNER TO regress_view_owner_another;
--Testcase 1039:
GRANT SELECT ON ft4 TO regress_view_owner_another;
--Testcase 1040:
ALTER FOREIGN TABLE ft4_a_child OPTIONS (ADD use_remote_estimate 'true');
-- The following should query the remote backing table of ft4 as user
-- regress_view_owner_another, the view owner, though it fails as expected
-- due to the lack of a user mapping for that user.

--Testcase 1041:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;
-- Likewise, but with the query under an UNION ALL
--Testcase 1042:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT * FROM v4 UNION ALL SELECT * FROM v4);
-- Should not get that error once a user mapping is created
--Testcase 1043:
CREATE USER MAPPING FOR regress_view_owner_another SERVER mysql_svr;
--Testcase 1044:
CREATE USER MAPPING FOR regress_view_owner_another SERVER spdsrv;
--Testcase 1045:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;
--Testcase 1046:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT * FROM v4 UNION ALL SELECT * FROM v4);
--Testcase 1047:
DROP USER MAPPING FOR regress_view_owner_another SERVER mysql_svr;
--Testcase 1048:
DROP USER MAPPING FOR regress_view_owner_another SERVER spdsrv;
--Testcase 1049:
DROP OWNED BY regress_view_owner_another;
--Testcase 1050:
DROP ROLE regress_view_owner_another;
--Testcase 1051:
ALTER FOREIGN TABLE ft4_a_child OPTIONS (SET use_remote_estimate 'false');

-- cleanup
--Testcase 213:
DROP OWNED BY regress_view_owner;
--Testcase 214:
DROP ROLE regress_view_owner;


-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================

-- Simple aggregates
--Testcase 215:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;
--Testcase 216:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

--Testcase 217:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;
--Testcase 218:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()
--Testcase 219:
explain (verbose, costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query
--Testcase 220:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;
--Testcase 221:
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel
--Testcase 222:
explain (verbose, costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions
--Testcase 223:
explain (verbose, costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;
--Testcase 224:
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are pushed down.
set enable_incremental_sort = off;
--Testcase 225:
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
--Testcase 226:
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
reset enable_incremental_sort;

-- Aggregate is still pushed down by taking unshippable expression out
--Testcase 227:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;
--Testcase 228:
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
--Testcase 229:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
--Testcase 230:
explain (verbose, costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
--Testcase 231:
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
--Testcase 232:
explain (verbose, costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);
--Testcase 233:
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability
--Testcase 234:
explain (verbose, costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;
--Testcase 235:
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
--Testcase 236:
explain (verbose, costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
--Testcase 237:
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
--Testcase 238:
explain (verbose, costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
--Testcase 239:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1;
--Testcase 240:
select exists(select 1 from pg_enum), sum(c1) from ft1;

--Testcase 241:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;
--Testcase 242:
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;


-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order
--Testcase 243:
explain (verbose, costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
--Testcase 244:
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC
--Testcase 245:
explain (verbose, costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;
--Testcase 246:
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate
--Testcase 247:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 248:
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
--Testcase 249:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 250:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 251:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 252:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- FILTER within aggregate
--Testcase 253:
explain (verbose, costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
--Testcase 254:
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
--Testcase 255:
explain (verbose, costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;
--Testcase 256:
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query
--Testcase 257:
explain (verbose, costs off)
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 258:
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
-- Inner query is aggregation query
--Testcase 259:
explain (verbose, costs off)
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 260:
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
--Testcase 261:
explain (verbose, costs off)
select sum(c1) filter (where (c1 / c1) * random() <= 1) from ft1 group by c2 order by 1;
--Testcase 262:
explain (verbose, costs off)
select sum(c2) filter (where c2 in (select c2 from ft1 where c2 < 5)) from ft1;

-- Ordered-sets within aggregate
--Testcase 263:
explain (verbose, costs off)
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;
--Testcase 264:
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

-- Using multiple arguments within aggregates
--Testcase 265:
explain (verbose, costs off)
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;
--Testcase 266:
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC
--Testcase 267:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
--Testcase 268:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.
--Testcase 269:
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate
--Testcase 270:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension
--Testcase 271:
alter extension mysql_fdw add function least_accum(anyelement, variadic anyarray);
--Testcase 272:
alter extension mysql_fdw add aggregate least_agg(variadic items anyarray);
--Testcase 273:
alter server mysql_svr options (set extensions 'mysql_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.
--Testcase 274:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;
--Testcase 275:
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension
--Testcase 276:
alter extension mysql_fdw drop function least_accum(anyelement, variadic anyarray);
--Testcase 277:
alter extension mysql_fdw drop aggregate least_agg(variadic items anyarray);
--Testcase 278:
alter server mysql_svr options (set extensions 'mysql_fdw');

-- Not pushed down as we have dropped objects from extension.
--Testcase 279:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup
--Testcase 280:
reset enable_hashagg;
--Testcase 281:
drop aggregate least_agg(variadic items anyarray);
--Testcase 282:
drop function least_accum(anyelement, variadic anyarray);


-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.
--Testcase 283:
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

--Testcase 284:
create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

--Testcase 285:
create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

--Testcase 286:
create operator family my_op_family using btree;

--Testcase 287:
create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

--Testcase 288:
create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.
--Testcase 289:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- This should not be pushed either.
--Testcase 879:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Update local stats on ft2
-- ANALYZE ft2;

-- Add into extension
--Testcase 290:
alter extension mysql_fdw add operator class my_op_class using btree;
--Testcase 291:
alter extension mysql_fdw add function my_op_cmp(a int, b int);
--Testcase 292:
alter extension mysql_fdw add operator family my_op_family using btree;
--Testcase 293:
alter extension mysql_fdw add operator public.<^(int, int);
--Testcase 294:
alter extension mysql_fdw add operator public.=^(int, int);
--Testcase 295:
alter extension mysql_fdw add operator public.>^(int, int);
--Testcase 296:
alter server mysql_svr options (set extensions 'mysql_fdw');

-- Now this will be pushed as sort operator is part of the extension.
-- alter server loopback options (add fdw_tuple_cost '0.5');
--Testcase 297:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
--Testcase 298:
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
-- alter server loopback options (drop fdw_tuple_cost);

-- This should be pushed too.
-- MYSQL not support user defined operator.
--Testcase 880:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Remove from extension
--Testcase 299:
alter extension mysql_fdw drop operator class my_op_class using btree;
--Testcase 300:
alter extension mysql_fdw drop function my_op_cmp(a int, b int);
--Testcase 301:
alter extension mysql_fdw drop operator family my_op_family using btree;
--Testcase 302:
alter extension mysql_fdw drop operator public.<^(int, int);
--Testcase 303:
alter extension mysql_fdw drop operator public.=^(int, int);
--Testcase 304:
alter extension mysql_fdw drop operator public.>^(int, int);
--Testcase 305:
alter server mysql_svr options (set extensions 'mysql_fdw');

-- This will not be pushed as sort operator is now removed from the extension.
--Testcase 306:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup
--Testcase 307:
drop operator class my_op_class using btree;
--Testcase 308:
drop function my_op_cmp(a int, b int);
--Testcase 309:
drop operator family my_op_family using btree;
--Testcase 310:
drop operator public.>^(int, int);
--Testcase 311:
drop operator public.=^(int, int);
--Testcase 312:
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
--Testcase 313:
explain (verbose, costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate
--Testcase 314:
explain (verbose, costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;
--Testcase 315:
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
--Testcase 316:
explain (verbose, costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;
--Testcase 317:
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
--Testcase 318:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);
--Testcase 319:
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
--Testcase 320:
explain (verbose, costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;
--Testcase 321:
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization
-- Disable remote estimation temporary because sum() was not pushed down if enabled.
--Testcase 749:
ALTER FOREIGN TABLE ft2_a_child OPTIONS (set use_remote_estimate 'false');
--Testcase 322:
set enable_hashagg to false;
--Testcase 323:
explain (verbose, costs off)
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1."C 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C 1" < 100 order by 1;
--Testcase 324:
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1."C 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C 1" < 100 order by 1;
--Testcase 325:
reset enable_hashagg;
--Testcase 750:
ALTER FOREIGN TABLE ft2_a_child OPTIONS (set use_remote_estimate 'true');

-- bug #15613: bad plan for foreign table scan with lateral reference
--Testcase 326:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."T 1" AS ref_0,
    LATERAL (
        SELECT ref_0."C 1" c1, subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0."C 1" < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0."C 1";

--Testcase 327:
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."T 1" AS ref_0,
    LATERAL (
        SELECT ref_0."C 1" c1, subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0."C 1" < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0."C 1";

-- Check with placeHolderVars
--Testcase 328:
explain (verbose, costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);
--Testcase 329:
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);


-- Not supported cases
-- Grouping sets
--Testcase 330:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 331:
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 332:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 333:
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 334:
explain (verbose, costs off)
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 335:
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 336:
explain (verbose, costs off)
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;
--Testcase 337:
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
-- Disable remote estimation temporary because sum() was not pushed down if enabled.
--Testcase 751:
ALTER FOREIGN TABLE ft2_a_child OPTIONS (set use_remote_estimate 'false');
--Testcase 338:
explain (verbose, costs off)
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;
--Testcase 339:
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

-- WindowAgg
--Testcase 340:
explain (verbose, costs off)
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 341:
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 342:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 343:
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 344:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 345:
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 752:
ALTER FOREIGN TABLE ft2_a_child OPTIONS (set use_remote_estimate 'true');


-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join
--Testcase 346:
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
--Testcase 347:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);
--Testcase 348:
EXECUTE st1(1, 1);
--Testcase 349:
EXECUTE st1(101, 101);
SET enable_hashjoin TO off;
SET enable_sort TO off;
-- subquery using stable function (can't be sent to remote)
--Testcase 350:
SET datestyle TO "ISO, YMD";
--Testcase 351:
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;
--Testcase 352:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);
--Testcase 353:
EXECUTE st2(10, 20);
--Testcase 354:
EXECUTE st2(101, 121);
RESET enable_hashjoin;
RESET enable_sort;
-- subquery using immutable function (can be sent to remote)
--Testcase 355:
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;
--Testcase 356:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);
--Testcase 357:
EXECUTE st3(10, 20);
--Testcase 358:
EXECUTE st3(20, 30);
--Testcase 359:
SET datestyle TO "Postgres, MDY";
-- custom plan should be chosen initially
--Testcase 360:
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;
--Testcase 361:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 362:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 363:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 364:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 365:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
--Testcase 366:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
--Testcase 367:
PREPARE st5(user_enum,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
--Testcase 368:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 369:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 370:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 371:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 372:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 373:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 374:
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
--Testcase 375:
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 376:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 377:
PREPARE st7 AS INSERT INTO ft1 (c1,c2,c3) VALUES (1001,101,'foo');
--Testcase 378:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
-- ALTER TABLE "S 1"."T 1" RENAME TO "T 0";
-- ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'T 0');
--Testcase 379:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 380:
EXECUTE st6;
--Testcase 381:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
-- ALTER TABLE "S 1"."T 0" RENAME TO "T 1";
-- ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'T 1');

--Testcase 382:
PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 383:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--Testcase 384:
ALTER SERVER mysql_svr OPTIONS (DROP extensions);
--Testcase 385:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--Testcase 386:
EXECUTE st8;
--Testcase 387:
ALTER SERVER mysql_svr OPTIONS (ADD extensions 'mysql_fdw');

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
--Testcase 753:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;
--Testcase 754:
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1_a'::regclass LIMIT 1;
--Testcase 755:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 756:
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 757:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 758:
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 759:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;
--Testcase 760:
SELECT ctid, * FROM ft1 t1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================
--Testcase 388:
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
--Testcase 389:
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
--Testcase 390:
SELECT f_test(100);
--Testcase 391:
DROP FUNCTION f_test(int);

-- This test does not suitable with PGSpider Extension.
-- -- ===================================================================
-- REINDEX
-- ===================================================================
-- -- remote table is not created here
-- CREATE FOREIGN TABLE reindex_foreign (c1 int, c2 int)
--   SERVER mysql_svr2 OPTIONS (table_name 'reindex_local');
-- REINDEX TABLE reindex_foreign; -- error
-- REINDEX TABLE CONCURRENTLY reindex_foreign; -- error
-- DROP FOREIGN TABLE reindex_foreign;
-- -- partitions and foreign tables
-- CREATE TABLE reind_fdw_parent (c1 int) PARTITION BY RANGE (c1);
-- CREATE TABLE reind_fdw_0_10 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (0) TO (10);
-- CREATE FOREIGN TABLE reind_fdw_10_20 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (10) TO (20)
--   SERVER mysql_svr OPTIONS (table_name 'reind_local_10_20');
-- REINDEX TABLE reind_fdw_parent; -- ok
-- REINDEX TABLE CONCURRENTLY reind_fdw_parent; -- ok
-- DROP TABLE reind_fdw_parent;

-- ===================================================================
-- conversion error
-- ===================================================================
--Testcase 392:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE int;
--Testcase 393:
SELECT * FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8) WHERE x1 = 1;  -- ERROR
--Testcase 394:
SELECT ftx.x1, ft2.c2, ftx.x8 FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
  WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
--Testcase 395:
SELECT ftx.x1, ft2.c2, ftx FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
  WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
--Testcase 396:
SELECT sum(c2), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR
-- ANALYZE ft1; -- ERROR
--Testcase 397:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE user_enum;

-- ===================================================================
-- local type can be different from remote type in some cases,
-- in particular if similarly-named operators do equivalent things
-- ===================================================================
--Testcase 892:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE text;
ALTER TABLE ft1 ALTER COLUMN c8 TYPE text;
--Testcase 881:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c8 = 'foo' LIMIT 1;
--Testcase 882:
SELECT * FROM ft1 WHERE c8 = 'foo' LIMIT 1;
--Testcase 883:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE 'foo' = c8 LIMIT 1;
--Testcase 884:
SELECT * FROM ft1 WHERE 'foo' = c8 LIMIT 1;
-- we declared c8 to be text locally, but it's still the same type on
-- the remote which will balk if we try to do anything incompatible
-- with that remote type
-- Can not create user define type in MySQL Server.
-- Type c8 of table ft1 and remote table T1 are 
-- match. These case below not error with mysql_fdw. 
--Testcase 890:
SELECT * FROM ft1 WHERE c8 LIKE 'foo' LIMIT 1; -- ERROR
--Testcase 891:
SELECT * FROM ft1 WHERE c8::text LIKE 'foo' LIMIT 1; -- ERROR; cast not pushed down

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;
--Testcase 398:
FETCH c;
SAVEPOINT s;
ERROR OUT;          -- ERROR
ROLLBACK TO s;
--Testcase 399:
FETCH c;
SAVEPOINT s;
--Testcase 400:
SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK TO s;
--Testcase 401:
FETCH c;
--Testcase 402:
SELECT * FROM ft1 ORDER BY c1 LIMIT 1;
COMMIT;

-- ===================================================================
-- test handling of collations
-- ===================================================================
--Testcase 920:
create foreign table ft3_a_child (f1 text collate "C", f2 text, f3 varchar(10))
  server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct8', use_remote_estimate 'true');

--Testcase 921:
create table ft3 (f1 text collate "C", f2 text, f3 varchar(10), spdurl text) PARTITION BY LIST (spdurl);

--Testcase 922:
create foreign table ft3_a PARTITION OF ft3 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- can be sent to remote
--Testcase 923:
explain (verbose, costs off) select * from ft3 where f1 = 'foo';
--Testcase 924:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "C" = 'foo';
--Testcase 925:
explain (verbose, costs off) select * from ft3 where f2 = 'foo';
--Testcase 926:
explain (verbose, costs off) select * from ft3 where f3 = 'foo';
--Testcase 927:
explain (verbose, costs off) select * from ft3 f, ft3 l
  where f.f3 = l.f3 and l.f1 = 'foo';
-- can't be sent to remote
--Testcase 928:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';
--Testcase 929:
explain (verbose, costs off) select * from ft3 where f1 = 'foo' COLLATE "C";
--Testcase 930:
explain (verbose, costs off) select * from ft3 where f2 COLLATE "C" = 'foo';
--Testcase 931:
explain (verbose, costs off) select * from ft3 where f2 = 'foo' COLLATE "C";
--Testcase 932:
explain (verbose, costs off) select * from ft3 f, ft3 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

-- ===================================================================
-- test SEMI-JOIN pushdown
-- ===================================================================
--Testcase 1059:
EXPLAIN (verbose, costs off)
SELECT ft2.*, ft4.* FROM ft2 INNER JOIN ft4 ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  AND EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)
  ORDER BY ft2.c1;
--Testcase 1060:
SELECT ft2.*, ft4.* FROM ft2 INNER JOIN ft4 ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  AND EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)
  ORDER BY ft2.c1;

-- The same query, different join order
--Testcase 1061:
EXPLAIN (verbose, costs off)
SELECT ft2.*, ft4.* FROM ft2 INNER JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1;
--Testcase 1062:
SELECT ft2.*, ft4.* FROM ft2 INNER JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1;

-- Left join
--Testcase 1063:
EXPLAIN (verbose, costs off)
SELECT ft2.*, ft4.* FROM ft2 LEFT JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;
--Testcase 1064:
SELECT ft2.*, ft4.* FROM ft2 LEFT JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;

-- Several semi-joins per upper level join
--Testcase 1065:
EXPLAIN (verbose, costs off)
SELECT ft2.*, ft4.* FROM ft2 INNER JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  INNER JOIN (SELECT * FROM ft5 WHERE
  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c1 = ft5.c1)) ft5
  ON ft2.c2 <= ft5.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;
--Testcase 1066:
SELECT ft2.*, ft4.* FROM ft2 INNER JOIN
  (SELECT * FROM ft4 WHERE
  EXISTS (SELECT 1 FROM ft5 WHERE ft4.c1 = ft5.c1)) ft4
  ON ft2.c2 = ft4.c1
  INNER JOIN (SELECT * FROM ft5 WHERE
  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c1 = ft5.c1)) ft5
  ON ft2.c2 <= ft5.c1
  WHERE ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;

-- Semi-join below Semi-join
--Testcase 1067:
EXPLAIN (verbose, costs off)
SELECT ft2.* FROM ft2 WHERE
  c1 = ANY (
	SELECT c1 FROM ft2 WHERE
	  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c2 = ft2.c2))
  AND ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;
--Testcase 1068:
SELECT ft2.* FROM ft2 WHERE
  c1 = ANY (
	SELECT c1 FROM ft2 WHERE
	  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c2 = ft2.c2))
  AND ft2.c1 > 900
  ORDER BY ft2.c1 LIMIT 10;

-- Upper level relations shouldn't refer EXISTS() subqueries
--Testcase 1069:
EXPLAIN (verbose, costs off)
SELECT * FROM ft2 ftupper WHERE
   EXISTS (
	SELECT c1 FROM ft2 WHERE
	  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c2 = ft2.c2) AND c1 = ftupper.c1 )
  AND ftupper.c1 > 900
  ORDER BY ftupper.c1 LIMIT 10;
--Testcase 1070:
SELECT * FROM ft2 ftupper WHERE
   EXISTS (
	SELECT c1 FROM ft2 WHERE
	  EXISTS (SELECT 1 FROM ft4 WHERE ft4.c2 = ft2.c2) AND c1 = ftupper.c1 )
  AND ftupper.c1 > 900
  ORDER BY ftupper.c1 LIMIT 10;

-- EXISTS should be propagated to the highest upper inner join
--Testcase 1071:
EXPLAIN (verbose, costs off)
	SELECT ft2.*, ft4.* FROM ft2 INNER JOIN
	(SELECT * FROM ft4 WHERE EXISTS (
		SELECT 1 FROM ft2 WHERE ft2.c2 = ft4.c2)) ft4
	ON ft2.c2 = ft4.c1
	INNER JOIN
	(SELECT * FROM ft2 WHERE EXISTS (
		SELECT 1 FROM ft4 WHERE ft2.c2 = ft4.c2)) ft21
	ON ft2.c2 = ft21.c2
	WHERE ft2.c1 > 900
	ORDER BY ft2.c1 LIMIT 10;
--Testcase 1072:
SELECT ft2.*, ft4.* FROM ft2 INNER JOIN
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
--Testcase 1073:
EXPLAIN (verbose, costs off)
SELECT ft1.c1 FROM ft1 JOIN ft2 on ft1.c1 = ft2.c1 WHERE
	ft1.c1 IN (
		SELECT ft2.c1 FROM ft2 JOIN ft4 ON ft2.c1 = ft4.c1)
	ORDER BY ft1.c1 LIMIT 5;

-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================
--Testcase 403:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
--Testcase 404:
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
--Testcase 405:
INSERT INTO ft2_a_child (c1,c2,c3)
   VALUES (1101,201,'aaa'), (1102,202,'bbb'), (1103,203,'ccc');
-- MySQL does not support RETURNING, so we use SELECT with condition to check data after INSERT/UPDATE/DELETE.
--Testcase 406:
SELECT * FROM ft2 WHERE c1 IN (1101, 1102, 1103);
--Testcase 407:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1104,204,'ddd'), (1105,205,'eee');
--Testcase 408:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;              -- can be pushed down
--Testcase 409:
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;
--Testcase 410:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;  -- can be pushed down
--Testcase 411:
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;
-- MySQL does not support RETURNING, so we use SELECT with condition to check data after INSERT/UPDATE/DELETE.
--Testcase 412:
SELECT * FROM ft2 WHERE c1 % 10 = 7;
--Testcase 413:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
   FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;                               -- can be pushed down
--Testcase 414:
 UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
   FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;
--Testcase 415:
EXPLAIN (verbose, costs off)
   DELETE FROM ft2_a_child WHERE c1 % 10 = 5;                               -- can be pushed down
--Testcase 416:
DELETE FROM ft2_a_child WHERE c1 % 10 = 5;
-- MySQL does not support RETURNING, so we use SELECT with condition to check data after INSERT/UPDATE/DELETE.
--Testcase 417:
SELECT c1, c4 FROM ft2 WHERE c1 % 10 = 5; -- empty result
--Testcase 418:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child USING ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 2;                -- can be pushed down
--Testcase 419:
DELETE FROM ft2_a_child USING ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 2;
--Testcase 420:
SELECT c1,c2,c3,c4 FROM ft2 ORDER BY c1;
--Testcase 421:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 422:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 423:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200;             -- can be pushed down
--Testcase 424:
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200;
--Testcase 425:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child WHERE c1 = 1200;                       -- can be pushed down
--Testcase 426:
DELETE FROM ft2_a_child WHERE c1 = 1200;

-- -- Test UPDATE/DELETE with RETURNING on a three-table join
-- -- INSERT INTO ft2 (c1,c2,c3)
--   SELECT id, id - 1200, to_char(id, 'FM00000') FROM generate_series(1201, 1300) id;
-- -- EXPLAIN (verbose, costs off)
-- UPDATE ft2 SET c3 = 'foo'
--   FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
--   WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1;       -- can be pushed down
-- -- UPDATE ft2 SET c3 = 'foo'
--   FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
--   WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1;
-- -- SELECT ft2, ft2.*, ft4, ft4.*
--   FROM ft2 INNER JOIN ft4 ON (ft2.c1 > 1200 AND ft2.c2 = ft4.c1)
--   INNER JOIN ft5 ON (ft4.c1 = ft5.c1);
-- EXPLAIN (verbose, costs off)
-- DELETE FROM ft2
--   USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
--   WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1;                          -- can be pushed down
-- DELETE FROM ft2
--   USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
--   WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1;
-- SELECT 100 FROM ft2, ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
--   WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1;

-- DELETE FROM ft2 WHERE ft2.c1 > 1200;

-- -- Test UPDATE with a MULTIEXPR sub-select
-- -- (maybe someday this'll be remotely executable, but not today)
-- EXPLAIN (verbose, costs off)
-- UPDATE ft2 AS target SET (c2, c7) = (
--     SELECT c2 * 10, c7
--         FROM ft2 AS src
--         WHERE target.c1 = src.c1
-- ) WHERE c1 > 1100;
-- UPDATE ft2 AS target SET (c2, c7) = (
--     SELECT c2 * 10, c7
--         FROM ft2 AS src
--         WHERE target.c1 = src.c1
-- ) WHERE c1 > 1100;

-- UPDATE ft2 AS target SET (c2) = (
--     SELECT c2 / 10
--         FROM ft2 AS src
--         WHERE target.c1 = src.c1
-- ) WHERE c1 > 1100;

-- -- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- -- user-defined operators/functions
--Testcase 427:
ALTER SERVER mysql_svr OPTIONS (DROP extensions);
--Testcase 428:
INSERT INTO ft2_a_child (c1,c2,c3)
  SELECT id, id % 10, to_char(id, 'FM00000') FROM generate_series(2001, 2010) id;
--Testcase 429:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE mysql_fdw_abs(c1) > 2000;            -- can't be pushed down
--Testcase 430:
SELECT * FROM ft2_a_child WHERE mysql_fdw_abs(c1) > 2000;
--Testcase 431:
UPDATE ft2_a_child SET c3 = 'bar' WHERE mysql_fdw_abs(c1) > 2000;
--Testcase 432:
SELECT * FROM ft2 WHERE mysql_fdw_abs(c1) > 2000;
--Testcase 433:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1;                         -- can't be pushed down
--Testcase 434:
SELECT ft2.*, ft4.*, ft5.*
  FROM ft2, ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1;
--Testcase 435:
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1;
--Testcase 436:
SELECT ft2.*, ft4.*, ft5.*
  FROM ft2, ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1;
--Testcase 437:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1;                           -- can't be pushed down
--Testcase 438:
SELECT ft2.c1, ft2.c2, ft2.c3 FROM ft2, ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1;
--Testcase 439:
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1;
--Testcase 440:
SELECT ft2.c1, ft2.c2, ft2.c3 FROM ft2, ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1;
--Testcase 441:
DELETE FROM ft2_a_child WHERE ft2_a_child.c1 > 2000;
--Testcase 442:
ALTER SERVER mysql_svr OPTIONS (ADD extensions 'mysql_fdw');

-- Test that trigger on remote table works as expected
-- CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
-- BEGIN
--     NEW.c3 = NEW.c3 || '_trig_update';
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
-- CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
--     ON "S 1"."T 1" FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

-- INSERT INTO ft2 (c1,c2,c3) VALUES (1208, 818, 'fff');
-- INSERT INTO ft2 (c1,c2,c3,c6) VALUES (1218, 818, 'ggg', '(--;');
-- UPDATE ft2 SET c2 = c2 + 600 WHERE c1 % 10 = 8 AND c1 < 1200;

-- -- Test errors thrown on remote side during update
--Testcase 761:
CREATE FOREIGN TABLE ft1_constraint_a_child (
	c1 int OPTIONS (key 'true'),
	c2 int NOT NULL,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 text
) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 't1_constraint');

--Testcase 443:
CREATE TABLE ft1_constraint (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 text,
	spdurl text) PARTITION BY LIST (spdurl);

--Testcase 762:
CREATE FOREIGN TABLE ft1_constraint_a PARTITION OF ft1_constraint FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 444:
INSERT INTO ft1_constraint_a_child SELECT * FROM ft1_a_child ON CONFLICT DO NOTHING;
-- c2 must be greater than or equal to 0, so this case is ignored.
--Testcase 445:
INSERT INTO ft1_constraint_a_child(c1, c2) VALUES (2222, -2) ON CONFLICT DO NOTHING; -- ignore, do nothing
--Testcase 446:
SELECT c1, c2 FROM ft1_constraint_a_child WHERE c1 = 2222 or c2 = -2; -- empty result
--Testcase 447:
ALTER FOREIGN TABLE ft1_a_child RENAME TO ft1_org;
--Testcase 448:
ALTER FOREIGN TABLE ft1_constraint_a_child RENAME TO ft1_a_child;
--Testcase 449:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12);  -- duplicate key
--Testcase 450:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT DO NOTHING; -- works
--Testcase 763:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO NOTHING; -- unsupported
--Testcase 451:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO UPDATE SET c3 = 'ffg'; -- unsupported
--Testcase 452:
INSERT INTO ft1_a_child(c1, c2) VALUES(1111, -2);  -- c2positive
--Testcase 453:
UPDATE ft1_a_child SET c2 = -c2 WHERE c1 = 1;  -- c2positive
--Testcase 454:
ALTER FOREIGN TABLE ft1_a_child RENAME TO ft1_constraint_a_child;
--Testcase 455:
ALTER FOREIGN TABLE ft1_org RENAME TO ft1_a_child;
-- -- Test savepoint/rollback behavior
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
-- begin;
-- update ft2 set c2 = 42 where c2 = 0;
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- savepoint s1;
-- update ft2 set c2 = 44 where c2 = 4;
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- release savepoint s1;
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- savepoint s2;
-- update ft2 set c2 = 46 where c2 = 6;
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- rollback to savepoint s2;
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- release savepoint s2;
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- savepoint s3;
-- update ft2 set c2 = -2 where c2 = 42 and c1 = 10; -- fail on remote side
-- rollback to savepoint s3;
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- release savepoint s3;
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- -- none of the above is committed yet remotely
-- select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
-- commit;
-- select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;

-- VACUUM ANALYZE "S 1"."T 1";

-- -- Above DMLs add data with c6 as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- -- FIRST behavior here.
-- -- ORDER BY DESC NULLS LAST options
-- EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;
-- SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795  LIMIT 10;
-- -- ORDER BY DESC NULLS FIRST options
-- EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- -- ORDER BY ASC NULLS FIRST options
-- EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

-- These test cases may work but have no meaning because we cannot port source code in ReScanForeignScan function
-- Test ReScan code path that recreates the cursor even when no parameters
-- change (bug #17889)
--Testcase 939:
CREATE FOREIGN TABLE loct1_a_child (c1 int OPTIONS (key 'true'))
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'loct1_rescan');
--Testcase 941:
CREATE TABLE loct1 (c1 int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 942:
CREATE FOREIGN TABLE loct1_a PARTITION OF loct1 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 943:
CREATE FOREIGN TABLE loct2_a_child (c1 int OPTIONS (key 'true'), c2 text)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'loct2_rescan');
--Testcase 944:
CREATE TABLE loct2 (c1 int, c2 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 945:
CREATE FOREIGN TABLE loct2_a PARTITION OF loct2 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 946:
INSERT INTO loct1 (c1, spdurl) VALUES (1001, '/node1/');
--Testcase 947:
INSERT INTO loct1 (c1, spdurl) VALUES (1002, '/node1/');
--Testcase 948:
INSERT INTO loct2 SELECT id, to_char(id, 'FM0000'), '/node1/' FROM generate_series(1, 1000) id;
--Testcase 949:
INSERT INTO loct2 (c1, c2, spdurl) VALUES (1001, 'foo', '/node1/');
--Testcase 950:
INSERT INTO loct2 (c1, c2, spdurl) VALUES (1002, 'bar', '/node1/');
--Testcase 951:
CREATE FOREIGN TABLE remt2_a_child (c1 int OPTIONS (key 'true'), c2 text)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'loct2_rescan');
--Testcase 952:
CREATE TABLE remt2 (c1 int, c2 text, spdurl text) PARTITION BY LIST (spdurl);
CREATE FOREIGN TABLE remt2_a PARTITION OF remt2 FOR VALUES IN ('/node1/') SERVER spdsrv;
-- ANALYZE loct1;
-- ANALYZE remt2;
SET enable_mergejoin TO false;
SET enable_hashjoin TO false;
SET enable_material TO false;
--Testcase 953:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE remt2 SET c2 = remt2.c2 || remt2.c2 FROM loct1 WHERE loct1.c1 = remt2.c1;
--Testcase 954:
UPDATE remt2 SET c2 = remt2.c2 || remt2.c2 FROM loct1 WHERE loct1.c1 = remt2.c1;
--Testcase 955:
SELECT remt2.c1, remt2.c2 FROM loct1, remt2 WHERE loct1.c1 = remt2.c1;
RESET enable_mergejoin;
RESET enable_hashjoin;
RESET enable_material;
--Testcase 956:
DROP FOREIGN TABLE remt2_a_child;
--Testcase 957:
DROP TABLE loct1;
--Testcase 958:
DROP TABLE loct2;
--Testcase 959:
DROP TABLE remt2;

-- -- ===================================================================
-- -- test check constraints
-- -- ===================================================================
--Testcase 456:
ALTER FOREIGN TABLE ft1_a_child RENAME TO ft1_org;
--Testcase 457:
ALTER FOREIGN TABLE ft1_constraint_a_child RENAME TO ft1_a_child;
-- Consistent check constraints provide consistent results
--Testcase 458:
ALTER TABLE ft1 ADD CONSTRAINT ft1_c2positive CHECK (c2 >= 0);
--Testcase 764:
SET constraint_exclusion = 'off';
--Testcase 459:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 460:
SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 461:
SET constraint_exclusion = 'on';
--Testcase 462:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 463:
SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 464:
RESET constraint_exclusion;
-- check constraint is enforced on the remote side, not locally
--Testcase 465:
INSERT INTO ft1_a_child(c1, c2) VALUES(1111, -2);  -- c2positive
--Testcase 466:
UPDATE ft1_a_child SET c2 = -c2 WHERE c1 = 1;  -- c2positive
--Testcase 467:
ALTER TABLE ft1 DROP CONSTRAINT ft1_c2positive;

-- But inconsistent check constraints provide inconsistent results
--Testcase 468:
ALTER TABLE ft1 ADD CONSTRAINT ft1_c2negative CHECK (c2 < 0);
--Testcase 765:
SET constraint_exclusion = 'off';
--Testcase 469:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 470:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 471:
SET constraint_exclusion = 'on';
--Testcase 472:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 473:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 474:
RESET constraint_exclusion;
-- local check constraint is not actually enforced
--Testcase 475:
INSERT INTO ft1_a_child(c1, c2) VALUES(1111, 2);
--Testcase 476:
UPDATE ft1_a_child SET c2 = c2 + 1 WHERE c1 = 1;
--Testcase 477:
ALTER TABLE ft1 DROP CONSTRAINT ft1_c2negative;

-- ===================================================================
-- test WITH CHECK OPTION constraints
-- ===================================================================
--Testcase 478:
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;

--Testcase 766:
CREATE FOREIGN TABLE foreign_tbl_a_child (id int, a int, b int)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'base_tbl');

--Testcase 479:
CREATE TABLE foreign_tbl (id int, a int, b int, spdurl text) PARTITION BY LIST (spdurl);

--Testcase 767:
CREATE FOREIGN TABLE foreign_tbl_a PARTITION OF foreign_tbl FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 480:
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl_a_child FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

--Testcase 481:
CREATE VIEW rw_view AS SELECT a, b FROM foreign_tbl_a_child
  WHERE a < b WITH CHECK OPTION;
--Testcase 482:
\d+ rw_view

--Testcase 483:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 5);
--Testcase 484:
INSERT INTO rw_view VALUES (0, 5); -- should fail
--Testcase 485:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 15);
--Testcase 486:
INSERT INTO rw_view VALUES (0, 15); -- ok
--Testcase 487:
SELECT a, b FROM foreign_tbl;

--Testcase 488:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 5;
--Testcase 489:
UPDATE rw_view SET b = b + 5; -- should fail
--Testcase 490:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 15;
--Testcase 491:
UPDATE rw_view SET b = b + 15; -- ok
--Testcase 492:
SELECT a, b FROM foreign_tbl;

-- We don't allow batch insert when there are any WCO constraints
ALTER SERVER mysql_svr OPTIONS (ADD batch_size '10');
--Testcase 914:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 15), (0, 5);
--Testcase 915:
INSERT INTO rw_view VALUES (0, 15), (0, 5); -- should fail
--Testcase 916:
SELECT a, b FROM foreign_tbl;
ALTER SERVER mysql_svr OPTIONS (DROP batch_size);

--Testcase 493:
DROP TRIGGER row_before_insupd_trigger ON foreign_tbl_a_child;
--Testcase 494:
DROP FOREIGN TABLE foreign_tbl_a_child CASCADE;

-- PGspider_Ext support only LIST partition rule 
-- -- test WCO for partitions

-- --Testcase 495:
-- CREATE FOREIGN TABLE foreign_tbl (id int, a int, b int)
--   SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'child_tbl');

-- --Testcase 496:
-- CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

-- --Testcase 497:
-- CREATE TABLE parent_tbl (id int, a int, b int) PARTITION BY RANGE(a);
-- --Testcase 498:
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);
-- -- Detach and re-attach once, to stress the concurrent detach case.
-- ALTER TABLE parent_tbl DETACH PARTITION foreign_tbl CONCURRENTLY;
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);
-- --Testcase 499:
-- CREATE VIEW rw_view AS SELECT a, b FROM parent_tbl
--   WHERE a < b WITH CHECK OPTION;
-- --Testcase 500:
-- \d+ rw_view

-- --Testcase 501:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 5);
-- --Testcase 502:
-- INSERT INTO rw_view VALUES (0, 5); -- should fail
-- --Testcase 503:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 15);
-- --Testcase 504:
-- INSERT INTO rw_view VALUES (0, 15); -- ok
-- --Testcase 505:
-- SELECT a, b FROM foreign_tbl;

-- --Testcase 506:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE rw_view SET b = b + 5;
-- --Testcase 507:
-- UPDATE rw_view SET b = b + 5; -- should fail
-- --Testcase 508:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE rw_view SET b = b + 15;
-- --Testcase 509:
-- UPDATE rw_view SET b = b + 15; -- ok
-- --Testcase 510:
-- SELECT a, b FROM foreign_tbl;

-- -- We don't allow batch insert when there are any WCO constraints
-- ALTER SERVER loopback OPTIONS (ADD batch_size '10');
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 15), (0, 5);
-- INSERT INTO rw_view VALUES (0, 15), (0, 5); -- should fail
-- SELECT * FROM foreign_tbl;
-- ALTER SERVER loopback OPTIONS (DROP batch_size);

-- --Testcase 511:
-- DROP TRIGGER row_before_insupd_trigger ON foreign_tbl;
-- --Testcase 512:
-- DROP FOREIGN TABLE foreign_tbl CASCADE;
-- --Testcase 513:
-- DROP TABLE parent_tbl CASCADE;

-- --Testcase 514:
-- DROP FUNCTION row_before_insupd_trigfunc;

-- Try a more complex permutation of WCO where there are multiple levels of
-- partitioned tables with columns not all in the same order
--Testcase 936:
CREATE TABLE parent_tbl (a int, b text, c numeric) PARTITION BY RANGE(a);
--Testcase 937:
CREATE TABLE sub_parent (c numeric, a int, b text) PARTITION BY RANGE(a);
ALTER TABLE parent_tbl ATTACH PARTITION sub_parent FOR VALUES FROM (1) TO (10);
--Testcase 938:
CREATE FOREIGN TABLE child_foreign (b text, c numeric, a int)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'child_local');
ALTER TABLE sub_parent ATTACH PARTITION child_foreign FOR VALUES FROM (1) TO (10);
--Testcase 939:
CREATE VIEW rw_view AS SELECT * FROM parent_tbl WHERE a < 5 WITH CHECK OPTION;

--Testcase 940:
INSERT INTO parent_tbl (a) VALUES(1),(5);
--Testcase 941:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = 'text', c = 123.456;
--Testcase 942:
UPDATE rw_view SET b = 'text', c = 123.456;
--Testcase 943:
SELECT * FROM parent_tbl ORDER BY a;

--Testcase 944:
DROP VIEW rw_view;
--Testcase 945:
DROP FOREIGN TABLE child_foreign;
--Testcase 946:
DROP TABLE sub_parent;
--Testcase 947:
DROP TABLE parent_tbl;

-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- ===================================================================
--Testcase 768:
create foreign table loc1_a_child (id int, f1 serial, f2 text)
  server mysql_svr options(dbname 'mysql_fdw_post', table_name 'loc1');

--Testcase 515:
create table loc1 (id int, f1 serial, f2 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 769:
create foreign table loc1_a PARTITION OF loc1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 770:
create foreign table rem1_a_child (id int, f1 serial, f2 text)
  server mysql_svr options(dbname 'mysql_fdw_post', table_name 'loc1');
--Testcase 516:
create table rem1 (id int, f1 serial, f2 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 771:
create foreign table rem1_a PARTITION OF rem1 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 517:
select pg_catalog.setval('rem1_a_child_f1_seq', 10, false);
--Testcase 518:
insert into loc1_a_child(f2) values('hi');
--Testcase 519:
insert into rem1_a_child(f2) values('hi remote');
--Testcase 520:
insert into loc1_a_child(f2) values('bye');
--Testcase 521:
insert into rem1_a_child(f2) values('bye remote');
--Testcase 522:
select f1, f2 from loc1;
--Testcase 523:
select f1, f2 from rem1;

-- ===================================================================
-- test generated columns
-- ===================================================================
--Testcase 772:
create foreign table grem1_a_child (
  id int,
  a int,
  b int generated always as (a * 2) stored)
  server mysql_svr options(dbname 'mysql_fdw_post', table_name 'gloc1_post14');

--Testcase 524:
create table grem1 (
  id int,
  a int,
  b int generated always as (a * 2) stored,
  spdurl text) PARTITION BY LIST (spdurl);
--Testcase 773:
create foreign table grem1_a PARTITION OF grem1 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 917:
explain (verbose, costs off)
insert into grem1_a_child (a) values (1), (2);
--Testcase 525:
insert into grem1_a_child (a) values (1), (2);
--Testcase 918:
explain (verbose, costs off)
update grem1_a_child set a = 22 where a = 2;
--Testcase 526:
update grem1_a_child set a = 22 where a = 2;
--Testcase 527:
select a, b from grem1;
--Testcase 919:
delete from grem1_a_child;

-- test copy from
copy grem1_a_child from stdin;
1   1
2   2
\.
--Testcase 747:
select * from grem1;
--Testcase 748:
delete from grem1_a_child;
-- test batch insert
--Testcase 749:
alter server mysql_svr options (add batch_size '10');
--Testcase 750:
explain (verbose, costs off)
insert into grem1_a_child (a) values (1), (2);
--Testcase 751:
insert into grem1_a_child (a) values (1), (2);
--Testcase 752:
select * from grem1;
--Testcase 753:
delete from grem1_a_child;

-- batch insert with foreign partitions.
-- This schema uses two partitions, one local and one remote with a modulo
-- to loop across all of them in batches.
--Testcase 948:
create table tab_batch_local (id int, data text);
--Testcase 949:
insert into tab_batch_local select i, 'test'|| i from generate_series(1, 45) i;
--Testcase 950:
create table tab_batch_sharded (id int, data text) partition by hash(id);
--Testcase 951:
create table tab_batch_sharded_p0 partition of tab_batch_sharded
  for values with (modulus 2, remainder 0);

--Testcase 952:
create foreign table tab_batch_sharded_p1 partition of tab_batch_sharded
  for values with (modulus 2, remainder 1)
  server mysql_svr options (dbname 'mysql_fdw_post', table_name 'tab_batch_sharded_p1_remote');
--Testcase 953:
insert into tab_batch_sharded select * from tab_batch_local;
--Testcase 954:
select count(*) from tab_batch_sharded;
--Testcase 955:
drop table tab_batch_local;
--Testcase 956:
drop table tab_batch_sharded;

--Testcase 754:
alter server mysql_svr options (drop batch_size);

-- ===================================================================
-- test local triggers
-- ===================================================================

-- Trigger functions "borrowed" from triggers regress test.
--Testcase 528:
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

--Testcase 957:
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE OR TRUNCATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 958:
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE OR TRUNCATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 529:
CREATE OR REPLACE FUNCTION trigger_data()  RETURNS trigger
LANGUAGE plpgsql AS $$

declare
	oldnew text[];
	relid text;
    argstr text;
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
	if TG_OP != 'INSERT' then
		oldnew := array_append(oldnew, format('OLD: %s', OLD));
	end if;

	if TG_OP != 'DELETE' then
		oldnew := array_append(oldnew, format('NEW: %s', NEW));
	end if;

    RAISE NOTICE '%', array_to_string(oldnew, ',');

	if TG_OP = 'DELETE' then
		return OLD;
	else
		return NEW;
	end if;
end;
$$;

-- Test basic functionality
--Testcase 959:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 960:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 961:
delete from rem1_a_child;
--Testcase 962:
insert into rem1_a_child(f1, f2) values(1,'insert');
--Testcase 963:
update rem1_a_child set f2  = 'update' where f1 = 1;
--Testcase 964:
update rem1_a_child set f2 = f2 || f2;
truncate rem1_a_child;


-- cleanup
--Testcase 965:
DROP TRIGGER trig_row_before ON rem1_a_child;
--Testcase 966:
DROP TRIGGER trig_row_after ON rem1_a_child;
--Testcase 967:
DROP TRIGGER trig_stmt_before ON rerem1_a_childm1;
--Testcase 968:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

--Testcase 969:
DELETE from rem1_a_child;

-- Test multiple AFTER ROW triggers on a foreign table
--Testcase 970:
CREATE TRIGGER trig_row_after1
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 971:
CREATE TRIGGER trig_row_after2
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 972:
insert into rem1_a_child(f1, f2) values(1,'insert');
--Testcase 973:
update rem1_a_child set f2  = 'update' where f1 = 1;
--Testcase 974:
update rem1_a_child set f2 = f2 || f2;
--Testcase 975:
delete from rem1_a_child;

-- cleanup
--Testcase 976:
DROP TRIGGER trig_row_after1 ON rem1_a_child;
--Testcase 977:
DROP TRIGGER trig_row_after2 ON rem1_a_child;

-- Test WHEN conditions

--Testcase 978:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 979:
CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens
--Testcase 980:
INSERT INTO rem1_a_child(f1, f2) values(1, 'insert');
--Testcase 981:
UPDATE rem1_a_child set f2 = 'test';

-- Insert or update matching: triggers are fired
--Testcase 982:
INSERT INTO rem1_a_child(f1, f2) values(2, 'update');
--Testcase 983:
UPDATE rem1_a_child set f2 = 'update update' where f1 = '2';

--Testcase 984:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 985:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1
--Testcase 986:
DELETE FROM rem1_a_child;

-- cleanup
--Testcase 987:
DROP TRIGGER trig_row_before_insupd ON rem1_a_child;
--Testcase 988:
DROP TRIGGER trig_row_after_insupd ON rem1_a_child;
--Testcase 989:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;
--Testcase 990:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;

-- MySQL FDW does not support RETURNING
-- Test various RETURN statements in BEFORE triggers.

--Testcase 530:
CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

-- CREATE TRIGGER trig_row_before_insupd
-- BEFORE INSERT OR UPDATE ON rem1
-- FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- -- The new values should have 'triggered' appended
-- INSERT INTO rem1(f1, f2) values(1, 'insert');
-- SELECT * from loc1;
-- INSERT INTO rem1(f1, f2) values(2, 'insert') RETURNING f2;
-- SELECT * from loc1;
-- UPDATE rem1 set f2 = '';
-- SELECT * from loc1;
-- UPDATE rem1 set f2 = 'skidoo' RETURNING f2;
-- SELECT * from loc1;

-- EXPLAIN (verbose, costs off)
-- UPDATE rem1 set f1 = 10;          -- all columns should be transmitted
-- UPDATE rem1 set f1 = 10;
-- SELECT * from loc1;

-- DELETE FROM rem1;

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger
-- CREATE TRIGGER trig_row_before_insupd2
-- BEFORE INSERT OR UPDATE ON rem1
-- FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- INSERT INTO rem1(f1, f2) values(1, 'insert');
-- SELECT * from loc1;
-- INSERT INTO rem1(f1, f2) values(2, 'insert') RETURNING f2;
-- SELECT * from loc1;
-- UPDATE rem1 set f2 = '';
-- SELECT * from loc1;
-- UPDATE rem1 set f2 = 'skidoo' RETURNING f2;
-- SELECT * from loc1;

-- DROP TRIGGER trig_row_before_insupd ON rem1;
-- DROP TRIGGER trig_row_before_insupd2 ON rem1;

--Testcase 991:
DELETE from rem1_a_child;

--Testcase 992:
INSERT INTO rem1_a_child(f1, f2) VALUES (1, 'test');

-- Test with a trigger returning NULL
--Testcase 531:
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

--Testcase 993:
CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.
--Testcase 994:
INSERT INTO rem1_a_child(f1, f2) VALUES (2, 'test2');

--Testcase 995:
SELECT * from loc1;

--Testcase 996:
UPDATE rem1_a_child SET f2 = 'test2';

--Testcase 997:
SELECT * from loc1;

--Testcase 998:
DELETE from rem1_a_child;

--Testcase 999:
SELECT * from loc1;

--Testcase 1000:
DROP TRIGGER trig_null ON rem1_a_child;
--Testcase 1001:
DELETE from rem1_a_child;

-- Cannot create trigger on remote table at runtime
-- Test a combination of local and remote triggers
-- CREATE TRIGGER trig_row_before
-- BEFORE INSERT OR UPDATE OR DELETE ON rem1
-- FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- CREATE TRIGGER trig_row_after
-- AFTER INSERT OR UPDATE OR DELETE ON rem1
-- FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- CREATE TRIGGER trig_local_before BEFORE INSERT OR UPDATE ON loc1
-- FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- INSERT INTO rem1(f2) VALUES ('test');
-- UPDATE rem1 SET f2 = 'testo';

-- -- Test returning a system attribute
-- INSERT INTO rem1(f2) VALUES ('test') RETURNING ctid;

-- cleanup
-- DROP TRIGGER trig_row_before ON rem1;
-- DROP TRIGGER trig_row_after ON rem1;
-- DROP TRIGGER trig_local_before ON loc1;


-- Test direct foreign table modification functionality
--Testcase 863:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 864:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child WHERE false;     -- currently can't be pushed down

-- Test with statement-level triggers
--Testcase 1002:
CREATE TRIGGER trig_stmt_before
	BEFORE DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 1003:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 1004:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 1005:
DROP TRIGGER trig_stmt_before ON rem1_a_child;

--Testcase 1006:
CREATE TRIGGER trig_stmt_after
	AFTER DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 1007:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 1008:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 1009:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

-- Test with row-level ON INSERT triggers
--Testcase 1010:
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 1011:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 1012:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 1013:
DROP TRIGGER trig_row_before_insert ON rem1_a_child;

--Testcase 1014:
CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 1015:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 1016:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 1017:
DROP TRIGGER trig_row_after_insert ON rem1_a_child;

-- Test with row-level ON UPDATE triggers
--Testcase 1018:
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 1019:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down
--Testcase 1020:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 1021:
DROP TRIGGER trig_row_before_update ON rem1_a_child;

--Testcase 1022:
CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 1023:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down
--Testcase 1024:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 1025:
DROP TRIGGER trig_row_after_update ON rem1_a_child;

-- Test with row-level ON DELETE triggers
--Testcase 1026:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 1027:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 1028:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down
--Testcase 1029:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;

--Testcase 1030:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 1031:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 1032:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down
--Testcase 1033:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;

-- ===================================================================
-- test inheritance features
-- ===================================================================

--Testcase 532:
CREATE TABLE a (aa TEXT);
--Testcase 533:
CREATE TABLE loct (aa TEXT, bb TEXT);
--Testcase 534:
ALTER TABLE a SET (autovacuum_enabled = 'false');
--Testcase 535:
ALTER TABLE loct SET (autovacuum_enabled = 'false');

--Testcase 774:
CREATE FOREIGN TABLE b_a_child (bb TEXT) INHERITS (a)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'loct');
--Testcase 536:
CREATE TABLE b (aa TEXT, bb TEXT, spdurl text)
   PARTITION BY LIST (spdurl);
--Testcase 775:
CREATE FOREIGN TABLE b_a PARTITION OF b FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 537:
INSERT INTO a(aa) VALUES('aaa');
--Testcase 538:
INSERT INTO a(aa) VALUES('aaaa');
--Testcase 539:
INSERT INTO a(aa) VALUES('aaaaa');

--Testcase 540:
INSERT INTO b_a_child(aa) VALUES('bbb');
--Testcase 541:
INSERT INTO b_a_child(aa) VALUES('bbbb');
--Testcase 542:
INSERT INTO b_a_child(aa) VALUES('bbbbb');

--Testcase 776:
SELECT tableoid::regclass, * FROM a;
--Testcase 777:
SELECT tableoid::regclass, * FROM b;
--Testcase 778:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 543:
UPDATE a SET aa = 'zzzzzz' WHERE aa LIKE 'aaaa%';

--Testcase 779:
SELECT tableoid::regclass, * FROM a;
--Testcase 780:
SELECT tableoid::regclass, * FROM b;
--Testcase 781:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 544:
UPDATE b_a_child SET aa = 'new';

--Testcase 782:
SELECT tableoid::regclass, * FROM a;
--Testcase 783:
SELECT tableoid::regclass, * FROM b;
--Testcase 784:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 545:
UPDATE a SET aa = 'newtoo';

--Testcase 785:
SELECT tableoid::regclass, * FROM a;
--Testcase 786:
SELECT tableoid::regclass, * FROM b;
--Testcase 787:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 546:
DELETE FROM a;

--Testcase 788:
SELECT tableoid::regclass, * FROM a;
--Testcase 789:
SELECT tableoid::regclass, * FROM b;
--Testcase 790:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 547:
DROP TABLE a CASCADE;
--Testcase 548:
DROP TABLE loct;

-- Check SELECT FOR UPDATE/SHARE with an inherited source table
--Testcase 549:
create table foo (f1 int, f2 int);

--Testcase 791:
create foreign table foo2_a_child (f3 int) inherits (foo)
  server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct1');
--Testcase 550:
create table foo2 (f3 int, spdurl text)
   PARTITION BY LIST (spdurl);
--Testcase 551:
CREATE FOREIGN TABLE foo2_a PARTITION OF foo2 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 792:
create table bar (f1 int, f2 int);

--Testcase 793:
create foreign table bar2_a_child (f3 int) inherits (bar)
  server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct2');

--Testcase 552:
create table bar2 (f3 int, spdurl text)
  PARTITION BY LIST (spdurl);

--Testcase 794:
CREATE FOREIGN TABLE bar2_a PARTITION OF bar2 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 553:
alter table foo set (autovacuum_enabled = 'false');
--Testcase 554:
alter table bar set (autovacuum_enabled = 'false');

--Testcase 555:
insert into foo values(1,1);
--Testcase 556:
insert into foo values(3,3);
--Testcase 557:
insert into foo2_a_child values(2,2,2);
--Testcase 558:
insert into foo2_a_child values(4,4,4);
--Testcase 559:
insert into bar values(1,11);
--Testcase 560:
insert into bar values(2,22);
--Testcase 561:
insert into bar values(6,66);
--Testcase 562:
insert into bar2_a_child values(3,33,33);
--Testcase 563:
insert into bar2_a_child values(4,44,44);
--Testcase 564:
insert into bar2_a_child values(7,77,77);

--Testcase 565:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for update;
--Testcase 566:
select * from bar where f1 in (select f1 from foo) for update;

--Testcase 567:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for share;
--Testcase 568:
select * from bar where f1 in (select f1 from foo) for share;

-- Check UPDATE with inherited target and an inherited source table
--Testcase 569:
explain (verbose, costs off)
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);
--Testcase 570:
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

--Testcase 571:
select tableoid::regclass, * from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery
--Testcase 572:
explain (verbose, costs off)
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;
--Testcase 573:
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

--Testcase 574:
select tableoid::regclass, * from bar order by 1,2;

-- Test forcing the remote server to produce sorted data for a merge join,
-- but the foreign table is an inheritance child.
--truncate table foo2;
--Testcase 575:
delete from foo2_a_child;
truncate table only foo;
\set num_rows_foo 2000
--Testcase 576:
insert into foo2_a_child select generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2);
--Testcase 577:
insert into foo select generate_series(1, :num_rows_foo, 2), generate_series(1, :num_rows_foo, 2);
--Testcase 578:
SET enable_hashjoin to false;
--Testcase 579:
SET enable_nestloop to false;
--alter foreign table foo2 options (use_remote_estimate 'true');
--create index i_loct1_f1 on loct1(f1);
--create index i_foo_f1 on foo(f1);
--analyze foo;
--analyze loct1;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 580:
explain (verbose, costs off)
	select foo.f1, foo2_a_child.f1 from foo join foo2_a_child on (foo.f1 = foo2_a_child.f1) order by foo.f2 offset 10 limit 10;
--Testcase 581:
select foo.f1, foo2_a_child.f1 from foo join foo2_a_child on (foo.f1 = foo2_a_child.f1) order by foo.f2 offset 10 limit 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 582:
explain (verbose, costs off)
	select foo.f1, foo2_a_child.f1 from foo left join foo2_a_child on (foo.f1 = foo2_a_child.f1) order by foo.f2 offset 10 limit 10;
--Testcase 583:
select foo.f1, foo2_a_child.f1 from foo left join foo2_a_child on (foo.f1 = foo2_a_child.f1) order by foo.f2 offset 10 limit 10;
--Testcase 584:
RESET enable_hashjoin;
--Testcase 585:
RESET enable_nestloop;
-- mysql does not support transaction
-- -- Test that WHERE CURRENT OF is not supported
-- begin;
-- declare c cursor for select * from bar where f1 = 7;
-- fetch from c;
-- update bar set f2 = null where current of c;
-- rollback;

-- explain (verbose, costs off)
-- delete from foo where f1 < 5 returning *;
-- delete from foo where f1 < 5 returning *;
-- explain (verbose, costs off)
-- update bar set f2 = f2 + 100 returning *;
-- update bar set f2 = f2 + 100 returning *;

-- -- Test that UPDATE/DELETE with inherited target works with row-level triggers
-- CREATE TRIGGER trig_row_before
-- BEFORE UPDATE OR DELETE ON bar2
-- FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- CREATE TRIGGER trig_row_after
-- AFTER UPDATE OR DELETE ON bar2
-- FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- explain (verbose, costs off)
-- update bar set f2 = f2 + 100;
-- update bar set f2 = f2 + 100;

-- explain (verbose, costs off)
-- delete from bar where f2 < 400;
-- delete from bar where f2 < 400;

-- -- cleanup
-- drop table foo cascade;
-- drop table bar cascade;
-- drop table loct1;
-- drop table loct2;

-- -- Test pushing down UPDATE/DELETE joins to the remote server
-- create table parent (a int, b text);
-- create table loct1 (a int, b text);
-- create table loct2 (a int, b text);
-- create foreign table remt1 (a int, b text)
--   server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct1');
-- create foreign table remt2 (a int, b text)
--   server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct2');
-- alter foreign table remt1 inherit parent;

-- insert into remt1 values (1, 'foo');
-- insert into remt1 values (2, 'bar');
-- insert into remt2 values (1, 'foo');
-- insert into remt2 values (2, 'bar');

-- -- analyze remt1;
-- -- analyze remt2;

-- explain (verbose, costs off)
-- update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a returning *;
-- update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a returning *;
-- explain (verbose, costs off)
-- delete from parent using remt2 where parent.a = remt2.a returning parent;
-- delete from parent using remt2 where parent.a = remt2.a returning parent;

-- -- cleanup
-- drop foreign table remt1;
-- drop foreign table remt2;
-- drop table loct1;
-- drop table loct2;
-- drop table parent;

-- PGSpider Extension does not support INSERT/UPDATE/DELETE directly on
-- parent table, so we skip these test cases.
-- -- ===================================================================
-- -- test tuple routing for foreign-table partitions
-- -- ===================================================================

-- Test insert tuple routing
-- --Testcase 586:
-- create table itrtest (id int, a int, b text) partition by list (a);
-- --Testcase 587:
-- create foreign table remp1 (id int, a int check (a in (1)), b text) server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct5');
-- --Testcase 588:
-- create foreign table remp2 (id int, b text, a int check (a in (2))) server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct6');
-- --Testcase 589:
-- alter table itrtest attach partition remp1 for values in (1);
-- --Testcase 590:
-- alter table itrtest attach partition remp2 for values in (2);

-- --Testcase 591:
-- insert into itrtest(a, b) values (1, 'foo');
-- --Testcase 592:
-- insert into itrtest(a, b) values (1, 'bar') returning a, b;
-- --Testcase 593:
-- insert into itrtest(a, b) values (2, 'baz');
-- --Testcase 594:
-- insert into itrtest(a, b) values (2, 'qux') returning a, b;
-- --Testcase 595:
-- insert into itrtest(a, b) values (1, 'test1'), (2, 'test2') returning a, b;

-- --Testcase 596:
-- select tableoid::regclass, a, b FROM itrtest;
-- --Testcase 597:
-- select tableoid::regclass, a, b FROM remp1;
-- --Testcase 598:
-- select tableoid::regclass, a, b FROM remp2;

-- --Testcase 599:
-- delete from itrtest;

-- -- DO NOTHING without an inference specification is supported
-- --Testcase 600:
-- insert into itrtest values (1, 1, 'foo') on conflict do nothing returning *;
-- --Testcase 601:
-- insert into itrtest values (1, 1, 'foo') on conflict do nothing returning *;

-- -- But other cases are not supported
-- --Testcase 602:
-- insert into itrtest values (1, 1, 'bar') on conflict (a) do nothing;
-- --Testcase 603:
-- insert into itrtest values (1, 1, 'bar') on conflict (a) do update set b = excluded.b;

-- --Testcase 604:
-- select tableoid::regclass, * FROM itrtest;

-- --Testcase 605:
-- delete from itrtest;

-- -- Test that remote triggers work with insert tuple routing
-- --Testcase 606:
-- create function br_insert_trigfunc() returns trigger as $$
-- begin
-- 	new.b := new.b || ' triggered !';
-- 	return new;
-- end
-- $$ language plpgsql;

-- --Testcase 607:
-- create trigger remp1_br_insert_trigger before insert on remp1
-- 	for each row execute procedure br_insert_trigfunc();
-- --Testcase 608:
-- create trigger remp2_br_insert_trigger before insert on remp2
-- 	for each row execute procedure br_insert_trigfunc();
-- -- The new values are concatenated with ' triggered !'
-- --Testcase 609:
-- insert into itrtest(a, b) values (1, 'foo') returning *;
-- --Testcase 610:
-- insert into itrtest(a, b) values (2, 'qux') returning *;
-- --Testcase 611:
-- insert into itrtest(a, b) values (1, 'test1'), (2, 'test2') returning *;
-- --Testcase 612:
-- with result as (insert into itrtest(a, b) values (1, 'test1'), (2, 'test2') returning *) select * from result;

-- --Testcase 613:
-- drop trigger remp1_br_insert_trigger on remp1;
-- --Testcase 614:
-- drop trigger remp2_br_insert_trigger on remp2;

-- --Testcase 615:
-- drop foreign table remp1;
-- --Testcase 616:
-- drop foreign table remp2;
-- --Testcase 617:
-- drop table itrtest;

-- -- Test update tuple routing
-- --Testcase 618:
-- create table utrtest (id int, a int, b text) partition by list (a);
-- --Testcase 619:
-- create foreign table remp (id int, a int check (a in (1)), b text) server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct10');
-- --Testcase 620:
-- create table locp (id int, a int check (a in (2)), b text);
-- --Testcase 621:
-- alter table utrtest attach partition remp for values in (1);
-- --Testcase 622:
-- alter table utrtest attach partition locp for values in (2);

-- --Testcase 623:
-- insert into utrtest values (1, 1, 'foo');
-- --Testcase 624:
-- insert into utrtest values (2, 2, 'qux');

-- --Testcase 625:
-- select tableoid::regclass, a, b FROM utrtest;
-- --Testcase 626:
-- select tableoid::regclass, a, b FROM remp;
-- --Testcase 627:
-- select tableoid::regclass, a, b FROM locp;

-- -- It's not allowed to move a row from a partition that is foreign to another
-- --Testcase 628:
-- update utrtest set a = 2 where b = 'foo';

-- -- But the reverse is allowed
-- --Testcase 629:
-- update utrtest set a = 1 where b = 'qux';
-- --Testcase 630:
-- select a, b from utrtest where b = 'qux';

-- --Testcase 631:
-- select tableoid::regclass, a, b FROM utrtest;
-- --Testcase 632:
-- select tableoid::regclass, a, b FROM remp;
-- --Testcase 633:
-- select tableoid::regclass, a, b FROM locp;

-- -- The executor should not let unexercised FDWs shut down
-- --Testcase 634:
-- update utrtest set a = 1 where b = 'foo';

-- -- Test that remote triggers work with update tuple routing
-- --Testcase 635:
-- create trigger remp_br_insert_trigger before insert on remp
-- 	for each row execute procedure br_insert_trigfunc();

-- --Testcase 636:
-- delete from utrtest;
-- --Testcase 637:
-- insert into utrtest values (1, 2, 'qux');

-- -- Check case where the foreign partition is a subplan target rel
-- --Testcase 638:
-- explain (verbose, costs off)
-- update utrtest set a = 1 where a = 1 or a = 2;
-- -- The new values are concatenated with ' triggered !'
-- --Testcase 639:
-- update utrtest set a = 1 where a = 1 or a = 2;
-- --Testcase 640:
-- select a, b from utrtest;

-- --Testcase 641:
-- delete from utrtest;
-- --Testcase 642:
-- insert into utrtest values (1, 2, 'qux');

-- -- Check case where the foreign partition isn't a subplan target rel
-- --Testcase 643:
-- explain (verbose, costs off)
-- update utrtest set a = 1 where a = 2;
-- -- The new values are concatenated with ' triggered !'
-- --Testcase 644:
-- update utrtest set a = 1 where a = 2;
-- --Testcase 645:
-- select a, b from utrtest;

-- --Testcase 646:
-- drop trigger remp_br_insert_trigger on remp;

-- -- We can move rows to a foreign partition that has been updated already,
-- -- but can't move rows to a foreign partition that hasn't been updated yet

-- --Testcase 647:
-- delete from utrtest;
-- --Testcase 648:
-- insert into utrtest values (1, 1, 'foo');
-- --Testcase 649:
-- insert into utrtest values (2, 2, 'qux');

-- -- Test the former case:
-- -- with a direct modification plan
-- --Testcase 650:
-- explain (verbose, costs off)
-- update utrtest set a = 1;
-- --Testcase 651:
-- update utrtest set a = 1;
-- --Testcase 652:
-- select a, b from utrtest;

-- --Testcase 653:
-- delete from utrtest;
-- --Testcase 654:
-- insert into utrtest(id, a, b) values (3, 1, 'foo');
-- --Testcase 655:
-- insert into utrtest(id, a, b) values (4, 2, 'qux');

-- -- with a non-direct modification plan
-- --Testcase 656:
-- explain (verbose, costs off)
-- update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x;
-- --Testcase 657:
-- update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x;
-- --Testcase 658:
-- select a, b from utrtest;

-- -- Change the definition of utrtest so that the foreign partition get updated
-- -- after the local partition
-- --Testcase 659:
-- delete from utrtest;
-- --Testcase 660:
-- alter table utrtest detach partition remp;
-- --Testcase 661:
-- drop foreign table remp;
-- --Testcase 662:
-- create foreign table remp (id int, a int check (a in (3)), b text) server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct11');
-- --Testcase 663:
-- alter table utrtest attach partition remp for values in (3);
-- --Testcase 664:
-- insert into utrtest values (1, 2, 'qux');
-- --Testcase 665:
-- insert into utrtest values (2, 3, 'xyzzy');

-- -- Test the latter case:
-- -- with a direct modification plan
-- --Testcase 666:
-- explain (verbose, costs off)
-- update utrtest set a = 3;
-- --Testcase 667:
-- update utrtest set a = 3; -- ERROR

-- -- with a non-direct modification plan
-- --Testcase 668:
-- explain (verbose, costs off)
-- update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x;
-- --Testcase 669:
-- update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x; -- ERROR

-- --Testcase 670:
-- drop foreign table remp;
-- --Testcase 671:
-- drop table utrtest;

-- -- Test copy tuple routing
-- --Testcase 672:
-- create table ctrtest (id int, a int, b text) partition by list (a);
-- --Testcase 673:
-- create foreign table remp1 (id int, a int check (a in (1)), b text) server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct12');
-- --Testcase 674:
-- create foreign table remp2 (id int, b text, a int check (a in (2))) server mysql_svr options (dbname 'mysql_fdw_post', table_name 'loct13');
-- --Testcase 675:
-- alter table ctrtest attach partition remp1 for values in (1);
-- --Testcase 676:
-- alter table ctrtest attach partition remp2 for values in (2);

-- copy ctrtest from stdin;
-- 1	1	foo
-- 2	2	qux
-- \.

-- --Testcase 677:
-- select tableoid::regclass, * FROM ctrtest;
-- --Testcase 678:
-- select tableoid::regclass, * FROM remp1;
-- --Testcase 679:
-- select tableoid::regclass, * FROM remp2;

-- -- Copying into foreign partitions directly should work as well
-- copy remp1 from stdin;
-- 3	1	bar
-- \.

-- --Testcase 680:
-- select tableoid::regclass, * FROM remp1;

-- delete from ctrtest;

-- -- Test copy tuple routing with the batch_size option enabled
-- alter server mysql_svr options (add batch_size '2');

-- copy ctrtest from stdin;
-- 1	1	foo
-- 2	1	bar
-- 3	2	baz
-- 4	2	qux
-- 5	1	test1
-- 6	2	test2
-- \.

-- select tableoid::regclass, * FROM ctrtest;
-- select tableoid::regclass, * FROM remp1;
-- select tableoid::regclass, * FROM remp2;

-- delete from ctrtest;

-- alter server mysql_svr options (drop batch_size);

-- --Testcase 681:
-- drop foreign table remp1;
-- --Testcase 682:
-- drop foreign table remp2;
-- --Testcase 683:
-- drop table ctrtest;

-- ===================================================================
-- test COPY FROM
-- ===================================================================
--Testcase 795:
create foreign table rem2_a_child (id int, f1 int, f2 text) server mysql_svr options(dbname 'mysql_fdw_post', table_name 'loc2');

--Testcase 684:
create table rem2 (id int, f1 int, f2 text, spdurl text) PARTITION BY LIST (spdurl);

--Testcase 796:
CREATE FOREIGN TABLE rem2_a PARTITION OF rem2 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- Test basic functionality
copy rem2_a_child from stdin;
1	1	foo
2	2	bar
\.
--Testcase 685:
select * from rem2;

--Testcase 686:
delete from rem2_a_child;

-- Test check constraints
--Testcase 797:
create foreign table rem4_a_child (id int, f1 int, f2 text) server mysql_svr options(dbname 'mysql_fdw_post', table_name 'loc4');

--Testcase 687:
create table rem4 (id int, f1 int, f2 text, spdurl text) PARTITION BY LIST (spdurl);

--Testcase 798:
CREATE FOREIGN TABLE rem4_a PARTITION OF rem4 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 688:
alter foreign table rem4_a_child add constraint rem4_f1positive check (f1 >= 0);

-- check constraint is enforced on the remote side, not locally
copy rem4_a_child from stdin;
1	1	foo
2	2	bar
\.
copy rem4_a_child from stdin; -- ERROR
3	-1	xyzzy
\.
--Testcase 689:
select * from rem4;

--Testcase 690:
alter foreign table rem4_a_child drop constraint rem4_f1positive;
--Testcase 691:
drop foreign table rem4_a_child;

-- Test local triggers
--Testcase 692:
create trigger trig_stmt_before before insert on rem2_a_child
	for each statement execute procedure trigger_func();
--Testcase 693:
create trigger trig_stmt_after after insert on rem2_a_child
	for each statement execute procedure trigger_func();
--Testcase 694:
create trigger trig_row_before before insert on rem2_a_child
	for each row execute procedure trigger_data(23,'skidoo');
--Testcase 695:
create trigger trig_row_after after insert on rem2_a_child
	for each row execute procedure trigger_data(23,'skidoo');

copy rem2_a_child from stdin;
1	1	foo
2	2	bar
\.
--Testcase 696:
select * from rem2;

--Testcase 697:
drop trigger trig_row_before on rem2_a_child;
--Testcase 698:
drop trigger trig_row_after on rem2_a_child;
--Testcase 699:
drop trigger trig_stmt_before on rem2_a_child;
--Testcase 700:
drop trigger trig_stmt_after on rem2_a_child;

--Testcase 701:
delete from rem2_a_child;

--Testcase 702:
create trigger trig_row_before_insert before insert on rem2_a_child
	for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
copy rem2_a_child from stdin;
1	1	foo
2	2	bar
\.
--Testcase 703:
select * from rem2;

--Testcase 704:
drop trigger trig_row_before_insert on rem2_a_child;

--Testcase 705:
delete from rem2_a_child;

--Testcase 706:
create trigger trig_null before insert on rem2_a_child
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2_a_child from stdin;
1	1	foo
2	2	bar
\.
--Testcase 707:
select * from rem2;

--Testcase 708:
drop trigger trig_null on rem2_a_child;

--Testcase 709:
delete from rem2_a_child;

-- Cannot create trigger on remote table at runtime
-- -- Test remote triggers
-- create trigger trig_row_before_insert before insert on loc2
-- 	for each row execute procedure trig_row_before_insupdate();

-- -- The new values are concatenated with ' triggered !'
-- copy rem2 from stdin;
-- 1	1	foo
-- 2	2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_row_before_insert on loc2;

-- delete from rem2;

-- create trigger trig_null before insert on loc2
-- 	for each row execute procedure trig_null();

-- -- Nothing happens
-- copy rem2 from stdin;
-- 1	1	foo
-- 2	2	bar
-- \.
-- select * from rem2;

-- drop trigger trig_null on loc2;

-- delete from rem2;

-- Cannot create trigger on remote table at runtime
-- Test a combination of local and remote triggers
-- create trigger rem2_trig_row_before before insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- create trigger rem2_trig_row_after after insert on rem2
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- create trigger loc2_trig_row_before_insert before insert on loc2
-- 	for each row execute procedure trig_row_before_insupdate();

-- copy rem2 from stdin;
-- 1	1	foo
-- 2	2	bar
-- \.
-- select * from rem2;

-- drop trigger rem2_trig_row_before on rem2;
-- drop trigger rem2_trig_row_after on rem2;
-- drop trigger loc2_trig_row_before_insert on loc2;

-- delete from rem2;

-- test COPY FROM with foreign table created in the same transaction
begin;
--Testcase 799:
create foreign table rem3_a_child (id int, f1 int, f2 text)
	server mysql_svr options(dbname 'mysql_fdw_post', table_name 'loc3');
--Testcase 710:
create table rem3 (id int, f1 int, f2 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 807:
CREATE FOREIGN TABLE rem3_a PARTITION OF rem3 FOR VALUES IN ('/node1/') SERVER spdsrv;
copy rem3_a_child from stdin;
1	1	foo
2	2	bar
\.
commit;
--Testcase 711:
select * from rem3;
--Testcase 712:
drop foreign table rem3_a_child;

-- Test COPY FROM with the batch_size option enabled
alter server mysql_svr options (add batch_size '2');

-- Test basic functionality
copy rem2_a_child from stdin;
1	1	foo
2	2	bar
3	3	baz
\.
--Testcase 1034:
select * from rem2;

--Testcase 1035:
delete from rem2_a_child;

-- Cannot alter remote table at runtime.
-- Test check constraints
-- alter table loc2 add constraint loc2_f1positive check (f1 >= 0);
-- alter foreign table rem2 add constraint rem2_f1positive check (f1 >= 0);

-- check constraint is enforced on the remote side, not locally
-- copy rem2 from stdin;
-- 1	1	foo
-- 2	2	bar
-- 3	3	baz
-- \.
-- copy rem2 from stdin; -- ERROR
-- 4	-1	xyzzy
-- \.
-- select * from rem2;

-- alter foreign table rem2 drop constraint rem2_f1positive;
-- alter table loc2 drop constraint loc2_f1positive;

-- delete from rem2;

-- Cannot create trigger on remote table at runtime.
-- Test remote triggers
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

-- Cannot alter remote table at runtime.
-- Check with zero-column foreign table; batch insert will be disabled
-- alter table loc2 drop column f1;
-- alter table loc2 drop column f2;
-- alter table rem2 drop column f1;
-- alter table rem2 drop column f2;
-- copy rem2 from stdin;



-- \.
-- select * from rem2;

-- delete from rem2;

alter server mysql_svr options (drop batch_size);

-- ===================================================================
-- test for TRUNCATE
-- Mysql only support simple truncate, other options canot suport
-- ===================================================================
--CREATE TABLE tru_rtable0 (id int primary key);
-- CREATE FOREIGN TABLE tru_ftable (id int)
--        SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'tru_rtable', batch_size '10');
-- INSERT INTO tru_ftable (SELECT x FROM generate_series(1,10) x);

-- CREATE FOREIGN TABLE tru_ftable2 (id int)
--        SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'tru_rtable2', batch_size '10');

-- CREATE TABLE tru_ptable (id int) PARTITION BY HASH(id);
-- CREATE TABLE tru_ptable__p0 PARTITION OF tru_ptable
--                            FOR VALUES WITH (MODULUS 2, REMAINDER 0);
-- CREATE TABLE tru_rtable1 (id int primary key);
-- CREATE FOREIGN TABLE tru_ftable__p1 PARTITION OF tru_ptable
--                                    FOR VALUES WITH (MODULUS 2, REMAINDER 1)
--        SERVER mysql_svr OPTIONS (table_name 'tru_rtable1');
-- INSERT INTO tru_ptable (SELECT x FROM generate_series(11,20) x);

-- CREATE TABLE tru_pk_table(id int primary key);
-- CREATE TABLE tru_fk_table(fkey int references tru_pk_table(id));
-- INSERT INTO tru_pk_table (SELECT x FROM generate_series(1,10) x);
-- INSERT INTO tru_fk_table (SELECT x % 10 + 1 FROM generate_series(5,25) x);
-- CREATE FOREIGN TABLE tru_pk_ftable (id int)
--        SERVER mysql_svr OPTIONS (table_name 'tru_pk_table');

-- CREATE TABLE tru_rtable_parent (id int);
-- CREATE TABLE tru_rtable_child (id int);
-- CREATE FOREIGN TABLE tru_ftable_parent (id int)
--        SERVER mysql_svr OPTIONS (table_name 'tru_rtable_parent');
-- CREATE FOREIGN TABLE tru_ftable_child () INHERITS (tru_ftable_parent)
--        SERVER mysql_svr OPTIONS (table_name 'tru_rtable_child');
-- INSERT INTO tru_rtable_parent (SELECT x FROM generate_series(1,8) x);
-- INSERT INTO tru_rtable_child  (SELECT x FROM generate_series(10, 18) x);

-- normal truncate
-- SELECT sum(id) FROM tru_ftable;        -- 55
-- TRUNCATE tru_ftable;
-- -- SELECT count(*) FROM tru_rtable0;		-- 0
-- SELECT count(*) FROM tru_ftable;		-- 0

-- -- 'truncatable' option
-- ALTER SERVER mysql_svr OPTIONS (ADD truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (ADD truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted:
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER SERVER mysql_svr OPTIONS (DROP truncatable);
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted

-- partitioned table with both local and foreign tables as partitions
-- SELECT sum(id) FROM tru_ptable;        -- 155
-- TRUNCATE tru_ptable;
-- SELECT count(*) FROM tru_ptable;		-- 0
-- SELECT count(*) FROM tru_ptable__p0;	-- 0
-- SELECT count(*) FROM tru_ftable__p1;	-- 0
-- SELECT count(*) FROM tru_rtable1;		-- 0

-- 'CASCADE' option
--SELECT sum(id) FROM tru_pk_ftable;      -- 55
--TRUNCATE tru_pk_ftable;	-- failed by FK reference
--TRUNCATE tru_pk_ftable CASCADE;
--SELECT count(*) FROM tru_pk_ftable;    -- 0
--SELECT count(*) FROM tru_fk_table;		-- also truncated,0

-- truncate two tables at a command
-- INSERT INTO tru_ftable (SELECT x FROM generate_series(1,8) x);
-- INSERT INTO tru_ftable2 (SELECT x FROM generate_series(3,10) x);
-- SELECT count(*) from tru_ftable; -- 8
-- SELECT count(*) from tru_ftable2; -- 8
-- TRUNCATE tru_ftable, tru_ftable2; --CASCADE;
-- SELECT count(*) from tru_ftable; -- 0
-- SELECT count(*) from tru_ftable2; -- 0

-- truncate with ONLY clause
-- Since ONLY is specified, the table tru_ftable_child that inherits
-- tru_ftable_parent locally is not truncated.
-- TRUNCATE ONLY tru_ftable_parent;
-- SELECT sum(id) FROM tru_ftable_parent;  -- 126
-- TRUNCATE tru_ftable_parent;
-- SELECT count(*) FROM tru_ftable_parent; -- 0

-- in case when remote table has inherited children
-- CREATE TABLE tru_rtable0_child () INHERITS (tru_rtable0);
-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(5,9) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(10,14) x);
-- SELECT sum(id) FROM tru_ftable;   -- 95

-- Both parent and child tables in the foreign server are truncated
-- even though ONLY is specified because ONLY has no effect
-- when truncating a foreign table.
-- TRUNCATE ONLY tru_ftable;
-- SELECT count(*) FROM tru_ftable;   -- 0

-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(21,25) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(26,30) x);
-- SELECT sum(id) FROM tru_ftable;		-- 255
-- TRUNCATE tru_ftable;			-- truncate both of parent and child
-- SELECT count(*) FROM tru_ftable;    -- 0

-- cleanup
-- DROP FOREIGN TABLE tru_ftable;

-- ===================================================================
-- test IMPORT FOREIGN SCHEMA
-- ===================================================================

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
--CREATE TABLE import_source.t4_part2 PARTITION OF import_source.t4
--  FOR VALUES FROM (100) TO (200);

-- CREATE SCHEMA import_dest1;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER mysql_svr INTO import_dest1;
-- \det+ import_dest1.*
-- \d import_dest1.*

-- -- Options
-- CREATE SCHEMA import_dest2;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER mysql_svr INTO import_dest2
--   OPTIONS (import_default 'true');
-- \det+ import_dest2.*
-- \d import_dest2.*
-- CREATE SCHEMA import_dest3;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER mysql_svr INTO import_dest3
--   OPTIONS (import_collate 'false', import_generated 'false', import_not_null 'false');
-- \det+ import_dest3.*
-- \d import_dest3.*

-- -- Check LIMIT TO and EXCEPT
-- CREATE SCHEMA import_dest4;
-- IMPORT FOREIGN SCHEMA import_source LIMIT TO (t1, nonesuch, t4_part)
--   FROM SERVER mysql_svr INTO import_dest4;
-- \det+ import_dest4.*
-- IMPORT FOREIGN SCHEMA import_source EXCEPT (t1, "x 4", nonesuch, t4_part)
--   FROM SERVER mysql_svr INTO import_dest4;
-- \det+ import_dest4.*

-- -- Assorted error cases
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER mysql_svr INTO import_dest4;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER mysql_svr INTO import_dest4;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER mysql_svr INTO notthere;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER nowhere INTO notthere;

-- -- Check case of a type present only on the remote server.
-- -- We can fake this by dropping the type locally in our transaction.
-- CREATE TYPE "Colors" AS ENUM ('red', 'green', 'blue');
-- CREATE TABLE import_source.t5 (c1 int, c2 text collate "C", "Col" "Colors");

-- CREATE SCHEMA import_dest5;
-- BEGIN;
-- DROP TYPE "Colors" CASCADE;
-- IMPORT FOREIGN SCHEMA import_source LIMIT TO (t5)
--   FROM SERVER mysql_svr INTO import_dest5;  -- ERROR

-- ROLLBACK;

-- mysql_fdw does not support fetch_size option.
-- BEGIN;


-- CREATE SERVER fetch101 FOREIGN DATA WRAPPER mysql_fdw OPTIONS( fetch_size '101' );

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

-- mysql_fdw does not support PARTITION in local table.
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
-- 	SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'fprt1_p1', use_remote_estimate 'true');
-- CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (250) TO (500)
-- 	SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', TABLE_NAME 'fprt1_p2');
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
-- 	SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'fprt2_p1', use_remote_estimate 'true');
-- ALTER TABLE fprt2 ATTACH PARTITION ftprt2_p1 FOR VALUES FROM (0) TO (250);
-- CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (250) TO (500)
-- 	SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'fprt2_p2', use_remote_estimate 'true');
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
-- CREATE FOREIGN TABLE fpagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0) TO (10) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'pagg_tab_p1');
-- CREATE FOREIGN TABLE fpagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10) TO (20) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'pagg_tab_p2');
-- CREATE FOREIGN TABLE fpagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20) TO (30) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'pagg_tab_p3');

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

-- -- ===================================================================
-- -- access rights and superuser
-- -- ===================================================================

-- -- Non-superuser cannot create a FDW without a password in the connstr
-- CREATE ROLE regress_nosuper NOSUPERUSER;

-- GRANT USAGE ON FOREIGN DATA WRAPPER mysql_fdw TO regress_nosuper;

-- SET ROLE regress_nosuper;

-- SHOW is_superuser;

-- -- This will be OK, we can create the FDW
-- DO $d$
--     BEGIN
--         EXECUTE $$CREATE SERVER mysql_svr_nopw FOREIGN DATA WRAPPER mysql_fdw
--             OPTIONS (dbname '$$||current_database()||$$',
--                      port '$$||current_setting('port')||$$'
--             )$$;
--     END;
-- $d$;

-- -- But creation of user mappings for non-superusers should fail
-- CREATE USER MAPPING FOR public SERVER mysql_svr_nopw;
-- CREATE USER MAPPING FOR CURRENT_USER SERVER mysql_svr_nopw;

-- CREATE FOREIGN TABLE pg_temp.ft1_nopw (
-- 	c1 int NOT NULL,
-- 	c2 int NOT NULL,
-- 	c3 text,
-- 	c4 timestamptz,
-- 	c5 timestamp,
-- 	c6 varchar(10),
-- 	c7 char(10) default 'ft1',
-- 	c8 user_enum
-- ) SERVER mysql_svr_nopw OPTIONS (table_name 'ft1');

-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- If we add a password to the connstr it'll fail, because we don't allow passwords
-- -- in connstrs only in user mappings.

-- ALTER SERVER loopback_nopw OPTIONS (ADD password 'dummypw');

-- -- If we add a password for our user mapping instead, we should get a different
-- -- error because the password wasn't actually *used* when we run with trust auth.
-- --
-- -- This won't work with installcheck, but neither will most of the FDW checks.

-- ALTER USER MAPPING FOR CURRENT_USER SERVER mysql_svr_nopw OPTIONS (ADD password 'dummypw');

-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- Unpriv user cannot make the mapping passwordless
-- ALTER USER MAPPING FOR CURRENT_USER SERVER mysql_svr_nopw OPTIONS (ADD password_required 'false');


-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- RESET ROLE;

-- -- But the superuser can
-- ALTER USER MAPPING FOR regress_nosuper SERVER mysql_svr_nopw OPTIONS (ADD password_required 'false');

-- SET ROLE regress_nosuper;

-- -- Should finally work now
-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- unpriv user also cannot set sslcert / sslkey on the user mapping
-- -- first set password_required so we see the right error messages
-- ALTER USER MAPPING FOR CURRENT_USER SERVER mysql_svr_nopw OPTIONS (SET password_required 'true');
-- ALTER USER MAPPING FOR CURRENT_USER SERVER mysql_svr_nopw OPTIONS (ADD sslcert 'foo.crt');
-- ALTER USER MAPPING FOR CURRENT_USER SERVER mysql_svr_nopw OPTIONS (ADD sslkey 'foo.key');

-- -- We're done with the role named after a specific user and need to check the
-- -- changes to the public mapping.
-- DROP USER MAPPING FOR CURRENT_USER SERVER mysql_svr_nopw;

-- -- This will fail again as it'll resolve the user mapping for public, which
-- -- lacks password_required=false
-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- RESET ROLE;

-- -- The user mapping for public is passwordless and lacks the password_required=false
-- -- mapping option, but will work because the current user is a superuser.
-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- cleanup
-- DROP USER MAPPING FOR public SERVER mysql_svr_nopw;
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

-- ===================================================================
-- test connection invalidation cases
-- ===================================================================
-- This test case is for closing the connection in mysql_xact_callback
BEGIN;
-- Connection xact depth becomes 1 i.e. the connection is in midst of the xact.
--Testcase 800:
SELECT 1 FROM ft1 LIMIT 1;
-- Connection is not closed at the end of the alter statement in
-- mysql_xact_callback. That's because the connection is in midst of this
-- xact, it is just marked as invalid.
--Testcase 801:
ALTER SERVER mysql_svr OPTIONS (ADD use_remote_estimate 'off');
-- The invalid connection gets closed in mysql_xact_callback during commit.
COMMIT;

-- ===================================================================
-- reestablish new connection
-- ===================================================================

--Testcase 788:
SELECT * FROM ft1 LIMIT 10;
\! ./sql/init_data/mysql_fdw/mysql_restart_service.sh
--Testcase 789:
SELECT * FROM ft1 LIMIT 10;

-- Change application_name of remote connection to special one
-- so that we can easily terminate the connection later.
-- ALTER SERVER mysql_svr OPTIONS (application_name 'fdw_retry_check');

-- Make sure we have a remote connection.
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
-- =============================================================================
-- test connection invalidation cases and mysql_fdw_get_connections function
-- =============================================================================
-- Let's ensure to close all the existing cached connections.
--Testcase 790:
SELECT 1 FROM mysql_fdw_disconnect_all();
-- No cached connections, so no records should be output.
--Testcase 791:
SELECT server_name FROM mysql_fdw_get_connections() ORDER BY 1;
-- This test case is for closing the connection in pgfdw_xact_callback
BEGIN;
-- Connection xact depth becomes 1 i.e. the connection is in midst of the xact.
--Testcase 792:
SELECT 1 FROM ft1 LIMIT 1;
--Testcase 793:
SELECT 1 FROM ft7 LIMIT 1;
-- List all the existing cached connections. mysql_svr and mysql_svr3 should be
-- output.
--Testcase 794:
SELECT server_name FROM mysql_fdw_get_connections() ORDER BY 1;
-- Connections are not closed at the end of the alter and drop statements.
-- That's because the connections are in midst of this xact,
-- they are just marked as invalid in pgfdw_inval_callback.
--Testcase 795:
ALTER SERVER mysql_svr OPTIONS (SET use_remote_estimate 'off');
--Testcase 796:
DROP SERVER mysql_svr3 CASCADE;
-- List all the existing cached connections. mysql_svr and mysql_svr3
-- should be output as invalid connections. Also the server name for
-- mysql_svr3 should be NULL because the server was dropped.
--Testcase 797:
SELECT * FROM mysql_fdw_get_connections() ORDER BY 1;
-- The invalid connection gets closed in mysql_xact_callback during commit.
COMMIT;
-- All cached connections were closed while committing above xact, so no
-- records should be output.
--Testcase 798:
SELECT server_name FROM mysql_fdw_get_connections() ORDER BY 1;

-- =======================================================================
-- test mysql_fdw_disconnect and mysql_fdw_disconnect_all functions
-- =======================================================================
BEGIN;
-- Ensure to cache mysql_svr connection.
--Testcase 799:
SELECT 1 FROM ft1 LIMIT 1;
-- Ensure to cache mysql_svr2 connection.
--Testcase 800:
SELECT 1 FROM ft6 LIMIT 1;
-- List all the existing cached connections. mysql_svr and mysql_svr2 should be
-- output.
--Testcase 801:
SELECT server_name FROM mysql_fdw_get_connections() ORDER BY 1;
-- Issue a warning and return false as mysql_svr connection is still in use and
-- can not be closed.
--Testcase 802:
SELECT mysql_fdw_disconnect('mysql_svr');
-- List all the existing cached connections. mysql_svr and mysql_svr2 should be
-- output.
--Testcase 803:
SELECT server_name FROM mysql_fdw_get_connections() ORDER BY 1;
-- Return false as connections are still in use, warnings are issued.
-- But disable warnings temporarily because the order of them is not stable.
--Testcase 804:
SET client_min_messages = 'ERROR';
--Testcase 805:
SELECT mysql_fdw_disconnect_all();
--Testcase 806:
RESET client_min_messages;
COMMIT;
-- Ensure that mysql_svr2 connection is closed.
--Testcase 807:
SELECT 1 FROM mysql_fdw_disconnect('mysql_svr2');
--Testcase 808:
SELECT server_name FROM mysql_fdw_get_connections() WHERE server_name = 'mysql_svr2';
-- Return false as mysql_svr2 connection is closed already.
--Testcase 809:
SELECT mysql_fdw_disconnect('mysql_svr2');
-- Return an error as there is no foreign server with given name.
--Testcase 810:
SELECT mysql_fdw_disconnect('unknownserver');
-- Let's ensure to close all the existing cached connections.
--Testcase 811:
SELECT 1 FROM mysql_fdw_disconnect_all();
-- No cached connections, so no records should be output.
--Testcase 812:
SELECT server_name FROM mysql_fdw_get_connections() ORDER BY 1;

-- =============================================================================
-- test case for having multiple cached connections for a foreign server
-- =============================================================================
--Testcase 813:
CREATE ROLE regress_multi_conn_user1 SUPERUSER;
--Testcase 814:
CREATE ROLE regress_multi_conn_user2 SUPERUSER;
--Testcase 815:
CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER mysql_svr
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);
--Testcase 893:
CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER spdsrv;
--Testcase 816:
CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER mysql_svr
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);
--Testcase 894:
CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER spdsrv;

BEGIN;
-- Will cache mysql_svr connection with user mapping for regress_multi_conn_user1
--Testcase 817:
SET ROLE regress_multi_conn_user1;
--Testcase 818:
SELECT 1 FROM ft1 LIMIT 1;
--Testcase 819:
RESET ROLE;

-- Will cache mysql_svr connection with user mapping for regress_multi_conn_user2
--Testcase 820:
SET ROLE regress_multi_conn_user2;
--Testcase 821:
SELECT 1 FROM ft1 LIMIT 1;
--Testcase 822:
RESET ROLE;

-- Should output two connections for mysql_svr server
--Testcase 823:
SELECT server_name FROM mysql_fdw_get_connections() ORDER BY 1;
COMMIT;
-- Let's ensure to close all the existing cached connections.
--Testcase 824:
SELECT 1 FROM mysql_fdw_disconnect_all();
-- No cached connections, so no records should be output.
--Testcase 825:
SELECT server_name FROM mysql_fdw_get_connections() ORDER BY 1;

-- mysql_fdw does not support query cancel
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

-- Clean up
--Testcase 826:
DROP USER MAPPING FOR regress_multi_conn_user1 SERVER mysql_svr;
--Testcase 827:
DROP USER MAPPING FOR regress_multi_conn_user2 SERVER mysql_svr;
--Testcase 825:
DROP USER MAPPING FOR regress_multi_conn_user1 SERVER spdsrv;
--Testcase 826:
DROP USER MAPPING FOR regress_multi_conn_user2 SERVER spdsrv;
--Testcase 828:
DROP ROLE regress_multi_conn_user1;
--Testcase 829:
DROP ROLE regress_multi_conn_user2;

-- ===================================================================
-- Test foreign server level option keep_connections
-- ===================================================================
-- By default, the connections associated with foreign server are cached i.e.
-- keep_connections option is on. Set it to off.
--Testcase 830:
ALTER SERVER mysql_svr OPTIONS (keep_connections 'off');
-- connection to mysql_svr server is closed at the end of xact
-- as keep_connections was set to off.
--Testcase 831:
SELECT 1 FROM ft1 LIMIT 1;
-- No cached connections, so no records should be output.
--Testcase 832:
SELECT server_name FROM mysql_fdw_get_connections() ORDER BY 1;
--Testcase 833:
ALTER SERVER mysql_svr OPTIONS (SET keep_connections 'on');
-- ===================================================================
-- batch insert
-- ===================================================================

BEGIN;

--Testcase 834:
CREATE SERVER batch10 FOREIGN DATA WRAPPER mysql_fdw OPTIONS( batch_size '10' );

--Testcase 835:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=10'];

--Testcase 836:
ALTER SERVER batch10 OPTIONS( SET batch_size '20' );

--Testcase 837:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=10'];

--Testcase 838:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=20'];

--Testcase 839:
CREATE FOREIGN TABLE table30_a_child ( x int ) SERVER batch10 OPTIONS ( batch_size '30' );
--Testcase 897:
CREATE TABLE table30 ( x int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 898:
CREATE FOREIGN TABLE table30_a PARTITION OF table30 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 840:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30_a_child'::regclass
AND ftoptions @> array['batch_size=30'];

--Testcase 841:
ALTER FOREIGN TABLE table30_a_child OPTIONS ( SET batch_size '40');

--Testcase 842:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30_a_child'::regclass
AND ftoptions @> array['batch_size=30'];

--Testcase 843:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30_a_child'::regclass
AND ftoptions @> array['batch_size=40'];

ROLLBACK;

--Testcase 844:
CREATE FOREIGN TABLE ftable_a_child ( x int ) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'batch_table', batch_size '10' );
--Testcase 899:
CREATE TABLE ftable ( x int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 900:
CREATE FOREIGN TABLE ftable_a PARTITION OF ftable FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 845:
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable_a_child SELECT * FROM generate_series(1, 10) i;
--Testcase 846:
INSERT INTO ftable_a_child SELECT * FROM generate_series(1, 10) i;
--Testcase 847:
INSERT INTO ftable_a_child SELECT * FROM generate_series(11, 31) i;
--Testcase 848:
INSERT INTO ftable_a_child VALUES (32);
--Testcase 849:
INSERT INTO ftable_a_child VALUES (33), (34);
--Testcase 854:
SELECT COUNT(*) FROM ftable;
TRUNCATE ftable_a_child;
--Testcase 855:
DROP FOREIGN TABLE ftable_a_child;
--Testcase 904:
DROP TABLE ftable;

-- Disable batch insert
--Testcase 856:
CREATE FOREIGN TABLE ftable_a_child ( x int ) SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'batch_table', batch_size '1' );
--Testcase 905:
CREATE TABLE ftable ( x int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 906:
CREATE FOREIGN TABLE ftable_a PARTITION OF ftable FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 857:
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable_a_child VALUES (1), (2);
--Testcase 858:
INSERT INTO ftable_a_child VALUES (1), (2);
--Testcase 859:
SELECT COUNT(*) FROM ftable;

-- Disable batch inserting into foreign tables with BEFORE ROW INSERT triggers
-- even if the batch_size option is enabled.
ALTER FOREIGN TABLE ftable_a_child OPTIONS ( SET batch_size '10' );
--Testcase 885:
CREATE TRIGGER trig_row_before BEFORE INSERT ON ftable_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 886:
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable_a_child VALUES (3), (4);
--Testcase 887:
INSERT INTO ftable_a_child VALUES (3), (4);
--Testcase 888:
SELECT COUNT(*) FROM ftable;

-- Clean up
--Testcase 889:
DROP TRIGGER trig_row_before ON ftable_a_child;
--Testcase 860:
DROP FOREIGN TABLE ftable_a_child;
--Testcase 907:
DROP TABLE ftable;
-- --Testcase 861:
-- DROP TABLE batch_table;

-- Use partitioning
--CREATE TABLE batch_table ( x int ) PARTITION BY HASH (x);

--CREATE TABLE batch_table_p0 (LIKE batch_table);
--CREATE FOREIGN TABLE batch_table_p0f
--	PARTITION OF batch_table
--	FOR VALUES WITH (MODULUS 3, REMAINDER 0)
--	SERVER mysql_svr
--	OPTIONS (table_name 'batch_table_p0', batch_size '10');

--CREATE TABLE batch_table_p1 (LIKE batch_table);
--CREATE FOREIGN TABLE batch_table_p1f
--	PARTITION OF batch_table
--	FOR VALUES WITH (MODULUS 3, REMAINDER 1)
--	SERVER mysql_svr
--	OPTIONS (table_name 'batch_table_p1', batch_size '1');

--CREATE TABLE batch_table_p2
--	PARTITION OF batch_table
--	FOR VALUES WITH (MODULUS 3, REMAINDER 2);

--INSERT INTO batch_table SELECT * FROM generate_series(1, 66) i;
--SELECT COUNT(*) FROM batch_table;

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

-- Test that pending inserts are handled properly when needed
--Testcase 1074:
CREATE TABLE ftable (a text, b int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 1075:
CREATE FOREIGN TABLE ftable_a_child (a text, b int)
	SERVER mysql_svr
	OPTIONS (dbname 'mysql_fdw_post', table_name 'batch_table_2', batch_size '2');
--Testcase 1076:
CREATE FOREIGN TABLE ftable_a PARTITION OF ftable FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 1077:
CREATE TABLE ltable (a text, b int);
--Testcase 1078:
CREATE FUNCTION ftable_rowcount_trigf() RETURNS trigger LANGUAGE plpgsql AS
$$
begin
	raise notice '%: there are % rows in ftable',
		TG_NAME, (SELECT count(*) FROM ftable);
	if TG_OP = 'DELETE' then
		return OLD;
	else
		return NEW;
	end if;
end;
$$;
--Testcase 1079:
CREATE TRIGGER ftable_rowcount_trigger
BEFORE INSERT OR UPDATE OR DELETE ON ltable
FOR EACH ROW EXECUTE PROCEDURE ftable_rowcount_trigf();

--Testcase 1080:
WITH t AS (
	INSERT INTO ltable VALUES ('AAA', 42), ('BBB', 42) RETURNING *
)
INSERT INTO ftable_a_child SELECT * FROM t;

--Testcase 1081:
SELECT * FROM ltable;
--Testcase 1082:
SELECT * FROM ftable;
--Testcase 1083:
DELETE FROM ftable_a_child;

--Testcase 1084:
WITH t AS (
	UPDATE ltable SET b = b + 100 RETURNING *
)
INSERT INTO ftable_a_child SELECT * FROM t;

--Testcase 1085:
SELECT * FROM ltable;
--Testcase 1086:
SELECT * FROM ftable;
--Testcase 1087:
DELETE FROM ftable_a_child;

--Testcase 1088:
WITH t AS (
	DELETE FROM ltable RETURNING *
)
INSERT INTO ftable_a_child SELECT * FROM t;

--Testcase 1089:
SELECT * FROM ltable;
--Testcase 1090:
SELECT * FROM ftable;
--Testcase 1091:
DELETE FROM ftable_a_child;

-- Clean up
--Testcase 1092:
DELETE FROM ftable_a_child;
--Testcase 1093:
DROP FOREIGN TABLE ftable_a_child;
--Testcase 1094:
DROP TABLE ftable;
--Testcase 1095:
DROP TRIGGER ftable_rowcount_trigger ON ltable;
--Testcase 1096:
DROP TABLE ltable;

--Testcase 1097:
CREATE TABLE parent (a text, b int) PARTITION BY LIST (a);

--Testcase 1098:
CREATE FOREIGN TABLE ftable
	PARTITION OF parent
	FOR VALUES IN ('AAA')
	SERVER mysql_svr
	OPTIONS (dbname 'mysql_fdw_post', table_name 'batch_table_2', batch_size '2');
--Testcase 1099:
CREATE TABLE ltable
	PARTITION OF parent
	FOR VALUES IN ('BBB');
--Testcase 1100:
CREATE TRIGGER ftable_rowcount_trigger
BEFORE INSERT ON ltable
FOR EACH ROW EXECUTE PROCEDURE ftable_rowcount_trigf();

--Testcase 1101:
INSERT INTO parent VALUES ('AAA', 42), ('BBB', 42), ('AAA', 42), ('BBB', 42);

--Testcase 1102:
SELECT tableoid::regclass, * FROM parent;

-- Clean up
--Testcase 1103:
DROP FOREIGN TABLE ftable;
--Testcase 1104:
DROP TRIGGER ftable_rowcount_trigger ON ltable;
--Testcase 1105:
DROP TABLE ltable;
--Testcase 1106:
DROP TABLE parent;
--Testcase 1107:
DROP FUNCTION ftable_rowcount_trigf;

-- ===================================================================
-- test asynchronous execution
-- ===================================================================

-- ALTER SERVER mysql_svr OPTIONS (DROP extensions);
-- ALTER SERVER mysql_svr OPTIONS (ADD async_capable 'true');
-- ALTER SERVER mysql_svr2 OPTIONS (ADD async_capable 'true');

-- CREATE TABLE async_pt (a int, b int, c text) PARTITION BY RANGE (a);
-- CREATE TABLE base_tbl1 (a int, b int, c text);
-- CREATE TABLE base_tbl2 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p1 PARTITION OF async_pt FOR VALUES FROM (1000) TO (2000)
--   SERVER mysql_svr OPTIONS (table_name 'base_tbl1');
-- CREATE FOREIGN TABLE async_p2 PARTITION OF async_pt FOR VALUES FROM (2000) TO (3000)
--  SERVER mysql_svr2 OPTIONS (table_name 'base_tbl2');
-- INSERT INTO async_p1 SELECT 1000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- INSERT INTO async_p2 SELECT 2000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- simple queries
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

-- Check case where multiple partitions use the same connection
-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p3 PARTITION OF async_pt FOR VALUES FROM (3000) TO (4000)
--   SERVER mysql_svr2 OPTIONS (table_name 'base_tbl3');
-- INSERT INTO async_p3 SELECT 3000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- DROP FOREIGN TABLE async_p3;
-- DROP TABLE base_tbl3;

-- Check case where the partitioned table has local/remote partitions
-- CREATE TABLE async_p3 PARTITION OF async_pt FOR VALUES FROM (3000) TO (4000);
-- INSERT INTO async_p3 SELECT 3000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- partitionwise joins
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

-- Test interaction of async execution with plan-time partition pruning
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt WHERE a < 3000;

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt WHERE a < 2000;

-- Test interaction of async execution with run-time partition pruning
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

-- Test that pending requests are processed properly
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

-- Check with foreign modify

-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE remote_tbl (a int, b int, c text)
--   SERVER mysql_svr OPTIONS (table_name 'base_tbl3');
-- INSERT INTO remote_tbl VALUES (2505, 505, 'bar');

-- CREATE TABLE base_tbl4 (a int, b int, c text);
-- CREATE FOREIGN TABLE insert_tbl (a int, b int, c text)
--   SERVER mysql_svr OPTIONS (table_name 'base_tbl4');

-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO insert_tbl (SELECT * FROM local_tbl UNION ALL SELECT * FROM remote_tbl);
-- INSERT INTO insert_tbl (SELECT * FROM local_tbl UNION ALL SELECT * FROM remote_tbl);

-- SELECT * FROM insert_tbl ORDER BY a;

-- Check with direct modify
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

-- Test that UPDATE/DELETE with inherited target works with async_capable enabled
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE async_pt SET c = c || c WHERE b = 0 RETURNING *;
-- UPDATE async_pt SET c = c || c WHERE b = 0 RETURNING *;
-- EXPLAIN (VERBOSE, COSTS OFF)
-- DELETE FROM async_pt WHERE b = 0 RETURNING *;
-- DELETE FROM async_pt WHERE b = 0 RETURNING *;

-- Check EXPLAIN ANALYZE for a query that scans empty partitions asynchronously
-- DELETE FROM async_p1;
-- DELETE FROM async_p2;
-- DELETE FROM async_p3;

-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM async_pt;

-- Clean up
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

-- ALTER SERVER mysql_svr OPTIONS (DROP async_capable);
-- ALTER SERVER mysql_svr2 OPTIONS (DROP async_capable);

-- ===================================================================
-- test invalid server, foreign table and foreign data wrapper options
-- ===================================================================
-- Invalid fdw_startup_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER mysql_fdw
-- 	OPTIONS(fdw_startup_cost '100$%$#$#');
-- -- Invalid fdw_tuple_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER mysql_fdw
-- 	OPTIONS(fdw_tuple_cost '100$%$#$#');
-- -- Invalid fetch_size option
-- CREATE FOREIGN TABLE inv_fsz (c1 int )
-- 	SERVER mysql_svr OPTIONS (fetch_size '100$%$#$#');
-- -- Invalid batch_size option
-- CREATE FOREIGN TABLE inv_bsz (c1 int )
-- 	SERVER mysql_svr OPTIONS (batch_size '100$%$#$#');

-- -- No option is allowed to be specified at foreign data wrapper level
-- ALTER FOREIGN DATA WRAPPER mysql_fdw OPTIONS (nonexistent 'fdw');

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

-- ===================================================================
-- test parallel commit and parallel abort
-- ===================================================================
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

-- CREATE FOREIGN TABLE analyze_ftable (id int, a text, b bigint)
--        SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_post', table_name 'analyze_table');

-- INSERT INTO analyze_ftable (SELECT x FROM generate_series(1,60000) x);
-- ANALYZE analyze_ftable;

-- SET default_statistics_target = 10;
-- ANALYZE analyze_ftable;

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

-- -- cleanup
-- DROP FOREIGN TABLE analyze_ftable;

--Testcase 802:
SET client_min_messages TO warning;
--Testcase 387:
DROP USER MAPPING FOR PUBLIC SERVER mysql_svr;
--Testcase 714:
DROP USER MAPPING FOR PUBLIC SERVER mysql_svr2;
--Testcase 389:
DROP SERVER mysql_svr CASCADE;
--Testcase 716:
DROP SERVER mysql_svr2 CASCADE;
--Testcase 391:
DROP SERVER spdsrv CASCADE;
--Testcase 805:
DROP EXTENSION pgspider_ext CASCADE;

--Testcase 806:
DROP EXTENSION mysql_fdw CASCADE;
--Testcase 1108:
DROP SCHEMA "S 1" CASCADE;
--Testcase 1109:
DROP TYPE user_enum CASCADE;
--Testcase 1110:
DROP TABLE ft1;
--Testcase 1111:
DROP TABLE ft1_constraint;
--Testcase 1112:
DROP TABLE ft2;
--Testcase 1113:
DROP TABLE ft3;
--Testcase 1114:
DROP TABLE ft4;
--Testcase 1115:
DROP TABLE ft5;
--Testcase 1116:
DROP TABLE ft6;
--Testcase 1117:
DROP TABLE ft7;
--Testcase 1118:
DROP TABLE ft_empty;
--Testcase 1119:
DROP TABLE loc1;
--Testcase 1120:
DROP TABLE foreign_tbl;
--Testcase 1121:
DROP TABLE rem1;
--Testcase 1122:
DROP TABLE rem2;
--Testcase 1123:
DROP TABLE rem3;
--Testcase 1124:
DROP TABLE rem4;
--Testcase 1125:
DROP TABLE grem1;
--Testcase 1126:
DROP TABLE foo CASCADE;
--Testcase 1127:
DROP TABLE bar CASCADE;
--Testcase 1128:
DROP TABLE foo2;
--Testcase 1129:
DROP TABLE bar2;
--Testcase 1130:
DROP TABLE b;
--Testcase 1131:
DROP FUNCTION trigger_func CASCADE;
--Testcase 1132:
DROP FUNCTION trig_row_before_insupdate CASCADE;
--Testcase 1133:
DROP FUNCTION trig_null CASCADE;
--Testcase 1134:
DROP FUNCTION row_before_insupd_trigfunc CASCADE;
--Testcase 1135:
DROP FUNCTION trigger_data CASCADE;
