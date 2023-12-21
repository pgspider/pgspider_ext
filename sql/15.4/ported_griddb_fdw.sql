\set ECHO none
\ir sql/parameters/griddb_parameters.conf
\set ECHO all

--SET client_min_messages TO WARNING;

--Testcase 1075:
CREATE EXTENSION pgspider_ext;
--Testcase 1076:
CREATE SERVER spdsrv FOREIGN DATA WRAPPER pgspider_ext;
--Testcase 1077:
CREATE USER MAPPING FOR CURRENT_USER SERVER spdsrv;

--Testcase 1:
CREATE EXTENSION IF NOT EXISTS griddb_fdw;

--Testcase 2:
CREATE SERVER griddb_svr FOREIGN DATA WRAPPER griddb_fdw
    OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 3:
CREATE SERVER griddb_svr2 FOREIGN DATA WRAPPER griddb_fdw
    OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 4:
CREATE SERVER griddb_svr3 FOREIGN DATA WRAPPER griddb_fdw
    OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

--Testcase 5:
CREATE SERVER testserver1 FOREIGN DATA WRAPPER griddb_fdw;

--Testcase 6:
CREATE USER MAPPING FOR public SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 7:
CREATE USER MAPPING FOR public SERVER griddb_svr2 OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 8:
CREATE USER MAPPING FOR public SERVER griddb_svr3 OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);

--Testcase 9:
CREATE USER MAPPING FOR public SERVER testserver1 OPTIONS (username 'value', password 'value');

--Testcase 10:
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');

--Testcase 11:
CREATE SCHEMA "S 1";
IMPORT FOREIGN SCHEMA griddb_schema LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1, ft2, ft4, ft5, base_tbl,
	loc1, loc2, loct, loct1, loct2, loct3, loct4, locp1, locp2,
	fprt1_p1, fprt1_p2, fprt2_p1, fprt2_p2, pagg_tab_p1, pagg_tab_p2, pagg_tab_p3)
	FROM SERVER griddb_svr INTO "S 1";
--SET client_min_messages to NOTICE;

-- GridDB containers must be created for this test on GridDB server

--Testcase 12:
INSERT INTO "S 1"."T1"
	SELECT id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
	       '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
	       id % 10,
	       id % 10,
	       'foo'
	FROM generate_series(1, 1000) id;

--Testcase 13:
INSERT INTO "S 1"."T2"
	SELECT id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;

--Testcase 14:
INSERT INTO "S 1"."T3"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;

--Testcase 15:
DELETE FROM "S 1"."T3" WHERE c1 % 2 != 0;	-- delete for outer join tests

--Testcase 16:
INSERT INTO "S 1"."T4"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;

--Testcase 17:
DELETE FROM "S 1"."T4" WHERE c1 % 3 != 0;	-- delete for outer join tests

-- ===================================================================
-- create foreign tables
-- ===================================================================

--Testcase 18:
CREATE FOREIGN TABLE ft1_a_child(
	-- c0 int,
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text,
	c7 text default 'ft1',
	c8 text
) SERVER griddb_svr;

--Testcase 1078:
CREATE TABLE ft1 (
	-- c0 int,
	c1 int,
	c2 int NOT NULL,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text,
	c7 text default 'ft1',
	c8 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 1079:
CREATE FOREIGN TABLE ft1_a PARTITION OF ft1 FOR VALUES IN ('/node1/') SERVER spdsrv;
-- ALTER FOREIGN TABLE ft1 DROP COLUMN c0;

--Testcase 19:
CREATE FOREIGN TABLE ft2_a_child (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	-- cx int,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text,
	c7 text default 'ft2',
	c8 text
) SERVER griddb_svr;

--Testcase 1080:
CREATE TABLE ft2 (
	c1 int,
	c2 int NOT NULL,
	-- cx int,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text,
	c7 text default 'ft2',
	c8 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 1081:
CREATE FOREIGN TABLE ft2_a PARTITION OF ft2 FOR VALUES IN ('/node1/') SERVER spdsrv;
-- ALTER FOREIGN TABLE ft2 DROP COLUMN cx;

--Testcase 20:
CREATE FOREIGN TABLE ft4_a_child (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER griddb_svr OPTIONS (table_name 'T3');

--Testcase 1082:
CREATE TABLE ft4 (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 1083:
CREATE FOREIGN TABLE ft4_a PARTITION OF ft4 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 21:
CREATE FOREIGN TABLE ft5_a_child (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER griddb_svr OPTIONS (table_name 'T4');

--Testcase 1084:
CREATE TABLE ft5 (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 1085:
CREATE FOREIGN TABLE ft5_a PARTITION OF ft5 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 22:
CREATE FOREIGN TABLE ft6_a_child (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER griddb_svr2 OPTIONS (table_name 'T4');

--Testcase 1086:
CREATE TABLE ft6 (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 1087:
CREATE FOREIGN TABLE ft6_a PARTITION OF ft6 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 23:
CREATE FOREIGN TABLE ft7_a_child (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER griddb_svr3 OPTIONS (table_name 'T4');

--Testcase 1088:
CREATE TABLE ft7 (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 1089:
CREATE FOREIGN TABLE ft7_a PARTITION OF ft7 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- Enable to pushdown aggregate
SET enable_partitionwise_aggregate TO on;

-- Turn off leader node participation to avoid duplicate data error when executing
-- parallel query
SET parallel_leader_participation TO off;

-- ===================================================================
-- tests for validator
-- ===================================================================
-- requiressl, krbsrvname and gsslib are omitted because they depend on
-- configure options
-- HINT: valid options in this context are: host, port, clustername, database,
-- notification_member, updatable, fdw_startup_cost, fdw_tuple_cost

--Testcase 24:
ALTER SERVER testserver1 OPTIONS (
	--use_remote_estimate 'false',
	updatable 'true',
	fdw_startup_cost '123.456',
	fdw_tuple_cost '0.123',
	--service 'value',
	--connect_timeout 'value',
	--dbname 'value',
	host 'value',
	--hostaddr 'value',
	port 'value',
	clustername 'value'
	--client_encoding 'value',
	--application_name 'value',
	--fallback_application_name 'value',
	--keepalives 'value',
	--keepalives_idle 'value',
	--keepalives_interval 'value',
	--tcp_user_timeout 'value',
	-- requiressl 'value',
	--sslcompression 'value',
	--sslmode 'value',
	--sslcert 'value',
	--sslkey 'value',
	--sslrootcert 'value',
	--sslcrl 'value',
	--requirepeer 'value',
	--krbsrvname 'value',
	--gsslib 'value'
	--replication 'value'
);
-- GridDB does not support 'extensions' option
-- Error, invalid list syntax
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo; bar');

-- OK but gets a warning
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo, bar');
-- ALTER SERVER testserver1 OPTIONS (DROP extensions);

--Testcase 25:
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (DROP username, DROP password);

--Testcase 26:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (table_name 'T1');

--Testcase 27:
ALTER FOREIGN TABLE ft2_a_child OPTIONS (table_name 'T1');

--Testcase 28:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c1 OPTIONS (column_name 'C_1');

--Testcase 29:
ALTER FOREIGN TABLE ft2_a_child ALTER COLUMN c1 OPTIONS (column_name 'C_1');

--Testcase 30:
\det+

-- skip does not support dbname
-- Test that alteration of server options causes reconnection
-- Remote's errors might be non-English, so hide them to ensure stable results
/*
\set VERBOSITY terse

SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
ALTER SERVER griddb_svr OPTIONS (SET dbname 'no such database');

SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER griddb_svr
            OPTIONS (SET dbname '$$||current_database()||$$')$$;
    END;
$d$;

SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
*/

-- skip, does not support option 'user'
/*
-- Test that alteration of user mapping options causes reconnection
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_svr
  OPTIONS (ADD user 'no such user');

SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_svr
  OPTIONS (DROP user);

SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
\set VERBOSITY default

-- Now we should be able to run ANALYZE.
-- To exercise multiple code paths, we use local stats on ft1
-- and remote-estimate mode on ft2.
ANALYZE ft1;
ALTER FOREIGN TABLE ft2 OPTIONS (use_remote_estimate 'true');
*/

-- ===================================================================
-- test error case for create publication on foreign table
-- ===================================================================
--Testcase 1126:
CREATE PUBLICATION testpub_ftbl FOR TABLE ft1_a_child;  -- should fail

-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias

--Testcase 31:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;

--Testcase 32:
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
-- single table with alias - also test that tableoid sort is not pushed to remote side

--Testcase 33:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;

--Testcase 34:
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference

--Testcase 35:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

--Testcase 36:
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result

--Testcase 37:
SELECT * FROM ft1 WHERE false;
-- with WHERE clause

--Testcase 38:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6::char = '1' AND t1.c7::char >= '1';

--Testcase 39:
SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6::char = '1' AND t1.c7::char >= '1';
-- with FOR UPDATE/SHARE

--Testcase 40:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;

--Testcase 41:
SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;

--Testcase 42:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;

--Testcase 43:
SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
-- aggregate

--Testcase 44:
SELECT COUNT(*) FROM ft1 t1;
-- subquery

--Testcase 45:
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX

--Testcase 46:
SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;
-- used in CTE

--Testcase 47:
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values

--Testcase 48:
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.

--Testcase 49:
SET enable_hashjoin TO false;

--Testcase 50:
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list

--Testcase 51:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C_1" FROM ft2 t1 JOIN "S 1"."T1" t2 ON (t1.c1 = t2."C_1") OFFSET 100 LIMIT 10;

--Testcase 52:
SELECT t1.c1, t2."C_1" FROM ft2 t1 JOIN "S 1"."T1" t2 ON (t1.c1 = t2."C_1") OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query

--Testcase 53:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C_1" FROM ft2 t1 LEFT JOIN "S 1"."T1" t2 ON (t1.c1 = t2."C_1") OFFSET 100 LIMIT 10;

--Testcase 54:
SELECT t1.c1, t2."C_1" FROM ft2 t1 LEFT JOIN "S 1"."T1" t2 ON (t1.c1 = t2."C_1") OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.

--Testcase 55:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C_1" FROM "S 1"."T1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;

--Testcase 56:
SELECT t1."C_1" FROM "S 1"."T1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.

--Testcase 57:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C_1", t2.c1, t3.c1 FROM "S 1"."T1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;

--Testcase 58:
SELECT t1."C_1", t2.c1, t3.c1 FROM "S 1"."T1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins

--Testcase 59:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C_1", t2.c1, t3.c1 FROM "S 1"."T1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;

--Testcase 60:
SELECT t1."C_1", t2.c1, t3.c1 FROM "S 1"."T1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C_1") OFFSET 100 LIMIT 10;

--Testcase 61:
RESET enable_hashjoin;

--Testcase 62:
RESET enable_nestloop;

-- Test executing assertion in estimate_path_cost_size() that makes sure that
-- retrieved_rows for foreign rel re-used to cost pre-sorted foreign paths is
-- a sensible value even when the rel has tuples=0
--Testcase 1071:
CREATE FOREIGN TABLE ft_empty_a_child (id int OPTIONS (rowkey 'true'), c1 int NOT NULL, c2 text)
  SERVER griddb_svr OPTIONS (table_name 'loct_empty');

--Testcase 1090:
CREATE TABLE ft_empty (id int, c1 int NOT NULL, c2 text, spdurl text)
   PARTITION BY LIST (spdurl);

--Testcase 1091:
CREATE FOREIGN TABLE ft_empty_a PARTITION OF ft_empty FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 1072:
INSERT INTO ft_empty_a_child
  SELECT id, id, 'AAA' || to_char(id, 'FM000') FROM generate_series(1, 100) id;

--Testcase 1073:
DELETE FROM ft_empty_a_child;

-- ANALYZE ft_empty;
--Testcase 1074:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_empty ORDER BY c1;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================

--Testcase 63:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const

--Testcase 64:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr

--Testcase 65:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NULL;        -- NullTest

--Testcase 66:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NOT NULL;    -- NullTest

--Testcase 67:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr

--Testcase 68:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr

--Testcase 69:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr

--Testcase 70:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef

--Testcase 71:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars

--Testcase 72:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote
-- parameterized remote path for foreign table

--Testcase 73:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM "S 1"."T1" a, ft2 b WHERE a."C_1" = 47 AND b.c1 = a.c2;

--Testcase 74:
SELECT * FROM ft2 a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;

-- check both safe and unsafe join conditions

--Testcase 75:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);

--Testcase 76:
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters

--Testcase 77:
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5));

--Testcase 78:
SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5));
-- we should not push order by clause with volatile expressions or unsafe
-- collations

--Testcase 79:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, random();

--Testcase 80:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- user-defined operator/function

--Testcase 81:
CREATE FUNCTION griddb_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;

--Testcase 82:
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- built-in operators and functions can be shipped for remote execution

--Testcase 83:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);

--Testcase 84:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);

--Testcase 85:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

--Testcase 86:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot

--Testcase 87:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);

--Testcase 88:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);

--Testcase 89:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

--Testcase 90:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ORDER BY can be shipped, though

--Testcase 91:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

--Testcase 92:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- but let's put them in an extension ...

--Testcase 93:
ALTER EXTENSION griddb_fdw ADD FUNCTION griddb_fdw_abs(int);

--Testcase 94:
ALTER EXTENSION griddb_fdw ADD OPERATOR === (int, int);

-- ... now they can be shipped

--Testcase 95:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);

--Testcase 96:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = griddb_fdw_abs(t1.c2);

--Testcase 97:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

--Testcase 98:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- and both ORDER BY and LIMIT can be shipped

--Testcase 99:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

--Testcase 100:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- GridDB does not support CASE expr, so CASE cannot be pushed down
--Testcase 1127:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;
--Testcase 1128:
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;

-- Nested CASE
--Testcase 1129:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;
--Testcase 1130:
SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;

-- CASE arg WHEN
--Testcase 1131:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE mod(c1, 4) WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- CASE cannot be pushed down because of unshippable arg clause
--Testcase 1132:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE random()::integer WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- these are shippable
--Testcase 1133:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c6 WHEN 'foo' THEN true ELSE c3 < 'bar' END;
--Testcase 1134:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 WHEN c6 THEN true ELSE c3 < 'bar' END;

-- but this is not because of collation
--Testcase 1135:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 COLLATE "C" WHEN c6 THEN true ELSE c3 < 'bar' END;

-- check schema-qualification of regconfig constant
CREATE TEXT SEARCH CONFIGURATION public.custom_search
  (COPY = pg_catalog.english);
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
-- ===================================================================
-- JOIN queries
-- ===================================================================
-- Analyze ft4 and ft5 so that we have better statistics. These tables do not
-- have use_remote_estimate set.
--ANALYZE ft4;
--ANALYZE ft5;

-- join two tables

--Testcase 101:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

--Testcase 102:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join three tables

--Testcase 103:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;

--Testcase 104:
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join

--Testcase 105:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 106:
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables

--Testcase 107:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

--Testcase 108:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side

--Testcase 109:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;

--Testcase 110:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side

--Testcase 111:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;

--Testcase 112:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
-- right outer join

--Testcase 113:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;

--Testcase 114:
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
-- right outer join three tables

--Testcase 115:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

--Testcase 116:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join

--Testcase 117:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;

--Testcase 118:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations

--Testcase 119:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;

--Testcase 120:
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;

--Testcase 121:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;

--Testcase 122:
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
-- b. one of the joining relations is a base relation and the other is a join
-- relation

--Testcase 123:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;

--Testcase 124:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries

--Testcase 125:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;

--Testcase 126:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries

--Testcase 127:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;

--Testcase 128:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join

--Testcase 129:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;

--Testcase 130:
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
-- full outer join three tables

--Testcase 131:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

--Testcase 132:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + right outer join

--Testcase 133:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

--Testcase 134:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + full outer join

--Testcase 135:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;

--Testcase 136:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- full outer join + left outer join

--Testcase 137:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

--Testcase 138:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + full outer join

--Testcase 139:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;

--Testcase 140:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- right outer join + left outer join

--Testcase 141:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

--Testcase 142:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + right outer join

--Testcase 143:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

--Testcase 144:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows

--Testcase 145:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 146:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set

--Testcase 147:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE griddb_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--ALTER SERVER griddb_svr OPTIONS (DROP extensions);
-- full outer join + WHERE clause with shippable extensions not set

--Testcase 148:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE griddb_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--ALTER SERVER griddb_svr OPTIONS (ADD extensions 'griddb_fdw');
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks

--Testcase 149:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;

--Testcase 150:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;

--Testcase 151:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- Skip test case: Relate #112
--SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause

--Testcase 152:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;

--Testcase 153:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;

--Testcase 154:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- Skip test case: Relate #112
--SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE

--Testcase 155:
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;

--Testcase 156:
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference

--Testcase 157:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- SEMI JOIN, not pushed down

--Testcase 158:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;

--Testcase 159:
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down

--Testcase 160:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;

--Testcase 161:
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- CROSS JOIN, not pushed down

--Testcase 162:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;

--Testcase 163:
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.

--Testcase 164:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;

--Testcase 165:
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.

--Testcase 166:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;

--Testcase 167:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.

--Testcase 168:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

--Testcase 169:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.

--Testcase 170:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

--Testcase 171:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs

--Testcase 172:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;

--Testcase 173:
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference

--Testcase 174:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1."C_1" FROM "S 1"."T1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C_1" OFFSET 10 LIMIT 10;

--Testcase 175:
SELECT t1."C_1" FROM "S 1"."T1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C_1" OFFSET 10 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}

--Testcase 176:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

--Testcase 177:
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}

--Testcase 178:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;

--Testcase 179:
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;

-- join with nullable side with some columns with null values

--Testcase 180:
UPDATE ft5_a_child SET c3 = null where c1 % 9 = 0;

--Testcase 181:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

--Testcase 182:
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)

--Testcase 183:
CREATE FOREIGN TABLE local_tbl_a_child (c1 int OPTIONS(rowkey 'true'), c2 int, c3 text) SERVER griddb_svr OPTIONS (table_name 'local_tbl');

--Testcase 1092:
CREATE TABLE local_tbl (c1 int, c2 int, c3 text, spdurl text) PARTITION BY LIST (spdurl);

--Testcase 1093:
CREATE FOREIGN TABLE local_tbl_a PARTITION OF local_tbl FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 184:
INSERT INTO local_tbl_a_child(c1, c2, c3) SELECT id, id % 10, to_char(id, 'FM0000') FROM generate_series(1, 1000) id;
--ANALYZE local_tbl;

--Testcase 185:
SET enable_nestloop TO false;

--Testcase 186:
SET enable_hashjoin TO false;

--Testcase 187:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;
-- Skip test case: Relate #112
--SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
--  AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;

--Testcase 188:
RESET enable_nestloop;

--Testcase 189:
RESET enable_hashjoin;

-- test that add_paths_with_pathkeys_for_rel() arranges for the epq_path to
-- return columns needed by the parent ForeignScan node
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.*, COALESCE(ft1.c3 || ft2.c3, 'foobar') FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100)) ss ON (local_tbl.c1 = ss.c1) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;

-- ALTER SERVER loopback OPTIONS (DROP extensions);
-- ALTER SERVER loopback OPTIONS (ADD fdw_startup_cost '10000.0');
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.* FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100 AND ft1.c1 = postgres_fdw_abs(ft2.c2))) ss ON (local_tbl.c3 = ss.c3) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;
-- ALTER SERVER loopback OPTIONS (DROP fdw_startup_cost);
-- ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

--Testcase 190:
DROP FOREIGN TABLE local_tbl_a_child;

--Testcase 1094:
DROP TABLE local_tbl;

-- check join pushdown in situations where multiple userids are involved

--Testcase 191:
CREATE ROLE regress_view_owner SUPERUSER;

--Testcase 192:
CREATE USER MAPPING FOR regress_view_owner SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;

--Testcase 193:
CREATE VIEW v4 AS SELECT * FROM ft4;

--Testcase 194:
CREATE VIEW v5 AS SELECT * FROM ft5;

--Testcase 195:
ALTER VIEW v5 OWNER TO regress_view_owner;

--Testcase 196:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners

--Testcase 197:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 198:
ALTER VIEW v4 OWNER TO regress_view_owner;

--Testcase 199:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down

--Testcase 200:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 201:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user

--Testcase 202:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 203:
ALTER VIEW v4 OWNER TO CURRENT_USER;

--Testcase 204:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down

--Testcase 205:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 206:
ALTER VIEW v4 OWNER TO regress_view_owner;

-- cleanup

--Testcase 207:
DROP OWNED BY regress_view_owner;

--Testcase 208:
DROP ROLE regress_view_owner;

-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================

-- Simple aggregates

--Testcase 209:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

--Testcase 210:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

--Testcase 211:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

--Testcase 212:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()

--Testcase 213:
explain (verbose, costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query

--Testcase 214:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

--Testcase 215:
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel

--Testcase 216:
explain (verbose, costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions

--Testcase 217:
explain (verbose, costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

--Testcase 218:
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are pushed down.

--Testcase 219:
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;

--Testcase 220:
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out

--Testcase 221:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

--Testcase 222:
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed

--Testcase 223:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression

--Testcase 224:
explain (verbose, costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

--Testcase 225:
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function

--Testcase 226:
explain (verbose, costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

--Testcase 227:
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability

--Testcase 228:
explain (verbose, costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

--Testcase 229:
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down

--Testcase 230:
explain (verbose, costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

--Testcase 231:
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down

--Testcase 232:
explain (verbose, costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- GridDB does not create type user_enum so pg_enum table has no record.
-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781

--Testcase 233:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1;

--Testcase 234:
select exists(select 1 from pg_enum), sum(c1) from ft1;

--Testcase 235:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;

--Testcase 236:
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;

-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order

--Testcase 237:
explain (verbose, costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

--Testcase 238:
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC

--Testcase 239:
explain (verbose, costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

--Testcase 240:
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate

--Testcase 241:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 242:
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate

--Testcase 243:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 244:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 245:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 246:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- FILTER within aggregate

--Testcase 247:
explain (verbose, costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

--Testcase 248:
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate

--Testcase 249:
explain (verbose, costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

--Testcase 250:
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query

--Testcase 251:
explain (verbose, costs off)
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

--Testcase 252:
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
-- Inner query is aggregation query

--Testcase 253:
explain (verbose, costs off)
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

--Testcase 254:
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable

--Testcase 255:
explain (verbose, costs off)
select sum(c1) filter (where (c1 / c1) * random() <= 1) from ft1 group by c2 order by 1;

--Testcase 256:
explain (verbose, costs off)
select sum(c2) filter (where c2 in (select c2 from ft1 where c2 < 5)) from ft1;

-- Ordered-sets within aggregate

--Testcase 257:
explain (verbose, costs off)
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

--Testcase 258:
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

-- Using multiple arguments within aggregates

--Testcase 259:
explain (verbose, costs off)
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

--Testcase 260:
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC

--Testcase 261:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';

--Testcase 262:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.

--Testcase 263:
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate

--Testcase 264:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension

--Testcase 265:
alter extension griddb_fdw add function least_accum(anyelement, variadic anyarray);

--Testcase 266:
alter extension griddb_fdw add aggregate least_agg(variadic items anyarray);
--alter server griddb_svr options (set extensions 'griddb_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.

--Testcase 267:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

--Testcase 268:
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension

--Testcase 269:
alter extension griddb_fdw drop function least_accum(anyelement, variadic anyarray);

--Testcase 270:
alter extension griddb_fdw drop aggregate least_agg(variadic items anyarray);
--alter server griddb_svr options (set extensions 'griddb_fdw');

-- Not pushed down as we have dropped objects from extension.

--Testcase 271:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup

--Testcase 272:
reset enable_hashagg;

--Testcase 273:
drop aggregate least_agg(variadic items anyarray);

--Testcase 274:
drop function least_accum(anyelement, variadic anyarray);

-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.

--Testcase 275:
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

--Testcase 276:
create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

--Testcase 277:
create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

--Testcase 278:
create operator family my_op_family using btree;

--Testcase 279:
create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

--Testcase 280:
create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.

--Testcase 281:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- This should not be pushed either.
--Testcase 1136:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Update local stats on ft2
--ANALYZE ft2;

-- Add into extension

--Testcase 282:
alter extension griddb_fdw add operator class my_op_class using btree;

--Testcase 283:
alter extension griddb_fdw add function my_op_cmp(a int, b int);

--Testcase 284:
alter extension griddb_fdw add operator family my_op_family using btree;

--Testcase 285:
alter extension griddb_fdw add operator public.<^(int, int);

--Testcase 286:
alter extension griddb_fdw add operator public.=^(int, int);

--Testcase 287:
alter extension griddb_fdw add operator public.>^(int, int);
--alter server griddb_svr options (set extensions 'griddb_fdw');

-- Now this will be pushed as sort operator is part of the extension.

--Testcase 288:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

--Testcase 289:
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Does not support push-down user defined operator
--Testcase 1137:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Remove from extension

--Testcase 290:
alter extension griddb_fdw drop operator class my_op_class using btree;

--Testcase 291:
alter extension griddb_fdw drop function my_op_cmp(a int, b int);

--Testcase 292:
alter extension griddb_fdw drop operator family my_op_family using btree;

--Testcase 293:
alter extension griddb_fdw drop operator public.<^(int, int);

--Testcase 294:
alter extension griddb_fdw drop operator public.=^(int, int);

--Testcase 295:
alter extension griddb_fdw drop operator public.>^(int, int);

-- This will not be pushed as sort operator is now removed from the extension.

--Testcase 296:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup

--Testcase 297:
drop operator class my_op_class using btree;

--Testcase 298:
drop function my_op_cmp(a int, b int);

--Testcase 299:
drop operator family my_op_family using btree;

--Testcase 300:
drop operator public.>^(int, int);

--Testcase 301:
drop operator public.=^(int, int);

--Testcase 302:
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.

--Testcase 303:
explain (verbose, costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate

--Testcase 304:
explain (verbose, costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

--Testcase 305:
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING

--Testcase 306:
explain (verbose, costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

--Testcase 307:
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.

--Testcase 308:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

--Testcase 309:
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.

--Testcase 310:
explain (verbose, costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

--Testcase 311:
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization

--Testcase 312:
set enable_hashagg to false;

--Testcase 313:
explain (verbose, costs off)
select c2, sum from "S 1"."T1" t1, lateral (select sum(t2.c1 + t1."C_1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C_1" < 100 order by 1;

--Testcase 314:
select c2, sum from "S 1"."T1" t1, lateral (select sum(t2.c1 + t1."C_1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C_1" < 100 order by 1;

--Testcase 315:
reset enable_hashagg;

-- bug #15613: bad plan for foreign table scan with lateral reference

--Testcase 316:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."T1" AS ref_0,
    LATERAL (
        SELECT ref_0."C_1", subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0."C_1" < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0."C_1";

--Testcase 317:
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."T1" AS ref_0,
    LATERAL (
        SELECT ref_0."C_1", subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0."C_1" < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0."C_1";

-- Check with placeHolderVars

--Testcase 318:
explain (verbose, costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);

--Testcase 319:
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);

-- Not supported cases
-- Grouping sets

--Testcase 320:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;

--Testcase 321:
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;

--Testcase 322:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;

--Testcase 323:
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;

--Testcase 324:
explain (verbose, costs off)
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;

--Testcase 325:
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;

--Testcase 326:
explain (verbose, costs off)
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

--Testcase 327:
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed

--Testcase 328:
explain (verbose, costs off)
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

--Testcase 329:
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

-- WindowAgg

--Testcase 330:
explain (verbose, costs off)
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;

--Testcase 331:
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;

--Testcase 332:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;

--Testcase 333:
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;

--Testcase 334:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;

--Testcase 335:
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;

-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join

--Testcase 336:
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;

--Testcase 337:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);

--Testcase 338:
EXECUTE st1(1, 1);

--Testcase 339:
EXECUTE st1(101, 101);
-- subquery using stable function (can't be sent to remote)

--Testcase 340:
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;

--Testcase 341:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);

--Testcase 342:
EXECUTE st2(10, 20);

--Testcase 343:
EXECUTE st2(101, 121);
-- subquery using immutable function (can be sent to remote)

--Testcase 344:
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;

--Testcase 345:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);

--Testcase 346:
EXECUTE st3(10, 20);

--Testcase 347:
EXECUTE st3(20, 30);
-- custom plan should be chosen initially

--Testcase 348:
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;

--Testcase 349:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);

--Testcase 350:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);

--Testcase 351:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);

--Testcase 352:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);

--Testcase 353:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan

--Testcase 354:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote

--Testcase 355:
PREPARE st5(text,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;

--Testcase 356:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);

--Testcase 357:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);

--Testcase 358:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);

--Testcase 359:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);

--Testcase 360:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);

--Testcase 361:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);

--Testcase 362:
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning

--Testcase 363:
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2;

--Testcase 364:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;

--Testcase 365:
PREPARE st7 AS INSERT INTO ft1_a_child (c1,c2,c3) VALUES (1001,101,'foo');

--Testcase 366:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;

--Testcase 367:
INSERT INTO "S 1"."T0" SELECT * FROM "S 1"."T1";

--Testcase 368:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (SET table_name 'T0');

-- pgspider cannot detect the option change of child foreign table. The query already prepared is
-- not updated. It mean "T 0" is not used but "T 1".
--Testcase 369:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;

--Testcase 370:
EXECUTE st6;

--Testcase 371:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;

--Testcase 372:
DELETE FROM "S 1"."T0";

--Testcase 373:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (SET table_name 'T1');

--Testcase 374:
PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--ALTER SERVER griddb_svr OPTIONS (DROP extensions);

--Testcase 375:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;

--Testcase 376:
EXECUTE st8;
--ALTER SERVER griddb_svr OPTIONS (ADD extensions 'griddb_fdw');

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

--Testcase 377:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;

--Testcase 378:
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1_a'::regclass LIMIT 1;

--Testcase 379:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;

--Testcase 380:
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;

--Testcase 381:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
-- ctid cannot be pushed down, so the result is empty

--Testcase 382:
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';

--Testcase 383:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;

--Testcase 384:
SELECT ctid, * FROM ft1 t1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================

--Testcase 385:
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN

--Testcase 386:
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;

--Testcase 387:
SELECT f_test(100);

--Testcase 388:
DROP FUNCTION f_test(int);

-- This test does not suitable with PGSpider Extension.
-- -- ===================================================================
-- -- REINDEX
-- -- ===================================================================
-- -- remote table is not created here

-- --Testcase 389:
-- CREATE FOREIGN TABLE reindex_foreign_a_child (c1 int, c2 int)
--   SERVER griddb_svr2 OPTIONS (table_name 'reindex_local');

-- CREATE TABLE reindex_foreign (c1 int, c2 int, spdurl text) PARTITION BY LIST (spdurl);

-- CREATE FOREIGN TABLE reindex_foreign_a PARTITION OF reindex_foreign FOR VALUES IN ('/node1/') SERVER spdsrv;

-- REINDEX TABLE reindex_foreign; -- error
-- REINDEX TABLE CONCURRENTLY reindex_foreign; -- error

-- --Testcase 390:
-- DROP FOREIGN TABLE reindex_foreign_a_child;

-- DROP TABLE reindex_foreign;
-- -- partitions and foreign tables

-- --Testcase 391:
-- CREATE TABLE reind_fdw_parent (c1 int) PARTITION BY RANGE (c1);

-- --Testcase 392:
-- CREATE TABLE reind_fdw_0_10 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (0) TO (10);

-- --Testcase 393:
-- CREATE FOREIGN TABLE reind_fdw_10_20 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (10) TO (20)
--   SERVER griddb_svr OPTIONS (table_name 'reind_local_10_20');
-- REINDEX TABLE reind_fdw_parent; -- ok
-- REINDEX TABLE CONCURRENTLY reind_fdw_parent; -- ok

-- --Testcase 394:
-- DROP TABLE reind_fdw_parent;

-- ===================================================================
-- conversion error
-- ===================================================================

--Testcase 395:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE int;

--Testcase 396:
SELECT * FROM ft1 WHERE c1 = 1;  -- ERROR

--Testcase 397:
SELECT  ft1.c1, ft2.c2, ft1.c8 FROM ft1, ft2 WHERE ft1.c1 = ft2.c1 AND ft1.c1 = 1; -- ERROR

--Testcase 398:
SELECT  ft1.c1, ft2.c2, ft1 FROM ft1, ft2 WHERE ft1.c1 = ft2.c1 AND ft1.c1 = 1; -- ERROR

--Testcase 399:
SELECT sum(c2), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR

--Testcase 400:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE text;

-- ===================================================================
-- local type can be different from remote type in some cases,
-- in particular if similarly-named operators do equivalent things
-- ===================================================================
--Testcase 1138:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c8 = 'foo' LIMIT 1;
--Testcase 1139:
SELECT * FROM ft1 WHERE c8 = 'foo' LIMIT 1;
EXPLAIN (VERBOSE, COSTS OFF)
--Testcase 1140:
SELECT * FROM ft1 WHERE 'foo' = c8 LIMIT 1;
--Testcase 1141:
SELECT * FROM ft1 WHERE 'foo' = c8 LIMIT 1;
-- we declared c8 to be text locally, but it's still the same type on
-- the remote which will balk if we try to do anything incompatible
-- with that remote type
-- c8 is text type column in remote GridDB, so below error cannot occur.
--Testcase 1142:
SELECT * FROM ft1 WHERE c8 LIKE 'foo' LIMIT 1; -- ERROR
--Testcase 1143:
SELECT * FROM ft1 WHERE c8::text LIKE 'foo' LIMIT 1; -- ERROR; cast not pushed down

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;

--Testcase 401:
FETCH c;
SAVEPOINT s;        -- Not support
ERROR OUT;
ROLLBACK TO s;

--Testcase 402:
FETCH c;
SAVEPOINT s;

--Testcase 403:
SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK TO s;

--Testcase 404:
FETCH c;

--Testcase 405:
SELECT * FROM ft1 ORDER BY c1 LIMIT 1;
COMMIT;

-- ===================================================================
-- test handling of collations
-- ===================================================================

--Testcase 406:
create foreign table loct3_a_child (
	f1 text OPTIONS (rowkey 'true'), 
	f2 text, 
	f3 text OPTIONS (rowkey 'true'))
  server griddb_svr options (table_name 'loct3');

--Testcase 1095:
create table loct3 (
	f1 text, 
	f2 text, 
	f3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 1096:
CREATE FOREIGN TABLE loct3_a PARTITION OF loct3 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 407:
create foreign table ft3_a_child (
	f1 text OPTIONS (rowkey 'true'), 
	f2 text, 
	f3 text OPTIONS (rowkey 'true'))
  server griddb_svr options (table_name 'loct3');

--Testcase 1097:
create table ft3 (
	f1 text, 
	f2 text, 
	f3 text,
	spdurl text
) PARTITION BY LIST (spdurl);

--Testcase 1098:
CREATE FOREIGN TABLE ft3_a PARTITION OF ft3 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- can be sent to remote

--Testcase 408:
explain (verbose, costs off) select * from ft3 where f1 = 'foo';

--Testcase 409:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "C" = 'foo';

--Testcase 410:
explain (verbose, costs off) select * from ft3 where f2 = 'foo';

--Testcase 411:
explain (verbose, costs off) select * from ft3 where f3 = 'foo';

--Testcase 412:
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 and l.f1 = 'foo';
-- can't be sent to remote

--Testcase 413:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';

--Testcase 414:
explain (verbose, costs off) select * from ft3 where f1 = 'foo' COLLATE "C";

--Testcase 415:
explain (verbose, costs off) select * from ft3 where f2 COLLATE "C" = 'foo';

--Testcase 416:
explain (verbose, costs off) select * from ft3 where f2 = 'foo' COLLATE "C";

--Testcase 417:
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================

--Testcase 418:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;

--Testcase 419:
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
-- RETURNING is not supported by GridDB. Use SELECT instead.

--Testcase 420:
INSERT INTO ft2_a_child (c1,c2,c3)
  VALUES (1101,201,'aaa'), (1102,202,'bbb'), (1103,203,'ccc');

--Testcase 421:
SELECT * FROM ft2 WHERE c1 > 1100 AND c1 < 1104;

--Testcase 422:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1104,204,'ddd'), (1105,205,'eee');

--Testcase 423:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;              -- can be pushed down

--Testcase 424:
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;

--Testcase 425:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;              -- can be pushed down

--Testcase 426:
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;

--Testcase 427:
SELECT * FROM ft2 WHERE c1 % 10 = 7;

--Testcase 428:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;                               -- can be pushed down

--Testcase 429:
UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;

--Testcase 430:
EXPLAIN (verbose, costs off)
  DELETE FROM ft2_a_child WHERE c1 % 10 = 5;                                                -- can be pushed down

--Testcase 431:
SELECT c1,c4 FROM ft2 WHERE c1 % 10 = 5;

--Testcase 432:
DELETE FROM ft2_a_child WHERE c1 % 10 = 5;

--Testcase 433:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child USING ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 2;                -- can be pushed down

--Testcase 434:
DELETE FROM ft2_a_child USING ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 2;

--Testcase 435:
SELECT c1,c2,c3,c4 FROM ft2 ORDER BY c1;

--Testcase 436:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');

--Testcase 437:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');

--Testcase 438:
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;

--Testcase 439:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200;                                          -- can be pushed down

--Testcase 440:
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200;

--Testcase 441:
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;

--Testcase 442:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child WHERE c1 = 1200;                                                    -- can be pushed down

--Testcase 443:
SELECT tableoid::regclass FROM ft2 WHERE c1 = 1200;

--Testcase 444:
DELETE FROM ft2_a_child WHERE c1 = 1200;

-- Test UPDATE/DELETE with RETURNING on a three-table join

--Testcase 445:
INSERT INTO ft2_a_child (c1,c2,c3)
  SELECT id, id - 1200, to_char(id, 'FM00000') FROM generate_series(1201, 1300) id;

--Testcase 446:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c2 = ft4.c1;

--Testcase 447:
UPDATE ft2_a_child SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c2 = ft4.c1;

--Testcase 448:
SELECT ft2, ft2.*, ft4, ft4.* FROM ft2, ft4 WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1 AND ft2.c3 = 'foo';

--Testcase 449:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c1 % 10 = 0 AND ft2_a_child.c2 = ft4.c1;

--Testcase 450:
SELECT 100 FROM ft2, ft4 WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1;

--Testcase 451:
DELETE FROM ft2_a_child 
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c1 % 10 = 0 AND ft2_a_child.c2 = ft4.c1;

--Testcase 452:
DELETE FROM ft2_a_child WHERE ft2_a_child.c1 > 1200;

-- Test UPDATE with a MULTIEXPR sub-select
-- (maybe someday this'll be remotely executable, but not today)

--Testcase 453:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

--Testcase 454:
UPDATE ft2_a_child AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

--Testcase 455:
UPDATE ft2_a_child AS target SET (c2) = (
    SELECT c2 / 10
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

-- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- user-defined operators/functions
--ALTER SERVER griddb_svr OPTIONS (DROP extensions);

--Testcase 456:
INSERT INTO ft2_a_child (c1,c2,c3)
  SELECT id, id % 10, to_char(id, 'FM00000') FROM generate_series(2001, 2010) id;

--Testcase 457:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE griddb_fdw_abs(c1) > 2000;                          -- can't be pushed down

--Testcase 458:
UPDATE ft2_a_child SET c3 = 'bar' WHERE griddb_fdw_abs(c1) > 2000;

--Testcase 459:
SELECT * FROM ft2 WHERE griddb_fdw_abs(c1) > 2000;

--Testcase 460:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1;                                        -- can't be pushed down

--Testcase 461:
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1;

--Testcase 462:
SELECT ft2.*, ft4.*, ft5.* FROM ft2
  INNER JOIN ft4 ON (ft2.c1 > 2000 AND ft2.c2 === ft4.c1)
  INNER JOIN ft5 ON (ft4.c1 = ft5.c1);

--Testcase 463:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1;                                          -- can't be pushed down

--Testcase 464:
SELECT ft2.c1, ft2.c2, ft2.c3 FROM ft2
  INNER JOIN ft4 ON (ft2.c1 > 2000 AND ft2.c2 = ft4.c1)
  INNER JOIN ft5 ON (ft4.c1 === ft5.c1);

--Testcase 465:
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1;

--Testcase 466:
DELETE FROM ft2_a_child WHERE ft2_a_child.c1 > 2000;
--ALTER SERVER griddb_svr OPTIONS (ADD extensions 'griddb_fdw');

-- Test that trigger on remote table works as expected

--Testcase 467:
CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.c3 = NEW.c3 || '_trig_update';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Testcase 468:
CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
    ON ft2_a_child FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

--Testcase 469:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1208, 818, 'fff');

--Testcase 470:
SELECT * FROM ft2 WHERE c1 = 1208;

--Testcase 471:
INSERT INTO ft2_a_child (c1,c2,c3,c6) VALUES (1218, 818, 'ggg', '(--;');

--Testcase 472:
SELECT * FROM ft2 WHERE c1 = 1218;

--Testcase 473:
UPDATE ft2_a_child SET c2 = c2 + 600 WHERE c1 % 10 = 8 AND c1 < 1200;

--Testcase 474:
SELECT * FROM ft2 WHERE c1 % 10 = 8 AND c1 < 1200;

--Testcase 475:
DROP TRIGGER t1_br_insert ON ft2_a_child;

-- Test errors thrown on remote side during update

--Testcase 476:
ALTER FOREIGN TABLE ft1_a_child ADD CONSTRAINT c2positive CHECK (c2 >= 0);

-- row was updated instead of insert because same row key has already existed.

--INSERT INTO ft1(c1, c2) VALUES(11, 12);
-- ON CONFLICT is not suported

--Testcase 477:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT DO NOTHING; -- not supported

--Testcase 478:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO NOTHING; -- unsupported

--Testcase 479:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO UPDATE SET c3 = 'ffg'; -- unsupported
-- GridDB not support constraints

--INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive

--UPDATE ft1 SET c2 = -c2 WHERE c1 = 1;  -- c2positive

-- Test savepoint/rollback behavior

--Testcase 480:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;

--Testcase 481:
select c2, count(*) from "S 1"."T1" where c2 < 500 group by 1 order by 1;
begin;

--Testcase 482:
update ft2_a_child set c2 = 42 where c2 = 0;

--Testcase 483:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s1;

--Testcase 484:
update ft2_a_child set c2 = 44 where c2 = 4;

--Testcase 485:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s1;

--Testcase 486:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s2;

--Testcase 487:
update ft2_a_child set c2 = 46 where c2 = 6;

--Testcase 488:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
rollback to savepoint s2;
-- savepoint not supported.

--Testcase 489:
update ft2_a_child set c2 = 6 where c2 = 46;

--Testcase 490:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s2;

--Testcase 491:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s3;

-- GridDB not support constraints
--update ft2 set c2 = -2 where c2 = 42 and c1 = 10; -- fail on remote side
rollback to savepoint s3;

--Testcase 492:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s3;

--Testcase 493:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- none of the above is committed yet remotely

--Testcase 494:
select c2, count(*) from "S 1"."T1" where c2 < 500 group by 1 order by 1;
commit;

--Testcase 495:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;

--Testcase 496:
select c2, count(*) from "S 1"."T1" where c2 < 500 group by 1 order by 1;

--VACUUM ANALYZE "S 1"."T 1";

-- Above DMLs add data with c6 as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- FIRST behavior here.
-- ORDER BY DESC NULLS LAST options

--Testcase 497:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;

--Testcase 498:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795  LIMIT 10;
-- ORDER BY DESC NULLS FIRST options

--Testcase 499:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

--Testcase 500:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- ORDER BY ASC NULLS FIRST options

--Testcase 501:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

--Testcase 502:
SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

/*
-- GridDB not support constraints
-- ===================================================================
-- test check constraints
-- ===================================================================
-- Consistent check constraints provide consistent results
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2positive CHECK (c2 >= 0);

EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;

SELECT count(*) FROM ft1 WHERE c2 < 0;
SET constraint_exclusion = 'on';

EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;

SELECT count(*) FROM ft1 WHERE c2 < 0;
RESET constraint_exclusion;
-- check constraint is enforced on the remote side, not locally

INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive

UPDATE ft1 SET c2 = -c2 WHERE c1 = 1;  -- c2positive
ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2positive;

-- But inconsistent check constraints provide inconsistent results
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2negative CHECK (c2 < 0);

EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;

SELECT count(*) FROM ft1 WHERE c2 >= 0;
SET constraint_exclusion = 'on';

EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;

SELECT count(*) FROM ft1 WHERE c2 >= 0;
RESET constraint_exclusion;
-- local check constraint is not actually enforced

INSERT INTO ft1(c1, c2) VALUES(1111, 2);

UPDATE ft1 SET c2 = c2 + 1 WHERE c1 = 1;
ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2negative;
*/

-- ===================================================================
-- test WITH CHECK OPTION constraints
-- ===================================================================

--Testcase 503:
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;

--Testcase 504:
CREATE FOREIGN TABLE foreign_tbl_a_child (id serial OPTIONS (rowkey 'true'), a int, b int)
  SERVER griddb_svr OPTIONS(table_name 'base_tbl');

--Testcase 1099:
CREATE TABLE foreign_tbl (id serial, a int, b int, spdurl text) PARTITION BY LIST (spdurl);

--Testcase 1100:
CREATE FOREIGN TABLE foreign_tbl_a PARTITION OF foreign_tbl FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 505:
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl_a_child FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

--Testcase 506:
CREATE VIEW rw_view AS SELECT * FROM foreign_tbl_a_child
  WHERE a < b WITH CHECK OPTION;

--Testcase 507:
\d+ rw_view

--Testcase 508:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view(a, b) VALUES (0, 5);

--Testcase 509:
INSERT INTO rw_view(a, b) VALUES (0, 5); -- should fail

--Testcase 510:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view(a, b) VALUES (0, 15);

--Testcase 511:
INSERT INTO rw_view(a, b) VALUES (0, 15); -- ok

--Testcase 512:
SELECT a, b FROM foreign_tbl;

--Testcase 513:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 5;

--Testcase 514:
UPDATE rw_view SET b = b + 5; -- should fail

--Testcase 515:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 15;

--Testcase 516:
UPDATE rw_view SET b = b + 15; -- ok

--Testcase 517:
SELECT a, b FROM foreign_tbl;

-- We don't allow batch insert when there are any WCO constraints
-- ALTER SERVER loopback OPTIONS (ADD batch_size '10');
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 15), (0, 5);
-- INSERT INTO rw_view VALUES (0, 15), (0, 5); -- should fail
-- SELECT * FROM foreign_tbl;
-- ALTER SERVER loopback OPTIONS (DROP batch_size);

--Testcase 518:
DROP TRIGGER row_before_insupd_trigger ON foreign_tbl_a_child;

--Testcase 519:
DROP FOREIGN TABLE foreign_tbl_a_child CASCADE;

--Testcase 1101:
DROP TABLE foreign_tbl;

-- This test conflicts with the mechanism of PGSpider Extension (only support Partition by List).
-- -- test WCO for partitions

-- --Testcase 520:
-- CREATE FOREIGN TABLE foreign_tbl_child (id serial OPTIONS (rowkey 'true'), a int, b int)
--   SERVER griddb_svr OPTIONS (table_name 'child_tbl');

-- CREATE TABLE foreign_tbl (id serial, a int, b int, spdurl text) PARTITION BY LIST (spdurl);

-- CREATE FOREIGN TABLE foreign_tbl_a PARTITION OF foreign_tbl FOR VALUES IN ('/node1/') SERVER spdsrv;

-- --Testcase 521:
-- CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

-- --Testcase 522:
-- CREATE TABLE parent_tbl (id serial, a int, b int) PARTITION BY RANGE(a);

-- --Testcase 523:
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);

-- -- Detach and re-attach once, to stress the concurrent detach case.
-- ALTER TABLE parent_tbl DETACH PARTITION foreign_tbl CONCURRENTLY;
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);

-- --Testcase 524:
-- CREATE VIEW rw_view AS SELECT * FROM parent_tbl
--   WHERE a < b WITH CHECK OPTION;

-- --Testcase 525:
-- \d+ rw_view

-- --Testcase 526:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view(a, b) VALUES (0, 5);

-- --Testcase 527:
-- INSERT INTO rw_view(a, b) VALUES (0, 5); -- should fail

-- --Testcase 528:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view(a, b) VALUES (0, 15);

-- --Testcase 529:
-- INSERT INTO rw_view(a, b) VALUES (0, 15); -- ok

-- --Testcase 530:
-- SELECT a, b FROM foreign_tbl;

-- --Testcase 531:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE rw_view SET b = b + 5;

-- --Testcase 532:
-- UPDATE rw_view SET b = b + 5; -- should fail

-- --Testcase 533:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE rw_view SET b = b + 15;

-- --Testcase 534:
-- UPDATE rw_view SET b = b + 15; -- ok

-- --Testcase 535:
-- SELECT a, b FROM foreign_tbl;
-- We don't allow batch insert when there are any WCO constraints
-- ALTER SERVER loopback OPTIONS (ADD batch_size '10');
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 15), (0, 5);
-- INSERT INTO rw_view VALUES (0, 15), (0, 5); -- should fail
-- SELECT * FROM foreign_tbl;
-- ALTER SERVER loopback OPTIONS (DROP batch_size);

-- --Testcase 536:
-- DROP TRIGGER row_before_insupd_trigger ON foreign_tbl;

-- --Testcase 537:
-- DROP FOREIGN TABLE foreign_tbl CASCADE;

-- --Testcase 538:
-- DROP TABLE parent_tbl CASCADE;

-- --Testcase 539:
-- DROP FUNCTION row_before_insupd_trigfunc;

-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- ===================================================================

--Testcase 540:
create foreign table rem1_a_child (id serial OPTIONS (rowkey 'true'), f1 serial, f2 text)
  server griddb_svr options(table_name 'loct13');

--Testcase 1102:
create table rem1 (id serial, f1 serial, f2 text, spdurl text) PARTITION BY LIST (spdurl);

--Testcase 1103:
CREATE FOREIGN TABLE rem1_a PARTITION OF rem1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 541:
insert into rem1_a_child(f2) values('hi');

--Testcase 542:
insert into rem1_a_child(f2) values('bye');

--Testcase 543:
select pg_catalog.setval('rem1_a_child_f1_seq', 10, false);

--Testcase 544:
insert into rem1_a_child(f2) values('hi remote');

--Testcase 545:
insert into rem1_a_child(f2) values('bye remote');

--Testcase 546:
select f1, f2 from rem1;

-- ===================================================================
-- test generated columns
-- ===================================================================
--create table gloc1 (a int, b int);
--alter table gloc1 set (autovacuum_enabled = 'false');

--Testcase 547:
create foreign table grem1_a_child (
  id serial OPTIONS (rowkey 'true'),
  a int,
  b int generated always as (a * 2) stored)
  server griddb_svr options(table_name 'gloc1');

--Testcase 1104:
create table grem1 (
  id serial,
  a int,
  b int generated always as (a * 2) stored,
  spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 1105:
CREATE FOREIGN TABLE grem1_a PARTITION OF grem1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 548:
insert into grem1_a_child (a) values (1), (2);

--Testcase 549:
update grem1_a_child set a = 22 where a = 2;

--Testcase 550:
select a, b from grem1;

--Testcase 1077:
delete from grem1_a_child;

-- test copy from
copy grem1_a_child(a) from stdin;
1
2
\.
-- --Testcase 1101:
-- select a,b from gloc1;
--Testcase 1078:
select a, b from grem1;
--Testcase 1079:
delete from grem1_a_child;

-- test batch insert
--Testcase 1080:
alter server griddb_svr options (add batch_size '10');
--Testcase 1081:
explain (verbose, costs off)
insert into grem1_a_child (a) values (1), (2);
--Testcase 1082:
insert into grem1_a_child (a) values (1), (2);
-- --Testcase 1102:
-- select a, b from gloc1;
--Testcase 1083:
select a, b from grem1;
--Testcase 1084:
delete from grem1_a_child;
--Testcase 1085:
alter server griddb_svr options (drop batch_size);

-- ===================================================================
-- test local triggers
-- ===================================================================

-- Trigger functions "borrowed" from triggers regress test.

--Testcase 551:
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

--Testcase 552:
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 553:
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 554:
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

--Testcase 555:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 556:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 557:
delete from rem1_a_child;

--Testcase 558:
insert into rem1_a_child(f1, f2) values(1,'insert');

--Testcase 559:
update rem1_a_child set f2  = 'update' where f1 = 1;

--Testcase 560:
update rem1_a_child set f2 = f2 || f2;

-- cleanup

--Testcase 561:
DROP TRIGGER trig_row_before ON rem1_a_child;

--Testcase 562:
DROP TRIGGER trig_row_after ON rem1_a_child;

--Testcase 563:
DROP TRIGGER trig_stmt_before ON rem1_a_child;

--Testcase 564:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

--Testcase 565:
DELETE from rem1_a_child;

-- Test multiple AFTER ROW triggers on a foreign table

--Testcase 566:
CREATE TRIGGER trig_row_after1
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 567:
CREATE TRIGGER trig_row_after2
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 568:
insert into rem1_a_child(f1, f2) values(1,'insert');

--Testcase 569:
update rem1_a_child set f2  = 'update' where f1 = 1;

--Testcase 570:
update rem1_a_child set f2 = f2 || f2;

--Testcase 571:
delete from rem1_a_child;

-- cleanup

--Testcase 572:
DROP TRIGGER trig_row_after1 ON rem1_a_child;

--Testcase 573:
DROP TRIGGER trig_row_after2 ON rem1_a_child;

-- Test WHEN conditions

--Testcase 574:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 575:
CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens

--Testcase 576:
INSERT INTO rem1_a_child(f1, f2) values(1, 'insert');

--Testcase 577:
UPDATE rem1_a_child set f2 = 'test';

-- Insert or update matching: triggers are fired

--Testcase 578:
INSERT INTO rem1_a_child(f1, f2) values(2, 'update');

--Testcase 579:
UPDATE rem1_a_child set f2 = 'update update' where f1 = '2';

--Testcase 580:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 581:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1

--Testcase 582:
DELETE FROM rem1_a_child;

-- cleanup

--Testcase 583:
DROP TRIGGER trig_row_before_insupd ON rem1_a_child;

--Testcase 584:
DROP TRIGGER trig_row_after_insupd ON rem1_a_child;

--Testcase 585:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;

--Testcase 586:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;

-- Test various RETURN statements in BEFORE triggers.

--Testcase 587:
CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

--Testcase 588:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- The new values should have 'triggered' appended

--Testcase 589:
INSERT INTO rem1_a_child(f1, f2) values(1, 'insert');

--Testcase 590:
SELECT f1, f2 from rem1;

--Testcase 591:
INSERT INTO rem1_a_child(f1, f2) values(2, 'insert');

--Testcase 592:
SELECT f1, f2 from rem1;

--Testcase 593:
UPDATE rem1_a_child set f2 = '';

--Testcase 594:
SELECT f1, f2 from rem1;

--Testcase 595:
UPDATE rem1_a_child set f2 = 'skidoo';

--Testcase 596:
SELECT f1, f2 from rem1;

--Testcase 597:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f1 = 10;          -- all columns should be transmitted

--Testcase 598:
UPDATE rem1_a_child set f1 = 10;

--Testcase 599:
SELECT f1, f2 from rem1;

--Testcase 600:
DELETE FROM rem1_a_child;

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger

--Testcase 601:
CREATE TRIGGER trig_row_before_insupd2
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 602:
INSERT INTO rem1_a_child(f1, f2) values(1, 'insert');

--Testcase 603:
SELECT f1, f2 from rem1;

--Testcase 604:
INSERT INTO rem1_a_child(f1, f2) values(2, 'insert');

--Testcase 605:
SELECT f1, f2 from rem1;

--Testcase 606:
UPDATE rem1_a_child set f2 = '';

--Testcase 607:
SELECT f1, f2 from rem1;

--Testcase 608:
UPDATE rem1_a_child set f2 = 'skidoo';

--Testcase 609:
SELECT f1, f2 from rem1;

--Testcase 610:
DROP TRIGGER trig_row_before_insupd ON rem1_a_child;

--Testcase 611:
DROP TRIGGER trig_row_before_insupd2 ON rem1_a_child;

--Testcase 612:
DELETE from rem1_a_child;

--Testcase 613:
INSERT INTO rem1_a_child(f1, f2) VALUES (1, 'test');

-- Test with a trigger returning NULL

--Testcase 614:
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

--Testcase 615:
CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.

--Testcase 616:
INSERT INTO rem1_a_child(f1, f2) VALUES (2, 'test2');

--Testcase 617:
SELECT f1, f2 from rem1;

--Testcase 618:
UPDATE rem1_a_child SET f2 = 'test2';

--Testcase 619:
SELECT f1, f2 from rem1;

--Testcase 620:
DELETE from rem1_a_child;

--Testcase 621:
SELECT f1, f2 from rem1;

--Testcase 622:
DROP TRIGGER trig_null ON rem1_a_child;

--Testcase 623:
DELETE from rem1_a_child;

-- Test a combination of local and remote triggers

--Testcase 624:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 625:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 626:
CREATE TRIGGER trig_local_before 
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 627:
INSERT INTO rem1_a_child(f2) VALUES ('test');

--Testcase 628:
UPDATE rem1_a_child SET f2 = 'testo';

-- Test returning a system attribute

--Testcase 629:
INSERT INTO rem1_a_child(f2) VALUES ('test');

--Testcase 630:
SELECT ctid FROM rem1 WHERE f2 = 'test triggered !';

-- cleanup

--Testcase 631:
DROP TRIGGER trig_row_before ON rem1_a_child;

--Testcase 632:
DROP TRIGGER trig_row_after ON rem1_a_child;

--Testcase 633:
DROP TRIGGER trig_local_before ON rem1_a_child;

-- Test direct foreign table modification functionality
-- GridDB does not support direct modification
-- --Testcase 1086:
-- EXPLAIN (verbose, costs off)
-- DELETE FROM rem1;                 -- can't be pushed down
-- --Testcase 1087:
-- EXPLAIN (verbose, costs off)
-- DELETE FROM rem1 WHERE false;     -- currently can't be pushed down

-- Test with statement-level triggers

--Testcase 634:
CREATE TRIGGER trig_stmt_before
	BEFORE DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 635:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down

--Testcase 636:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down

--Testcase 637:
DROP TRIGGER trig_stmt_before ON rem1_a_child;

--Testcase 638:
CREATE TRIGGER trig_stmt_after
	AFTER DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 639:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down

--Testcase 640:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down

--Testcase 641:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

-- Test with row-level ON INSERT triggers

--Testcase 642:
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 643:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down

--Testcase 644:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down

--Testcase 645:
DROP TRIGGER trig_row_before_insert ON rem1_a_child;

--Testcase 646:
CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 647:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down

--Testcase 648:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down

--Testcase 649:
DROP TRIGGER trig_row_after_insert ON rem1_a_child;

-- Test with row-level ON UPDATE triggers

--Testcase 650:
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 651:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down

--Testcase 652:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down

--Testcase 653:
DROP TRIGGER trig_row_before_update ON rem1_a_child;

--Testcase 654:
CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 655:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down

--Testcase 656:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down

--Testcase 657:
DROP TRIGGER trig_row_after_update ON rem1_a_child;

-- Test with row-level ON DELETE triggers

--Testcase 658:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 659:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down

--Testcase 660:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down

--Testcase 661:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;

--Testcase 662:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 663:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down

--Testcase 664:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down

--Testcase 665:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;

-- ===================================================================
-- test inheritance features
-- ===================================================================

--Testcase 666:
CREATE TABLE a (id serial, aa TEXT);

--Testcase 667:
ALTER TABLE a SET (autovacuum_enabled = 'false');

--Testcase 668:
CREATE FOREIGN TABLE b_a_child (bb TEXT) INHERITS (a)
  SERVER griddb_svr OPTIONS (table_name 'loct');

--Testcase 1106:
CREATE TABLE b (id integer, aa TEXT, bb TEXT, spdurl text)
   PARTITION BY LIST (spdurl);
--Testcase 1107:
CREATE FOREIGN TABLE b_a PARTITION OF b FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 669:
ALTER FOREIGN TABLE b_a_child ALTER COLUMN id OPTIONS (rowkey 'true');

--Testcase 670:
INSERT INTO a(aa) VALUES('aaa');

--Testcase 671:
INSERT INTO a(aa) VALUES('aaaa');

--Testcase 672:
INSERT INTO a(aa) VALUES('aaaaa');

--Testcase 673:
INSERT INTO b_a_child(aa) VALUES('bbb');

--Testcase 674:
INSERT INTO b_a_child(aa) VALUES('bbbb');

--Testcase 675:
INSERT INTO b_a_child(aa) VALUES('bbbbb');

--Testcase 676:
SELECT tableoid::regclass, aa FROM a;

--Testcase 677:
SELECT tableoid::regclass, aa, bb FROM b;

--Testcase 678:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 679:
UPDATE a SET aa = 'zzzzzz' WHERE aa LIKE 'aaaa%'; -- limitation

--Testcase 680:
SELECT tableoid::regclass, aa FROM a;

--Testcase 681:
SELECT tableoid::regclass, aa, bb FROM b;

--Testcase 682:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 683:
UPDATE b_a_child SET aa = 'new';

--Testcase 684:
SELECT tableoid::regclass, aa FROM a;

--Testcase 685:
SELECT tableoid::regclass, aa, bb FROM b;

--Testcase 686:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 687:
UPDATE a SET aa = 'newtoo';

--Testcase 688:
SELECT tableoid::regclass, aa FROM a;

--Testcase 689:
SELECT tableoid::regclass, aa, bb FROM b;

--Testcase 690:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 691:
DELETE FROM a;

--Testcase 692:
SELECT tableoid::regclass, aa FROM a;

--Testcase 693:
SELECT tableoid::regclass, aa, bb FROM b;

--Testcase 694:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 695:
DROP TABLE a CASCADE;

-- Check SELECT FOR UPDATE/SHARE with an inherited source table

--Testcase 696:
create table foo (f1 int, f2 int);

--Testcase 697:
create foreign table foo2_a_child (f3 int) inherits (foo)
  server griddb_svr options (table_name 'loct1');

--Testcase 1108:
create table foo2 (f1 int, f2 int, f3 int, spdurl text)
   PARTITION BY LIST (spdurl);

--Testcase 1109:
CREATE FOREIGN TABLE foo2_a PARTITION OF foo2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 698:
create table bar (f1 int, f2 int);

--Testcase 699:
create foreign table bar2_a_child (f3 int) inherits (bar)
  server griddb_svr options (table_name 'loct2');

--Testcase 1110:
create table bar2 (f3 int, spdurl text)
  PARTITION BY LIST (spdurl);

--Testcase 1111:
CREATE FOREIGN TABLE bar2_a PARTITION OF bar2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 700:
alter table foo set (autovacuum_enabled = 'false');

--Testcase 701:
alter table bar set (autovacuum_enabled = 'false');

--Testcase 702:
alter foreign table foo2_a_child alter column f1 options (rowkey 'true');

--Testcase 703:
alter foreign table bar2_a_child alter column f1 options (rowkey 'true');

--Testcase 704:
insert into foo values(1,1);

--Testcase 705:
insert into foo values(3,3);

--Testcase 706:
insert into foo2_a_child values(2,2,2);

--Testcase 707:
insert into foo2_a_child values(4,4,4);

--Testcase 708:
insert into bar values(1,11);

--Testcase 709:
insert into bar values(2,22);

--Testcase 710:
insert into bar values(6,66);

--Testcase 711:
insert into bar2_a_child values(3,33,33);

--Testcase 712:
insert into bar2_a_child values(4,44,44);

--Testcase 713:
insert into bar2_a_child values(7,77,77);

--Testcase 714:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for update;

--Testcase 715:
select * from bar where f1 in (select f1 from foo) for update;

--Testcase 716:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for share;

--Testcase 717:
select * from bar where f1 in (select f1 from foo) for share;

-- Check UPDATE with inherited target and an inherited source table

--Testcase 718:
explain (verbose, costs off)
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

--Testcase 719:
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

--Testcase 720:
select tableoid::regclass, * from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery

--Testcase 721:
explain (verbose, costs off)
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

--Testcase 722:
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

--Testcase 723:
select tableoid::regclass, * from bar order by 1,2;

-- Test forcing the remote server to produce sorted data for a merge join,
-- but the foreign table is an inheritance child.

--Testcase 724:
delete from "S 1".loct1;
truncate table only foo;
\set num_rows_foo 2000

--Testcase 725:
insert into "S 1".loct1 select generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2);

--Testcase 726:
insert into foo select generate_series(1, :num_rows_foo, 2), generate_series(1, :num_rows_foo, 2);

--Testcase 727:
SET enable_hashjoin to false;

--Testcase 728:
SET enable_nestloop to false;
-- skip, does not support 'use_remote_estimate'
/*
alter foreign table foo2 options (use_remote_estimate 'true');
create index i_loct1_f1 on loct1(f1);
create index i_foo_f1 on foo(f1);
analyze foo;
analyze loct1;
*/
-- inner join; expressions in the clauses appear in the equivalence class list

--Testcase 729:
explain (verbose, costs off)
	select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;

--Testcase 730:
select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query

--Testcase 731:
explain (verbose, costs off)
	select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;

--Testcase 732:
select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;

--Testcase 733:
RESET enable_hashjoin;

--Testcase 734:
RESET enable_nestloop;

-- Test that WHERE CURRENT OF is not supported
begin;
declare c cursor for select * from bar where f1 = 7;

--Testcase 735:
fetch from c;

--Testcase 736:
update bar set f2 = null where current of c;
rollback;

--Testcase 737:
explain (verbose, costs off)
delete from foo where f1 < 5;

--Testcase 738:
select * from foo where f1 < 5;

--Testcase 739:
delete from foo where f1 < 5;

--Testcase 740:
explain (verbose, costs off)
update bar set f2 = f2 + 100;

--Testcase 741:
update bar set f2 = f2 + 100;

--Testcase 742:
select * from bar;

-- Test that UPDATE/DELETE with inherited target works with row-level triggers

--Testcase 743:
CREATE TRIGGER trig_row_before
BEFORE UPDATE OR DELETE ON bar2_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 744:
CREATE TRIGGER trig_row_after
AFTER UPDATE OR DELETE ON bar2_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 745:
explain (verbose, costs off)
update bar set f2 = f2 + 100;

--Testcase 746:
update bar set f2 = f2 + 100;

--Testcase 747:
explain (verbose, costs off)
delete from bar where f2 < 400;

--Testcase 748:
delete from bar where f2 < 400;

-- cleanup

--Testcase 749:
drop table foo cascade;

--Testcase 750:
drop table bar cascade;

-- Test pushing down UPDATE/DELETE joins to the remote server

--Testcase 751:
create table parent (a int, b text);

--Testcase 752:
create foreign table remt1_a_child (a int, b text)
  server griddb_svr options (table_name 'loct11');

--Testcase 1112:
create table remt1 (a int, b text, spdurl text)
  PARTITION BY LIST (spdurl);

--Testcase 1113:
CREATE FOREIGN TABLE remt1_a PARTITION OF remt1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 753:
create foreign table remt2_a_child (a int, b text)
  server griddb_svr options (table_name 'loct22');

--Testcase 1114:
create table remt2 (a int, b text, spdurl text)
  PARTITION BY LIST (spdurl);

--Testcase 1115:
CREATE FOREIGN TABLE remt2_a PARTITION OF remt2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 754:
alter foreign table remt1_a_child inherit parent;

--Testcase 755:
alter foreign table remt1_a_child alter column a options (rowkey 'true');

--Testcase 756:
alter foreign table remt2_a_child alter column a options (rowkey 'true');

--Testcase 757:
insert into remt1_a_child values (1, 'foo');

--Testcase 758:
insert into remt1_a_child values (2, 'bar');

--Testcase 759:
insert into remt2_a_child values (1, 'foo');

--Testcase 760:
insert into remt2_a_child values (2, 'bar');

--Testcase 761:
explain (verbose, costs off)
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;

--Testcase 762:
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;

--Testcase 763:
select * from parent, remt2 where parent.a = remt2.a;

--Testcase 764:
explain (verbose, costs off)
delete from parent using remt2 where parent.a = remt2.a;

--Testcase 765:
select parent.* from parent, remt2 where parent.a = remt2.a;

--Testcase 766:
delete from parent using remt2 where parent.a = remt2.a;

-- cleanup

--Testcase 767:
drop foreign table remt1_a_child;

--Testcase 1116:
drop table remt1;

--Testcase 768:
drop foreign table remt2_a_child;

--Testcase 1117:
drop table remt2;

--Testcase 769:
drop table parent;

-- PGSpider Extension does not support INSERT/UPDATE/DELETE directly on
-- parent table, so we skip these test cases.
-- -- ===================================================================
-- -- test tuple routing for foreign-table partitions
-- -- ===================================================================

-- -- Test insert tuple routing

-- --Testcase 770:
-- create table itrtest (id serial, a int, b text) partition by list (a);

-- --Testcase 771:
-- create foreign table remp1_a_child (id serial, a int, b text) server griddb_svr options (table_name 'loct12');

-- create table remp1 (id serial, a int, b text, spdurl text) PARTITION BY LIST (spdurl);

-- CREATE FOREIGN TABLE remp1_a PARTITION OF remp1 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- --Testcase 772:
-- create foreign table remp2_a_child (id serial, a int, b text) server griddb_svr options (table_name 'loct21');

-- create table remp2 (id serial, a int, b text, spdurl text) PARTITION BY LIST (spdurl);

-- CREATE FOREIGN TABLE remp2_a PARTITION OF remp2 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- --Testcase 773:
-- alter foreign table remp1_a_child alter column id options (rowkey 'true');

-- --Testcase 774:
-- alter foreign table remp2_a_child alter column id options (rowkey 'true');

-- --Testcase 775:
-- alter table itrtest attach partition remp1 for values in (1);

-- --Testcase 776:
-- alter table itrtest attach partition remp2 for values in (2);

-- --Testcase 777:
-- insert into itrtest(a, b) values (1, 'foo');

-- --Testcase 778:
-- insert into itrtest(a, b) values (1, 'bar');

-- --Testcase 779:
-- insert into itrtest(a, b) values (2, 'baz');

-- --Testcase 780:
-- insert into itrtest(a, b) values (2, 'qux');

-- --Testcase 781:
-- insert into itrtest(a, b) values (1, 'test1'), (2, 'test2');

-- --Testcase 782:
-- select tableoid::regclass, a, b FROM itrtest;

-- --Testcase 783:
-- select tableoid::regclass, a, b FROM remp1;

-- --Testcase 784:
-- select tableoid::regclass, b, a FROM remp2;

-- --Testcase 785:
-- delete from itrtest;

-- -- skip, griddb does not support on conflict
-- --create unique index loct1_idx on loct1 (a);

-- -- DO NOTHING without an inference specification is supported
-- --insert into itrtest values (1, 'foo') on conflict do nothing returning *;
-- --insert into itrtest values (1, 'foo') on conflict do nothing returning *;

-- -- But other cases are not supported
-- --insert into itrtest values (1, 'bar') on conflict (a) do nothing;
-- --insert into itrtest values (1, 'bar') on conflict (a) do update set b = excluded.b;

-- --select tableoid::regclass, * FROM itrtest;

-- --delete from itrtest;

-- --drop index loct1_idx;

-- -- Test that remote triggers work with insert tuple routing

-- --Testcase 786:
-- create function br_insert_trigfunc() returns trigger as $$
-- begin
-- 	new.b := new.b || ' triggered !';
-- 	return new;
-- end
-- $$ language plpgsql;

-- --Testcase 787:
-- create trigger remp1_br_insert_trigger before insert on remp1
-- 	for each row execute procedure br_insert_trigfunc();

-- --Testcase 788:
-- create trigger remp2_br_insert_trigger before insert on remp2
-- 	for each row execute procedure br_insert_trigfunc();

-- -- The new values are concatenated with ' triggered !'

-- --Testcase 789:
-- insert into itrtest(a, b) values (1, 'foo');

-- --Testcase 790:
-- insert into itrtest(a, b) values (2, 'qux');

-- --Testcase 791:
-- insert into itrtest(a, b) values (1, 'test1'), (2, 'test2');

-- --Testcase 792:
-- with result as (insert into itrtest(a ,b) values (1, 'test1'), (2, 'test2') returning *) select a, b from result;

-- --Testcase 793:
-- drop trigger remp1_br_insert_trigger on remp1;

-- --Testcase 794:
-- drop trigger remp2_br_insert_trigger on remp2;

-- --Testcase 795:
-- delete from itrtest;

-- --Testcase 796:
-- drop table itrtest;

-- -- Test update tuple routing

-- --Testcase 797:
-- create table utrtest (id serial, a int, b text) partition by list (a);

-- --Testcase 798:
-- create foreign table remp_a_child (id serial, a int check (a in (1)), b text) server griddb_svr options (table_name 'loct12');

-- create foreign table remp (id serial, a int check (a in (1)), b text, spdurl text) PARTITION BY LIST (spdurl);

-- CREATE FOREIGN TABLE remp_a PARTITION OF remp FOR VALUES IN ('/node1/') SERVER spdsrv;

-- --Testcase 799:
-- alter foreign table remp_a_child alter column id options (rowkey 'true');

-- --Testcase 800:
-- create table locp (id serial, a int check (a in (2)), b text);

-- --Testcase 801:
-- alter table utrtest attach partition remp_a_child for values in (1);

-- --Testcase 802:
-- alter table utrtest attach partition locp for values in (2);

-- --Testcase 803:
-- insert into utrtest(a, b) values (1, 'foo');

-- --Testcase 804:
-- insert into utrtest(a, b) values (2, 'qux');

-- --Testcase 805:
-- select tableoid::regclass, a, b FROM utrtest;

-- --Testcase 806:
-- select tableoid::regclass, a, b FROM remp;

-- --Testcase 807:
-- select tableoid::regclass, a, b FROM locp;

-- -- GridDB not support
-- -- It's not allowed to move a row from a partition that is foreign to another

-- --update utrtest set a = 2 where b = 'foo' returning *;

-- -- But the reverse is allowed

-- --Testcase 808:
-- update utrtest set a = 1 where b = 'qux';

-- --Testcase 809:
-- select a, b from utrtest where b = 'qux';

-- --Testcase 810:
-- select tableoid::regclass, a, b FROM utrtest;

-- --Testcase 811:
-- select tableoid::regclass, a, b FROM remp;

-- --Testcase 812:
-- select tableoid::regclass, a, b FROM locp;

-- -- The executor should not let unexercised FDWs shut down

-- --Testcase 813:
-- update utrtest set a = 1 where b = 'foo';

-- -- Test that remote triggers work with update tuple routing

-- --Testcase 814:
-- create trigger remp_br_insert_trigger before insert on remp
-- 	for each row execute procedure br_insert_trigfunc();

-- --Testcase 815:
-- delete from utrtest;

-- --Testcase 816:
-- insert into utrtest(a, b) values (2, 'qux');

-- -- Check case where the foreign partition is a subplan target rel

-- --Testcase 817:
-- explain (verbose, costs off)
-- update utrtest set a = 1 where a = 1 or a = 2;
-- -- The new values are concatenated with ' triggered !'

-- --Testcase 818:
-- update utrtest set a = 1 where a = 1 or a = 2;

-- --Testcase 819:
-- select a, b from utrtest;

-- --Testcase 820:
-- delete from utrtest;

-- --Testcase 821:
-- insert into utrtest(a, b) values (2, 'qux');

-- -- Check case where the foreign partition isn't a subplan target rel

-- --Testcase 822:
-- explain (verbose, costs off)
-- update utrtest set a = 1 where a = 2;
-- -- The new values are concatenated with ' triggered !'

-- --Testcase 823:
-- update utrtest set a = 1 where a = 2;

-- --Testcase 824:
-- select a, b from utrtest;

-- --Testcase 825:
-- drop trigger remp_br_insert_trigger on remp;

-- -- We can move rows to a foreign partition that has been updated already,
-- -- but can't move rows to a foreign partition that hasn't been updated yet

-- --Testcase 826:
-- delete from utrtest;

-- --Testcase 827:
-- insert into utrtest(a, b) values (1, 'foo');

-- --Testcase 828:
-- insert into utrtest(a, b) values (2, 'qux');

-- -- Test the former case:
-- -- with a direct modification plan

-- --Testcase 829:
-- explain (verbose, costs off)
-- update utrtest set a = 1;

-- --Testcase 830:
-- update utrtest set a = 1;

-- --Testcase 831:
-- select a, b from utrtest;

-- --Testcase 832:
-- delete from utrtest;

-- --Testcase 833:
-- insert into utrtest(a, b) values (1, 'foo');

-- --Testcase 834:
-- insert into utrtest(a, b) values (2, 'qux');

-- -- with a non-direct modification plan

-- --Testcase 835:
-- explain (verbose, costs off)
-- update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x;

-- --Testcase 836:
-- update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x;

-- --Testcase 837:
-- select * from utrtest;

-- -- Change the definition of utrtest so that the foreign partition get updated
-- -- after the local partition

-- --Testcase 838:
-- delete from utrtest;

-- --Testcase 839:
-- alter table utrtest detach partition remp;

-- --Testcase 840:
-- drop foreign table remp_a_child;

-- drop table remp;

-- --Testcase 841:
-- create foreign table remp_a_child (id serial, a int check (a in (3)), b text) server griddb_svr options (table_name 'loct21');

-- create table remp (id serial, a int check (a in (3)), b text, spdurl text) PARTITION BY LIST (spdurl);

-- CREATE FOREIGN TABLE remp_a PARTITION OF remp FOR VALUES IN ('/node1/') SERVER spdsrv;

-- --Testcase 842:
-- alter foreign table remp_a_child alter column id options (rowkey 'true');

-- --Testcase 843:
-- alter foreign table remp_a_child drop constraint remp_a_check;

-- --Testcase 844:
-- alter foreign table remp_a_child add check (a in (3));

-- --Testcase 845:
-- alter table utrtest attach partition remp_a_child for values in (3);

-- --Testcase 846:
-- insert into utrtest(a, b) values (2, 'qux');

-- --Testcase 847:
-- insert into utrtest(a, b) values (3, 'xyzzy');

-- -- Test the latter case:
-- -- with a direct modification plan

-- --Testcase 848:
-- explain (verbose, costs off)
-- update utrtest set a = 3;

-- --Testcase 849:
-- update utrtest set a = 3; -- ERROR

-- -- with a non-direct modification plan

-- --Testcase 850:
-- explain (verbose, costs off)
-- update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x;
-- update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x; -- ERROR

-- --Testcase 851:
-- delete from utrtest;

-- --Testcase 852:
-- drop table utrtest;
-- --drop table loct;

-- -- Test copy tuple routing

-- --Testcase 853:
-- create table ctrtest (id serial, a int, b text) partition by list (a);

-- --Testcase 854:
-- create foreign table remp1_a_child (id serial, a int, b text) server griddb_svr options (table_name 'loct12');

-- create table remp1 (id serial, a int, b text, spdurl text) PARTITION BY LIST (spdurl);

-- CREATE FOREIGN TABLE remp1_a PARTITION OF remp1 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- --Testcase 855:
-- create foreign table remp2_a_child (id serial, a int, b text) server griddb_svr options (table_name 'loct21');

-- create table remp2 (id serial, a int, b text, spdurl text) PARTITION BY LIST (spdurl);

-- CREATE FOREIGN TABLE remp2_a PARTITION OF remp2 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- --Testcase 856:
-- alter foreign table remp1_a_child alter column id options (rowkey 'true');

-- --Testcase 857:
-- alter foreign table remp2_a_child alter column id options (rowkey 'true');

-- --Testcase 858:
-- alter table ctrtest attach partition remp1_a_child for values in (1);

-- --Testcase 859:
-- alter table ctrtest attach partition remp2 for values in (2);

-- --Testcase 860:
-- insert into ctrtest(a, b) values (1, 'foo'), (2, 'qux');

-- --Testcase 861:
-- select tableoid::regclass, a, b FROM ctrtest;

-- --Testcase 862:
-- select tableoid::regclass, a, b FROM remp1;

-- --Testcase 863:
-- select tableoid::regclass, b, a FROM remp2;

-- -- GridDB not support partitions by
-- -- Copying into foreign partitions directly should work as well
-- copy remp1(a, b) from stdin;
-- 1	bar
-- \.

-- --Testcase 864:
-- select tableoid::regclass, a, b FROM remp1;

-- --Testcase 865:
-- delete from ctrtest;

-- --Testcase 866:
-- drop table ctrtest;

-- ===================================================================
-- test COPY FROM
-- ===================================================================

--Testcase 867:
create foreign table rem2_a_child (id serial, f1 int, f2 text) server griddb_svr options(table_name 'loct12');

--Testcase 1118:
create table rem2 (id serial, f1 int, f2 text, spdurl text) PARTITION BY LIST (spdurl);

--Testcase 1119:
CREATE FOREIGN TABLE rem2_a PARTITION OF rem2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 868:
alter foreign table rem2_a_child alter column id options (rowkey 'true');

-- Test basic functionality

--Testcase 869:
insert into rem2_a_child(f1, f2) values (1, 'foo'), (2, 'bar');

--Testcase 870:
select f1, f2 from rem2;

--Testcase 871:
delete from rem2_a_child;

-- Test check constraints

--Testcase 872:
alter foreign table rem2_a_child add constraint rem2_f1positive check (f1 >= 0);

-- check constraint is enforced on the remote side, not locally

--Testcase 873:
insert into rem2_a_child(f1, f2) values (1, 'foo'), (2, 'bar');
-- GridDB not support constraint

--insert into rem2(f1, f2) values (-1, 'xyzzy');

--Testcase 874:
select f1, f2 from rem2;

--Testcase 875:
alter foreign table rem2_a_child drop constraint rem2_f1positive;
--alter table loc2 drop constraint loc2_f1positive;

--Testcase 876:
delete from rem2_a_child;

-- Test local triggers

--Testcase 877:
create trigger trig_stmt_before before insert on rem2_a_child
	for each statement execute procedure trigger_func();

--Testcase 878:
create trigger trig_stmt_after after insert on rem2_a_child
	for each statement execute procedure trigger_func();

--Testcase 879:
create trigger trig_row_before before insert on rem2_a_child
	for each row execute procedure trigger_data(23,'skidoo');

--Testcase 880:
create trigger trig_row_after after insert on rem2_a_child
	for each row execute procedure trigger_data(23,'skidoo');

copy rem2_a_child(f1, f2) from stdin;
1	foo
2	bar
\.

--Testcase 881:
select f1, f2 from rem2;

--Testcase 882:
drop trigger trig_row_before on rem2_a_child;

--Testcase 883:
drop trigger trig_row_after on rem2_a_child;

--Testcase 884:
drop trigger trig_stmt_before on rem2_a_child;

--Testcase 885:
drop trigger trig_stmt_after on rem2_a_child;

--Testcase 886:
delete from rem2_a_child;

--Testcase 887:
CREATE FUNCTION trig_row_before_insupdate1() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

--Testcase 888:
create trigger trig_row_before_insert before insert on rem2_a_child
	for each row execute procedure trig_row_before_insupdate1();

-- The new values are concatenated with ' triggered !'
copy rem2_a_child(f1, f2) from stdin;
1	foo
2	bar
\.

--Testcase 889:
select f1, f2 from rem2;

--Testcase 890:
drop trigger trig_row_before_insert on rem2_a_child;

--Testcase 891:
delete from rem2_a_child;

--Testcase 892:
create trigger trig_null before insert on rem2_a_child
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2_a_child(f1, f2) from stdin;
1	foo
2	bar
\.

--Testcase 893:
select f1, f2 from rem2;

--Testcase 894:
drop trigger trig_null on rem2_a_child;

--Testcase 895:
delete from rem2_a_child;

-- Test remote triggers

--Testcase 896:
create trigger trig_row_before_insert before insert on rem2_a_child
	for each row execute procedure trig_row_before_insupdate1();

-- The new values are concatenated with ' triggered !'
copy rem2_a_child(f1, f2) from stdin;
1	foo
2	bar
\.

--Testcase 897:
select f1, f2 from rem2;

--Testcase 898:
drop trigger trig_row_before_insert on rem2_a_child;

--Testcase 899:
delete from rem2_a_child;

--Testcase 900:
create trigger trig_null before insert on rem2_a_child
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2_a_child(f1, f2) from stdin;
1	foo
2	bar
\.

--Testcase 901:
select f1, f2 from rem2;

--Testcase 902:
drop trigger trig_null on rem2_a_child;

--Testcase 903:
delete from rem2_a_child;

-- Test a combination of local and remote triggers

--Testcase 904:
create trigger rem2_trig_row_before before insert on rem2_a_child
	for each row execute procedure trigger_data(23,'skidoo');

--Testcase 905:
create trigger rem2_trig_row_after after insert on rem2_a_child
	for each row execute procedure trigger_data(23,'skidoo');

--Testcase 906:
create trigger loc2_trig_row_before_insert before insert on rem2_a_child
	for each row execute procedure trig_row_before_insupdate1();

copy rem2_a_child(f1, f2) from stdin;
1	foo
2	bar
\.

--Testcase 907:
select f1, f2 from rem2;

--Testcase 908:
drop trigger rem2_trig_row_before on rem2_a_child;

--Testcase 909:
drop trigger rem2_trig_row_after on rem2_a_child;

--Testcase 910:
drop trigger loc2_trig_row_before_insert on rem2_a_child;

--Testcase 911:
delete from rem2_a_child;

-- test COPY FROM with foreign table created in the same transaction
--create table loc3 (f1 int, f2 text);
begin;

--Testcase 912:
create foreign table rem3_a_child (f1 int, f2 text)
	server griddb_svr options(table_name 'loc3');

--Testcase 1120:
create table rem3 (f1 int, f2 text, spdurl text) PARTITION BY LIST (spdurl);

--Testcase 1121:
CREATE FOREIGN TABLE rem3_a PARTITION OF rem3 FOR VALUES IN ('/node1/') SERVER spdsrv;

copy rem3_a_child(f1, f2) from stdin;
1	foo
2	bar
\.
commit;

--Testcase 913:
select * from rem3;

--Testcase 914:
drop foreign table rem3_a_child;

--Testcase 1122:
drop table rem3;
--drop table loc3;

-- -- ===================================================================
-- -- test for TRUNCATE -- griddb not support TRUNCATE command
-- -- ===================================================================
-- 
-- CREATE FOREIGN TABLE tru_ftable (id int)
--        SERVER griddb_svr OPTIONS (table_name 'tru_rtable0');
-- 
-- INSERT INTO tru_ftable (SELECT x FROM generate_series(1,10) x);

-- -- local partition
-- 
-- CREATE TABLE tru_ptable (id int) PARTITION BY HASH(id);
-- 
-- CREATE TABLE tru_ptable__p0 PARTITION OF tru_ptable
--                             FOR VALUES WITH (MODULUS 2, REMAINDER 0);
-- -- remote partition
-- 
-- CREATE FOREIGN TABLE tru_ftable__p1 PARTITION OF tru_ptable
--                                     FOR VALUES WITH (MODULUS 2, REMAINDER 1)
--        SERVER griddb_svr OPTIONS (table_name 'tru_rtable1');

-- 
-- INSERT INTO tru_ptable (SELECT x FROM generate_series(11,20) x);

-- references not support for foreign table -> can not test
-- CREATE FOREIGN TABLE tru_pk_table (id int)
--        SERVER griddb_svr OPTIONS (table_name 'tru_pk_table');
-- CREATE TABLE tru_fk_table(fkey int references tru_pk_table(id));
-- INSERT INTO tru_pk_table (SELECT x FROM generate_series(1,10) x);
-- INSERT INTO tru_fk_table (SELECT x % 10 + 1 FROM generate_series(5,25) x);
-- CREATE FOREIGN TABLE tru_pk_ftable (id int)
--        SERVER griddb_svr OPTIONS (table_name 'tru_pk_table');

-- -- INHERITS
-- 
-- CREATE FOREIGN TABLE tru_ftable_parent (id int)
--        SERVER griddb_svr OPTIONS (table_name 'tru_rtable_parent');
-- 
-- CREATE FOREIGN TABLE tru_ftable_child () INHERITS (tru_ftable_parent)
--        SERVER griddb_svr OPTIONS (table_name 'tru_rtable_child');
-- 
-- INSERT INTO tru_ftable_parent (SELECT x FROM generate_series(1,8) x);
-- 
-- INSERT INTO tru_ftable_child  (SELECT x FROM generate_series(10, 18) x);

-- -- normal truncate
-- 
-- SELECT sum(id) FROM tru_ftable;        -- 55
-- TRUNCATE tru_ftable;
-- -- SELECT count(*) FROM tru_rtable0;		-- 0
-- 
-- SELECT count(*) FROM tru_ftable;		-- 0

-- -- 'truncatable' option
-- 
-- ALTER SERVER griddb_svr OPTIONS (ADD truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- 
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (ADD truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted
-- 
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- 
-- ALTER SERVER griddb_svr OPTIONS (DROP truncatable);
-- 
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- 
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted

-- -- partitioned table with both local and foreign tables as partitions
-- 
-- SELECT sum(id) FROM tru_ptable;        -- 155
-- TRUNCATE tru_ptable;
-- 
-- SELECT count(*) FROM tru_ptable;		-- 0
-- 
-- SELECT count(*) FROM tru_ptable__p0;	-- 0
-- 
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

-- truncate with ONLY clause
-- Since ONLY is specified, the table tru_ftable_child that inherits
-- tru_ftable_parent locally is not truncated.
-- TRUNCATE ONLY tru_ftable_parent;
-- 
-- SELECT sum(id) FROM tru_ftable_parent;  -- 126
-- TRUNCATE tru_ftable_parent;
-- 
-- SELECT count(*) FROM tru_ftable_parent; -- 0

-- griddb have not INHERITS feature -- can not check this case
-- -- in case when remote table has inherited children
-- CREATE TABLE tru_rtable0_child () INHERITS (tru_rtable0);
-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(5,9) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(10,14) x);
-- SELECT sum(id) FROM tru_ftable;   -- 95

-- Both parent and child tables in the foreign server are truncated
-- even though ONLY is specified because ONLY has no effect
-- when truncating a foreign table.
-- TRUNCATE ONLY tru_ftable;
-- SELECT count(*) FROM tru_ftable;   -- 0

-- INSERT INTO tru_ftable (SELECT x FROM generate_series(21,25) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(26,30) x);
-- SELECT sum(id) FROM tru_ftable;		-- 255
-- TRUNCATE tru_ftable;			-- truncate both of parent and child
-- SELECT count(*) FROM tru_ftable;    -- 0

-- -- cleanup
-- 
-- DROP FOREIGN TABLE tru_ftable_parent, tru_ftable_child, tru_ftable__p1, tru_ftable;
-- 
-- DROP TABLE tru_ptable, tru_ptable__p0, tru_pk_table, tru_rtable_parent,tru_rtable_child;

-- ===================================================================
-- test IMPORT FOREIGN SCHEMA
-- ===================================================================

--Testcase 915:
CREATE SCHEMA import_grid1;
IMPORT FOREIGN SCHEMA "S 1" LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1)
	FROM SERVER griddb_svr INTO import_grid1;

--Testcase 916:
\det+ import_grid1.*

--Testcase 917:
\d import_grid1.*

-- Options
-- GridDB does not support the option "import_default"
/*
CREATE SCHEMA import_grid2;
IMPORT FOREIGN SCHEMA "S 1" LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1)
	FROM SERVER griddb_svr INTO import_grid2
  OPTIONS (import_default 'true');

\det+ import_grid2.*

\d import_grid2.*

CREATE SCHEMA import_grid3;
IMPORT FOREIGN SCHEMA "S 1" LIMIT TO
	("T0", "T1", "T2", "T3", "T4", ft1)
	FROM SERVER griddb_svr INTO import_grid3
  OPTIONS (import_collate 'false', import_not_null 'false');

\det+ import_grid3.*

\d import_grid3.*
*/
-- Check LIMIT TO and EXCEPT

--Testcase 918:
CREATE SCHEMA import_grid4;
IMPORT FOREIGN SCHEMA griddb_schema LIMIT TO ("T1", nonesuch)
  FROM SERVER griddb_svr INTO import_grid4;

--Testcase 919:
\det+ import_grid4.*

IMPORT FOREIGN SCHEMA griddb_schema EXCEPT ("T1", "T2", nonesuch)
FROM SERVER griddb_svr INTO import_grid4;

--Testcase 920:
\det+ import_grid4.*

-- Assorted error cases
IMPORT FOREIGN SCHEMA griddb_schema FROM SERVER griddb_svr INTO import_grid4;
IMPORT FOREIGN SCHEMA nonesuch FROM SERVER griddb_svr INTO import_grid4; -- same as 'public'
IMPORT FOREIGN SCHEMA nonesuch FROM SERVER griddb_svr INTO notthere;
IMPORT FOREIGN SCHEMA nonesuch FROM SERVER nowhere INTO notthere;

-- Check case of a type present only on the remote server.
-- We can fake this by dropping the type locally in our transaction.

--Testcase 921:
CREATE SCHEMA import_grid5;
BEGIN;
IMPORT FOREIGN SCHEMA griddb_schema LIMIT TO ("T1")
FROM SERVER griddb_svr INTO import_grid5; --ERROR
ROLLBACK;

-- Skip, does not support option 'fetch_size'
--BEGIN;
--CREATE SERVER fetch101 FOREIGN DATA WRAPPER griddb_fdw OPTIONS( fetch_size '101' );
/*

SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'fetch101'
AND srvoptions @> array['fetch_size=101'];

ALTER SERVER fetch101 OPTIONS( SET fetch_size '202' );

SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'fetch101'
AND srvoptions @> array['fetch_size=101'];

SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'fetch101'
AND srvoptions @> array['fetch_size=202'];

CREATE FOREIGN TABLE table30000 ( x int ) SERVER fetch101 OPTIONS ( fetch_size '30000' );

SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30000'::regclass
AND ftoptions @> array['fetch_size=30000'];

ALTER FOREIGN TABLE table30000 OPTIONS ( SET fetch_size '60000');

SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30000'::regclass
AND ftoptions @> array['fetch_size=30000'];

SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30000'::regclass
AND ftoptions @> array['fetch_size=60000'];

ROLLBACK;
*/
-- Drop schemas

--Testcase 922:
SET client_min_messages to WARNING;

--Testcase 923:
DROP SCHEMA import_grid1 CASCADE;

--Testcase 924:
DROP SCHEMA import_grid2 CASCADE;

--Testcase 925:
DROP SCHEMA import_grid3 CASCADE;

--Testcase 926:
DROP SCHEMA import_grid4 CASCADE;

--Testcase 927:
DROP SCHEMA import_grid5 CASCADE;

--Testcase 928:
SET client_min_messages to NOTICE;

-- PGSpider Extension only support Partition by List. This test is not
-- suitable.
-- -- ===================================================================
-- -- test partitionwise joins
-- -- ===================================================================

-- --Testcase 929:
-- SET enable_partitionwise_join=on;

-- --Testcase 930:
-- CREATE TABLE fprt1 (a int, b int, c text) PARTITION BY RANGE(a);

-- --Testcase 931:
-- INSERT INTO "S 1".fprt1_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 2) i;

-- --Testcase 932:
-- INSERT INTO "S 1".fprt1_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 2) i;

-- --Testcase 933:
-- CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (0) TO (250)
-- 	SERVER griddb_svr OPTIONS (table_name 'fprt1_p1');

-- --Testcase 934:
-- CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (250) TO (500)
-- 	SERVER griddb_svr OPTIONS (TABLE_NAME 'fprt1_p2');
-- --ANALYZE fprt1;
-- --ANALYZE fprt1_p1;
-- --ANALYZE fprt1_p2;

-- --Testcase 935:
-- CREATE TABLE fprt2 (a int, b int, c text) PARTITION BY RANGE(b);

-- --Testcase 936:
-- INSERT INTO "S 1".fprt2_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 3) i;

-- --Testcase 937:
-- INSERT INTO "S 1".fprt2_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 3) i;

-- --Testcase 938:
-- CREATE FOREIGN TABLE ftprt2_p1 (a int, b int, c text)
-- 	SERVER griddb_svr OPTIONS (table_name 'fprt2_p1');

-- --Testcase 939:
-- ALTER TABLE fprt2 ATTACH PARTITION ftprt2_p1 FOR VALUES FROM (0) TO (250);

-- --Testcase 940:
-- CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (250) TO (500)
-- 	SERVER griddb_svr OPTIONS (table_name 'fprt2_p2');
-- --ANALYZE fprt2;
-- --ANALYZE fprt2_p1;
-- --ANALYZE fprt2_p2;
-- -- inner join three tables

-- --Testcase 941:
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;

-- --Testcase 942:
-- SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;

-- -- left outer join + nullable clasue

-- --Testcase 943:
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;

-- --Testcase 944:
-- SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;

-- -- with whole-row reference; partitionwise join does not apply

-- --Testcase 945:
-- EXPLAIN (COSTS OFF)
-- SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;

-- --Testcase 946:
-- SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;

-- -- join with lateral reference

-- --Testcase 947:
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;

-- --Testcase 948:
-- SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;

-- -- with PHVs, partitionwise join selected but no join pushdown

-- --Testcase 949:
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- --Testcase 950:
-- SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- -- test FOR UPDATE; partitionwise join does not apply

-- --Testcase 951:
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;

-- --Testcase 952:
-- SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;

-- --Testcase 953:
-- RESET enable_partitionwise_join;

-- -- ===================================================================
-- -- test partitionwise aggregates
-- -- ===================================================================

-- --Testcase 954:
-- CREATE TABLE pagg_tab (t int, a int, b int, c text) PARTITION BY RANGE(a);

-- --Testcase 955:
-- INSERT INTO "S 1".pagg_tab_p1 SELECT i, i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 10;

-- --Testcase 956:
-- INSERT INTO "S 1".pagg_tab_p2 SELECT i, i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 20 and (i % 30) >= 10;

-- --Testcase 957:
-- INSERT INTO "S 1".pagg_tab_p3 SELECT i, i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 30 and (i % 30) >= 20;

-- -- Create foreign partitions

-- --Testcase 958:
-- CREATE FOREIGN TABLE fpagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0) TO (10) SERVER griddb_svr OPTIONS (table_name 'pagg_tab_p1');

-- --Testcase 959:
-- CREATE FOREIGN TABLE fpagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10) TO (20) SERVER griddb_svr OPTIONS (table_name 'pagg_tab_p2');;

-- --Testcase 960:
-- CREATE FOREIGN TABLE fpagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20) TO (30) SERVER griddb_svr OPTIONS (table_name 'pagg_tab_p3');;
-- --ANALYZE pagg_tab;
-- --ANALYZE fpagg_tab_p1;
-- --ANALYZE fpagg_tab_p2;
-- --ANALYZE fpagg_tab_p3;
-- -- When GROUP BY clause matches with PARTITION KEY.
-- -- Plan with partitionwise aggregates is disabled

-- --Testcase 961:
-- SET enable_partitionwise_aggregate TO false;

-- --Testcase 962:
-- EXPLAIN (COSTS OFF)
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- -- Plan with partitionwise aggregates is enabled

-- --Testcase 963:
-- SET enable_partitionwise_aggregate TO true;

-- --Testcase 964:
-- EXPLAIN (COSTS OFF)
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- --Testcase 965:
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- -- Check with whole-row reference
-- -- Should have all the columns in the target list for the given relation

-- --Testcase 966:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- --Testcase 967:
-- SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- -- When GROUP BY clause does not match with PARTITION KEY.

-- --Testcase 968:
-- EXPLAIN (COSTS OFF)
-- SELECT b, avg(a), max(a), count(*) FROM pagg_tab GROUP BY b HAVING sum(a) < 700 ORDER BY 1;

-- Skip test because GridDB not support no super user
-- ===================================================================
-- access rights and superuser
-- ===================================================================
/*
-- Non-superuser cannot create a FDW without a password in the connstr
CREATE ROLE regress_nosuper NOSUPERUSER;

GRANT USAGE ON FOREIGN DATA WRAPPER griddb_fdw TO regress_nosuper;

SET ROLE regress_nosuper;

SHOW is_superuser;

-- This will be OK, we can create the FDW
CREATE SERVER griddb_fdw_nopw FOREIGN DATA WRAPPER griddb_fdw
    OPTIONS (host :GRIDDB_HOST, port :GRIDDB_PORT, clustername 'griddbfdwTestCluster');

-- But creation of user mappings for non-superusers should fail
CREATE USER MAPPING FOR public SERVER griddb_fdw_nopw OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
CREATE USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw;

CREATE FOREIGN TABLE ft1_nopw (
	c1 int OPTIONS (rowkey 'true'),
	c2 int NOT NULL,
	c3 text,
	c4 timestamp,
	c5 timestamp,
	c6 text,
	c7 text default 'ft1',
	c8 text
) SERVER griddb_fdw_nopw OPTIONS (table_name 'ft1');

SELECT * FROM ft1_nopw LIMIT 1;

-- If we add a password to the connstr it'll fail, because we don't allow passwords
-- in connstrs only in user mappings.

DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER griddb_fdw_nopw OPTIONS (ADD password 'dummypw')$$;
    END;
$d$;

-- If we add a password for our user mapping instead, we should get a different
-- error because the password wasn't actually *used* when we run with trust auth.
--
-- This won't work with installcheck, but neither will most of the FDW checks.

ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (ADD password 'dummypw');

SELECT * FROM ft1_nopw LIMIT 1;

-- Unpriv user cannot make the mapping passwordless
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (ADD password_required 'false');

SELECT * FROM ft1_nopw LIMIT 1;

RESET ROLE;

-- But the superuser can
ALTER USER MAPPING FOR regress_nosuper SERVER griddb_fdw_nopw OPTIONS (ADD password_required 'false');

SET ROLE regress_nosuper;

-- Should finally work now
SELECT * FROM ft1_nopw LIMIT 1;

-- unpriv user also cannot set sslcert / sslkey on the user mapping
-- first set password_required so we see the right error messages
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (SET password_required 'true');
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (ADD sslcert 'foo.crt');
ALTER USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw OPTIONS (ADD sslkey 'foo.key');

-- We're done with the role named after a specific user and need to check the
-- changes to the public mapping.
DROP USER MAPPING FOR CURRENT_USER SERVER griddb_fdw_nopw;

-- This will fail again as it'll resolve the user mapping for public, which
-- lacks password_required=false
SELECT * FROM ft1_nopw LIMIT 1;

RESET ROLE;

-- The user mapping for public is passwordless and lacks the password_required=false
-- mapping option, but will work because the current user is a superuser.
SELECT * FROM ft1_nopw LIMIT 1;

-- cleanup
DROP USER MAPPING FOR public SERVER griddb_fdw_nopw;
DROP OWNED BY regress_nosuper;
DROP ROLE regress_nosuper;
*/

-- Clean-up

--Testcase 969:
RESET enable_partitionwise_aggregate;

-- GridDB has different result because GridDB does not run test check constraints
-- Two-phase transactions are not supported.
BEGIN;

--Testcase 970:
SELECT count(*) FROM ft1;
-- error here

--Testcase 971:
PREPARE TRANSACTION 'fdw_tpc';
ROLLBACK;

--Testcase 972:
SET client_min_messages to WARNING;


-- ===================================================================
-- reestablish new connection
-- ===================================================================
--Testcase 974:
SELECT * FROM ft1 LIMIT 10;
\! ./sql/init_data/griddb_fdw/griddb_restart_service.sh > /dev/null
--Testcase 975:
SELECT * FROM ft1 LIMIT 10;

-- =============================================================================
-- test connection invalidation cases and griddb_get_connections function
-- =============================================================================
-- Let's ensure to close all the existing cached connections.

--Testcase 976:
SELECT 1 FROM griddb_disconnect_all();
-- No cached connections, so no records should be output.

--Testcase 977:
SELECT server_name FROM griddb_get_connections() ORDER BY 1;
-- This test case is for closing the connection in pgfdw_xact_callback
BEGIN;
-- Connection xact depth becomes 1 i.e. the connection is in midst of the xact.

--Testcase 978:
SELECT 1 FROM ft1 LIMIT 1;

--Testcase 979:
SELECT 1 FROM ft7 LIMIT 1;
-- List all the existing cached connections. griddb_svr and griddb_svr3 should be
-- output.

--Testcase 980:
SELECT server_name FROM griddb_get_connections() ORDER BY 1;
-- Connections are not closed at the end of the alter and drop statements.
-- That's because the connections are in midst of this xact,
-- they are just marked as invalid in pgfdw_inval_callback.

--Testcase 981:
ALTER SERVER griddb_svr OPTIONS (ADD batch_size '10');

--Testcase 982:
DROP SERVER griddb_svr3 CASCADE;
-- List all the existing cached connections. griddb_svr and griddb_svr3
-- should be output as invalid connections. Also the server name for
-- griddb_svr3 should be NULL because the server was dropped.

--Testcase 983:
SELECT * FROM griddb_get_connections() ORDER BY 1;
-- The invalid connections get closed in pgfdw_xact_callback during commit.
COMMIT;
-- All cached connections were closed while committing above xact, so no
-- records should be output.

--Testcase 984:
SELECT server_name FROM griddb_get_connections() ORDER BY 1;

--Testcase 985:
ALTER SERVER griddb_svr OPTIONS (DROP batch_size);
-- =======================================================================
-- test griddb_disconnect and griddb_disconnect_all functions
-- =======================================================================
BEGIN;
-- Ensure to cache griddb_svr connection.

--Testcase 986:
SELECT 1 FROM ft1 LIMIT 1;
-- Ensure to cache griddb_svr2 connection.

--Testcase 987:
SELECT 1 FROM ft6 LIMIT 1;
-- List all the existing cached connections. griddb_svr and griddb_svr2 should be
-- output.

--Testcase 988:
SELECT server_name FROM griddb_get_connections() ORDER BY 1;
-- Issue a warning and return false as griddb_svr connection is still in use and
-- can not be closed.

--Testcase 989:
SELECT griddb_disconnect('griddb_svr');
-- List all the existing cached connections. griddb_svr and griddb_svr2 should be
-- output.

--Testcase 990:
SELECT server_name FROM griddb_get_connections() ORDER BY 1;
-- Return false as connections are still in use, warnings are issued.
-- But disable warnings temporarily because the order of them is not stable.

--Testcase 991:
SET client_min_messages = 'ERROR';

--Testcase 992:
SELECT griddb_disconnect_all();

--Testcase 993:
RESET client_min_messages;
COMMIT;
-- Ensure that griddb_svr2 connection is closed.

--Testcase 994:
SELECT 1 FROM griddb_disconnect('griddb_svr2');

--Testcase 995:
SELECT server_name FROM griddb_get_connections() WHERE server_name = 'griddb_svr2';
-- Return false as griddb_svr2 connection is closed already.

--Testcase 996:
SELECT griddb_disconnect('griddb_svr2');
-- Return an error as there is no foreign server with given name.

--Testcase 997:
SELECT griddb_disconnect('unknownserver');
-- Let's ensure to close all the existing cached connections.

--Testcase 998:
SELECT 1 FROM griddb_disconnect_all();
-- No cached connections, so no records should be output.

--Testcase 999:
SELECT server_name FROM griddb_get_connections() ORDER BY 1;

-- =============================================================================
-- test case for having multiple cached connections for a foreign server
-- =============================================================================

--Testcase 1000:
CREATE ROLE regress_multi_conn_user1 SUPERUSER;

--Testcase 1001:
CREATE ROLE regress_multi_conn_user2 SUPERUSER;

--Testcase 1002:
CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
--Testcase 1144:
CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER spdsrv;

--Testcase 1003:
CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER griddb_svr OPTIONS (username :GRIDDB_USER, password :GRIDDB_PASS);
--Testcase 1145:
CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER spdsrv;

BEGIN;
-- Will cache griddb_svr connection with user mapping for regress_multi_conn_user1

--Testcase 1004:
SET ROLE regress_multi_conn_user1;

--Testcase 1005:
SELECT 1 FROM ft1 LIMIT 1;

--Testcase 1006:
RESET ROLE;

-- Will cache griddb_svr connection with user mapping for regress_multi_conn_user2

--Testcase 1007:
SET ROLE regress_multi_conn_user2;

--Testcase 1008:
SELECT 1 FROM ft1 LIMIT 1;

--Testcase 1009:
RESET ROLE;

-- Should output two connections for griddb_svr server

--Testcase 1010:
SELECT server_name FROM griddb_get_connections() ORDER BY 1;
COMMIT;
-- Let's ensure to close all the existing cached connections.

--Testcase 1011:
SELECT 1 FROM griddb_disconnect_all();
-- No cached connections, so no records should be output.

--Testcase 1012:
SELECT server_name FROM griddb_get_connections() ORDER BY 1;

-- Clean up

--Testcase 1013:
DROP USER MAPPING FOR regress_multi_conn_user1 SERVER griddb_svr;

--Testcase 1014:
DROP USER MAPPING FOR regress_multi_conn_user2 SERVER griddb_svr;

--Testcase 1146:
DROP USER MAPPING FOR regress_multi_conn_user1 SERVER spdsrv;

--Testcase 1147:
DROP USER MAPPING FOR regress_multi_conn_user2 SERVER spdsrv;

--Testcase 1015:
DROP ROLE regress_multi_conn_user1;

--Testcase 1016:
DROP ROLE regress_multi_conn_user2;

-- ===================================================================
-- Test foreign server level option keep_connections
-- ===================================================================
-- By default, the connections associated with foreign server are cached i.e.
-- keep_connections option is on. Set it to off.

--Testcase 1017:
ALTER SERVER griddb_svr OPTIONS (keep_connections 'off');
-- connection to griddb_svr server is closed at the end of xact
-- as keep_connections was set to off.

--Testcase 1018:
SELECT 1 FROM ft1 LIMIT 1;
-- No cached connections, so no records should be output.

--Testcase 1019:
SELECT server_name FROM griddb_get_connections() ORDER BY 1;

--Testcase 1020:
ALTER SERVER griddb_svr OPTIONS (SET keep_connections 'on');

-- ===================================================================
-- batch insert
-- ===================================================================

BEGIN;

--Testcase 1021:
CREATE SERVER batch10 FOREIGN DATA WRAPPER griddb_fdw OPTIONS( batch_size '10' );

--Testcase 1022:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=10'];

--Testcase 1023:
ALTER SERVER batch10 OPTIONS( SET batch_size '20' );

--Testcase 1024:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=10'];

--Testcase 1025:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=20'];

--Testcase 1026:
CREATE FOREIGN TABLE table30_a_child ( x int ) SERVER batch10 OPTIONS ( batch_size '30' );

CREATE TABLE table30 ( x int, spdurl text) PARTITION BY LIST (spdurl);

CREATE FOREIGN TABLE table30_a PARTITION OF table30 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 1027:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30_a_child'::regclass
AND ftoptions @> array['batch_size=30'];

--Testcase 1028:
ALTER FOREIGN TABLE table30_a_child OPTIONS ( SET batch_size '40');

--Testcase 1029:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30_a_child'::regclass
AND ftoptions @> array['batch_size=30'];

--Testcase 1030:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30_a_child'::regclass
AND ftoptions @> array['batch_size=40'];

ROLLBACK;

--Testcase 1031:
CREATE FOREIGN TABLE ftable_a_child ( x int OPTIONS (rowkey 'true') ) SERVER griddb_svr OPTIONS ( table_name 'batch_table', batch_size '10' );

CREATE TABLE ftable ( x int, spdurl text) PARTITION BY LIST (spdurl);

CREATE FOREIGN TABLE ftable_a PARTITION OF ftable FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 1032:
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable_a_child SELECT * FROM generate_series(1, 10) i;

--Testcase 1033:
INSERT INTO ftable_a_child SELECT * FROM generate_series(1, 10) i;

--Testcase 1034:
INSERT INTO ftable_a_child SELECT * FROM generate_series(11, 31) i;

--Testcase 1035:
INSERT INTO ftable_a_child VALUES (32);

--Testcase 1036:
INSERT INTO ftable_a_child VALUES (33), (34);

--Testcase 1037:
SELECT COUNT(*) FROM ftable;
--Testcase 1038:
DELETE FROM ftable_a_child;

DROP FOREIGN TABLE ftable_a_child;

--Testcase 1039:
DROP TABLE ftable;

-- try if large batches exceed max number of bind parameters
--Testcase 1040:
CREATE FOREIGN TABLE ftable_a_child ( x int OPTIONS (rowkey 'true') ) SERVER griddb_svr OPTIONS ( table_name 'batch_table', batch_size '100000' );

CREATE TABLE ftable ( x int, spdurl text) PARTITION BY LIST (spdurl);

CREATE FOREIGN TABLE ftable_a PARTITION OF ftable FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 1041:
INSERT INTO ftable_a_child SELECT * FROM generate_series(1, 70000) i;

--Testcase 1042:
SELECT COUNT(*) FROM ftable;

-- griddb_fdw can not delete the large number of rows in one query
-- because of the transaction timeout, so we delete small parts
-- from foreign table
--Testcase 1089:
DELETE FROM ftable_a_child WHERE x < 10000;
--Testcase 1090:
DELETE FROM ftable_a_child WHERE x < 20000;
--Testcase 1091:
DELETE FROM ftable_a_child WHERE x < 30000;
--Testcase 1092:
DELETE FROM ftable_a_child WHERE x < 40000;
--Testcase 1093:
DELETE FROM ftable_a_child WHERE x < 50000;
--Testcase 1094:
DELETE FROM ftable_a_child WHERE x < 60000;
--Testcase 1095:
DELETE FROM ftable_a_child WHERE x < 70000;
--Testcase 1096:
DELETE FROM ftable_a_child;

--Testcase 1043:
DROP FOREIGN TABLE ftable_a_child;

DROP TABLE ftable;

-- Disable batch insert
--Testcase 1044:
CREATE FOREIGN TABLE ftable_a_child ( x int OPTIONS (rowkey 'true') ) SERVER griddb_svr OPTIONS ( table_name 'batch_table', batch_size '1' );

CREATE TABLE ftable ( x int, spdurl text) PARTITION BY LIST (spdurl);

CREATE FOREIGN TABLE ftable_a PARTITION OF ftable FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 1045:
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable_a_child VALUES (1), (2);

--Testcase 1046:
INSERT INTO ftable_a_child VALUES (1), (2);

--Testcase 1047:
SELECT COUNT(*) FROM ftable;

-- Disable batch inserting into foreign tables with BEFORE ROW INSERT triggers
-- even if the batch_size option is enabled.
ALTER FOREIGN TABLE ftable_a_child OPTIONS ( SET batch_size '10' );
CREATE TRIGGER trig_row_before BEFORE INSERT ON ftable
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable VALUES (3), (4);
INSERT INTO ftable_a_child VALUES (3), (4);
SELECT COUNT(*) FROM ftable;

-- Clean up
DROP TRIGGER trig_row_before ON ftable;
--Testcase 1048:
DROP FOREIGN TABLE ftable_a_child;

DROP TABLE ftable;

-- PGSpider Extension only support Partition by List. This test is not
-- suitable.
-- -- Use partitioning

-- --Testcase 1049:
-- CREATE TABLE batch_table ( x int ) PARTITION BY HASH (x);

-- --Testcase 1050:
-- CREATE FOREIGN TABLE batch_table_p0f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 0)
-- 	SERVER griddb_svr
-- 	OPTIONS (table_name 'batch_table_p0', batch_size '10');

-- --Testcase 1051:
-- CREATE FOREIGN TABLE batch_table_p1f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 1)
-- 	SERVER griddb_svr
-- 	OPTIONS (table_name 'batch_table_p1', batch_size '1');

-- --Testcase 1052:
-- CREATE TABLE batch_table_p2
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 2);

-- --Testcase 1053:
-- INSERT INTO batch_table SELECT * FROM generate_series(1, 66) i;

-- --Testcase 1054:
-- SELECT COUNT(*) FROM batch_table;

-- -- Check that enabling batched inserts doesn't interfere with cross-partition
-- -- updates

-- --Testcase 1055:
-- CREATE TABLE batch_cp_upd_test (id int, a int) PARTITION BY LIST (a);

-- --Testcase 1056:
-- CREATE FOREIGN TABLE batch_cp_upd_test1_f (id int OPTIONS (rowkey 'true'), a int)
-- 	SERVER griddb_svr
-- 	OPTIONS (table_name 'batch_cp_upd_test1', batch_size '10');

-- --Testcase 1057:
-- ALTER TABLE batch_cp_upd_test ATTACH PARTITION batch_cp_upd_test1_f FOR VALUES IN (1);

-- --Testcase 1058:
-- CREATE TABLE batch_cp_up_test1 PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (2);

-- --Testcase 1059:
-- INSERT INTO batch_cp_upd_test VALUES (1, 1), (2, 2);

-- -- The following moves a row from the local partition to the foreign one

-- --Testcase 1060:
-- UPDATE batch_cp_upd_test t SET a = 1 FROM (VALUES (1), (2)) s(a) WHERE t.a = s.a;

-- --Testcase 1061:
-- SELECT tableoid::regclass, * FROM batch_cp_upd_test;

-- -- Clean up

-- --Testcase 1062:
-- DROP TABLE batch_table, batch_cp_upd_test CASCADE;

-- -- ===================================================================
-- -- test asynchronous execution
-- -- ===================================================================

-- ALTER SERVER griddb_svr OPTIONS (DROP extensions);
-- ALTER SERVER griddb_svr OPTIONS (ADD async_capable 'true');
-- ALTER SERVER griddb_svr2 OPTIONS (ADD async_capable 'true');

-- CREATE TABLE async_pt (a int, b int, c text) PARTITION BY RANGE (a);
-- CREATE TABLE base_tbl1 (a int, b int, c text);
-- CREATE TABLE base_tbl2 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p1 PARTITION OF async_pt FOR VALUES FROM (1000) TO (2000)
--   SERVER griddb_svr OPTIONS (table_name 'base_tbl1');
-- CREATE FOREIGN TABLE async_p2 PARTITION OF async_pt FOR VALUES FROM (2000) TO (3000)
--   SERVER griddb_svr2 OPTIONS (table_name 'base_tbl2');
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

-- -- Check case where multiple partitions use the same connection
-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p3 PARTITION OF async_pt FOR VALUES FROM (3000) TO (4000)
--   SERVER griddb_svr2 OPTIONS (table_name 'base_tbl3');
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

-- EXPLAIN (VERBOSE, COSTS OFF)
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;

-- -- Check with foreign modify
-- CREATE TABLE local_tbl (a int, b int, c text);
-- INSERT INTO local_tbl VALUES (1505, 505, 'foo');

-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE remote_tbl (a int, b int, c text)
--   SERVER griddb_svr OPTIONS (table_name 'base_tbl3');
-- INSERT INTO remote_tbl VALUES (2505, 505, 'bar');

-- CREATE TABLE base_tbl4 (a int, b int, c text);
-- CREATE FOREIGN TABLE insert_tbl (a int, b int, c text)
--   SERVER griddb_svr OPTIONS (table_name 'base_tbl4');

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
-- SELECT a FROM base_tbl WHERE a IN (SELECT a FROM foreign_tbl);
-- SELECT a FROM base_tbl WHERE a IN (SELECT a FROM foreign_tbl);

-- -- Clean up
-- DROP FOREIGN TABLE foreign_tbl CASCADE;
-- DROP TABLE base_tbl;

-- ALTER SERVER griddb_svr OPTIONS (DROP async_capable);
-- ALTER SERVER griddb_svr2 OPTIONS (DROP async_capable);

-- ===================================================================
-- test invalid server, foreign table and foreign data wrapper options
-- ===================================================================
-- Invalid fdw_startup_cost option
--Testcase 1097:
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER griddb_fdw
-- 	OPTIONS(fdw_startup_cost '100$%$#$#');
-- -- Invalid fdw_tuple_cost option
-- --Testcase 1098:
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER griddb_fdw
-- 	OPTIONS(fdw_tuple_cost '100$%$#$#');
-- -- Invalid fetch_size option
-- --CREATE FOREIGN TABLE inv_fsz (c1 int )
-- --	SERVER griddb_svr OPTIONS (fetch_size '100$%$#$#');
-- -- Invalid batch_size option
-- --Testcase 1099:
-- CREATE FOREIGN TABLE inv_bsz (c1 int )
-- 	SERVER griddb_svr OPTIONS (batch_size '100$%$#$#');

-- -- No option is allowed to be specified at foreign data wrapper level
-- ALTER FOREIGN DATA WRAPPER griddb_fdw OPTIONS (nonexistent 'fdw');

-- -- ===================================================================
-- -- test postgres_fdw.application_name GUC
-- -- ===================================================================
-- --- Turn debug_discard_caches off for this test to make sure that
-- --- the remote connection is alive when checking its application_name.
-- SET debug_discard_caches = 0;

-- -- Specify escape sequences in application_name option of a server
-- -- object so as to test that they are replaced with status information
-- -- expectedly.
-- --
-- -- Since pg_stat_activity.application_name may be truncated to less than
-- -- NAMEDATALEN characters, note that substring() needs to be used
-- -- at the condition of test query to make sure that the string consisting
-- -- of database name and process ID is also less than that.
-- ALTER SERVER loopback2 OPTIONS (application_name 'fdw_%d%p');
-- SELECT 1 FROM ft6 LIMIT 1;
-- SELECT pg_terminate_backend(pid, 180000) FROM pg_stat_activity
--   WHERE application_name =
--     substring('fdw_' || current_database() || pg_backend_pid() for
--       current_setting('max_identifier_length')::int);

-- -- postgres_fdw.application_name overrides application_name option
-- -- of a server object if both settings are present.
-- SET postgres_fdw.application_name TO 'fdw_%a%u%%';
-- SELECT 1 FROM ft6 LIMIT 1;
-- SELECT pg_terminate_backend(pid, 180000) FROM pg_stat_activity
--   WHERE application_name =
--     substring('fdw_' || current_setting('application_name') ||
--       CURRENT_USER || '%' for current_setting('max_identifier_length')::int);

-- -- Test %c (session ID) and %C (cluster name) escape sequences.
-- SET postgres_fdw.application_name TO 'fdw_%C%c';
-- SELECT 1 FROM ft6 LIMIT 1;
-- SELECT pg_terminate_backend(pid, 180000) FROM pg_stat_activity
--   WHERE application_name =
--     substring('fdw_' || current_setting('cluster_name') ||
--       to_hex(trunc(EXTRACT(EPOCH FROM (SELECT backend_start FROM
--       pg_stat_get_activity(pg_backend_pid()))))::integer) || '.' ||
--       to_hex(pg_backend_pid())
--       for current_setting('max_identifier_length')::int);

-- --Clean up
-- RESET postgres_fdw.application_name;
-- RESET debug_discard_caches;

-- -- ===================================================================
-- -- test parallel commit
-- -- ===================================================================
-- ALTER SERVER loopback OPTIONS (ADD parallel_commit 'true');
-- ALTER SERVER loopback2 OPTIONS (ADD parallel_commit 'true');

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

-- ALTER SERVER loopback OPTIONS (DROP parallel_commit);
-- ALTER SERVER loopback2 OPTIONS (DROP parallel_commit);

-- Drop all foreign tables

--Testcase 973:
DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename);
    END LOOP;
END $$;

--Testcase 1063:
DROP USER MAPPING FOR public SERVER griddb_svr;

--Testcase 1064:
DROP USER MAPPING FOR public SERVER griddb_svr2;

--Testcase 1065:
DROP USER MAPPING FOR public SERVER testserver1;

--Testcase 1123:
DROP USER MAPPING FOR CURRENT_USER SERVER spdsrv;

RESET client_min_messages;

--Testcase 1066:
DROP SERVER griddb_svr CASCADE;

--Testcase 1067:
DROP SERVER griddb_svr2 CASCADE;

--Testcase 1068:
DROP SERVER testserver1 CASCADE;

--Testcase 1124:
DROP SERVER spdsrv CASCADE;

--Testcase 1069:
DROP EXTENSION griddb_fdw CASCADE;

--Testcase 1125:
DROP EXTENSION pgspider_ext CASCADE;

--Testcase 1070:
SET client_min_messages to NOTICE;
