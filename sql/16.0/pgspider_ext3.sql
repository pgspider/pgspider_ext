-- This test is ported from postgres_fdw. 
-- ===================================================================
-- Create FDW objects
-- ===================================================================
--Testcase 1:
CREATE EXTENSION postgres_fdw;

--Testcase 2:
CREATE SERVER testserver1 FOREIGN DATA WRAPPER postgres_fdw;
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
        EXECUTE $$CREATE SERVER loopback2 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
		EXECUTE $$CREATE SERVER loopback3 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;

--Testcase 3:
CREATE USER MAPPING FOR public SERVER testserver1
	OPTIONS (user 'value', password 'value');
--Testcase 4:
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;
--Testcase 5:
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback2;
--Testcase 6:
CREATE USER MAPPING FOR public SERVER loopback3;

--Testcase 7:
CREATE EXTENSION pgspider_ext;
--Testcase 8:
CREATE SERVER spdsrv FOREIGN DATA WRAPPER pgspider_ext;
--Testcase 9:
CREATE USER MAPPING FOR CURRENT_USER SERVER spdsrv;

-- ===================================================================
-- Create objects used through FDW loopback server
-- ===================================================================
--Testcase 10:
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
--Testcase 11:
CREATE SCHEMA "S 1";
--Testcase 12:
CREATE TABLE "S 1"."T 1" (
	"C 1" int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10),
	c8 user_enum,
	CONSTRAINT t1_pkey PRIMARY KEY ("C 1")
);
--Testcase 13:
CREATE TABLE "S 1"."T 2" (
	c1 int NOT NULL,
	c2 text,
	CONSTRAINT t2_pkey PRIMARY KEY (c1)
);
--Testcase 14:
CREATE TABLE "S 1"."T 3" (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	CONSTRAINT t3_pkey PRIMARY KEY (c1)
);
--Testcase 15:
CREATE TABLE "S 1"."T 4" (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	CONSTRAINT t4_pkey PRIMARY KEY (c1)
);

-- Disable autovacuum for these tables to avoid unexpected effects of that
ALTER TABLE "S 1"."T 1" SET (autovacuum_enabled = 'false');
ALTER TABLE "S 1"."T 2" SET (autovacuum_enabled = 'false');
ALTER TABLE "S 1"."T 3" SET (autovacuum_enabled = 'false');
ALTER TABLE "S 1"."T 4" SET (autovacuum_enabled = 'false');

--Testcase 16:
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
--Testcase 17:
INSERT INTO "S 1"."T 2"
	SELECT id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 18:
INSERT INTO "S 1"."T 3"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 19:
DELETE FROM "S 1"."T 3" WHERE c1 % 2 != 0;	-- delete for outer join tests
--Testcase 20:
INSERT INTO "S 1"."T 4"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 21:
DELETE FROM "S 1"."T 4" WHERE c1 % 3 != 0;	-- delete for outer join tests

-- ===================================================================
-- Create foreign tables
-- ===================================================================
-- Child tables for pgspider
--Testcase 22:
CREATE FOREIGN TABLE ft1_orig (
	c0 int,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 user_enum
) SERVER loopback;
ALTER FOREIGN TABLE ft1_orig DROP COLUMN c0;

--Testcase 23:
CREATE FOREIGN TABLE ft2_orig (
	c1 int NOT NULL,
	c2 int NOT NULL,
	cx int,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 user_enum
) SERVER loopback;
ALTER FOREIGN TABLE ft2_orig DROP COLUMN cx;

--Testcase 24:
CREATE FOREIGN TABLE ft4_orig (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 3');

--Testcase 25:
CREATE FOREIGN TABLE ft5_orig (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER loopback OPTIONS (schema_name 'S 1', table_name 'T 4');

--Testcase 26:
CREATE FOREIGN TABLE ft6_orig (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER loopback2 OPTIONS (schema_name 'S 1', table_name 'T 4');

--Testcase 27:
CREATE FOREIGN TABLE ft7_orig (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER loopback3 OPTIONS (schema_name 'S 1', table_name 'T 4');

-- Parent tables of partition
--Testcase 28:
CREATE TABLE ft1(
	c0 int,
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 user_enum,
	__spd_url text
) PARTITION BY LIST (__spd_url);
ALTER TABLE ft1 DROP COLUMN c0;

--Testcase 29:
CREATE TABLE ft2 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	cx int,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 user_enum,
	__spd_url text
) PARTITION BY LIST (__spd_url);
ALTER TABLE ft2 DROP COLUMN cx;

--Testcase 30:
CREATE TABLE ft4 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	__spd_url text
) PARTITION BY LIST (__spd_url);

--Testcase 31:
CREATE TABLE ft5 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	__spd_url text
) PARTITION BY LIST (__spd_url);

--Testcase 32:
CREATE TABLE ft6 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	__spd_url text
) PARTITION BY LIST (__spd_url);

--Testcase 33:
CREATE TABLE ft7 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	__spd_url text
) PARTITION BY LIST (__spd_url);

-- Child tables of partition (= parent table for pgspider)
--Testcase 34:
CREATE FOREIGN TABLE ft1_child PARTITION OF ft1 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'ft1_orig');
--Testcase 35:
CREATE FOREIGN TABLE ft2_child PARTITION OF ft2 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'ft2_orig');
--Testcase 36:
CREATE FOREIGN TABLE ft4_child PARTITION OF ft4 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'ft4_orig');
--Testcase 37:
CREATE FOREIGN TABLE ft5_child PARTITION OF ft5 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'ft5_orig');
--Testcase 38:
CREATE FOREIGN TABLE ft6_child PARTITION OF ft6 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'ft6_orig');
--Testcase 39:
CREATE FOREIGN TABLE ft7_child PARTITION OF ft7 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'ft7_orig');

-- Enable to pushdown aggregate
SET enable_partitionwise_aggregate TO on;

-- Turn off leader node participation to avoid duplicate data error when executing
-- parallel query
SET parallel_leader_participation TO off;

-- ===================================================================
-- tests for validator
-- ===================================================================
-- requiressl and some other parameters are omitted because
-- valid values for them depend on configure options
ALTER SERVER testserver1 OPTIONS (
	use_remote_estimate 'false',
	updatable 'true',
	fdw_startup_cost '123.456',
	fdw_tuple_cost '0.123',
	service 'value',
	connect_timeout 'value',
	dbname 'value',
	host 'value',
	hostaddr 'value',
	port 'value',
	--client_encoding 'value',
	application_name 'value',
	--fallback_application_name 'value',
	keepalives 'value',
	keepalives_idle 'value',
	keepalives_interval 'value',
	tcp_user_timeout 'value',
	-- requiressl 'value',
	sslcompression 'value',
	sslmode 'value',
	sslcert 'value',
	sslkey 'value',
	sslrootcert 'value',
	sslcrl 'value',
	--requirepeer 'value',
	krbsrvname 'value',
	gsslib 'value',
	gssdelegation 'value'
	--replication 'value'
);

-- Error, invalid list syntax
ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo; bar');

-- OK but gets a warning
ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo, bar');
ALTER SERVER testserver1 OPTIONS (DROP extensions);

ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (DROP user, DROP password);

-- Attempt to add a valid option that's not allowed in a user mapping
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (ADD sslmode 'require');

-- But we can add valid ones fine
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (ADD sslpassword 'dummy');

-- Ensure valid options we haven't used in a user mapping yet are
-- permitted to check validation.
ALTER USER MAPPING FOR public SERVER testserver1
	OPTIONS (ADD sslkey 'value', ADD sslcert 'value');

ALTER FOREIGN TABLE ft1_orig OPTIONS (schema_name 'S 1', table_name 'T 1');
ALTER FOREIGN TABLE ft2_orig OPTIONS (schema_name 'S 1', table_name 'T 1');
ALTER FOREIGN TABLE ft1_orig ALTER COLUMN c1 OPTIONS (column_name 'C 1');
ALTER FOREIGN TABLE ft2_orig ALTER COLUMN c1 OPTIONS (column_name 'C 1');
--Testcase 40:
\det+

-- Test that alteration of server options causes reconnection
-- Remote's errors might be non-English, so hide them to ensure stable results
\set VERBOSITY terse
--Testcase 41:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
ALTER SERVER loopback OPTIONS (SET dbname 'no such database');
--Testcase 42:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER loopback
            OPTIONS (SET dbname '$$||current_database()||$$')$$;
    END;
$d$;
--Testcase 43:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again

-- Test that alteration of user mapping options causes reconnection
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback
  OPTIONS (ADD user 'no such user');
--Testcase 586:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback
  OPTIONS (DROP user);
--Testcase 587:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
\set VERBOSITY default
-- Now we should be able to run ANALYZE.
-- To exercise multiple code paths, we use local stats on ft1
-- and remote-estimate mode on ft2.
ANALYZE ft1_orig;
ALTER FOREIGN TABLE ft2_orig OPTIONS (use_remote_estimate 'true');

-- ===================================================================
-- test error case for create publication on foreign table
-- ===================================================================
--Testcase 44:
CREATE PUBLICATION testpub_ftbl FOR TABLE ft1_orig;  -- should fail

-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias
--Testcase 45:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
--Testcase 46:
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;

-- single table with alias - also test that tableoid sort is not pushed to remote side
--Testcase 47:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
--Testcase 48:
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;

-- whole-row reference
--Testcase 49:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 50:
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

-- empty result
--Testcase 51:
SELECT * FROM ft1 WHERE false;

-- with WHERE clause
--Testcase 52:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
--Testcase 53:
SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';

-- with FOR UPDATE/SHARE
--Testcase 54:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 55:
SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 56:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
--Testcase 57:
SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;

-- aggregate
--Testcase 58:
SELECT COUNT(*) FROM ft1 t1;

-- subquery
--Testcase 59:
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;

-- subquery+MAX
--Testcase 60:
SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;

-- used in CTE
--Testcase 61:
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;

-- fixed values
--Testcase 62:
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;

-- Test forcing the remote server to produce sorted data for a merge join.
SET enable_hashjoin TO false;
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 63:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
--Testcase 64:
SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;

-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 65:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
--Testcase 66:
SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;

-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
--Testcase 67:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 68:
SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;

-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
--Testcase 69:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 70:
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;

-- Test similar to above with all full outer joins
--Testcase 71:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 72:
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;

RESET enable_hashjoin;
RESET enable_nestloop;

-- Test executing assertion in estimate_path_cost_size() that makes sure that
-- retrieved_rows for foreign rel re-used to cost pre-sorted foreign paths is
-- a sensible value even when the rel has tuples=0
--Testcase 588:
CREATE TABLE loct_empty (c1 int NOT NULL, c2 text);
--Testcase 589:
CREATE FOREIGN TABLE ft_empty (c1 int NOT NULL, c2 text)
  SERVER loopback OPTIONS (table_name 'loct_empty');
--Testcase 590:
INSERT INTO loct_empty
  SELECT id, 'AAA' || to_char(id, 'FM000') FROM generate_series(1, 100) id;
--Testcase 591:
DELETE FROM loct_empty;
ANALYZE ft_empty;
--Testcase 592:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_empty ORDER BY c1;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
--Testcase 73:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
--Testcase 74:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
--Testcase 75:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c3 IS NULL;        -- NullTest
--Testcase 76:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c3 IS NOT NULL;    -- NullTest
--Testcase 77:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
--Testcase 78:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
--Testcase 79:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
--Testcase 80:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
--Testcase 81:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef
--Testcase 82:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
--Testcase 83:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote

-- parameterized remote path for foreign table
--Testcase 606:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM "S 1"."T 1" a, ft2 b WHERE a."C 1" = 47 AND b.c1 = a.c2;
--Testcase 607:
SELECT * FROM "S 1"."T 1" a, ft2 b WHERE a."C 1" = 47 AND b.c1 = a.c2;
--Testcase 84:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 WHERE c1 = (SELECT max(c1) FROM ft1);
--Testcase 85:
SELECT * FROM ft1 WHERE c1 = (SELECT max(c1) FROM ft1);

-- check both safe and unsafe join conditions
--Testcase 86:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
--Testcase 87:
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);

-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
--Testcase 88:
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5));
--Testcase 89:
SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5));

-- we should not push order by clause with volatile expressions or unsafe
-- collations
--Testcase 90:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, random();
--Testcase 91:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- user-defined operator/function
--Testcase 92:
CREATE FUNCTION postgres_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
--Testcase 93:
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);
-- built-in operators and functions can be shipped for remote execution
--Testcase 94:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 95:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 96:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 97:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot
--Testcase 98:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
--Testcase 99:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
--Testcase 100:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 101:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ORDER BY can be shipped, though
--Testcase 102:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 103:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- but let's put them in an extension ...
ALTER EXTENSION postgres_fdw ADD FUNCTION postgres_fdw_abs(int);
ALTER EXTENSION postgres_fdw ADD OPERATOR === (int, int);
ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');
-- ... now they can be shipped
--Testcase 104:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
--Testcase 105:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = postgres_fdw_abs(t1.c2);
--Testcase 106:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 107:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ORDER BY can be shipped. LIMIT is not pushed on partitioned table.
--Testcase 108:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 109:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- Test CASE pushdown
--Testcase 110:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;
--Testcase 111:
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;

-- Nested CASE
--Testcase 112:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;

--Testcase 113:
SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;

-- CASE arg WHEN
--Testcase 114:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE mod(c1, 4) WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- CASE cannot be pushed down because of unshippable arg clause
--Testcase 115:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE random()::integer WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- these are shippable
--Testcase 116:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c6 WHEN 'foo' THEN true ELSE c3 < 'bar' END;
--Testcase 117:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 WHEN c6 THEN true ELSE c3 < 'bar' END;

-- but this is not because of collation
--Testcase 118:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 COLLATE "C" WHEN c6 THEN true ELSE c3 < 'bar' END;

-- a regconfig constant referring to this text search configuration
-- is initially unshippable
--Testcase 119:
CREATE TEXT SEARCH CONFIGURATION public.custom_search
  (COPY = pg_catalog.english);
--Testcase 120:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
--Testcase 121:
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
-- but if it's in a shippable extension, it can be shipped
ALTER EXTENSION postgres_fdw ADD TEXT SEARCH CONFIGURATION public.custom_search;
-- however, that doesn't flush the shippability cache, so do a quick reconnect
\c -

-- Enable to pushdown aggregate after reconnect
SET enable_partitionwise_aggregate TO on;
-- Turn off leader node participation to avoid duplicate data error when executing
-- parallel query
SET parallel_leader_participation TO off;
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
ANALYZE ft4_orig;
ANALYZE ft5_orig;
-- join two tables
--Testcase 122:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 123:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

-- join three tables
--Testcase 124:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 125:
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join
--Testcase 126:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 127:
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables
--Testcase 128:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 129:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
--Testcase 130:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
--Testcase 131:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
--Testcase 132:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
--Testcase 133:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;

-- right outer join
--Testcase 134:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 135:
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;

-- right outer join three tables
--Testcase 136:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 137:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

-- full outer join
--Testcase 138:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
--Testcase 139:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;

-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
--Testcase 140:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 141:
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 142:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
--Testcase 143:
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;

-- b. one of the joining relations is a base relation and the other is a join
-- relation
--Testcase 144:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 145:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;

-- c. test deparsing the remote query as nested subqueries
--Testcase 146:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 147:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;

-- d. test deparsing rowmarked relations as subqueries
--Testcase 148:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
--Testcase 149:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;

-- full outer join + inner join
--Testcase 150:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
--Testcase 151:
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;

-- full outer join three tables
--Testcase 152:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 153:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

-- full outer join + right outer join
--Testcase 154:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 155:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

-- right outer join + full outer join
--Testcase 156:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY 1,2,3 OFFSET 10 LIMIT 10;
--Testcase 157:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY 1,2,3 OFFSET 10 LIMIT 10;

-- full outer join + left outer join
--Testcase 158:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 159:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

-- left outer join + full outer join
--Testcase 160:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY 1,2,3 OFFSET 10 LIMIT 10;
--Testcase 161:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY 1,2,3 OFFSET 10 LIMIT 10;

SET enable_memoize TO off;
-- right outer join + left outer join
--Testcase 162:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 163:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
RESET enable_memoize;
-- left outer join + right outer join
--Testcase 164:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 165:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;

-- full outer join + WHERE clause, only matched rows
--Testcase 166:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 167:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

-- full outer join + WHERE clause with shippable extensions set
--Testcase 168:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE postgres_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
ALTER SERVER loopback OPTIONS (DROP extensions);
-- full outer join + WHERE clause with shippable extensions not set
--Testcase 169:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE postgres_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;

ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
--Testcase 170:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 171:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 172:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
--Testcase 173:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;

-- join two tables with FOR SHARE clause
--Testcase 174:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 175:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 176:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
--Testcase 177:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;

-- join in CTE
--Testcase 178:
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
--Testcase 179:
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;

-- ctid with whole-row reference
--Testcase 180:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

-- SEMI JOIN, not pushed down
--Testcase 181:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 182:
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;

-- ANTI JOIN, not pushed down
--Testcase 183:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 184:
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;

-- CROSS JOIN, not pushed down
--Testcase 185:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 186:
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;

-- different server, not pushed down. No result expected.
--Testcase 187:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 188:
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;

-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
--Testcase 189:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 190:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
--Testcase 191:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 192:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
--Testcase 193:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 194:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

-- Aggregate after UNION, for testing setrefs
--Testcase 195:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
--Testcase 196:
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;

-- join with lateral reference
--Testcase 197:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;
--Testcase 198:
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;

-- join with pseudoconstant quals, not pushed down.
--Testcase 598:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1 AND CURRENT_USER = SESSION_USER) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
--Testcase 199:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;
--Testcase 200:
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
--Testcase 201:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;
--Testcase 202:
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;

-- join with nullable side with some columns with null values
--Testcase 203:
UPDATE ft5_orig SET c3 = null where c1 % 9 = 0;
--Testcase 204:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;
--Testcase 205:
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
--Testcase 206:
CREATE TABLE local_tbl (c1 int NOT NULL, c2 int NOT NULL, c3 text, CONSTRAINT local_tbl_pkey PRIMARY KEY (c1));
--Testcase 207:
INSERT INTO local_tbl SELECT id, id % 10, to_char(id, 'FM0000') FROM generate_series(1, 1000) id;
ANALYZE local_tbl;
SET enable_nestloop TO false;
SET enable_hashjoin TO false;

--Testcase 208:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;
--Testcase 209:
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 FOR UPDATE;

RESET enable_nestloop;
RESET enable_hashjoin;

-- test that add_paths_with_pathkeys_for_rel() arranges for the epq_path to
-- return columns needed by the parent ForeignScan node
--Testcase 210:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.*, COALESCE(ft1.c3 || ft2.c3, 'foobar') FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100)) ss ON (local_tbl.c1 = ss.c1) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;

ALTER SERVER loopback OPTIONS (DROP extensions);
ALTER SERVER loopback OPTIONS (ADD fdw_startup_cost '10000.0');
--Testcase 211:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.* FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100 AND (ft1.c1 - postgres_fdw_abs(ft2.c2)) = 0)) ss ON (local_tbl.c3 = ss.c3) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;
ALTER SERVER loopback OPTIONS (DROP fdw_startup_cost);
ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

--Testcase 212:
DROP TABLE local_tbl;

-- check join pushdown in situations where multiple userids are involved
--Testcase 213:
CREATE ROLE regress_view_owner SUPERUSER;
--Testcase 214:
CREATE USER MAPPING FOR regress_view_owner SERVER loopback;
CREATE USER MAPPING FOR regress_view_owner SERVER spdsrv;
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;
--Testcase 215:
CREATE VIEW v4 AS SELECT * FROM ft4;
--Testcase 216:
CREATE VIEW v5 AS SELECT * FROM ft5;
ALTER VIEW v5 OWNER TO regress_view_owner;
--Testcase 217:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
--Testcase 218:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

ALTER VIEW v4 OWNER TO regress_view_owner;
--Testcase 219:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 220:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 221:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
--Testcase 222:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

ALTER VIEW v4 OWNER TO CURRENT_USER;
--Testcase 223:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 224:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

ALTER VIEW v4 OWNER TO regress_view_owner;

-- ====================================================================
-- Check that userid to use when querying the remote table is correctly
-- propagated into foreign rels present in subqueries under an UNION ALL
-- ====================================================================
--Testcase 599:
CREATE ROLE regress_view_owner_another;
ALTER VIEW v4 OWNER TO regress_view_owner_another;
GRANT SELECT ON ft4 TO regress_view_owner_another;
ALTER FOREIGN TABLE ft4_orig OPTIONS (ADD use_remote_estimate 'true');
-- The following should query the remote backing table of ft4 as user
-- regress_view_owner_another, the view owner, though it fails as expected
-- due to the lack of a user mapping for that user.
--Testcase 600:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;
-- Likewise, but with the query under an UNION ALL
--Testcase 601:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT * FROM v4 UNION ALL SELECT * FROM v4);
-- Should not get that error once a user mapping is created
--Testcase 602:
CREATE USER MAPPING FOR regress_view_owner_another SERVER loopback OPTIONS (password_required 'false');
CREATE USER MAPPING FOR regress_view_owner_another SERVER spdsrv;
--Testcase 603:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM v4;
--Testcase 604:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT * FROM v4 UNION ALL SELECT * FROM v4);
--Testcase 605:
DROP USER MAPPING FOR regress_view_owner_another SERVER spdsrv;
DROP USER MAPPING FOR regress_view_owner_another SERVER loopback;
DROP OWNED BY regress_view_owner_another;
DROP ROLE regress_view_owner_another;
ALTER FOREIGN TABLE ft4_orig OPTIONS (SET use_remote_estimate 'false');

-- cleanup
--Testcase 225:
DROP OWNED BY regress_view_owner;
--Testcase 226:
DROP ROLE regress_view_owner;
-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================
-- Simple aggregates
--Testcase 227:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;
--Testcase 228:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

--Testcase 229:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;
--Testcase 230:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()
--Testcase 231:
explain (verbose, costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query
--Testcase 232:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;
--Testcase 233:
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel
--Testcase 234:
explain (verbose, costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions
--Testcase 235:
explain (verbose, costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;
--Testcase 236:
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are not pushed down because of cost.
set enable_incremental_sort = off;
--Testcase 237:
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
--Testcase 238:
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
reset enable_incremental_sort;

-- Aggregate is still pushed down by taking unshippable expression out
--Testcase 239:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;
--Testcase 240:
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
--Testcase 241:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
--Testcase 242:
explain (verbose, costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
--Testcase 243:
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
--Testcase 244:
explain (verbose, costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);
--Testcase 245:
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability. But not work with pgspider_ext
-- Unshippable HAVING clause will be evaluated locally
--Testcase 246:
explain (verbose, costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;
--Testcase 247:
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
--Testcase 248:
explain (verbose, costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
--Testcase 249:
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
--Testcase 250:
explain (verbose, costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
--Testcase 251:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1;
--Testcase 252:
select exists(select 1 from pg_enum), sum(c1) from ft1;
--Testcase 253:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;
--Testcase 254:
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;

-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates
-- ORDER BY within aggregate, same column used to order
--Testcase 255:
explain (verbose, costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
--Testcase 256:
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
-- ORDER BY within aggregate, different column used to order also using DESC
--Testcase 257:
explain (verbose, costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;
--Testcase 258:
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate
--Testcase 259:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 260:
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
--Testcase 261:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 262:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 263:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 264:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- FILTER within aggregate
--Testcase 265:
explain (verbose, costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
--Testcase 266:
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
--Testcase 267:
explain (verbose, costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;
--Testcase 268:
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query
--Testcase 269:
explain (verbose, costs off)
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 270:
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Inner query is aggregation query
--Testcase 271:
explain (verbose, costs off)
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 272:
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
--Testcase 273:
explain (verbose, costs off)
select sum(c1) filter (where (c1 / c1) * random() <= 1) from ft1 group by c2 order by 1;
--Testcase 274:
explain (verbose, costs off)
select sum(c2) filter (where c2 in (select c2 from ft1 where c2 < 5)) from ft1;

-- Ordered-sets within aggregate
--Testcase 275:
explain (verbose, costs off)
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;
--Testcase 276:
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

-- Using multiple arguments within aggregates
--Testcase 277:
explain (verbose, costs off)
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;
--Testcase 278:
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC
--Testcase 279:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
--Testcase 280:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);
-- Disable hash aggregation for plan stability.
set enable_hashagg to false;
-- Not pushed down due to user defined aggregate
--Testcase 281:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension
alter extension postgres_fdw add function least_accum(anyelement, variadic anyarray);
alter extension postgres_fdw add aggregate least_agg(variadic items anyarray);
alter server loopback options (set extensions 'postgres_fdw');
-- Still not pushed down because pgspider_ext does not have such function.
--Testcase 282:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;
--Testcase 283:
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension
alter extension postgres_fdw drop function least_accum(anyelement, variadic anyarray);
alter extension postgres_fdw drop aggregate least_agg(variadic items anyarray);
alter server loopback options (set extensions 'postgres_fdw');
-- Not pushed down as we have dropped objects from extension.
--Testcase 284:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup
reset enable_hashagg;
--Testcase 285:
drop aggregate least_agg(variadic items anyarray);
--Testcase 286:
drop function least_accum(anyelement, variadic anyarray);
-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.
--Testcase 287:
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);
--Testcase 288:
create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);
--Testcase 289:
create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);
--Testcase 290:
create operator family my_op_family using btree;
--Testcase 291:
create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;
--Testcase 292:
create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);
-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.
--Testcase 293:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- This should not be pushed either.
--Testcase 294:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Update local stats on ft2
ANALYZE ft2_orig;
-- Add into extension
alter extension postgres_fdw add operator class my_op_class using btree;
alter extension postgres_fdw add function my_op_cmp(a int, b int);
alter extension postgres_fdw add operator family my_op_family using btree;
alter extension postgres_fdw add operator public.<^(int, int);
alter extension postgres_fdw add operator public.=^(int, int);
alter extension postgres_fdw add operator public.>^(int, int);
alter server loopback options (set extensions 'postgres_fdw');
-- Still not pushed down because pgspider_ext does not have such operator.
alter server loopback options (add fdw_tuple_cost '0.5');
--Testcase 295:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
--Testcase 296:
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
alter server loopback options (drop fdw_tuple_cost);

-- This should be pushed too. But not work with pgspider_ext
--Testcase 297:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Remove from extension
alter extension postgres_fdw drop operator class my_op_class using btree;
alter extension postgres_fdw drop function my_op_cmp(a int, b int);
alter extension postgres_fdw drop operator family my_op_family using btree;
alter extension postgres_fdw drop operator public.<^(int, int);
alter extension postgres_fdw drop operator public.=^(int, int);
alter extension postgres_fdw drop operator public.>^(int, int);
alter server loopback options (set extensions 'postgres_fdw');
-- This will not be pushed as sort operator is now removed from the extension.
--Testcase 298:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup
--Testcase 299:
drop operator class my_op_class using btree;
--Testcase 300:
drop function my_op_cmp(a int, b int);
--Testcase 301:
drop operator family my_op_family using btree;
--Testcase 302:
drop operator public.>^(int, int);
--Testcase 303:
drop operator public.=^(int, int);
--Testcase 304:
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
--Testcase 305:
explain (verbose, costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate
--Testcase 306:
explain (verbose, costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;
--Testcase 307:
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
--Testcase 308:
explain (verbose, costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;
--Testcase 309:
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
--Testcase 310:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);
--Testcase 311:
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
--Testcase 312:
explain (verbose, costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;
--Testcase 313:
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization
set enable_hashagg to false;
--Testcase 314:
explain (verbose, costs off)
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1."C 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C 1" < 100 order by 1;
--Testcase 315:
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1."C 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C 1" < 100 order by 1;

reset enable_hashagg;
-- bug #15613: bad plan for foreign table scan with lateral reference
--Testcase 316:
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

--Testcase 317:
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
-- Disable remote estimation temporary because sum() was not pushed down if enabled.
ALTER FOREIGN TABLE ft2_orig OPTIONS (set use_remote_estimate 'false');
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
--Testcase 593:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 594:
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 595:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 596:
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
ALTER FOREIGN TABLE ft2_orig OPTIONS (set use_remote_estimate 'true');

-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join
--Testcase 332:
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
--Testcase 333:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);
--Testcase 334:
EXECUTE st1(1, 1);
--Testcase 335:
EXECUTE st1(101, 101);
-- subquery using stable function (can't be sent to remote)
--Testcase 336:
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;
--Testcase 337:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);
--Testcase 338:
EXECUTE st2(10, 20);
--Testcase 339:
EXECUTE st2(101, 121);
-- subquery using immutable function (can be sent to remote)
--Testcase 340:
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;
--Testcase 341:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);
--Testcase 342:
EXECUTE st3(10, 20);
--Testcase 343:
EXECUTE st3(20, 30);

-- custom plan should be chosen initially
--Testcase 344:
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;
--Testcase 345:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 346:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 347:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 348:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 349:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
--Testcase 350:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
--Testcase 351:
PREPARE st5(user_enum,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
--Testcase 352:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 353:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 354:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 355:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 356:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 357:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 358:
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
--Testcase 359:
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 360:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
-- PGSpider does not support DML. So we insert data via data source FDW.
--Testcase 361:
PREPARE st7 AS INSERT INTO ft1_orig (c1,c2,c3) VALUES (1001,101,'foo');
--Testcase 362:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
ALTER TABLE "S 1"."T 1" RENAME TO "T 0";
ALTER FOREIGN TABLE ft1_orig OPTIONS (SET table_name 'T 0');
-- pgspider cannot detect the option change of child foreign table. The query already prepared is
-- not updated. It mean "T 0" is not used but "T 1".
--Testcase 363:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 364:
EXECUTE st6;
--Testcase 365:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
ALTER TABLE "S 1"."T 0" RENAME TO "T 1";
ALTER FOREIGN TABLE ft1_orig OPTIONS (SET table_name 'T 1');
--Testcase 366:
PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 367:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
ALTER SERVER loopback OPTIONS (DROP extensions);
--Testcase 368:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--Testcase 369:
EXECUTE st8;
ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');
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
--Testcase 370:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;
--Testcase 371:
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1_child'::regclass LIMIT 1;
--Testcase 372:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 373:
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 374:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 375:
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 376:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;
--Testcase 377:
SELECT ctid, * FROM ft1 t1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================
--Testcase 378:
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
--Testcase 379:
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
--Testcase 380:
SELECT f_test(100);
--Testcase 381:
DROP FUNCTION f_test(int);

-- -- ===================================================================
-- PGSpider Extension only support Partition by List. This test is not 
-- suitable.
-- -- REINDEX
-- -- ===================================================================
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
-- local type can be different from remote type in some cases,
-- in particular if similarly-named operators do equivalent things
-- ===================================================================
ALTER FOREIGN TABLE ft1_orig ALTER COLUMN c8 TYPE text;
ALTER TABLE ft1 ALTER COLUMN c8 TYPE text;
--Testcase 382:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c8 = 'foo' LIMIT 1;
--Testcase 383:
SELECT * FROM ft1 WHERE c8 = 'foo' LIMIT 1;
--Testcase 384:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE 'foo' = c8 LIMIT 1;
--Testcase 385:
SELECT * FROM ft1 WHERE 'foo' = c8 LIMIT 1;
-- we declared c8 to be text locally, but it's still the same type on
-- the remote which will balk if we try to do anything incompatible
-- with that remote type
--Testcase 386:
SELECT * FROM ft1 WHERE c8 LIKE 'foo' LIMIT 1; -- ERROR
--Testcase 387:
SELECT * FROM ft1 WHERE c8::text LIKE 'foo' LIMIT 1; -- ERROR; cast not pushed down
ALTER FOREIGN TABLE ft1_orig ALTER COLUMN c8 TYPE user_enum;

-- Alter ft1 will raise error "ft1_child" is not a table 
-- because ft1_child is foreign table. Hence, drop ft1_child
-- then re-create ft1_child again after alter ft1.
--Testcase 388:
DROP FOREIGN TABLE ft1_child;
ALTER TABLE ft1 ALTER COLUMN c8 TYPE user_enum USING c8::text::user_enum;
--Testcase 389:
CREATE FOREIGN TABLE ft1_child PARTITION OF ft1 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'ft1_orig');

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;
--Testcase 390:
FETCH c;
SAVEPOINT s;
ERROR OUT;          -- ERROR
ROLLBACK TO s;
--Testcase 391:
FETCH c;
SAVEPOINT s;
--Testcase 392:
SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK TO s;
--Testcase 393:
FETCH c;
--Testcase 394:
SELECT * FROM ft1 ORDER BY c1 LIMIT 1;
COMMIT;
-- ===================================================================
-- test handling of collations
-- ===================================================================
--Testcase 395:
create table loct3 (f1 text collate "C" unique, f2 text, f3 varchar(10) unique);
--Testcase 396:
create table ft3 (f1 text collate "C", f2 text, f3 varchar(10), __spd_url text) PARTITION BY LIST (__spd_url);
--Testcase 397:
create foreign table ft3_child PARTITION OF ft3 FOR VALUES IN ('/node1/')
  server spdsrv options (child_name 'ft3_orig');
--Testcase 398:
create foreign table ft3_orig (f1 text collate "C", f2 text, f3 varchar(10))
  server loopback options (table_name 'loct3', use_remote_estimate 'true');

-- can be sent to remote
--Testcase 399:
explain (verbose, costs off) select * from ft3 where f1 = 'foo';
--Testcase 400:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "C" = 'foo';
--Testcase 401:
explain (verbose, costs off) select * from ft3 where f2 = 'foo';
--Testcase 402:
explain (verbose, costs off) select * from ft3 where f3 = 'foo';
--Testcase 403:
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 and l.f1 = 'foo';

-- can't be sent to remote
--Testcase 404:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';
--Testcase 405:
explain (verbose, costs off) select * from ft3 where f1 = 'foo' COLLATE "C";
--Testcase 406:
explain (verbose, costs off) select * from ft3 where f2 COLLATE "C" = 'foo';
--Testcase 407:
explain (verbose, costs off) select * from ft3 where f2 = 'foo' COLLATE "C";
--Testcase 408:
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

-- Test UPDATE involving a join that can be pushed down,
-- but a SET clause that can't be
--Testcase 409:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE ft2_orig d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;
--Testcase 410:
UPDATE ft2_orig d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;

-- PGSpider Extension only support Partition by List. This test is not 
-- suitable.
-- CREATE TABLE parent_tbl (a int, b int) PARTITION BY RANGE(a);
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);
-- -- Detach and re-attach once, to stress the concurrent detach case.
-- ALTER TABLE parent_tbl DETACH PARTITION foreign_tbl CONCURRENTLY;
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);

-- -- Try a more complex permutation of WCO where there are multiple levels of
-- -- partitioned tables with columns not all in the same order
-- CREATE TABLE parent_tbl (a int, b text, c numeric) PARTITION BY RANGE(a);
-- CREATE TABLE sub_parent (c numeric, a int, b text) PARTITION BY RANGE(a);
-- ALTER TABLE parent_tbl ATTACH PARTITION sub_parent FOR VALUES FROM (1) TO (10);
-- CREATE TABLE child_local (b text, c numeric, a int);
-- CREATE FOREIGN TABLE child_foreign (b text, c numeric, a int)
--   SERVER loopback OPTIONS (table_name 'child_local');
-- ALTER TABLE sub_parent ATTACH PARTITION child_foreign FOR VALUES FROM (1) TO (10);
-- CREATE VIEW rw_view AS SELECT * FROM parent_tbl WHERE a < 5 WITH CHECK OPTION;

-- INSERT INTO parent_tbl (a) VALUES(1),(5);
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE rw_view SET b = 'text', c = 123.456;
-- UPDATE rw_view SET b = 'text', c = 123.456;
-- SELECT * FROM parent_tbl ORDER BY a;

-- DROP VIEW rw_view;
-- DROP TABLE child_local;
-- DROP FOREIGN TABLE child_foreign;
-- DROP TABLE sub_parent;
-- DROP TABLE parent_tbl;

-- ===================================================================
-- test generated columns
-- ===================================================================
--Testcase 411:
CREATE TABLE gloc1 (
  a int,
  b int GENERATED ALWAYS AS (a * 2) stored);

--Testcase 412:
CREATE FOREIGN TABLE grem1_a_child (
  a int,
  b int GENERATED ALWAYS AS (a * 2) stored)
  SERVER loopback options(table_name 'gloc1');

--Testcase 413:
CREATE TABLE grem1 (
  a int,
  b int GENERATED ALWAYS AS (a * 2) stored,
  spdurl text
) PARTITION BY LIST (spdurl);
-- alter table gloc1 set (autovacuum_enabled = 'false');
--Testcase 414:
CREATE FOREIGN TABLE grem1_a PARTITION OF grem1 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- test batch insert
ALTER SERVER loopback OPTIONS (ADD batch_size '10');
--Testcase 415:
EXPLAIN (verbose, costs off)
INSERT INTO grem1_a_child (a) VALUES (1), (2);
--Testcase 416:
INSERT INTO grem1_a_child (a) VALUES (1), (2);
--Testcase 417:
SELECT * FROM gloc1;
--Testcase 418:
SELECT * FROM grem1;
--Testcase 419:
DELETE FROM grem1_a_child;

-- PGSpider Extension only support Partition by List. This test is not suitable.
-- -- batch insert with foreign partitions.
-- -- This schema uses two partitions, one local and one remote with a modulo
-- -- to loop across all of them in batches.
-- create table tab_batch_local (id int, data text);
-- insert into tab_batch_local select i, 'test'|| i from generate_series(1, 45) i;
-- create table tab_batch_sharded (id int, data text) partition by hash(id);
-- create table tab_batch_sharded_p0 partition of tab_batch_sharded
--   for values with (modulus 2, remainder 0);
-- create table tab_batch_sharded_p1_remote (id int, data text);
-- create foreign table tab_batch_sharded_p1 partition of tab_batch_sharded
--   for values with (modulus 2, remainder 1)
--   server loopback options (table_name 'tab_batch_sharded_p1_remote');
-- insert into tab_batch_sharded select * from tab_batch_local;
-- select count(*) from tab_batch_sharded;
-- drop table tab_batch_local;
-- drop table tab_batch_sharded;
-- drop table tab_batch_sharded_p1_remote;

ALTER SERVER loopback OPTIONS (DROP batch_size);


-- Test direct foreign table modification functionality
--Testcase 420:
EXPLAIN (verbose, costs off)
DELETE FROM grem1_a_child;                 -- can be pushed down
--Testcase 421:
EXPLAIN (verbose, costs off)
DELETE FROM grem1_a_child WHERE false;     -- currently can't be pushed down

-- -- ===================================================================
-- PGSpider Extension only support Partition by List. This test is not 
-- suitable.
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

-- PGSpider Extension not support BeginForeignInsert
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

-- -- ===================================================================
-- -- test COPY FROM
-- -- ===================================================================
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

-- ===================================================================
-- reestablish new connection
-- ===================================================================

-- Change application_name of remote connection to special one
-- so that we can easily terminate the connection later.
ALTER SERVER loopback OPTIONS (application_name 'fdw_retry_check');

-- Make sure we have a remote connection.
--Testcase 422:
SELECT 1 FROM ft1 LIMIT 1;

-- Terminate the remote connection and wait for the termination to complete.
-- (If a cache flush happens, the remote connection might have already been
-- dropped; so code this step in a way that doesn't fail if no connection.)
--Testcase 423:
DO $$ BEGIN
PERFORM pg_terminate_backend(pid, 180000) FROM pg_stat_activity
	WHERE application_name = 'fdw_retry_check';
END $$;

-- This query should detect the broken connection when starting new remote
-- transaction, reestablish new connection, and then succeed.
BEGIN;
--Testcase 424:
SELECT 1 FROM ft1 LIMIT 1;

-- If we detect the broken connection when starting a new remote
-- subtransaction, we should fail instead of establishing a new connection.
-- Terminate the remote connection and wait for the termination to complete.
--Testcase 425:
DO $$ BEGIN
PERFORM pg_terminate_backend(pid, 180000) FROM pg_stat_activity
	WHERE application_name = 'fdw_retry_check';
END $$;
SAVEPOINT s;
-- The text of the error might vary across platforms, so only show SQLSTATE.
\set VERBOSITY sqlstate
--Testcase 426:
SELECT 1 FROM ft1 LIMIT 1;    -- should fail
\set VERBOSITY default
COMMIT;

-- =============================================================================
-- test connection invalidation cases and postgres_fdw_get_connections function
-- =============================================================================
-- Let's ensure to close all the existing cached connections.
--Testcase 427:
SELECT 1 FROM postgres_fdw_disconnect_all();
-- No cached connections, so no records should be output.
--Testcase 428:
SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- This test case is for closing the connection in pgfdw_xact_callback
BEGIN;
-- Connection xact depth becomes 1 i.e. the connection is in midst of the xact.
--Testcase 429:
SELECT 1 FROM ft1 LIMIT 1;
--Testcase 430:
SELECT 1 FROM ft7 LIMIT 1;
-- List all the existing cached connections. loopback and loopback3 should be
-- output.
--Testcase 431:
SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- Connections are not closed at the end of the alter and drop statements.
-- That's because the connections are in midst of this xact,
-- they are just marked as invalid in pgfdw_inval_callback.
ALTER SERVER loopback OPTIONS (ADD use_remote_estimate 'off');
--Testcase 432:
DROP SERVER loopback3 CASCADE;
-- List all the existing cached connections. loopback and loopback3
-- should be output as invalid connections. Also the server name for
-- loopback3 should be NULL because the server was dropped.
--Testcase 433:
SELECT * FROM postgres_fdw_get_connections() ORDER BY 1;
-- The invalid connections get closed in pgfdw_xact_callback during commit.
COMMIT;
-- All cached connections were closed while committing above xact, so no
-- records should be output.
--Testcase 434:
SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;

-- =======================================================================
-- test postgres_fdw_disconnect and postgres_fdw_disconnect_all functions
-- =======================================================================
BEGIN;
-- Ensure to cache loopback connection.
--Testcase 435:
SELECT 1 FROM ft1 LIMIT 1;
-- Ensure to cache loopback2 connection.
--Testcase 436:
SELECT 1 FROM ft6 LIMIT 1;
-- List all the existing cached connections. loopback and loopback2 should be
-- output.
--Testcase 437:
SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- Issue a warning and return false as loopback connection is still in use and
-- can not be closed.
--Testcase 438:
SELECT postgres_fdw_disconnect('loopback');
-- List all the existing cached connections. loopback and loopback2 should be
-- output.
--Testcase 439:
SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- Return false as connections are still in use, warnings are issued.
-- But disable warnings temporarily because the order of them is not stable.
SET client_min_messages = 'ERROR';
--Testcase 440:
SELECT postgres_fdw_disconnect_all();
RESET client_min_messages;
COMMIT;
-- Ensure that loopback2 connection is closed.
--Testcase 441:
SELECT 1 FROM postgres_fdw_disconnect('loopback2');
--Testcase 442:
SELECT server_name FROM postgres_fdw_get_connections() WHERE server_name = 'loopback2';
-- Return false as loopback2 connection is closed already.
--Testcase 443:
SELECT postgres_fdw_disconnect('loopback2');
-- Return an error as there is no foreign server with given name.
--Testcase 444:
SELECT postgres_fdw_disconnect('unknownserver');
-- Let's ensure to close all the existing cached connections.
--Testcase 445:
SELECT 1 FROM postgres_fdw_disconnect_all();
-- No cached connections, so no records should be output.
--Testcase 446:
SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;

-- =============================================================================
-- test case for having multiple cached connections for a foreign server
-- =============================================================================
--Testcase 447:
CREATE ROLE regress_multi_conn_user1 SUPERUSER;
--Testcase 448:
CREATE ROLE regress_multi_conn_user2 SUPERUSER;
--Testcase 449:
CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER loopback;
--Testcase 450:
CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER spdsrv;
--Testcase 451:
CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER loopback;
--Testcase 452:
CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER spdsrv;

BEGIN;
-- Will cache loopback connection with user mapping for regress_multi_conn_user1
SET ROLE regress_multi_conn_user1;
--Testcase 453:
SELECT 1 FROM ft1 LIMIT 1;
RESET ROLE;

-- Will cache loopback connection with user mapping for regress_multi_conn_user2
SET ROLE regress_multi_conn_user2;
--Testcase 454:
SELECT 1 FROM ft1 LIMIT 1;
RESET ROLE;

-- Should output two connections for loopback server
--Testcase 455:
SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
COMMIT;
-- Let's ensure to close all the existing cached connections.
--Testcase 456:
SELECT 1 FROM postgres_fdw_disconnect_all();
-- No cached connections, so no records should be output.
--Testcase 457:
SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;

-- Clean up
--Testcase 458:
DROP USER MAPPING FOR regress_multi_conn_user1 SERVER loopback;
--Testcase 459:
DROP USER MAPPING FOR regress_multi_conn_user2 SERVER loopback;
--Testcase 460:
DROP USER MAPPING FOR regress_multi_conn_user1 SERVER spdsrv;
--Testcase 461:
DROP USER MAPPING FOR regress_multi_conn_user2 SERVER spdsrv;

--Testcase 462:
DROP ROLE regress_multi_conn_user1;
--Testcase 463:
DROP ROLE regress_multi_conn_user2;

-- ===================================================================
-- Test foreign server level option keep_connections
-- ===================================================================
-- By default, the connections associated with foreign server are cached i.e.
-- keep_connections option is on. Set it to off.
ALTER SERVER loopback OPTIONS (keep_connections 'off');
-- connection to loopback server is closed at the end of xact
-- as keep_connections was set to off.
--Testcase 464:
SELECT 1 FROM ft1 LIMIT 1;
-- No cached connections, so no records should be output.
--Testcase 465:
SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
ALTER SERVER loopback OPTIONS (SET keep_connections 'on');

-- ===================================================================
-- batch insert
-- ===================================================================

BEGIN;

--Testcase 466:
CREATE SERVER batch10 FOREIGN DATA WRAPPER postgres_fdw OPTIONS( batch_size '10' );

--Testcase 467:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=10'];

ALTER SERVER batch10 OPTIONS( SET batch_size '20' );

--Testcase 468:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=10'];

--Testcase 469:
SELECT count(*)
FROM pg_foreign_server
WHERE srvname = 'batch10'
AND srvoptions @> array['batch_size=20'];

--Testcase 470:
CREATE FOREIGN TABLE table30_a_child  ( x int ) SERVER batch10 OPTIONS ( batch_size '30' );
--Testcase 471:
CREATE TABLE table30 ( x int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 472:
CREATE FOREIGN TABLE table30_a PARTITION OF table30 FOR VALUES IN ('/node1/') SERVER spdsrv;


--Testcase 473:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30_a_child'::regclass
AND ftoptions @> array['batch_size=30'];

ALTER FOREIGN TABLE table30_a_child OPTIONS ( SET batch_size '40');

--Testcase 474:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30_a_child'::regclass
AND ftoptions @> array['batch_size=30'];

--Testcase 475:
SELECT COUNT(*)
FROM pg_foreign_table
WHERE ftrelid = 'table30_a_child'::regclass
AND ftoptions @> array['batch_size=40'];

ROLLBACK;

--Testcase 476:
CREATE TABLE batch_table ( x int );

--Testcase 477:
CREATE FOREIGN TABLE ftable_a_child ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table', batch_size '10' );
--Testcase 478:
CREATE TABLE ftable ( x int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 479:
CREATE FOREIGN TABLE ftable_a PARTITION OF ftable FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 480:
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable_a_child SELECT * FROM generate_series(1, 10) i;
--Testcase 481:
INSERT INTO ftable_a_child SELECT * FROM generate_series(1, 10) i;
--Testcase 482:
INSERT INTO ftable_a_child SELECT * FROM generate_series(11, 31) i;
--Testcase 483:
INSERT INTO ftable_a_child VALUES (32);
--Testcase 484:
INSERT INTO ftable_a_child VALUES (33), (34);
--Testcase 492:
SELECT COUNT(*) FROM ftable;
TRUNCATE batch_table;
--Testcase 493:
DROP FOREIGN TABLE ftable_a_child;
--Testcase 494:
DROP TABLE ftable;

-- Disable batch insert
--Testcase 495:
CREATE FOREIGN TABLE ftable_a_child ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table', batch_size '1' );
--Testcase 496:
CREATE TABLE ftable ( x int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 497:
CREATE FOREIGN TABLE ftable_a PARTITION OF ftable FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 498:
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable_a_child VALUES (1), (2);
--Testcase 499:
INSERT INTO ftable_a_child VALUES (1), (2);
--Testcase 500:
SELECT COUNT(*) FROM ftable;

-- Disable batch inserting into foreign tables with BEFORE ROW INSERT triggers
-- even if the batch_size option is enabled.
ALTER FOREIGN TABLE ftable_a_child OPTIONS ( SET batch_size '10' );
--Testcase 501:
CREATE TRIGGER trig_row_before BEFORE INSERT ON ftable_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 502:
EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable_a_child VALUES (3), (4);
--Testcase 503:
INSERT INTO ftable_a_child VALUES (3), (4);
--Testcase 504:
SELECT COUNT(*) FROM ftable;

-- Clean up
--Testcase 505:
DROP TRIGGER trig_row_before ON ftable_a_child;
--Testcase 506:
DROP FOREIGN TABLE ftable_a_child;
--Testcase 507:
DROP TABLE ftable;
--Testcase 508:
DROP TABLE batch_table;

-- PGSpider Extension only support Partition by List. This test is not 
-- suitable.
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
-- PGSpider Extension only support Partition by List. This test is not 
-- suitable.
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
-- SELECT a FROM base_tbl WHERE a IN (SELECT a FROM foreign_tbl);
-- SELECT a FROM base_tbl WHERE a IN (SELECT a FROM foreign_tbl);

-- -- Clean up
-- DROP FOREIGN TABLE foreign_tbl CASCADE;
-- DROP TABLE base_tbl;

-- ALTER SERVER loopback OPTIONS (DROP async_capable);
-- ALTER SERVER loopback2 OPTIONS (DROP async_capable);

-- ===================================================================
-- test invalid server, foreign table and foreign data wrapper options
-- ===================================================================
-- Invalid fdw_startup_cost option
--Testcase 509:
CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
	OPTIONS(fdw_startup_cost '100$%$#$#');
-- Invalid fdw_tuple_cost option
--Testcase 510:
CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
	OPTIONS(fdw_tuple_cost '100$%$#$#');
-- Invalid fetch_size option
--Testcase 511:
CREATE FOREIGN TABLE inv_fsz (c1 int )
	SERVER loopback OPTIONS (fetch_size '100$%$#$#');
-- Invalid batch_size option
--Testcase 512:
CREATE FOREIGN TABLE inv_bsz (c1 int )
	SERVER loopback OPTIONS (batch_size '100$%$#$#');

-- No option is allowed to be specified at foreign data wrapper level
ALTER FOREIGN DATA WRAPPER postgres_fdw OPTIONS (nonexistent 'fdw');

-- ===================================================================
-- test postgres_fdw.application_name GUC
-- ===================================================================
-- To avoid race conditions in checking the remote session's application_name,
-- use this view to make the remote session itself read its application_name.
CREATE VIEW my_application_name AS
  SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid();

CREATE FOREIGN TABLE remote_application_name (application_name text)
  SERVER loopback2
  OPTIONS (schema_name 'public', table_name 'my_application_name');

SELECT count(*) FROM remote_application_name;

-- Specify escape sequences in application_name option of a server
-- object so as to test that they are replaced with status information
-- expectedly.  Note that we are also relying on ALTER SERVER to force
-- the remote session to be restarted with its new application name.
--
-- Since pg_stat_activity.application_name may be truncated to less than
-- NAMEDATALEN characters, note that substring() needs to be used
-- at the condition of test query to make sure that the string consisting
-- of database name and process ID is also less than that.
--Testcase 513:
ALTER SERVER loopback2 OPTIONS (application_name 'fdw_%d%p');
--Testcase 514:
SELECT count(*) FROM remote_application_name
WHERE application_name =
    substring('fdw_' || current_database() || pg_backend_pid() for
      current_setting('max_identifier_length')::int);

-- postgres_fdw.application_name overrides application_name option
-- of a server object if both settings are present.
ALTER SERVER loopback2 OPTIONS (SET application_name 'fdw_wrong');
SET postgres_fdw.application_name TO 'fdw_%a%u%%';
--Testcase 515:
SELECT count(*) FROM remote_application_name
  WHERE application_name =
    substring('fdw_' || current_setting('application_name') ||
      CURRENT_USER || '%' for current_setting('max_identifier_length')::int);
--Testcase 516:
RESET postgres_fdw.application_name;

-- Test %c (session ID) and %C (cluster name) escape sequences.
ALTER SERVER loopback2 OPTIONS (SET application_name 'fdw_%C%c');
--Testcase 517:
SELECT count(*) FROM remote_application_name
  WHERE application_name =
    substring('fdw_' || current_setting('cluster_name') ||
      to_hex(trunc(EXTRACT(EPOCH FROM (SELECT backend_start FROM
      pg_stat_get_activity(pg_backend_pid()))))::integer) || '.' ||
      to_hex(pg_backend_pid())
      for current_setting('max_identifier_length')::int);

--Clean up
DROP FOREIGN TABLE remote_application_name;
DROP VIEW my_application_name;

-- ===================================================================
--  test parallel commit and parallel abort
-- ===================================================================
ALTER SERVER loopback OPTIONS (ADD parallel_commit 'true');
ALTER SERVER loopback OPTIONS (ADD parallel_abort 'true');
ALTER SERVER loopback2 OPTIONS (ADD parallel_commit 'true');
ALTER SERVER loopback2 OPTIONS (ADD parallel_abort 'true');

--Testcase 519:
CREATE TABLE ploc1 (f1 int, f2 text);
--Testcase 520:
CREATE FOREIGN TABLE prem1_orig (f1 int, f2 text)
  SERVER loopback OPTIONS (table_name 'ploc1');
--Testcase 521:
CREATE TABLE prem1 (f1 int, f2 text, __spd_url text)
  PARTITION BY LIST (__spd_url);
--Testcase 522:
CREATE FOREIGN TABLE prem1_child PARTITION OF prem1 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'prem1_orig');

--Testcase 523:
CREATE TABLE ploc2 (f1 int, f2 text);
--Testcase 524:
CREATE FOREIGN TABLE prem2_orig (f1 int, f2 text)
  SERVER loopback2 OPTIONS (table_name 'ploc2');
--Testcase 525:
CREATE TABLE prem2 (f1 int, f2 text, __spd_url text)
  PARTITION BY LIST (__spd_url);
--Testcase 526:
CREATE FOREIGN TABLE prem2_child PARTITION OF prem2 FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'prem2_orig');

BEGIN;
--Testcase 527:
INSERT INTO prem1_orig VALUES (101, 'foo');
--Testcase 528:
INSERT INTO prem2_orig VALUES (201, 'bar');
COMMIT;
--Testcase 529:
SELECT * FROM prem1;
--Testcase 530:
SELECT * FROM prem2;

BEGIN;
SAVEPOINT s;
--Testcase 531:
INSERT INTO prem1_orig VALUES (102, 'foofoo');
--Testcase 532:
INSERT INTO prem2_orig VALUES (202, 'barbar');
RELEASE SAVEPOINT s;
COMMIT;
--Testcase 533:
SELECT * FROM prem1;
--Testcase 534:
SELECT * FROM prem2;

-- This tests executing DEALLOCATE ALL against foreign servers in parallel
-- during pre-commit
BEGIN;
SAVEPOINT s;
--Testcase 535:
INSERT INTO prem1_orig VALUES (103, 'baz');
--Testcase 536:
INSERT INTO prem2_orig VALUES (203, 'qux');
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
--Testcase 537:
INSERT INTO prem1_orig VALUES (104, 'bazbaz');
--Testcase 538:
INSERT INTO prem2_orig VALUES (204, 'quxqux');
COMMIT;
--Testcase 539:
SELECT * FROM prem1;
--Testcase 540:
SELECT * FROM prem2;

BEGIN;
INSERT INTO prem1 VALUES (105, 'test1');
INSERT INTO prem2 VALUES (205, 'test2');
ABORT;
SELECT * FROM prem1;
SELECT * FROM prem2;

-- This tests executing DEALLOCATE ALL against foreign servers in parallel
-- during post-abort
BEGIN;
SAVEPOINT s;
INSERT INTO prem1 VALUES (105, 'test1');
INSERT INTO prem2 VALUES (205, 'test2');
ROLLBACK TO SAVEPOINT s;
RELEASE SAVEPOINT s;
INSERT INTO prem1 VALUES (105, 'test1');
INSERT INTO prem2 VALUES (205, 'test2');
ABORT;
SELECT * FROM prem1;
SELECT * FROM prem2;

ALTER SERVER loopback OPTIONS (DROP parallel_commit);
ALTER SERVER loopback OPTIONS (DROP parallel_abort);
ALTER SERVER loopback2 OPTIONS (DROP parallel_commit);
ALTER SERVER loopback2 OPTIONS (DROP parallel_abort);

-- ===================================================================
-- test for ANALYZE sampling
-- ===================================================================

CREATE TABLE analyze_table (id int, a text, b bigint);

CREATE FOREIGN TABLE analyze_ftable_orig (id int, a text, b bigint)
       SERVER loopback OPTIONS (table_name 'analyze_rtable1');

CREATE TABLE analyze_ftable (id int, a text, b bigint, __spd_url text)
  PARTITION BY LIST (__spd_url);

CREATE FOREIGN TABLE analyze_ftable_child PARTITION OF analyze_ftable FOR VALUES IN ('/node1/') SERVER spdsrv OPTIONS(child_name 'analyze_ftable_orig');

INSERT INTO analyze_table (SELECT x FROM generate_series(1,1000) x);
ANALYZE analyze_table;

SET default_statistics_target = 10;
ANALYZE analyze_table;

ALTER SERVER loopback OPTIONS (analyze_sampling 'invalid');

ALTER SERVER loopback OPTIONS (analyze_sampling 'auto');
ANALYZE analyze_table;

ALTER SERVER loopback OPTIONS (SET analyze_sampling 'system');
ANALYZE analyze_table;

ALTER SERVER loopback OPTIONS (SET analyze_sampling 'bernoulli');
ANALYZE analyze_table;

ALTER SERVER loopback OPTIONS (SET analyze_sampling 'random');
ANALYZE analyze_table;

ALTER SERVER loopback OPTIONS (SET analyze_sampling 'off');
ANALYZE analyze_table;

-- cleanup
DROP TABLE analyze_table;
DROP TABLE analyze_ftable;
DROP FOREIGN TABLE analyze_ftable_orig;

-- Clean up
-- ===================================================================
--Testcase 541:
DROP TABLE "S 1"."T 1";
--Testcase 542:
DROP TABLE "S 1"."T 2";
--Testcase 543:
DROP TABLE "S 1"."T 3";
--Testcase 544:
DROP TABLE "S 1"."T 4";
--Testcase 545:
DROP FOREIGN TABLE ft1_child;
--Testcase 546:
DROP FOREIGN TABLE ft2_child;
--Testcase 547:
DROP FOREIGN TABLE ft3_child;
--Testcase 548:
DROP FOREIGN TABLE ft4_child;
--Testcase 549:
DROP FOREIGN TABLE ft5_child;
--Testcase 550:
DROP FOREIGN TABLE ft6_child;
--Testcase 551:
DROP FOREIGN TABLE ft7_child;
--Testcase 552:
DROP FOREIGN TABLE grem1_a_child;
--Testcase 553:
DROP FOREIGN TABLE grem1_a;
--Testcase 554:
DROP FOREIGN TABLE prem1_child;
--Testcase 555:
DROP FOREIGN TABLE prem2_child;
--Testcase 556:
DROP TABLE ft1;
--Testcase 557:
DROP TABLE ft2;
--Testcase 558:
DROP TABLE ft3;
--Testcase 559:
DROP TABLE ft4;
--Testcase 560:
DROP TABLE ft5;
--Testcase 561:
DROP TABLE ft6;
--Testcase 562:
DROP TABLE ft7;
--Testcase 563:
DROP TABLE gloc1;
--Testcase 564:
DROP TABLE ploc1;
--Testcase 565:
DROP TABLE ploc2;
--Testcase 566:
DROP FOREIGN TABLE ft1_orig;
--Testcase 567:
DROP FOREIGN TABLE ft2_orig;
--Testcase 568:
DROP FOREIGN TABLE ft3_orig;
--Testcase 569:
DROP FOREIGN TABLE ft4_orig;
--Testcase 570:
DROP FOREIGN TABLE ft5_orig;
--Testcase 571:
DROP FOREIGN TABLE ft6_orig;
--Testcase 572:
DROP FOREIGN TABLE prem1_orig;
--Testcase 573:
DROP FOREIGN TABLE prem2_orig;
--Testcase 597:
DROP FOREIGN TABLE ft_empty;
--Testcase 574:
DROP SCHEMA "S 1";
--Testcase 575:
DROP USER MAPPING FOR CURRENT_USER SERVER spdsrv;
--Testcase 576:
DROP USER MAPPING FOR public SERVER testserver1;
--Testcase 577:
DROP USER MAPPING FOR CURRENT_USER SERVER loopback;
--Testcase 578:
DROP USER MAPPING FOR CURRENT_USER SERVER loopback2;
--Testcase 579:
DROP SERVER spdsrv;
--Testcase 580:
DROP SERVER testserver1;
--Testcase 581:
DROP SERVER loopback;
--Testcase 582:
DROP SERVER loopback2;
--Testcase 583:
DROP SERVER loopback3;
--Testcase 584:
DROP EXTENSION pgspider_ext;
--Testcase 585:
DROP EXTENSION postgres_fdw;
-- End
