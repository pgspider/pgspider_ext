-- ===================================================================
-- create FDW objects
-- ===================================================================
\set ECHO none
\ir sql/parameters/sqlumdashcs_parameters.conf
\set ECHO all

--Testcase 1:
CREATE EXTENSION sqlumdashcs_fdw;

--Testcase 730:
CREATE SERVER sqlumdash_svr FOREIGN DATA WRAPPER sqlumdashcs_fdw
  OPTIONS (host :SQLUMDASHCS_HOST, port :SQLUMDASHCS_PORT, dbname 'test_sc.db');
--Testcase 731:
CREATE SERVER sqlumdash_svr2 FOREIGN DATA WRAPPER sqlumdashcs_fdw
  OPTIONS (host :SQLUMDASHCS_HOST, port :SQLUMDASHCS_PORT, dbname 'test_sc.db');
--Testcase 750:
CREATE SERVER sqlumdash_svr3 FOREIGN DATA WRAPPER sqlumdashcs_fdw
  OPTIONS (host :SQLUMDASHCS_HOST, port :SQLUMDASHCS_PORT, dbname 'test_sc.db');

--Testcase 2:
CREATE USER MAPPING FOR CURRENT_USER SERVER sqlumdash_svr OPTIONS (username :SQLUMDASHCS_USER, password :SQLUMDASHCS_PASS);
--Testcase 3:
CREATE USER MAPPING FOR CURRENT_USER SERVER sqlumdash_svr2 OPTIONS (username :SQLUMDASHCS_USER, password :SQLUMDASHCS_PASS);
--Testcase 751:
CREATE USER MAPPING FOR public SERVER sqlumdash_svr3 OPTIONS (username :SQLUMDASHCS_USER, password :SQLUMDASHCS_PASS);

--Testcase 961:
CREATE EXTENSION pgspider_ext;
--Testcase 962:
CREATE SERVER spdsrv FOREIGN DATA WRAPPER pgspider_ext;
--Testcase 963:
CREATE USER MAPPING FOR CURRENT_USER SERVER spdsrv;

-- ===================================================================
-- create objects used through FDW SQLumDash server
-- ===================================================================
--Testcase 4:
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
--Testcase 5:
CREATE SCHEMA "S 1";
IMPORT FOREIGN SCHEMA public FROM SERVER sqlumdash_svr INTO "S 1";

--Testcase 6:
INSERT INTO "S 1"."T 1"
	SELECT id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
	       '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
	       id % 10,
	       id % 10,
	       'foo'
	FROM generate_series(1, 1000) id;
--Testcase 7:
INSERT INTO "S 1"."T 2"
	SELECT id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 8:
INSERT INTO "S 1"."T 3"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 9:
DELETE FROM "S 1"."T 3" WHERE c1 % 2 != 0;	-- delete for outer join tests
--Testcase 10:
INSERT INTO "S 1"."T 4"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 11:
DELETE FROM "S 1"."T 4" WHERE c1 % 3 != 0;	-- delete for outer join tests

/*ANALYZE "S 1"."T 1";
ANALYZE "S 1"."T 2";
ANALYZE "S 1"."T 3";
ANALYZE "S 1"."T 4";*/

-- ===================================================================
-- create foreign tables
-- ===================================================================
--Testcase 12:
CREATE FOREIGN TABLE ft1_a_child (
	c0 int,
	c1 int OPTIONS (key 'true'),
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 text
) SERVER sqlumdash_svr;
--Testcase 752:
ALTER FOREIGN TABLE ft1_a_child DROP COLUMN c0;

--Testcase 13:
CREATE FOREIGN TABLE ft2_a_child (
	c1 int OPTIONS (key 'true'),
	c2 int NOT NULL,
	cx int,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 text
) SERVER sqlumdash_svr;
--Testcase 753:
ALTER FOREIGN TABLE ft2_a_child DROP COLUMN cx;

--Testcase 14:
CREATE FOREIGN TABLE ft4_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER sqlumdash_svr OPTIONS (table_name 'T 3');

--Testcase 15:
CREATE FOREIGN TABLE ft5_a_child (
	c1 int OPTIONS (key 'true'),
	c2 int NOT NULL,
	c3 text
) SERVER sqlumdash_svr OPTIONS (table_name 'T 4');

--Testcase 16:
CREATE FOREIGN TABLE ft6_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER sqlumdash_svr2 OPTIONS (table_name 'T 4');

--Testcase 754:
CREATE FOREIGN TABLE ft7_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER sqlumdash_svr3 OPTIONS (table_name 'T 4');

--Testcase 755:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (table_name 'T 1');
--Testcase 756:
ALTER FOREIGN TABLE ft2_a_child OPTIONS (table_name 'T 1');
--Testcase 757:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c1 OPTIONS (column_name 'C 1');
--Testcase 758:
ALTER FOREIGN TABLE ft2_a_child ALTER COLUMN c1 OPTIONS (column_name 'C 1');

--Testcase 732:
\det+

--Testcase 964:
CREATE TABLE ft1(
	c1 int,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 text,
	spdurl text) PARTITION BY LIST (spdurl);
--Testcase 965:
CREATE FOREIGN TABLE ft1_a PARTITION OF ft1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 966:
CREATE TABLE ft2(
	c1 int,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 text,
	spdurl text) PARTITION BY LIST (spdurl);
--Testcase 967:
CREATE FOREIGN TABLE ft2_a PARTITION OF ft2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 968:
CREATE TABLE ft4 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text) PARTITION BY LIST (spdurl);

--Testcase 969:
CREATE FOREIGN TABLE ft4_a PARTITION OF ft4 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 970:
CREATE TABLE ft5 (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	spdurl text) PARTITION BY LIST (spdurl);

--Testcase 971:
CREATE FOREIGN TABLE ft5_a PARTITION OF ft5 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 972:
CREATE TABLE ft6 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text) PARTITION BY LIST (spdurl);

--Testcase 973:
CREATE FOREIGN TABLE ft6_a PARTITION OF ft6 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 974:
CREATE TABLE ft7 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text) PARTITION BY LIST (spdurl);
--Testcase 975:
CREATE FOREIGN TABLE ft7_a PARTITION OF ft7 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- Enable to pushdown aggregate
--Testcase 976:
SET enable_partitionwise_aggregate TO on;
--Testcase 977:
SET parallel_leader_participation = 'off';

-- Test that alteration of server options causes reconnection
-- Remote's errors might be non-English, so hide them to ensure stable results
\set VERBOSITY terse
--Testcase 18:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
--Testcase 759:
ALTER SERVER sqlumdash_svr OPTIONS (SET dbname 'no such database');
--Testcase 19:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER sqlumdash_svr
            OPTIONS (SET dbname 'test_sc.db')$$;
    END;
$d$;
--Testcase 20:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again

-- Test that alteration of user mapping options causes reconnection
/*ALTER USER MAPPING FOR CURRENT_USER SERVER loopback
  OPTIONS (ADD user 'no such user');
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback
  OPTIONS (DROP user);
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
\set VERBOSITY default*/

-- Now we should be able to run ANALYZE.
-- To exercise multiple code paths, we use local stats on ft1
-- and remote-estimate mode on ft2.
/*ANALYZE ft1;
ALTER FOREIGN TABLE ft2 OPTIONS (use_remote_estimate 'true');*/

-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias
--Testcase 21:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
--Testcase 22:
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
-- single table with alias - also test that tableoid sort is not pushed to remote side
--Testcase 23:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
--Testcase 24:
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference
--Testcase 25:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 26:
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result
--Testcase 27:
SELECT * FROM ft1 WHERE false;
-- with WHERE clause
--Testcase 28:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
--Testcase 29:
SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
-- with FOR UPDATE/SHARE
--Testcase 30:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 31:
SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 32:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
--Testcase 33:
SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
-- aggregate
--Testcase 34:
SELECT COUNT(*) FROM ft1 t1;
-- subquery
--Testcase 35:
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX
--Testcase 36:
SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;
-- used in CTE
--Testcase 37:
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values
--Testcase 38:
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.
--Testcase 760:
SET enable_hashjoin TO false;
--Testcase 761:
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 39:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
--Testcase 40:
SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 41:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
--Testcase 42:
SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
--Testcase 43:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 44:
SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
--Testcase 45:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 46:
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins
--Testcase 47:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 48:
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 762:
RESET enable_hashjoin;
--Testcase 763:
RESET enable_nestloop;

-- -- Test executing assertion in estimate_path_cost_size() that makes sure that
-- -- retrieved_rows for foreign rel re-used to cost pre-sorted foreign paths is
-- -- a sensible value even when the rel has tuples=0
-- -- CREATE TABLE loct_empty (c1 int NOT NULL, c2 text);
-- --Testcase 764:
-- CREATE FOREIGN TABLE ft_empty (c1 int NOT NULL, c2 text)
--   SERVER sqlumdash_svr OPTIONS (table_name 'loct_empty');
-- --Testcase 765:
-- INSERT INTO "S 1".loct_empty
--   SELECT id, 'AAA' || to_char(id, 'FM000') FROM generate_series(1, 100) id;
-- --Testcase 766:
-- DELETE FROM "S 1".loct_empty;
-- -- ANALYZE ft_empty;
-- --Testcase 767:
-- EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_empty ORDER BY c1;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
--Testcase 49:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
--Testcase 50:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
--Testcase 51:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NULL;        -- NullTest
--Testcase 52:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NOT NULL;    -- NullTest
--Testcase 53:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
--Testcase 54:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
--Testcase 56:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
--Testcase 57:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
--Testcase 58:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef
--Testcase 59:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
--Testcase 60:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote
-- parameterized remote path for foreign table
--Testcase 61:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM "S 1"."T 1" a, ft2 b WHERE a."C 1" = 47 AND b.c1 = a.c2;
--Testcase 62:
SELECT * FROM ft2 a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;

-- check both safe and unsafe join conditions
--Testcase 63:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
--Testcase 64:
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
--Testcase 65:
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5));
--Testcase 66:
SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5));
-- we should not push order by clause with volatile expressions or unsafe
-- collations
--Testcase 67:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, random();
--Testcase 68:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- user-defined operator/function
--Testcase 69:
CREATE FUNCTION sqlumdashcs_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
--Testcase 70:
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- built-in operators and functions can be shipped for remote execution
--Testcase 71:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 72:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 73:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 74:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot
--Testcase 75:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = sqlumdashcs_fdw_abs(t1.c2);
--Testcase 76:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = sqlumdashcs_fdw_abs(t1.c2);
--Testcase 77:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 78:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ORDER BY can be shipped, though
--Testcase 79:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 80:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- but let's put them in an extension ...
--Testcase 768:
ALTER EXTENSION sqlumdashcs_fdw ADD FUNCTION sqlumdashcs_fdw_abs(int);
--Testcase 769:
ALTER EXTENSION sqlumdashcs_fdw ADD OPERATOR === (int, int);

-- ... now they can be shipped
--Testcase 81:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = sqlumdashcs_fdw_abs(t1.c2);
--Testcase 82:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = sqlumdashcs_fdw_abs(t1.c2);
--Testcase 83:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 84:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- and both ORDER BY and LIMIT can be shipped
--Testcase 85:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 86:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- ===================================================================
-- JOIN queries
-- ===================================================================
-- Analyze ft4 and ft5 so that we have better statistics. These tables do not
-- have use_remote_estimate set.
-- ANALYZE ft4;
-- ANALYZE ft5;

-- join two tables
--Testcase 87:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 88:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join three tables
--Testcase 89:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 90:
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join
--Testcase 91:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 92:
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables
--Testcase 93:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 94:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
--Testcase 95:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
--Testcase 96:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
--Testcase 97:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
--Testcase 98:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
-- right outer join
--Testcase 99:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 100:
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
-- right outer join three tables
--Testcase 101:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 102:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join
--Testcase 103:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
--Testcase 104:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
--Testcase 105:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 106:
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 107:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
--Testcase 108:
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
-- b. one of the joining relations is a base relation and the other is a join
-- relation
--Testcase 109:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 110:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries
--Testcase 111:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 112:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries
--Testcase 113:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
--Testcase 114:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join
--Testcase 115:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
--Testcase 116:
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
-- full outer join three tables
--Testcase 117:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 118:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + right outer join
--Testcase 119:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 120:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + full outer join
--Testcase 121:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 122:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + left outer join
--Testcase 123:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 124:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + full outer join
--Testcase 125:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 126:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + left outer join
--Testcase 127:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 128:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- left outer join + right outer join
--Testcase 129:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 130:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows
--Testcase 131:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 132:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set
--Testcase 133:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE sqlumdashcs_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--ALTER SERVER sqlumdash_svr OPTIONS (DROP extensions);
-- full outer join + WHERE clause with shippable extensions not set
--Testcase 134:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE sqlumdashcs_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
--Testcase 135:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 136:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 137:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
--Testcase 138:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause
--Testcase 139:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 140:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 141:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
--Testcase 142:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE
--Testcase 143:
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
--Testcase 144:
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference
--Testcase 145:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- SEMI JOIN, not pushed down
--Testcase 146:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 147:
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down
--Testcase 148:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 149:
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- CROSS JOIN can be pushed down
--Testcase 150:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 151:
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.
--Testcase 152:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 153:
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
--Testcase 154:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 155:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
--Testcase 156:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 157:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
--Testcase 158:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 159:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs
--Testcase 160:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
--Testcase 161:
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference
--Testcase 162:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;
--Testcase 163:
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
--Testcase 164:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;
--Testcase 165:
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
--Testcase 166:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;
--Testcase 167:
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;

-- join with nullable side with some columns with null values
--Testcase 168:
UPDATE ft5_a_child SET c3 = null where c1 % 9 = 0;
--Testcase 169:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;
--Testcase 170:
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
--Testcase 171:
CREATE TABLE local_tbl (c1 int NOT NULL, c2 int NOT NULL, c3 text, CONSTRAINT local_tbl_pkey PRIMARY KEY (c1));
--Testcase 172:
INSERT INTO local_tbl SELECT id, id % 10, to_char(id, 'FM0000') FROM generate_series(1, 1000) id;
--ANALYZE local_tbl;
--Testcase 772:
SET enable_nestloop TO false;
--Testcase 773:
SET enable_hashjoin TO false;
--Testcase 173:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 ORDER BY ft1.c1 FOR UPDATE;
--Testcase 174:
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 ORDER BY ft1.c1 FOR UPDATE;
--Testcase 774:
RESET enable_nestloop;
--Testcase 775:
RESET enable_hashjoin;
--Testcase 175:
DROP TABLE local_tbl;

-- check join pushdown in situations where multiple userids are involved
--Testcase 176:
CREATE ROLE regress_view_owner SUPERUSER;
--Testcase 177:
CREATE USER MAPPING FOR regress_view_owner SERVER sqlumdash_svr;
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;

--Testcase 178:
CREATE VIEW v4 AS SELECT * FROM ft4;
--Testcase 179:
CREATE VIEW v5 AS SELECT * FROM ft5;
--Testcase 776:
ALTER VIEW v5 OWNER TO regress_view_owner;
--Testcase 180:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
--Testcase 181:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 777:
ALTER VIEW v4 OWNER TO regress_view_owner;
--Testcase 182:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 183:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 184:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
--Testcase 185:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 778:
ALTER VIEW v4 OWNER TO CURRENT_USER;
--Testcase 186:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 187:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 779:
ALTER VIEW v4 OWNER TO regress_view_owner;

-- cleanup
--Testcase 188:
DROP OWNED BY regress_view_owner;
--Testcase 189:
DROP ROLE regress_view_owner;


-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================

-- Simple aggregates
--Testcase 190:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;
--Testcase 191:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

--Testcase 192:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;
--Testcase 193:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()
--Testcase 194:
explain (verbose, costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query
--Testcase 195:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;
--Testcase 196:
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel
--Testcase 197:
explain (verbose, costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions
--Testcase 198:
explain (verbose, costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;
--Testcase 199:
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are pushed down.
--Testcase 200:
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
--Testcase 201:
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
--Testcase 202:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;
--Testcase 203:
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
--Testcase 204:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
--Testcase 205:
explain (verbose, costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
--Testcase 206:
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
--Testcase 207:
explain (verbose, costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);
--Testcase 208:
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability
--Testcase 209:
explain (verbose, costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;
--Testcase 210:
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
--Testcase 211:
explain (verbose, costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
--Testcase 212:
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
--Testcase 213:
explain (verbose, costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
--Testcase 214:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1;
--Testcase 215:
select exists(select 1 from pg_enum), sum(c1) from ft1;

--Testcase 216:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;
--Testcase 217:
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;


-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order
--Testcase 218:
explain (verbose, costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
--Testcase 219:
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC
--Testcase 220:
explain (verbose, costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;
--Testcase 221:
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate
--Testcase 222:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 223:
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
--Testcase 224:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 225:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 226:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 227:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- FILTER within aggregate
--Testcase 228:
explain (verbose, costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
--Testcase 229:
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
--Testcase 230:
explain (verbose, costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;
--Testcase 231:
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query
--Testcase 232:
explain (verbose, costs off)
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 233:
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
-- Inner query is aggregation query
--Testcase 234:
explain (verbose, costs off)
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 235:
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
--Testcase 236:
explain (verbose, costs off)
select sum(c1) filter (where (c1 / c1) * random() <= 1) from ft1 group by c2 order by 1;
--Testcase 237:
explain (verbose, costs off)
select sum(c2) filter (where c2 in (select c2 from ft1 where c2 < 5)) from ft1;

-- Ordered-sets within aggregate
--Testcase 238:
explain (verbose, costs off)
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;
--Testcase 239:
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

-- Using multiple arguments within aggregates
--Testcase 240:
explain (verbose, costs off)
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;
--Testcase 241:
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC
--Testcase 242:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
--Testcase 243:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.
--Testcase 780:
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate
--Testcase 244:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension
--Testcase 781:
alter extension sqlumdashcs_fdw add function least_accum(anyelement, variadic anyarray);
--Testcase 782:
alter extension sqlumdashcs_fdw add aggregate least_agg(variadic items anyarray);
--alter server sqlumdashcs_svr options (set extensions 'postgres_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.
--Testcase 245:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;
--Testcase 246:
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension
--Testcase 783:
alter extension sqlumdashcs_fdw drop function least_accum(anyelement, variadic anyarray);
--Testcase 784:
alter extension sqlumdashcs_fdw drop aggregate least_agg(variadic items anyarray);
--alter server sqlumdash_svr options (set extensions 'postgres_fdw');

-- Not pushed down as we have dropped objects from extension.
--Testcase 247:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup
--Testcase 785:
reset enable_hashagg;
--Testcase 248:
drop aggregate least_agg(variadic items anyarray);
--Testcase 249:
drop function least_accum(anyelement, variadic anyarray);


-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.
--Testcase 250:
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

--Testcase 251:
create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

--Testcase 252:
create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

--Testcase 253:
create operator family my_op_family using btree;

--Testcase 254:
create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

--Testcase 255:
create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.
--Testcase 256:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Update local stats on ft2
--ANALYZE ft2;

-- Add into extension
--Testcase 786:
alter extension sqlumdashcs_fdw add operator class my_op_class using btree;
--Testcase 787:
alter extension sqlumdashcs_fdw add function my_op_cmp(a int, b int);
--Testcase 788:
alter extension sqlumdashcs_fdw add operator family my_op_family using btree;
--Testcase 789:
alter extension sqlumdashcs_fdw add operator public.<^(int, int);
--Testcase 790:
alter extension sqlumdashcs_fdw add operator public.=^(int, int);
--Testcase 791:
alter extension sqlumdashcs_fdw add operator public.>^(int, int);
--alter server sqlumdash_svr options (set extensions 'postgres_fdw');

-- Now this will be pushed as sort operator is part of the extension.
--Testcase 257:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
--Testcase 258:
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Remove from extension
--Testcase 792:
alter extension sqlumdashcs_fdw drop operator class my_op_class using btree;
--Testcase 793:
alter extension sqlumdashcs_fdw drop function my_op_cmp(a int, b int);
--Testcase 794:
alter extension sqlumdashcs_fdw drop operator family my_op_family using btree;
--Testcase 795:
alter extension sqlumdashcs_fdw drop operator public.<^(int, int);
--Testcase 796:
alter extension sqlumdashcs_fdw drop operator public.=^(int, int);
--Testcase 797:
alter extension sqlumdashcs_fdw drop operator public.>^(int, int);
--alter server sqlumdash_svr options (set extensions 'postgres_fdw');

-- This will not be pushed as sort operator is now removed from the extension.
--Testcase 259:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup
--Testcase 260:
drop operator class my_op_class using btree;
--Testcase 261:
drop function my_op_cmp(a int, b int);
--Testcase 262:
drop operator family my_op_family using btree;
--Testcase 263:
drop operator public.>^(int, int);
--Testcase 264:
drop operator public.=^(int, int);
--Testcase 265:
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
--Testcase 266:
explain (verbose, costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate
--Testcase 267:
explain (verbose, costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;
--Testcase 268:
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
--Testcase 269:
explain (verbose, costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;
--Testcase 270:
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
--Testcase 271:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);
--Testcase 272:
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
--Testcase 273:
explain (verbose, costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;
--Testcase 274:
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization
--Testcase 798:
set enable_hashagg to false;
--Testcase 275:
explain (verbose, costs off)
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1."C 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C 1" < 100 order by 1;
--Testcase 276:
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1."C 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C 1" < 100 order by 1;
--Testcase 799:
reset enable_hashagg;

-- bug #15613: bad plan for foreign table scan with lateral reference
--Testcase 277:
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

--Testcase 278:
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
--Testcase 279:
explain (verbose, costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);
--Testcase 280:
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);


-- Not supported cases
-- Grouping sets
--Testcase 281:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 282:
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 283:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 284:
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 285:
explain (verbose, costs off)
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 286:
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 287:
explain (verbose, costs off)
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;
--Testcase 288:
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
--Testcase 289:
explain (verbose, costs off)
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;
--Testcase 290:
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

-- WindowAgg
--Testcase 291:
explain (verbose, costs off)
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 292:
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 293:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 294:
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 295:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 296:
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;


-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join
--Testcase 297:
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
--Testcase 298:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);
--Testcase 299:
EXECUTE st1(1, 1);
--Testcase 300:
EXECUTE st1(101, 101);
-- subquery using stable function (can't be sent to remote)
--Testcase 301:
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;
--Testcase 302:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);
--Testcase 303:
EXECUTE st2(10, 20);
--Testcase 304:
EXECUTE st2(101, 121);
-- subquery using immutable function (can be sent to remote)
--Testcase 305:
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;
--Testcase 306:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);
--Testcase 307:
EXECUTE st3(10, 20);
--Testcase 308:
EXECUTE st3(20, 30);
-- custom plan should be chosen initially
--Testcase 309:
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;
--Testcase 310:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 311:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 312:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 313:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 314:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
--Testcase 315:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
--Testcase 316:
PREPARE st5(text,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
--Testcase 317:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 318:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 319:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 320:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 321:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 322:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 323:
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
--Testcase 324:
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 325:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 326:
PREPARE st7 AS INSERT INTO ft1 (c1,c2,c3) VALUES (1001,101,'foo');
--Testcase 327:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
--Testcase 328:
INSERT INTO "S 1"."T 0" SELECT * FROM "S 1"."T 1";
--Testcase 800:
ALTER FOREIGN TABLE ft1 OPTIONS (SET table_name 'T 0');
--Testcase 329:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 330:
EXECUTE st6;
--Testcase 331:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
--Testcase 332:
DELETE FROM "S 1"."T 0";
--Testcase 801:
ALTER FOREIGN TABLE ft1_a_child OPTIONS (SET table_name 'T 1');

--Testcase 333:
PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 334:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--ALTER SERVER loopback OPTIONS (DROP extensions);
--Testcase 335:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--Testcase 336:
EXECUTE st8;
--ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

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
--Testcase 337:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;
--Testcase 338:
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1_a'::regclass LIMIT 1;
--Testcase 339:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 340:
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
-- No suport ctid ---

--Testcase 733:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';

--Testcase 734:
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 343:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;
--Testcase 344:
SELECT ctid, * FROM ft1 t1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================
--Testcase 345:
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
--Testcase 346:
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
--Testcase 347:
SELECT f_test(100);
--Testcase 348:
DROP FUNCTION f_test(int);

-- -- ===================================================================
-- -- REINDEX
-- -- ===================================================================
-- -- remote table is not created here
-- --Testcase 802:
-- CREATE FOREIGN TABLE reindex_foreign (c1 int, c2 int)
--   SERVER sqlumdash_svr2 OPTIONS (table_name 'reindex_local');
-- REINDEX TABLE reindex_foreign; -- error
-- REINDEX TABLE CONCURRENTLY reindex_foreign; -- error
-- --Testcase 803:
-- DROP FOREIGN TABLE reindex_foreign;
-- -- partitions and foreign tables
-- --Testcase 804:
-- CREATE TABLE reind_fdw_parent (c1 int) PARTITION BY RANGE (c1);
-- --Testcase 805:
-- CREATE TABLE reind_fdw_0_10 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (0) TO (10);
-- --Testcase 806:
-- CREATE FOREIGN TABLE reind_fdw_10_20 PARTITION OF reind_fdw_parent
--   FOR VALUES FROM (10) TO (20)
--   SERVER sqlumdash_svr OPTIONS (table_name 'reind_local_10_20');
-- REINDEX TABLE reind_fdw_parent; -- ok
-- REINDEX TABLE CONCURRENTLY reind_fdw_parent; -- ok
-- --Testcase 807:
-- DROP TABLE reind_fdw_parent;

-- ===================================================================
-- conversion error
-- ===================================================================
--Testcase 808:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE int;
--Testcase 349:
SELECT * FROM ft1 ftx(x1, x2, x3, x4, x5, x6, x7, x8) WHERE x1 = 1;  -- ERROR
--Testcase 350:
SELECT  ftx.x1,  ft2.c2, ftx.x8 FROM ft1 ftx(x1, x2, x3, x4, x5, x6, x7, x8), ft2 WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
--Testcase 351:
SELECT  ftx.x1,  ft2.c2, ftx FROM ft1 ftx(x1, x2, x3, x4, x5, x6, x7, x8), ft2 WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
--Testcase 352:
SELECT sum(c2), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR
--Testcase 809:
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE text;

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;
--Testcase 353:
FETCH c;
SAVEPOINT s;
ERROR OUT;          -- ERROR
ROLLBACK TO s;
--Testcase 354:
FETCH c;
SAVEPOINT s;
--Testcase 355:
explain verbose SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
--Testcase 735:
SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK TO s;
--Testcase 356:
FETCH c;
--Testcase 357:
SELECT * FROM ft1 ORDER BY c1 LIMIT 1;
COMMIT;

-- ===================================================================
-- test handling of collations
-- ===================================================================
--Testcase 358:
create foreign table loct31_a_child (f1 text, f2 text, f3 varchar(10))
  server sqlumdash_svr options (table_name 'loct31');
--Testcase 978:
create table loct31 (f1 text, f2 text, f3 varchar(10), spdurl text) PARTITION BY LIST (spdurl);
--Testcase 979:
create foreign table loct31_a PARTITION OF loct31 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 359:
create foreign table ft3_a_child (f1 text collate "C", f2 text, f3 varchar(10))
  server sqlumdash_svr options (table_name 'loct31');
--Testcase 980:
create table ft3 (f1 text collate "C", f2 text, f3 varchar(10), spdurl text) PARTITION BY LIST (spdurl);
--Testcase 981:
create foreign table ft3_a PARTITION OF ft3 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- can be sent to remote
--Testcase 360:
explain (verbose, costs off) select * from ft3 where f1 = 'foo';
--Testcase 361:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "C" = 'foo';
--Testcase 362:
explain (verbose, costs off) select * from ft3 where f2 = 'foo';
--Testcase 363:
explain (verbose, costs off) select * from ft3 where f3 = 'foo';
--Testcase 364:
explain (verbose, costs off) select * from ft3 f, loct31 l
  where f.f3 = l.f3 and l.f1 = 'foo';
-- can't be sent to remote
--Testcase 365:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';
--Testcase 366:
explain (verbose, costs off) select * from ft3 where f1 = 'foo' COLLATE "C";
--Testcase 367:
explain (verbose, costs off) select * from ft3 where f2 COLLATE "C" = 'foo';
--Testcase 368:
explain (verbose, costs off) select * from ft3 where f2 = 'foo' COLLATE "C";
--Testcase 369:
explain (verbose, costs off) select * from ft3 f, loct31 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================
--Testcase 370:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
--Testcase 371:
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 LIMIT 20;
--Testcase 372:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1101,201,'aaa'), (1102,202,'bbb'), (1103,203,'ccc');
--Testcase 373:
SELECT * FROM ft2 WHERE c1 >= 1101;
--Testcase 374:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1104,204,'ddd'), (1105,205,'eee');
--Testcase 375:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;              -- can be pushed down
--Testcase 376:
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;
--Testcase 377:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;  -- can be pushed down
--Testcase 378:
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;
--Testcase 379:
SELECT * FROM ft2 WHERE c1 % 10 = 7;
--Testcase 380:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;                               -- can be pushed down
--Testcase 381:
UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;
--Testcase 382:
EXPLAIN (verbose, costs off)
  DELETE FROM ft2_a_child WHERE c1 % 10 = 5;                               -- can be pushed down
--Testcase 383:
SELECT c1, c4 FROM ft2 WHERE c1 % 10 = 5;
--Testcase 384:
DELETE FROM ft2_a_child WHERE c1 % 10 = 5;
--Testcase 385:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child USING ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 2;                -- can be pushed down
--Testcase 386:
DELETE FROM ft2_a_child USING ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 2;
--Testcase 387:
SELECT c1,c2,c3,c4 FROM ft2 ORDER BY c1;
--Testcase 388:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 389:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 390:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200;             -- can be pushed down
--Testcase 391:
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200;
--Testcase 392:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child WHERE c1 = 1200;                       -- can be pushed down
--Testcase 393:
DELETE FROM ft2_a_child WHERE c1 = 1200;

-- Test UPDATE/DELETE on a three-table join
--Testcase 394:
INSERT INTO ft2_a_child (c1,c2,c3)
  SELECT id, id - 1200, to_char(id, 'FM00000') FROM generate_series(1201, 1300) id;
--Testcase 395:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c2 = ft4.c1;       -- can be pushed down
--Testcase 396:
UPDATE ft2_a_child SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c2 = ft4.c1;
--Testcase 397:
SELECT ft2, ft2.*, ft4, ft4.*
  FROM ft2 INNER JOIN ft4 ON (ft2.c1 > 1200 AND ft2.c2 = ft4.c1)
  INNER JOIN ft5 ON (ft4.c1 = ft5.c1);
--Testcase 398:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c1 % 10 = 0 AND ft2_a_child.c2 = ft4.c1;                          -- can be pushed down
--Testcase 399:
SELECT 100 FROM ft2,
  ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1;
--Testcase 400:
DELETE FROM ft2_a_child
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c1 % 10 = 0 AND ft2_a_child.c2 = ft4.c1;
--Testcase 401:
DELETE FROM ft2_a_child WHERE ft2_a_child.c1 > 1200;

-- Test UPDATE with a MULTIEXPR sub-select
-- (maybe someday this'll be remotely executable, but not today)
--Testcase 402:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;
--Testcase 736:
UPDATE ft2_a_child AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

--Testcase 737:
UPDATE ft2_a_child AS target SET c2 = (
   SELECT c2 / 10
       FROM ft2 AS src
       WHERE target.c1 = src.c1
) WHERE c1 > 1100;

-- Test UPDATE involving a join that can be pushed down,
-- but a SET clause that can't be
--Testcase 810:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE ft2_a_child d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;
--Testcase 811:
UPDATE ft2_a_child d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;

-- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- user-defined operators/functions
-- ALTER SERVER loopback OPTIONS (DROP extensions);
--Testcase 403:
INSERT INTO ft2_a_child (c1,c2,c3)
  SELECT id, id % 10, to_char(id, 'FM00000') FROM generate_series(2001, 2010) id;
--Testcase 404:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE sqlumdashcs_fdw_abs(c1) > 2000 ;            -- can't be pushed down
--Testcase 405:
UPDATE ft2_a_child SET c3 = 'bar' WHERE sqlumdashcs_fdw_abs(c1) > 2000 ;
--Testcase 406:
SELECT * FROM ft2 WHERE sqlumdashcs_fdw_abs(c1) > 2000 ;
--Testcase 407:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1;                                                -- can't be pushed down
--Testcase 408:
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1;
--Testcase 409:
SELECT ft2.*, ft4.*, ft5.* FROM ft2, 
  ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1;
--Testcase 410:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1;       -- can't be pushed down
--Testcase 411:
SELECT ft2.c1, ft2.c2, ft2.c3 FROM ft2, 
  ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1;
--Testcase 412:
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1;
--Testcase 413:
DELETE FROM ft2_a_child WHERE ft2_a_child.c1 > 2000;
-- ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

-- Test that trigger on remote table works as expected
--Testcase 414:
CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.c3 = NEW.c3 || '_trig_update';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--Testcase 415:
CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
    ON ft2_a_child FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

--Testcase 416:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1208, 818, 'fff');
--Testcase 417:
SELECT * FROM ft2 WHERE c1 = 1208;
--Testcase 418:
INSERT INTO ft2_a_child (c1,c2,c3,c6) VALUES (1218, 818, 'ggg', '(--;');
--Testcase 419:
SELECT * FROM ft2 WHERE c1 = 1218;
--Testcase 420:
UPDATE ft2_a_child SET c2 = c2 + 600 WHERE c1 % 10 = 8 AND c1 < 1200;
--Testcase 421:
SELECT * FROM ft2 WHERE c1 % 10 = 8 AND c1 < 1200;

-- Test errors thrown on remote side during update
-- create table in the remote server with check contraint
--Testcase 738:
CREATE FOREIGN TABLE ft1_constraint_a_child (
	c1 int OPTIONS (key 'true'),
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 text
) SERVER sqlumdash_svr OPTIONS (table_name 't1_constraint');

--Testcase 982:
CREATE TABLE ft1_constraint (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 text,
	spdurl text) PARTITION BY LIST (spdurl);

--Testcase 983:
CREATE FOREIGN TABLE ft1_constraint_a PARTITION OF ft1_constraint FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 747:
INSERT INTO ft1_constraint_a_child SELECT * FROM ft1_a_child ON CONFLICT DO NOTHING;
-- c2 must be greater than or equal to 0, so this case is ignored.
--Testcase 748:
INSERT INTO ft1_constraint_a_child(c1, c2) VALUES (2222, -2) ON CONFLICT DO NOTHING; -- ignore, do nothing
--Testcase 749:
SELECT c1, c2 FROM ft1_constraint WHERE c1 = 2222 or c2 = -2; -- empty result
--Testcase 812:
ALTER FOREIGN TABLE ft1_a_child RENAME TO ft1_org;
--Testcase 813:
ALTER FOREIGN TABLE ft1_constraint_a_child RENAME TO ft1_a_child;
--Testcase 422:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12);  -- duplicate key
--Testcase 424:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO NOTHING; -- unsupported
--Testcase 425:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO UPDATE SET c3 = 'ffg'; -- unsupported
--Testcase 740:
INSERT INTO ft1_a_child(c1, c2) VALUES(1111, -2);  -- c2positive
--Testcase 741:
UPDATE ft1_a_child SET c2 = -c2 WHERE c1 = 1;  -- c2positive
--Testcase 814:
ALTER FOREIGN TABLE ft1_a_child RENAME TO ft1_constraint_a_child;
--Testcase 815:
ALTER FOREIGN TABLE ft1_org RENAME TO ft1_a_child;

-- Test savepoint/rollback behavior
--Testcase 426:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 427:
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
begin;
--Testcase 428:
update ft2_a_child set c2 = 42 where c2 = 0;
--Testcase 429:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s1;
--Testcase 430:
update ft2_a_child set c2 = 44 where c2 = 4;
--Testcase 431:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s1;
--Testcase 432:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s2;
--Testcase 433:
update ft2_a_child set c2 = 46 where c2 = 6;
--Testcase 434:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
rollback to savepoint s2;
--Testcase 435:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s2;
--Testcase 436:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s3;
--update ft2 set c2 = -2 where c2 = 42 and c1 = 10; -- fail on remote side
rollback to savepoint s3;
--Testcase 437:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s3;
--Testcase 438:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- none of the above is committed yet remotely
--Testcase 439:
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
commit;
--Testcase 440:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 441:
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;

--VACUUM ANALYZE "S 1"."T 1";

-- Above DMLs add data with c6 as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- FIRST behavior here.
-- ORDER BY DESC NULLS LAST options
--Testcase 442:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;
--Testcase 443:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795  LIMIT 10;
-- ORDER BY DESC NULLS FIRST options
--Testcase 444:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 445:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- ORDER BY ASC NULLS FIRST options
--Testcase 446:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 447:
SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

-- ===================================================================
-- test check constraints
-- ===================================================================
--Testcase 816:
ALTER FOREIGN TABLE ft1_a_child RENAME TO ft1_org;
--Testcase 817:
ALTER FOREIGN TABLE ft1_constraint_a_child RENAME TO ft1_a_child;
-- Consistent check constraints provide consistent results
--Testcase 984:
ALTER TABLE ft1 ADD CONSTRAINT ft1_c2positive CHECK (c2 >= 0);
--Testcase 1009:
SET constraint_exclusion = 'off';
--Testcase 818:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 449:
SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 819:
SET constraint_exclusion = 'on';
--Testcase 450:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 451:
SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 820:
RESET constraint_exclusion;
-- check constraint is enforced on the remote side, not locally
--Testcase 742:
INSERT INTO ft1_a_child (c1, c2) VALUES(1111, -2);  -- c2positive
--Testcase 743:
UPDATE ft1_a_child SET c2 = -c2 WHERE c1 = 1;  -- c2positive
--Testcase 821:
ALTER TABLE ft1 DROP CONSTRAINT ft1_c2positive;

-- But inconsistent check constraints provide inconsistent results
--Testcase 822:
ALTER TABLE ft1 ADD CONSTRAINT ft1_c2negative CHECK (c2 < 0);
--Testcase 1010:
SET constraint_exclusion = 'off';
--Testcase 452:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 453:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 823:
SET constraint_exclusion = 'on';
--Testcase 454:
EXPLAIN (VERBOSE, COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 455:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 824:
RESET constraint_exclusion;
-- local check constraint is not actually enforced
--Testcase 456:
INSERT INTO ft1_a_child(c1, c2) VALUES(1111, 2);
--Testcase 457:
UPDATE ft1_a_child SET c2 = c2 + 1 WHERE c1 = 1;
--Testcase 825:
ALTER TABLE ft1 DROP CONSTRAINT ft1_c2negative;

-- ===================================================================
-- test WITH CHECK OPTION constraints
-- ===================================================================

--Testcase 458:
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;

--Testcase 459:
CREATE FOREIGN TABLE foreign_tbl_a_child (a int options(key 'true'), b int)
  SERVER sqlumdash_svr OPTIONS (table_name 'base_tbl');
--Testcase 985:
CREATE TABLE foreign_tbl (a int, b int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 986:
CREATE FOREIGN TABLE foreign_tbl_a PARTITION OF foreign_tbl FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 460:
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl_a_child FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();
--Testcase 461:
CREATE VIEW rw_view AS SELECT * FROM foreign_tbl_a_child
  WHERE a < b WITH CHECK OPTION;
--Testcase 462:
\d+ rw_view

--Testcase 463:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 5);
--Testcase 464:
INSERT INTO rw_view VALUES (0, 5); -- should fail
--Testcase 465:
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 15);
--Testcase 466:
INSERT INTO rw_view VALUES (0, 15); -- ok
--Testcase 467:
SELECT * FROM foreign_tbl;

--Testcase 468:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 5;
--Testcase 469:
UPDATE rw_view SET b = b + 5; -- should fail
--Testcase 470:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE rw_view SET b = b + 15;
--Testcase 471:
UPDATE rw_view SET b = b + 15; -- ok
--Testcase 472:
SELECT * FROM foreign_tbl;

--Testcase 473:
DELETE FROM foreign_tbl;
--Testcase 474:
DROP TRIGGER row_before_insupd_trigger ON foreign_tbl_a_child;
--Testcase 475:
DROP FOREIGN TABLE foreign_tbl CASCADE;

-- test WCO for partitions

-- --CREATE TABLE child_tbl (a int, b int);
-- --ALTER TABLE child_tbl SET (autovacuum_enabled = 'false');
-- --Testcase 476:
-- CREATE FOREIGN TABLE foreign_tbl (a int options(key 'true'), b int)
--   SERVER sqlumdash_svr OPTIONS (table_name 'base_tbl');
-- --Testcase 477:
-- CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

-- --Testcase 478:
-- CREATE TABLE parent_tbl (a int, b int) PARTITION BY RANGE(a);
-- --Testcase 826:
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);

-- --Testcase 479:
-- CREATE VIEW rw_view AS SELECT * FROM parent_tbl
--   WHERE a < b WITH CHECK OPTION;
-- --Testcase 480:
-- \d+ rw_view

-- --Testcase 481:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 5);
-- --Testcase 482:
-- INSERT INTO rw_view VALUES (0, 5); -- should fail
-- --Testcase 483:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 15);
-- --Testcase 484:
-- INSERT INTO rw_view VALUES (0, 15); -- ok
-- --Testcase 485:
-- SELECT * FROM foreign_tbl;

-- --Testcase 486:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE rw_view SET b = b + 5;
-- --Testcase 487:
-- UPDATE rw_view SET b = b + 5; -- should fail
-- --Testcase 488:
-- EXPLAIN (VERBOSE, COSTS OFF)
-- UPDATE rw_view SET b = b + 15;
-- --Testcase 489:
-- UPDATE rw_view SET b = b + 15; -- ok
-- --Testcase 490:
-- SELECT * FROM foreign_tbl;

-- --Testcase 491:
-- DELETE FROM foreign_tbl;
-- --Testcase 492:
-- DROP TRIGGER row_before_insupd_trigger ON foreign_tbl;
-- --Testcase 493:
-- DROP FOREIGN TABLE foreign_tbl CASCADE;
-- --Testcase 494:
-- DROP TABLE parent_tbl CASCADE;

-- --Testcase 495:
-- DROP FUNCTION row_before_insupd_trigfunc;

-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- ===================================================================
--Testcase 496:
create foreign table loc1_a_child (f1 serial, f2 text, id integer options (key 'true'))
  server sqlumdash_svr options (table_name 'loc1');
--Testcase 987:
create table loc1 (f1 serial, f2 text, id integer, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 988:
create foreign table loc1_a PARTITION OF loc1 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 744:
create foreign table rem1_a_child (f1 serial, f2 text, id integer OPTIONS (key 'true'))
  server sqlumdash_svr options(table_name 'loc1');
--Testcase 989:
create table rem1 (f1 serial, f2 text, id integer, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 990:
create foreign table rem1_a PARTITION OF rem1 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 497:
select pg_catalog.setval('rem1_a_child_f1_seq', 10, false);
--Testcase 745:
insert into loc1_a_child(f2) values('hi');
--Testcase 498:
insert into rem1_a_child(f2) values('hi remote');
--Testcase 746:
insert into loc1_a_child(f2) values('bye');
--Testcase 499:
insert into rem1_a_child(f2) values('bye remote');
--select * from loc1;
--Testcase 500:
select f1, f2 from rem1;

-- ===================================================================
-- test generated columns
-- ===================================================================

--Testcase 501:
create foreign table grem1_a_child (
  a int OPTIONS (key 'true'),
  b int generated always as (a * 2) stored)
  server sqlumdash_svr options(table_name 'gloc1');

--Testcase 991:
create table grem1 (
  a int,
  b int generated always as (a * 2) stored,
  spdurl text) PARTITION BY LIST (spdurl);

--Testcase 992:
create foreign table grem1_a PARTITION OF grem1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 502:
explain (verbose, costs off)
insert into grem1_a_child (a) values (1), (2);
--Testcase 827:
insert into grem1_a_child (a) values (1), (2);
--Testcase 503:
explain (verbose, costs off)
update grem1_a_child set a = 22 where a = 2;
--Testcase 828:
update grem1_a_child set a = 22 where a = 2;
--Testcase 504:
select * from grem1;
--Testcase 829:
delete from grem1_a_child;

-- -- SQLumdash does not support COPY FROM
-- -- test copy from
-- copy grem1 from stdin;
-- 1
-- 2
-- \.
-- select * from gloc1;
-- select * from grem1;
-- delete from grem1;

-- -- test batch insert
-- --Testcase 830:
-- alter server sqlumdash_svr options (add batch_size '10');
-- --Testcase 831:
-- explain (verbose, costs off)
-- insert into grem1 (a) values (1), (2);
-- --Testcase 832:
-- insert into grem1 (a) values (1), (2);
-- --Testcase 833:
-- select * from grem1;
-- --Testcase 834:
-- delete from grem1;
-- --Testcase 835:
-- alter server sqlumdash_svr options (drop batch_size);

-- ===================================================================
-- test local triggers
-- ===================================================================

-- Trigger functions "borrowed" from triggers regress test.
--Testcase 505:
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

--Testcase 506:
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 507:
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 508:
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
--Testcase 509:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 510:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 511:
delete from rem1_a_child;
--Testcase 512:
insert into rem1_a_child values(1,'insert');
--Testcase 513:
update rem1_a_child set f2  = 'update' where f1 = 1;
--Testcase 514:
update rem1_a_child set f2 = f2 || f2;


-- cleanup
--Testcase 515:
DROP TRIGGER trig_row_before ON rem1_a_child;
--Testcase 516:
DROP TRIGGER trig_row_after ON rem1_a_child;
--Testcase 517:
DROP TRIGGER trig_stmt_before ON rem1_a_child;
--Testcase 518:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

--Testcase 519:
DELETE from rem1_a_child;

-- Test multiple AFTER ROW triggers on a foreign table
--Testcase 520:
CREATE TRIGGER trig_row_after1
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 521:
CREATE TRIGGER trig_row_after2
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 522:
insert into rem1_a_child values(1,'insert');
--Testcase 523:
update rem1_a_child set f2  = 'update' where f1 = 1;
--Testcase 524:
update rem1_a_child set f2 = f2 || f2;
--Testcase 525:
delete from rem1_a_child;

-- cleanup
--Testcase 526:
DROP TRIGGER trig_row_after1 ON rem1_a_child;
--Testcase 527:
DROP TRIGGER trig_row_after2 ON rem1_a_child;

-- Test WHEN conditions

--Testcase 528:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 529:
CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens
--Testcase 530:
INSERT INTO rem1_a_child values(1, 'insert');
--Testcase 531:
UPDATE rem1_a_child set f2 = 'test';

-- Insert or update matching: triggers are fired
--Testcase 532:
INSERT INTO rem1_a_child values(2, 'update');
--Testcase 533:
UPDATE rem1_a_child set f2 = 'update update' where f1 = '2';

--Testcase 534:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 535:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1
--Testcase 536:
DELETE FROM rem1_a_child;

-- cleanup
--Testcase 537:
DROP TRIGGER trig_row_before_insupd ON rem1_a_child;
--Testcase 538:
DROP TRIGGER trig_row_after_insupd ON rem1_a_child;
--Testcase 539:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;
--Testcase 540:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;


-- Test various RETURN statements in BEFORE triggers.

--Testcase 541:
CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

--Testcase 542:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- The new values should have 'triggered' appended
--Testcase 543:
INSERT INTO rem1_a_child values(1, 'insert');
--Testcase 544:
SELECT f1, f2 from rem1;
--Testcase 545:
INSERT INTO rem1_a_child values(2, 'insert');
--Testcase 546:
SELECT f2 from rem1 WHERE f1 = 2;
--Testcase 547:
SELECT f1, f2 from rem1;
--Testcase 548:
UPDATE rem1_a_child set f2 = '';
--Testcase 549:
SELECT f1, f2 from rem1;
--Testcase 550:
UPDATE rem1_a_child set f2 = 'skidoo';
--Testcase 551:
SELECT f2 from rem1;
--Testcase 552:
SELECT f1, f2 from rem1;
--Testcase 553:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f1 = 10;          -- all columns should be transmitted
--Testcase 554:
UPDATE rem1_a_child set f1 = 10;
--Testcase 555:
SELECT f1, f2 from rem1;

--Testcase 556:
DELETE FROM rem1_a_child;

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger
--Testcase 557:
CREATE TRIGGER trig_row_before_insupd2
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 558:
INSERT INTO rem1_a_child values(1, 'insert');
--Testcase 559:
SELECT f1, f2 from rem1;
--Testcase 560:
INSERT INTO rem1_a_child values(2, 'insert');
--Testcase 561:
SELECT f2 from rem1 WHERE f1 = 2;
--Testcase 562:
SELECT f1, f2 from rem1;
--Testcase 563:
UPDATE rem1_a_child set f2 = '';
--Testcase 564:
SELECT f1, f2 from rem1;
--Testcase 565:
UPDATE rem1_a_child set f2 = 'skidoo';
--Testcase 566:
SELECT f2 from rem1;
--Testcase 567:
SELECT f1, f2 from rem1;

--Testcase 568:
DROP TRIGGER trig_row_before_insupd ON rem1_a_child;
--Testcase 569:
DROP TRIGGER trig_row_before_insupd2 ON rem1_a_child;

--Testcase 570:
DELETE from rem1_a_child;

--Testcase 571:
INSERT INTO rem1_a_child VALUES (1, 'test');

-- Test with a trigger returning NULL
--Testcase 572:
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

--Testcase 573:
CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.
--Testcase 574:
INSERT INTO rem1_a_child VALUES (2, 'test2');

--Testcase 575:
SELECT f1, f2 from rem1;

--Testcase 576:
UPDATE rem1_a_child SET f2 = 'test2';

--Testcase 577:
SELECT f1, f2 from rem1;

--Testcase 578:
DELETE from rem1_a_child;

--Testcase 579:
SELECT f1, f2 from rem1;

--Testcase 580:
DROP TRIGGER trig_null ON rem1_a_child;
--Testcase 581:
DELETE from rem1_a_child;

-- Test a combination of local and remote triggers
--Testcase 582:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 583:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 584:
CREATE TRIGGER trig_local_before BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 585:
INSERT INTO rem1_a_child(f2) VALUES ('test');
--Testcase 586:
UPDATE rem1_a_child SET f2 = 'testo';

-- Test returning a system attribute
--Testcase 587:
INSERT INTO rem1_a_child(f2) VALUES ('test');

-- cleanup
--Testcase 588:
DROP TRIGGER trig_row_before ON rem1_a_child;
--Testcase 589:
DROP TRIGGER trig_row_after ON rem1_a_child;
--Testcase 590:
DROP TRIGGER trig_local_before ON rem1_a_child;


-- Test direct foreign table modification functionality
--Testcase 836:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 837:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child WHERE false;     -- currently can't be pushed down

-- Test with statement-level triggers
--Testcase 591:
CREATE TRIGGER trig_stmt_before
	BEFORE DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 592:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 593:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 594:
DROP TRIGGER trig_stmt_before ON rem1_a_child;

--Testcase 595:
CREATE TRIGGER trig_stmt_after
	AFTER DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 596:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 597:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 598:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

-- Test with row-level ON INSERT triggers
--Testcase 599:
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 600:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 601:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 602:
DROP TRIGGER trig_row_before_insert ON rem1_a_child;

--Testcase 603:
CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 604:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 605:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 606:
DROP TRIGGER trig_row_after_insert ON rem1_a_child;

-- Test with row-level ON UPDATE triggers
--Testcase 607:
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 608:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down
--Testcase 609:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 610:
DROP TRIGGER trig_row_before_update ON rem1_a_child;

--Testcase 611:
CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 612:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down
--Testcase 613:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 614:
DROP TRIGGER trig_row_after_update ON rem1_a_child;

-- Test with row-level ON DELETE triggers
--Testcase 615:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 616:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 617:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down
--Testcase 618:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;

--Testcase 619:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 620:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 621:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down
--Testcase 622:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;

-- ===================================================================
-- test inheritance features
-- ===================================================================

--Testcase 623:
CREATE TABLE a (aa TEXT);
--Testcase 838:
ALTER TABLE a SET (autovacuum_enabled = 'false');
--Testcase 624:
CREATE FOREIGN TABLE b_b_child (bb TEXT OPTIONS (key 'true')) INHERITS (a)
SERVER sqlumdash_svr OPTIONS (table_name 'loct9');
--Testcase 993:
CREATE TABLE b (aa TEXT, bb TEXT, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 994:
CREATE FOREIGN TABLE b_b PARTITION OF b FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 625:
INSERT INTO a(aa) VALUES('aaa');
--Testcase 626:
INSERT INTO a(aa) VALUES('aaaa');
--Testcase 627:
INSERT INTO a(aa) VALUES('aaaaa');

--Testcase 628:
INSERT INTO b_b_child(aa) VALUES('bbb');
--Testcase 629:
INSERT INTO b_b_child(aa) VALUES('bbbb');
--Testcase 630:
INSERT INTO b_b_child(aa) VALUES('bbbbb');

--Testcase 631:
SELECT tableoid::regclass, * FROM a;
--Testcase 632:
SELECT tableoid::regclass, * FROM b;
--Testcase 633:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 634:
UPDATE a SET aa = 'zzzzzz' WHERE aa LIKE 'aaaa%';

--Testcase 635:
SELECT tableoid::regclass, * FROM a;
--Testcase 636:
SELECT tableoid::regclass, * FROM b;
--Testcase 637:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 638:
UPDATE b_b_child SET aa = 'new';

--Testcase 639:
SELECT tableoid::regclass, * FROM a;
--Testcase 640:
SELECT tableoid::regclass, * FROM b;
--Testcase 641:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 642:
UPDATE a SET aa = 'newtoo';

--Testcase 643:
SELECT tableoid::regclass, * FROM a;
--Testcase 644:
SELECT tableoid::regclass, * FROM b;
--Testcase 645:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 646:
DELETE FROM a;

--Testcase 647:
SELECT tableoid::regclass, * FROM a;
--Testcase 648:
SELECT tableoid::regclass, * FROM b;
--Testcase 649:
SELECT tableoid::regclass, * FROM ONLY a;

--Testcase 650:
DROP TABLE a CASCADE;

-- Check SELECT FOR UPDATE/SHARE with an inherited source table

--Testcase 651:
create table foo (f1 int, f2 int);
--Testcase 652:
create foreign table foo2_a_child (f3 int OPTIONS (key 'true')) inherits (foo)
  server sqlumdash_svr options (table_name 'loct1');
--Testcase 995:
create table foo2 (f1 int, f2 int, f3 int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 996:
create foreign table foo2_a PARTITION OF foo2 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 653:
create table bar (f1 int, f2 int);
--Testcase 654:
create foreign table bar2_a_child (f3 int OPTIONS (key 'true')) inherits (bar)
  server sqlumdash_svr options (table_name 'loct2');
--Testcase 997:
create table bar2 (f1 int, f2 int, f3 int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 998:
create foreign table bar2_a PARTITION OF bar2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 839:
alter table foo set (autovacuum_enabled = 'false');
--Testcase 840:
alter table bar set (autovacuum_enabled = 'false');

--Testcase 655:
insert into foo values(1,1);
--Testcase 656:
insert into foo values(3,3);
--Testcase 657:
insert into foo2_a_child values(2,2,2);
--Testcase 658:
insert into foo2_a_child values(4,4,4);
--Testcase 659:
insert into bar values(1,11);
--Testcase 660:
insert into bar values(2,22);
--Testcase 661:
insert into bar values(6,66);
--Testcase 662:
insert into bar2_a_child values(3,33,33);
--Testcase 663:
insert into bar2_a_child values(4,44,44);
--Testcase 664:
insert into bar2_a_child values(7,77,77);

--Testcase 665:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for update;
--Testcase 666:
select * from bar where f1 in (select f1 from foo) for update;

--Testcase 667:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for share;
--Testcase 668:
select * from bar where f1 in (select f1 from foo) for share;

-- Now check SELECT FOR UPDATE/SHARE with an inherited source table,
-- where the parent is itself a foreign table
--Testcase 841:
create foreign table foo2child_a_child (f3 int) inherits (foo2_a_child)
  server sqlumdash_svr options (table_name 'loct43');
--Testcase 999:
create table foo2child (f3 int, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 1000:
create foreign table foo2child_a PARTITION OF foo2child FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 842:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo2) for share;
--Testcase 843:
select * from bar where f1 in (select f1 from foo2) for share;

--Testcase 844:
drop foreign table foo2child_a_child;
--Testcase 1001:
drop table foo2child;

-- And with a local child relation of the foreign table parent
--Testcase 845:
create table foo2child (f3 int) inherits (foo2_a_child);

--Testcase 846:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo2) for share;
--Testcase 847:
select * from bar where f1 in (select f1 from foo2) for share;

--Testcase 848:
drop table foo2child;

-- Check UPDATE with inherited target and an inherited source table
--Testcase 669:
explain (verbose, costs off)
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);
--Testcase 670:
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

--Testcase 671:
select tableoid::regclass, * from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery
--Testcase 672:
explain (verbose, costs off)
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;
--Testcase 673:
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

--Testcase 674:
select tableoid::regclass, * from bar order by 1,2;

-- Test forcing the remote server to produce sorted data for a merge join,
-- but the foreign table is an inheritance child.
--truncate table foo2;
--Testcase 675:
delete from foo2_a_child;
truncate table only foo;
\set num_rows_foo 2000
--Testcase 676:
insert into foo2_a_child select generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2);
--Testcase 677:
insert into foo select generate_series(1, :num_rows_foo, 2), generate_series(1, :num_rows_foo, 2);
--Testcase 849:
SET enable_hashjoin to false;
--Testcase 850:
SET enable_nestloop to false;
--alter foreign table foo2 options (use_remote_estimate 'true');
--create index i_loct1_f1 on loct1(f1);
--create index i_foo_f1 on foo(f1);
--analyze foo;
--analyze loct1;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 678:
explain (verbose, costs off)
	select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 679:
select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 680:
explain (verbose, costs off)
	select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 681:
select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 851:
RESET enable_hashjoin;
--Testcase 852:
RESET enable_nestloop;

-- Test that WHERE CURRENT OF is not supported
begin;
declare c cursor for select * from bar where f1 = 7;
--Testcase 682:
fetch from c;
--Testcase 683:
update bar set f2 = null where current of c;
rollback;

--Testcase 684:
explain (verbose, costs off)
delete from foo where f1 < 5;
--Testcase 685:
select * from foo where f1 < 5;
--Testcase 686:
delete from foo where f1 < 5;
--Testcase 687:
explain (verbose, costs off)
update bar set f2 = f2 + 100;
--Testcase 688:
update bar set f2 = f2 + 100;
--Testcase 689:
select * from bar;

-- Test that UPDATE/DELETE with inherited target works with row-level triggers
--Testcase 690:
CREATE TRIGGER trig_row_before
BEFORE UPDATE OR DELETE ON bar2_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 691:
CREATE TRIGGER trig_row_after
AFTER UPDATE OR DELETE ON bar2_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 692:
explain (verbose, costs off)
update bar set f2 = f2 + 100;
--Testcase 693:
update bar set f2 = f2 + 100;

--Testcase 694:
explain (verbose, costs off)
delete from bar where f2 < 400;
--Testcase 695:
delete from bar where f2 < 400;

-- cleanup
--Testcase 696:
drop table foo cascade;
--Testcase 697:
drop table bar cascade;

-- Test pushing down UPDATE/DELETE joins to the remote server
--Testcase 698:
create table parent (a int, b text);
--Testcase 699:
create foreign table remt1_a_child (a int OPTIONS (key 'true'), b text)
  server sqlumdash_svr options (table_name 'loct3');
--Testcase 1002:
create table remt1 (a int, b text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 1003:
create foreign table remt1_a PARTITION OF remt1 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 700:
create foreign table remt2_a_child (a int OPTIONS (key 'true'), b text)
  server sqlumdash_svr options (table_name 'loct4');
--Testcase 1004:
create table remt2 (a int, b text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 1005:
create foreign table remt2_a PARTITION OF remt2 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 853:
alter foreign table remt1_a_child inherit parent;

--Testcase 701:
insert into remt1_a_child values (1, 'foo');
--Testcase 702:
insert into remt1_a_child values (2, 'bar');
--Testcase 703:
insert into remt2_a_child values (1, 'foo');
--Testcase 704:
insert into remt2_a_child values (2, 'bar');

--analyze remt1;
--analyze remt2;

--Testcase 705:
explain (verbose, costs off)
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;
--Testcase 706:
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a;
--Testcase 707:
SELECT * FROM parent, remt2 WHERE (parent.a = remt2.a);
--Testcase 708:
explain (verbose, costs off)
delete from parent using remt2 where parent.a = remt2.a;
--Testcase 709:
SELECT parent.* FROM parent, remt2 WHERE parent.a = remt2.a;
--Testcase 710:
delete from parent using remt2 where parent.a = remt2.a;

-- cleanup
--Testcase 711:
drop foreign table remt1_a_child;
--Testcase 712:
drop foreign table remt2_a_child;
--drop table loct1;
--drop table loct2;
--Testcase 713:
drop table parent;

-- Skip test because SQLumdash not support foreign-table partitions, check constraint, copy from
-- ===================================================================
-- test tuple routing for foreign-table partitions
-- ===================================================================
/*
-- Test insert tuple routing
create table itrtest (a int, b text) partition by list (a);
create table loct1 (a int check (a in (1)), b text);
create foreign table remp1 (a int check (a in (1)), b text) server loopback options (table_name 'loct1');
create table loct2 (a int check (a in (2)), b text);
create foreign table remp2 (b text, a int check (a in (2))) server loopback options (table_name 'loct2');
alter table itrtest attach partition remp1 for values in (1);
alter table itrtest attach partition remp2 for values in (2);

insert into itrtest values (1, 'foo');
insert into itrtest values (1, 'bar') returning *;
insert into itrtest values (2, 'baz');
insert into itrtest values (2, 'qux') returning *;
insert into itrtest values (1, 'test1'), (2, 'test2') returning *;

select tableoid::regclass, * FROM itrtest;
select tableoid::regclass, * FROM remp1;
select tableoid::regclass, * FROM remp2;

delete from itrtest;

create unique index loct1_idx on loct1 (a);

-- DO NOTHING without an inference specification is supported
insert into itrtest values (1, 'foo') on conflict do nothing returning *;
insert into itrtest values (1, 'foo') on conflict do nothing returning *;

-- But other cases are not supported
insert into itrtest values (1, 'bar') on conflict (a) do nothing;
insert into itrtest values (1, 'bar') on conflict (a) do update set b = excluded.b;

select tableoid::regclass, * FROM itrtest;

delete from itrtest;

drop index loct1_idx;

-- Test that remote triggers work with insert tuple routing
create function br_insert_trigfunc() returns trigger as $$
begin
	new.b := new.b || ' triggered !';
	return new;
end
$$ language plpgsql;
create trigger loct1_br_insert_trigger before insert on loct1
	for each row execute procedure br_insert_trigfunc();
create trigger loct2_br_insert_trigger before insert on loct2
	for each row execute procedure br_insert_trigfunc();

-- The new values are concatenated with ' triggered !'
insert into itrtest values (1, 'foo') returning *;
insert into itrtest values (2, 'qux') returning *;
insert into itrtest values (1, 'test1'), (2, 'test2') returning *;
with result as (insert into itrtest values (1, 'test1'), (2, 'test2') returning *) select * from result;

drop trigger loct1_br_insert_trigger on loct1;
drop trigger loct2_br_insert_trigger on loct2;

drop table itrtest;
drop table loct1;
drop table loct2;

-- Test update tuple routing
create table utrtest (a int, b text) partition by list (a);
create table loct (a int check (a in (1)), b text);
create foreign table remp (a int check (a in (1)), b text) server loopback options (table_name 'loct');
create table locp (a int check (a in (2)), b text);
alter table utrtest attach partition remp for values in (1);
alter table utrtest attach partition locp for values in (2);

insert into utrtest values (1, 'foo');
insert into utrtest values (2, 'qux');

select tableoid::regclass, * FROM utrtest;
select tableoid::regclass, * FROM remp;
select tableoid::regclass, * FROM locp;

-- It's not allowed to move a row from a partition that is foreign to another
update utrtest set a = 2 where b = 'foo' returning *;

-- But the reverse is allowed
update utrtest set a = 1 where b = 'qux' returning *;

select tableoid::regclass, * FROM utrtest;
select tableoid::regclass, * FROM remp;
select tableoid::regclass, * FROM locp;

-- The executor should not let unexercised FDWs shut down
update utrtest set a = 1 where b = 'foo';

-- Test that remote triggers work with update tuple routing
create trigger loct_br_insert_trigger before insert on loct
	for each row execute procedure br_insert_trigfunc();

delete from utrtest;
insert into utrtest values (2, 'qux');

-- Check case where the foreign partition is a subplan target rel
explain (verbose, costs off)
update utrtest set a = 1 where a = 1 or a = 2 returning *;
-- The new values are concatenated with ' triggered !'
update utrtest set a = 1 where a = 1 or a = 2 returning *;

delete from utrtest;
insert into utrtest values (2, 'qux');

-- Check case where the foreign partition isn't a subplan target rel
explain (verbose, costs off)
update utrtest set a = 1 where a = 2 returning *;
-- The new values are concatenated with ' triggered !'
update utrtest set a = 1 where a = 2 returning *;

drop trigger loct_br_insert_trigger on loct;

-- We can move rows to a foreign partition that has been updated already,
-- but can't move rows to a foreign partition that hasn't been updated yet

delete from utrtest;
insert into utrtest values (1, 'foo');
insert into utrtest values (2, 'qux');

-- Test the former case:
-- with a direct modification plan
explain (verbose, costs off)
update utrtest set a = 1 returning *;
update utrtest set a = 1 returning *;

delete from utrtest;
insert into utrtest values (1, 'foo');
insert into utrtest values (2, 'qux');

-- with a non-direct modification plan
explain (verbose, costs off)
update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x returning *;
update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x returning *;

-- Change the definition of utrtest so that the foreign partition get updated
-- after the local partition
delete from utrtest;
alter table utrtest detach partition remp;
drop foreign table remp;
alter table loct drop constraint loct_a_check;
alter table loct add check (a in (3));
create foreign table remp (a int check (a in (3)), b text) server loopback options (table_name 'loct');
alter table utrtest attach partition remp for values in (3);
insert into utrtest values (2, 'qux');
insert into utrtest values (3, 'xyzzy');

-- Test the latter case:
-- with a direct modification plan
explain (verbose, costs off)
update utrtest set a = 3 returning *;
update utrtest set a = 3 returning *; -- ERROR

-- with a non-direct modification plan
explain (verbose, costs off)
update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x returning *;
update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x returning *; -- ERROR

drop table utrtest;
drop table loct;

-- Test copy tuple routing
create table ctrtest (a int, b text) partition by list (a);
create table loct1 (a int check (a in (1)), b text);
create foreign table remp1 (a int check (a in (1)), b text) server loopback options (table_name 'loct1');
create table loct2 (a int check (a in (2)), b text);
create foreign table remp2 (b text, a int check (a in (2))) server loopback options (table_name 'loct2');
alter table ctrtest attach partition remp1 for values in (1);
alter table ctrtest attach partition remp2 for values in (2);

copy ctrtest from stdin;
1	foo
2	qux
\.

select tableoid::regclass, * FROM ctrtest;
select tableoid::regclass, * FROM remp1;
select tableoid::regclass, * FROM remp2;

-- Copying into foreign partitions directly should work as well
copy remp1 from stdin;
1	bar
\.

select tableoid::regclass, * FROM remp1;

drop table ctrtest;
drop table loct1;
drop table loct2;

-- ===================================================================
-- test COPY FROM
-- ===================================================================

create table loc2 (f1 int, f2 text);
alter table loc2 set (autovacuum_enabled = 'false');
create foreign table rem2 (f1 int, f2 text) server loopback options(table_name 'loc2');

-- Test basic functionality
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

delete from rem2;

-- Test check constraints
alter table loc2 add constraint loc2_f1positive check (f1 >= 0);
alter foreign table rem2 add constraint rem2_f1positive check (f1 >= 0);

-- check constraint is enforced on the remote side, not locally
copy rem2 from stdin;
1	foo
2	bar
\.
copy rem2 from stdin; -- ERROR
-1	xyzzy
\.
select * from rem2;

alter foreign table rem2 drop constraint rem2_f1positive;
alter table loc2 drop constraint loc2_f1positive;

delete from rem2;

-- Test local triggers
create trigger trig_stmt_before before insert on rem2
	for each statement execute procedure trigger_func();
create trigger trig_stmt_after after insert on rem2
	for each statement execute procedure trigger_func();
create trigger trig_row_before before insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
create trigger trig_row_after after insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');

copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_row_before on rem2;
drop trigger trig_row_after on rem2;
drop trigger trig_stmt_before on rem2;
drop trigger trig_stmt_after on rem2;

delete from rem2;

create trigger trig_row_before_insert before insert on rem2
	for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_row_before_insert on rem2;

delete from rem2;

create trigger trig_null before insert on rem2
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_null on rem2;

delete from rem2;

-- Test remote triggers
create trigger trig_row_before_insert before insert on loc2
	for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_row_before_insert on loc2;

delete from rem2;

create trigger trig_null before insert on loc2
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger trig_null on loc2;

delete from rem2;

-- Test a combination of local and remote triggers
create trigger rem2_trig_row_before before insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
create trigger rem2_trig_row_after after insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
create trigger loc2_trig_row_before_insert before insert on loc2
	for each row execute procedure trig_row_before_insupdate();

copy rem2 from stdin;
1	foo
2	bar
\.
select * from rem2;

drop trigger rem2_trig_row_before on rem2;
drop trigger rem2_trig_row_after on rem2;
drop trigger loc2_trig_row_before_insert on loc2;

delete from rem2;

-- test COPY FROM with foreign table created in the same transaction
create table loc3 (f1 int, f2 text);
begin;
create foreign table rem3 (f1 int, f2 text)
	server loopback options(table_name 'loc3');
copy rem3 from stdin;
1	foo
2	bar
\.
commit;
select * from rem3;
drop foreign table rem3;
drop table loc3;
*/

-- -- ===================================================================
-- -- test for TRUNCATE
-- -- ===================================================================
-- --Testcase 854:
-- CREATE FOREIGN TABLE tru_ftable (id int)
--        SERVER sqlumdash_svr OPTIONS (table_name 'tru_rtable0');
-- --Testcase 855:
-- INSERT INTO "S 1".tru_rtable0 (SELECT x FROM generate_series(1,10) x);

-- -- CREATE TABLE tru_ptable (id int) PARTITION BY HASH(id);
-- -- CREATE TABLE tru_ptable__p0 PARTITION OF tru_ptable
-- --                             FOR VALUES WITH (MODULUS 2, REMAINDER 0);
-- -- CREATE TABLE tru_rtable1 (id int primary key);
-- -- CREATE FOREIGN TABLE tru_ftable__p1 (id int)
-- --        SERVER sqlite_svr OPTIONS (table 'tru_ptable');
-- -- INSERT INTO tru_ptable (SELECT x FROM generate_series(11,20) x);

-- --Testcase 856:
-- INSERT INTO "S 1".tru_pk_table (SELECT x FROM generate_series(1,10) x);
-- --Testcase 857:
-- INSERT INTO "S 1".tru_fk_table (SELECT x % 10 + 1 FROM generate_series(5,25) x);
-- --Testcase 858:
-- CREATE FOREIGN TABLE tru_pk_ftable (id int)
--        SERVER sqlumdash_svr OPTIONS (table_name 'tru_pk_table');

-- --Testcase 859:
-- CREATE FOREIGN TABLE tru_ftable_parent (id int)
--        SERVER sqlumdash_svr OPTIONS (table_name 'tru_rtable_parent');
-- --Testcase 860:
-- CREATE FOREIGN TABLE tru_ftable_child () INHERITS (tru_ftable_parent)
--        SERVER sqlumdash_svr OPTIONS (table_name 'tru_rtable_child');
-- --Testcase 861:
-- INSERT INTO "S 1".tru_rtable_parent (SELECT x FROM generate_series(1,8) x);
-- --Testcase 862:
-- INSERT INTO "S 1".tru_rtable_child  (SELECT x FROM generate_series(10, 18) x);

-- -- normal truncate
-- --Testcase 863:
-- SELECT sum(id) FROM tru_ftable;        -- 55
-- TRUNCATE tru_ftable;
-- --Testcase 864:
-- SELECT count(*) FROM "S 1".tru_rtable0;		-- 0
-- --Testcase 865:
-- SELECT count(*) FROM tru_ftable;		-- 0

-- -- 'truncatable' option
-- --Testcase 866:
-- ALTER SERVER sqlumdash_svr OPTIONS (ADD truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- --Testcase 867:
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (ADD truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted
-- --Testcase 868:
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- --Testcase 869:
-- ALTER SERVER sqlumdash_svr OPTIONS (DROP truncatable);
-- --Testcase 870:
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- --Testcase 871:
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted

-- -- -- partitioned table with both local and foreign tables as partitions
-- -- SELECT sum(id) FROM tru_ptable;        -- 155
-- -- TRUNCATE tru_ptable;
-- -- SELECT count(*) FROM tru_ptable;		-- 0
-- -- SELECT count(*) FROM tru_ptable__p0;	-- 0
-- -- SELECT count(*) FROM tru_ftable__p1;	-- 0
-- -- SELECT count(*) FROM tru_rtable1;		-- 0

-- -- 'CASCADE' option
-- --Testcase 872:
-- SELECT sum(id) FROM tru_pk_ftable;      -- 55
-- -- SQLumDashCS FDW support TRUNCATE command by executing DELETE statement without WHERE clause.
-- -- In order to delete records in parent and child table subsequently,
-- -- SQLumDashCS FDW executes "PRAGMA foreign_keys = ON" before executing DELETE statement.
-- TRUNCATE tru_pk_ftable; -- success
-- TRUNCATE tru_pk_ftable CASCADE; -- success
-- --Testcase 873:
-- SELECT count(*) FROM tru_pk_ftable;    -- 0
-- --Testcase 874:
-- SELECT count(*) FROM "S 1".tru_fk_table;		-- also truncated,0

-- -- truncate two tables at a command
-- --Testcase 875:
-- INSERT INTO tru_ftable (SELECT x FROM generate_series(1,8) x);
-- --Testcase 876:
-- INSERT INTO tru_pk_ftable (SELECT x FROM generate_series(3,10) x);
-- --Testcase 877:
-- SELECT count(*) from tru_ftable; -- 8
-- --Testcase 878:
-- SELECT count(*) from tru_pk_ftable; -- 8
-- TRUNCATE tru_ftable, tru_pk_ftable;
-- --Testcase 879:
-- SELECT count(*) from tru_ftable; -- 0
-- --Testcase 880:
-- SELECT count(*) from tru_pk_ftable; -- 0

-- -- -- truncate with ONLY clause
-- -- -- Since ONLY is specified, the table tru_ftable_child that inherits
-- -- -- tru_ftable_parent locally is not truncated.
-- TRUNCATE ONLY tru_ftable_parent;
-- --Testcase 881:
-- SELECT sum(id) FROM tru_ftable_parent;  -- 126
-- TRUNCATE tru_ftable_parent;
-- --Testcase 882:
-- SELECT count(*) FROM tru_ftable_parent; -- 0

-- -- -- in case when remote table has inherited children
-- -- CREATE TABLE tru_rtable0_child () INHERITS (tru_rtable0);
-- -- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(5,9) x);
-- -- INSERT INTO "S 1".tru_rtable0_child (SELECT x FROM generate_series(10,14) x);
-- -- SELECT sum(id) FROM tru_ftable;   -- 95

-- -- -- Both parent and child tables in the foreign server are truncated
-- -- -- even though ONLY is specified because ONLY has no effect
-- -- -- when truncating a foreign table.
-- -- TRUNCATE ONLY tru_ftable;
-- -- SELECT count(*) FROM tru_ftable;   -- 0

-- -- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(21,25) x);
-- -- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(26,30) x);
-- -- SELECT sum(id) FROM tru_ftable;		-- 255
-- -- TRUNCATE tru_ftable;			-- truncate both of parent and child
-- -- SELECT count(*) FROM tru_ftable;    -- 0

-- -- -- cleanup
-- --Testcase 883:
-- DROP FOREIGN TABLE tru_ftable_parent, tru_ftable_child, tru_pk_ftable,tru_ftable;
-- -- DROP TABLE tru_rtable0, tru_rtable1, tru_ptable, tru_ptable__p0, tru_pk_table, tru_fk_table,
-- -- tru_rtable_parent,tru_rtable_child, tru_rtable0_child;

-- ===================================================================
-- test IMPORT FOREIGN SCHEMA
-- ===================================================================

--CREATE SCHEMA import_source;
--CREATE TABLE import_source.t1 (c1 int, c2 varchar NOT NULL);
--CREATE TABLE import_source.t2 (c1 int default 42, c2 varchar NULL, c3 text collate "POSIX");
--CREATE TYPE typ1 AS (m1 int, m2 varchar);
--CREATE TABLE import_source.t3 (c1 timestamptz default now(), c2 typ1);
--CREATE TABLE import_source."x 4" (c1 float8, "C 2" text, c3 varchar(42));
--CREATE TABLE import_source."x 5" (c1 float8);
--ALTER TABLE import_source."x 5" DROP COLUMN c1;
-- CREATE TABLE import_source."x 6" (c1 int, c2 int generated always as (c1 * 2) stored);
--CREATE TABLE import_source.t4 (c1 int) PARTITION BY RANGE (c1);
--CREATE TABLE import_source.t4_part PARTITION OF import_source.t4
--  FOR VALUES FROM (1) TO (100);

--Testcase 714:
CREATE SCHEMA import_dest1;
IMPORT FOREIGN SCHEMA public FROM SERVER sqlumdash_svr INTO import_dest1;
--Testcase 715:
\det+ import_dest1.*
--Testcase 716:
\d import_dest1.*

-- Options
--Testcase 717:
CREATE SCHEMA import_dest2;
IMPORT FOREIGN SCHEMA public FROM SERVER sqlumdash_svr INTO import_dest2
  OPTIONS (import_default 'true');
--Testcase 718:
\det+ import_dest2.*
--Testcase 719:
\d import_dest2.*
-- Skip test case because sqlumdash not support import_collate
/*
CREATE SCHEMA import_dest3;
IMPORT FOREIGN SCHEMA public FROM SERVER sqlumdash_svr INTO import_dest3
  OPTIONS (import_collate 'false', import_generated 'false', import_not_null 'false');
\det+ import_dest3.*
\d import_dest3.*
*/
-- Check LIMIT TO and EXCEPT
--Testcase 720:
CREATE SCHEMA import_dest4;
IMPORT FOREIGN SCHEMA public LIMIT TO ("T 1", loct6, nonesuch)
  FROM SERVER sqlumdash_svr INTO import_dest4;
--Testcase 721:
\det+ import_dest4.*
IMPORT FOREIGN SCHEMA public EXCEPT ("T 1", loct6, nonesuch)
  FROM SERVER sqlumdash_svr INTO import_dest4;
--Testcase 722:
\det+ import_dest4.*

-- Assorted error cases
IMPORT FOREIGN SCHEMA public FROM SERVER sqlumdash_svr INTO import_dest4;
IMPORT FOREIGN SCHEMA public FROM SERVER sqlumdash_svr INTO notthere;
IMPORT FOREIGN SCHEMA public FROM SERVER nowhere INTO notthere;
/*
-- Skip these test, sqlumdash fdw does not support fetch_size option, partition table
-- Check case of a type present only on the remote server.
-- We can fake this by dropping the type locally in our transaction.
CREATE TYPE "Colors" AS ENUM ('red', 'green', 'blue');
CREATE TABLE import_source.t5 (c1 int, c2 text collate "C", "Col" "Colors");

CREATE SCHEMA import_dest5;
BEGIN;
DROP TYPE "Colors" CASCADE;
IMPORT FOREIGN SCHEMA import_source LIMIT TO (t5)
  FROM SERVER loopback INTO import_dest5;  -- ERROR

ROLLBACK;

BEGIN;


CREATE SERVER fetch101 FOREIGN DATA WRAPPER postgres_fdw OPTIONS( fetch_size '101' );

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

-- ===================================================================
-- test partitionwise joins
-- ===================================================================
SET enable_partitionwise_join=on;

CREATE TABLE fprt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE fprt1_p1 (LIKE fprt1);
CREATE TABLE fprt1_p2 (LIKE fprt1);
ALTER TABLE fprt1_p1 SET (autovacuum_enabled = 'false');
ALTER TABLE fprt1_p2 SET (autovacuum_enabled = 'false');
INSERT INTO fprt1_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 2) i;
INSERT INTO fprt1_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 2) i;
CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (0) TO (250)
	SERVER loopback OPTIONS (table_name 'fprt1_p1', use_remote_estimate 'true');
CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (250) TO (500)
	SERVER loopback OPTIONS (TABLE_NAME 'fprt1_p2');
ANALYZE fprt1;
ANALYZE fprt1_p1;
ANALYZE fprt1_p2;

CREATE TABLE fprt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE fprt2_p1 (LIKE fprt2);
CREATE TABLE fprt2_p2 (LIKE fprt2);
ALTER TABLE fprt2_p1 SET (autovacuum_enabled = 'false');
ALTER TABLE fprt2_p2 SET (autovacuum_enabled = 'false');
INSERT INTO fprt2_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 3) i;
INSERT INTO fprt2_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 3) i;
CREATE FOREIGN TABLE ftprt2_p1 (b int, c varchar, a int)
	SERVER loopback OPTIONS (table_name 'fprt2_p1', use_remote_estimate 'true');
ALTER TABLE fprt2 ATTACH PARTITION ftprt2_p1 FOR VALUES FROM (0) TO (250);
CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (250) TO (500)
	SERVER loopback OPTIONS (table_name 'fprt2_p2', use_remote_estimate 'true');
ANALYZE fprt2;
ANALYZE fprt2_p1;
ANALYZE fprt2_p2;

-- inner join three tables
EXPLAIN (COSTS OFF)
SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;
SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;

-- left outer join + nullable clause
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;
SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;

-- with whole-row reference; partitionwise join does not apply
EXPLAIN (COSTS OFF)
SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;
SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;

-- join with lateral reference
EXPLAIN (COSTS OFF)
SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;
SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;

-- with PHVs, partitionwise join selected but no join pushdown
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- test FOR UPDATE; partitionwise join does not apply
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;
SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;

RESET enable_partitionwise_join;


-- ===================================================================
-- test partitionwise aggregates
-- ===================================================================

CREATE TABLE pagg_tab (a int, b int, c text) PARTITION BY RANGE(a);

CREATE TABLE pagg_tab_p1 (LIKE pagg_tab);
CREATE TABLE pagg_tab_p2 (LIKE pagg_tab);
CREATE TABLE pagg_tab_p3 (LIKE pagg_tab);

INSERT INTO pagg_tab_p1 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 10;
INSERT INTO pagg_tab_p2 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 20 and (i % 30) >= 10;
INSERT INTO pagg_tab_p3 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 30 and (i % 30) >= 20;

-- Create foreign partitions
CREATE FOREIGN TABLE fpagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0) TO (10) SERVER loopback OPTIONS (table_name 'pagg_tab_p1');
CREATE FOREIGN TABLE fpagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10) TO (20) SERVER loopback OPTIONS (table_name 'pagg_tab_p2');
CREATE FOREIGN TABLE fpagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20) TO (30) SERVER loopback OPTIONS (table_name 'pagg_tab_p3');

ANALYZE pagg_tab;
ANALYZE fpagg_tab_p1;
ANALYZE fpagg_tab_p2;
ANALYZE fpagg_tab_p3;

-- When GROUP BY clause matches with PARTITION KEY.
-- Plan with partitionwise aggregates is disabled
SET enable_partitionwise_aggregate TO false;
EXPLAIN (COSTS OFF)
SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- Plan with partitionwise aggregates is enabled
SET enable_partitionwise_aggregate TO true;
EXPLAIN (COSTS OFF)
SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- Check with whole-row reference
-- Should have all the columns in the target list for the given relation
EXPLAIN (VERBOSE, COSTS OFF)
SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- When GROUP BY clause does not match with PARTITION KEY.
EXPLAIN (COSTS OFF)
SELECT b, avg(a), max(a), count(*) FROM pagg_tab GROUP BY b HAVING sum(a) < 700 ORDER BY 1;
*/

/*
-- Skip these tests, sqlumdash fdw does not support nosuper user.
-- ===================================================================
-- access rights and superuser
-- ===================================================================

-- Non-superuser cannot create a FDW without a password in the connstr
CREATE ROLE regress_nosuper NOSUPERUSER;

GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO regress_nosuper;

SET ROLE regress_nosuper;

SHOW is_superuser;

-- This will be OK, we can create the FDW
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback_nopw FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;

-- But creation of user mappings for non-superusers should fail
CREATE USER MAPPING FOR public SERVER loopback_nopw;
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

CREATE FOREIGN TABLE ft1_nopw (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 user_enum
) SERVER loopback_nopw OPTIONS (schema_name 'public', table_name 'ft1');

SELECT 1 FROM ft1_nopw LIMIT 1;

-- If we add a password to the connstr it'll fail, because we don't allow passwords
-- in connstrs only in user mappings.

DO $d$
    BEGIN
        EXECUTE $$ALTER SERVER loopback_nopw OPTIONS (ADD password 'dummypw')$$;
    END;
$d$;

-- If we add a password for our user mapping instead, we should get a different
-- error because the password wasn't actually *used* when we run with trust auth.
--
-- This won't work with installcheck, but neither will most of the FDW checks.

ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password 'dummypw');

SELECT 1 FROM ft1_nopw LIMIT 1;

-- Unpriv user cannot make the mapping passwordless
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password_required 'false');


SELECT 1 FROM ft1_nopw LIMIT 1;

RESET ROLE;

-- But the superuser can
ALTER USER MAPPING FOR regress_nosuper SERVER loopback_nopw OPTIONS (ADD password_required 'false');

SET ROLE regress_nosuper;

-- Should finally work now
SELECT 1 FROM ft1_nopw LIMIT 1;

-- unpriv user also cannot set sslcert / sslkey on the user mapping
-- first set password_required so we see the right error messages
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (SET password_required 'true');
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslcert 'foo.crt');
ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslkey 'foo.key');

-- We're done with the role named after a specific user and need to check the
-- changes to the public mapping.
DROP USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

-- This will fail again as it'll resolve the user mapping for public, which
-- lacks password_required=false
SELECT 1 FROM ft1_nopw LIMIT 1;

RESET ROLE;

-- The user mapping for public is passwordless and lacks the password_required=false
-- mapping option, but will work because the current user is a superuser.
SELECT 1 FROM ft1_nopw LIMIT 1;

-- cleanup
DROP USER MAPPING FOR public SERVER loopback_nopw;
DROP OWNED BY regress_nosuper;
DROP ROLE regress_nosuper;

-- Clean-up
RESET enable_partitionwise_aggregate;
*/

-- Two-phase transactions are not supported.
BEGIN;
--Testcase 723:
SELECT count(*) FROM ft1;
-- error here
--Testcase 724:
PREPARE TRANSACTION 'fdw_tpc';
ROLLBACK;

-- ===================================================================
-- reestablish new connection
-- ===================================================================
-- --Testcase 884:
-- SELECT 1 FROM ft1 LIMIT 1;
-- \! ./sqlumdashcs_test_restart.sh
-- --Testcase 885:
-- SELECT 1 FROM ft1 LIMIT 1;
-- -- Test case relative with option application_name is not suitable for SQLumDashCS FDW.
-- -- Because this option is in libpq of postgres.
-- -- Change application_name of remote connection to special one
-- -- so that we can easily terminate the connection later.
-- ALTER SERVER loopback OPTIONS (application_name 'fdw_retry_check');

-- -- If debug_discard_caches is active, it results in
-- -- dropping remote connections after every transaction, making it
-- -- impossible to test termination meaningfully.  So turn that off
-- -- for this test.
-- SET debug_discard_caches = 0;

-- -- Make sure we have a remote connection.
-- SELECT 1 FROM ft1 LIMIT 1;

-- -- Terminate the remote connection and wait for the termination to complete.
-- SELECT pg_terminate_backend(pid, 180000) FROM pg_stat_activity
-- 	WHERE application_name = 'fdw_retry_check';

-- -- This query should detect the broken connection when starting new remote
-- -- transaction, reestablish new connection, and then succeed.
-- BEGIN;
-- SELECT 1 FROM ft1 LIMIT 1;

-- -- If we detect the broken connection when starting a new remote
-- -- subtransaction, we should fail instead of establishing a new connection.
-- -- Terminate the remote connection and wait for the termination to complete.
-- SELECT pg_terminate_backend(pid, 180000) FROM pg_stat_activity
-- 	WHERE application_name = 'fdw_retry_check';
-- SAVEPOINT s;
-- -- The text of the error might vary across platforms, so only show SQLSTATE.
-- \set VERBOSITY sqlstate
-- SELECT 1 FROM ft1 LIMIT 1;    -- should fail
-- \set VERBOSITY default
-- COMMIT;

-- RESET debug_discard_caches;

-- -- =============================================================================
-- -- test connection invalidation cases and sqlumdashcs_fdw_get_connections function
-- -- =============================================================================
-- -- Let's ensure to close all the existing cached connections.
-- --Testcase 886:
-- SELECT 1 FROM sqlumdashcs_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- --Testcase 887:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;
-- -- This test case is for closing the connection in sqlumdashcs_xact_callback
-- BEGIN;
-- -- Connection xact depth becomes 1 i.e. the connection is in midst of the xact.
-- --Testcase 888:
-- SELECT 1 FROM ft1 LIMIT 1;
-- --Testcase 889:
-- SELECT 1 FROM ft7 LIMIT 1;
-- -- List all the existing cached connections. sqlumdash_svr and sqlumdash_svr3 should be
-- -- output.
-- --Testcase 890:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;
-- -- Connections are not closed at the end of the alter and drop statements.
-- -- That's because the connections are in midst of this xact,
-- -- they are just marked as invalid in sqlumdashcs_inval_callback.
-- --Testcase 891:
-- ALTER SERVER sqlumdash_svr OPTIONS (ADD keep_connections 'off');
-- --Testcase 892:
-- DROP SERVER sqlumdash_svr3 CASCADE;
-- -- List all the existing cached connections. sqlumdash_svr and sqlumdash_svr3
-- -- should be output as invalid connections. Also the server name for
-- -- sqlumdash_svr3 should be NULL because the server was dropped.
-- --Testcase 893:
-- SELECT * FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;
-- -- The invalid connections get closed in sqlumdashcs_xact_callback during commit.
-- COMMIT;
-- --Testcase 894:
-- ALTER SERVER sqlumdash_svr OPTIONS (DROP keep_connections);
-- -- All cached connections were closed while committing above xact, so no
-- -- records should be output.
-- --Testcase 895:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;

-- -- =======================================================================
-- -- test sqlumdashcs_fdw_disconnect and sqlumdashcs_fdw_disconnect_all functions
-- -- =======================================================================
-- BEGIN;
-- -- Ensure to cache sqlumdash_svr connection.
-- --Testcase 896:
-- SELECT 1 FROM ft1 LIMIT 1;
-- -- Ensure to cache sqlumdash_svr2 connection.
-- --Testcase 897:
-- SELECT 1 FROM ft6 LIMIT 1;
-- -- List all the existing cached connections. sqlumdash_svr and sqlumdash_svr2 should be
-- -- output.
-- --Testcase 898:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;
-- -- Issue a warning and return false as sqlumdash_svr connection is still in use and
-- -- can not be closed.
-- --Testcase 899:
-- SELECT sqlumdashcs_fdw_disconnect('sqlumdash_svr');
-- -- List all the existing cached connections. sqlumdash_svr and sqlumdash_svr2 should be
-- -- output.
-- --Testcase 900:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;
-- -- Return false as connections are still in use, warnings are issued.
-- -- But disable warnings temporarily because the order of them is not stable.
-- --Testcase 901:
-- SET client_min_messages = 'ERROR';
-- --Testcase 902:
-- SELECT sqlumdashcs_fdw_disconnect_all();
-- --Testcase 903:
-- RESET client_min_messages;
-- COMMIT;
-- -- Ensure that sqlumdash_svr2 connection is closed.
-- --Testcase 904:
-- SELECT 1 FROM sqlumdashcs_fdw_disconnect('sqlumdash_svr2');
-- --Testcase 905:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() WHERE server_name = 'sqlumdash_svr2';
-- -- Return false as sqlumdash_svr2 connection is closed already.
-- --Testcase 906:
-- SELECT sqlumdashcs_fdw_disconnect('sqlumdash_svr2');
-- -- Return an error as there is no foreign server with given name.
-- --Testcase 907:
-- SELECT sqlumdashcs_fdw_disconnect('unknownserver');
-- -- Let's ensure to close all the existing cached connections.
-- --Testcase 908:
-- SELECT 1 FROM sqlumdashcs_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- --Testcase 909:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;

-- -- =============================================================================
-- -- test case for having multiple cached connections for a foreign server
-- -- =============================================================================
-- --Testcase 910:
-- CREATE ROLE regress_multi_conn_user1 SUPERUSER;
-- --Testcase 911:
-- CREATE ROLE regress_multi_conn_user2 SUPERUSER;
-- --Testcase 912:
-- CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER sqlumdash_svr OPTIONS (username :SQLUMDASHCS_USER, password :SQLUMDASHCS_PASS);
-- --Testcase 913:
-- CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER sqlumdash_svr OPTIONS (username :SQLUMDASHCS_USER, password :SQLUMDASHCS_PASS);

-- BEGIN;
-- -- Will cache loopback connection with user mapping for regress_multi_conn_user1
-- --Testcase 914:
-- SET ROLE regress_multi_conn_user1;
-- --Testcase 915:
-- SELECT 1 FROM ft1 LIMIT 1;
-- --Testcase 916:
-- RESET ROLE;

-- -- Will cache loopback connection with user mapping for regress_multi_conn_user2
-- --Testcase 917:
-- SET ROLE regress_multi_conn_user2;
-- --Testcase 918:
-- SELECT 1 FROM ft1 LIMIT 1;
-- --Testcase 919:
-- RESET ROLE;

-- -- Should output two connections for sqlumdash_svr server
-- --Testcase 920:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;
-- COMMIT;
-- -- Let's ensure to close all the existing cached connections.
-- --Testcase 921:
-- SELECT 1 FROM sqlumdashcs_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- --Testcase 922:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;

-- -- Clean up
-- --Testcase 923:
-- DROP USER MAPPING FOR regress_multi_conn_user1 SERVER sqlumdash_svr;
-- --Testcase 924:
-- DROP USER MAPPING FOR regress_multi_conn_user2 SERVER sqlumdash_svr;
-- --Testcase 925:
-- DROP ROLE regress_multi_conn_user1;
-- --Testcase 926:
-- DROP ROLE regress_multi_conn_user2;

-- -- ===================================================================
-- -- Test foreign server level option keep_connections
-- -- ===================================================================
-- -- By default, the connections associated with foreign server are cached i.e.
-- -- keep_connections option is on. Set it to off.
-- --Testcase 927:
-- ALTER SERVER sqlumdash_svr OPTIONS (keep_connections 'off');
-- -- connection to sqlumdash_svr server is closed at the end of xact
-- -- as keep_connections was set to off.
-- --Testcase 928:
-- SELECT 1 FROM ft1 LIMIT 1;
-- -- No cached connections, so no records should be output.
-- --Testcase 929:
-- SELECT server_name FROM sqlumdashcs_fdw_get_connections() ORDER BY 1;
-- --Testcase 930:
-- ALTER SERVER sqlumdash_svr OPTIONS (SET keep_connections 'on');

-- -- ===================================================================
-- -- batch insert
-- -- ===================================================================

-- BEGIN;

-- --Testcase 931:
-- CREATE SERVER batch10 FOREIGN DATA WRAPPER sqlumdashcs_fdw OPTIONS( batch_size '10' );

-- --Testcase 932:
-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=10'];

-- --Testcase 933:
-- ALTER SERVER batch10 OPTIONS( SET batch_size '20' );

-- --Testcase 934:
-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=10'];

-- --Testcase 935:
-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=20'];

-- --Testcase 936:
-- CREATE FOREIGN TABLE table30 ( x int ) SERVER batch10 OPTIONS ( batch_size '30' );

-- --Testcase 937:
-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=30'];

-- --Testcase 938:
-- ALTER FOREIGN TABLE table30 OPTIONS ( SET batch_size '40');

-- --Testcase 939:
-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=30'];

-- --Testcase 940:
-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=40'];

-- ROLLBACK;

-- --Testcase 941:
-- CREATE FOREIGN TABLE ftable ( x int  OPTIONS (key 'true') ) SERVER sqlumdash_svr OPTIONS ( table_name 'batch_table', batch_size '10' );
-- --Testcase 942:
-- EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;

-- --Testcase 943:
-- INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;
-- --Testcase 944:
-- INSERT INTO ftable SELECT * FROM generate_series(11, 31) i;
-- --Testcase 945:
-- INSERT INTO ftable VALUES (32);
-- --Testcase 946:
-- INSERT INTO ftable VALUES (33), (34);
-- --Testcase 947:
-- SELECT COUNT(*) FROM ftable;
-- --Testcase 948:
-- DELETE FROM ftable;
-- --Testcase 949:
-- DROP FOREIGN TABLE ftable;

-- -- try if large batches exceed max number of bind parameters
-- --Testcase 950:
-- CREATE FOREIGN TABLE ftable ( x int OPTIONS (key 'true') ) SERVER sqlumdash_svr OPTIONS ( table_name 'batch_table', batch_size '100000' );
-- -- The maximum number of records in a transaction can be inserted is 26211,
-- -- But in this transaction, number of record is still 70000, although breaking 70000 to batches by using batch_size.
-- -- The insert operation will be failed.
-- --Testcase 951:
-- INSERT INTO ftable SELECT * FROM generate_series(1, 70000) i; -- should fail
-- --Testcase 952:
-- SELECT COUNT(*) FROM ftable; -- 0
-- --Testcase 953:
-- DELETE FROM ftable;
-- --Testcase 954:
-- DROP FOREIGN TABLE ftable;

-- -- Disable batch insert
-- --Testcase 955:
-- CREATE FOREIGN TABLE ftable ( x int ) SERVER sqlumdash_svr OPTIONS ( table_name 'batch_table', batch_size '1' );
-- --Testcase 956:
-- EXPLAIN (VERBOSE, COSTS OFF) INSERT INTO ftable VALUES (1), (2);
-- --Testcase 957:
-- INSERT INTO ftable VALUES (1), (2);
-- --Testcase 958:
-- SELECT COUNT(*) FROM ftable;
-- --Testcase 959:
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

-- -- Check that enabling batched inserts doesn't interfere with cross-partition
-- -- updates
-- CREATE TABLE batch_cp_upd_test (a int) PARTITION BY LIST (a);
-- CREATE TABLE batch_cp_upd_test1 (LIKE batch_cp_upd_test);
-- CREATE FOREIGN TABLE batch_cp_upd_test1_f
-- 	PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (1)
-- 	SERVER loopback
-- 	OPTIONS (table_name 'batch_cp_upd_test1', batch_size '10');
-- CREATE TABLE batch_cp_up_test1 PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (2);
-- INSERT INTO batch_cp_upd_test VALUES (1), (2);

-- -- The following moves a row from the local partition to the foreign one
-- UPDATE batch_cp_upd_test t SET a = 1 FROM (VALUES (1), (2)) s(a) WHERE t.a = s.a;
-- SELECT tableoid::regclass, * FROM batch_cp_upd_test;

-- -- Clean up
-- DROP TABLE batch_table, batch_cp_upd_test, batch_table_p0, batch_table_p1 CASCADE;

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

-- ALTER SERVER loopback OPTIONS (DROP batch_size);

-- ===================================================================
-- test asynchronous execution
-- ===================================================================

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
-- CREATE TABLE local_tbl (a int, b int, c text);
-- INSERT INTO local_tbl VALUES (1505, 505, 'foo');

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

-- ALTER SERVER loopback OPTIONS (DROP async_capable);
-- ALTER SERVER loopback2 OPTIONS (DROP async_capable);

-- ===================================================================
-- test invalid server and foreign table options
-- ===================================================================
-- -- Invalid fdw_startup_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
-- 	OPTIONS(fdw_startup_cost '100$%$#$#');
-- -- Invalid fdw_tuple_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER postgres_fdw
-- 	OPTIONS(fdw_tuple_cost '100$%$#$#');
-- -- Invalid fetch_size option
-- CREATE FOREIGN TABLE inv_fsz (c1 int )
-- 	SERVER loopback OPTIONS (fetch_size '100$%$#$#');
-- Invalid batch_size option
--Testcase 960:
-- CREATE FOREIGN TABLE inv_bsz (c1 int )
-- 	SERVER sqlumdash_svr OPTIONS (batch_size '100$%$#$#');

-- Clean-up
--Testcase 725:
DROP USER MAPPING FOR CURRENT_USER SERVER spdsrv;
--Testcase 1006:
DROP USER MAPPING FOR CURRENT_USER SERVER sqlumdash_svr;
--Testcase 726:
DROP USER MAPPING FOR CURRENT_USER SERVER sqlumdash_svr2;
--Testcase 727:
DROP SERVER spdsrv CASCADE;
--Testcase 1007:
DROP SERVER sqlumdash_svr CASCADE;
--Testcase 728:
DROP SERVER sqlumdash_svr2 CASCADE;
--Testcase 729:
DROP EXTENSION pgspider_ext CASCADE;
--Testcase 1008:
DROP EXTENSION sqlumdashcs_fdw CASCADE;

