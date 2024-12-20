\set ECHO none
\ir sql/parameters/dynamodb_parameters.conf
\set ECHO all

--Testcase 1:
CREATE EXTENSION pgspider_ext;
--Testcase 2:
CREATE SERVER spdsrv FOREIGN DATA WRAPPER pgspider_ext;
--Testcase 3:
CREATE USER MAPPING FOR CURRENT_USER SERVER spdsrv;

--Testcase 4:
CREATE EXTENSION dynamodb_fdw;

--Testcase 5:
CREATE SERVER dynamodb_server FOREIGN DATA WRAPPER dynamodb_fdw
  OPTIONS (endpoint :DYNAMODB_ENDPOINT);

--Testcase 6:
CREATE USER MAPPING FOR public SERVER dynamodb_server 
  OPTIONS (user :DYNAMODB_USER, password :DYNAMODB_PASSWORD);

-- ===================================================================
-- create objects used through FDW loopback server
-- ===================================================================
--Testcase 7:
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
--Testcase 8:
CREATE SCHEMA "S 1";
-- DynamoDB does not have timestamp or timestamptz data type. DynamoDB FDW also
-- does not support this type. Therefore, change data type of c4, c5 to text.
--Testcase 9:
CREATE FOREIGN TABLE "S 1"."T 1" (
	"C 1" int options (column_name 'C_1'),
	c2 int NOT NULL,
	c3 text,
	c4 text,
	c5 text,
	c6 varchar(10),
	c7 char(10),
	c8 text
) SERVER dynamodb_server OPTIONS (table_name 'T_1', partition_key 'C_1');
--Testcase 10:
CREATE FOREIGN TABLE "S 1"."T 2" (
	c1 int NOT NULL,
	c2 text
) SERVER dynamodb_server OPTIONS (table_name 'T_2', partition_key 'c1');
--Testcase 11:
CREATE FOREIGN TABLE "S 1"."T 3" (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER dynamodb_server OPTIONS (table_name 'T_3', partition_key 'c1');
--Testcase 12:
CREATE FOREIGN TABLE "S 1"."T 4" (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER dynamodb_server OPTIONS (table_name 'T_4', partition_key 'c1');

--Testcase 13:
INSERT INTO "S 1"."T 1"
	SELECT id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       ('1970-01-01'::timestamptz + ((id % 100) || ' days')::interval)::text,
	       ('1970-01-01'::timestamp + ((id % 100) || ' days')::interval)::text,
	       id % 10,
	       id % 10,
	       'foo'
	FROM generate_series(1, 1000) id;
--Testcase 14:
INSERT INTO "S 1"."T 2"
	SELECT id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 15:
INSERT INTO "S 1"."T 3"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 16:
DELETE FROM "S 1"."T 3" WHERE c1 % 2 != 0;	-- delete for outer join tests
--Testcase 17:
INSERT INTO "S 1"."T 4"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 18:
DELETE FROM "S 1"."T 4" WHERE c1 % 3 != 0;	-- delete for outer join tests

-- ===================================================================
-- create foreign tables
-- ===================================================================
--Testcase 19:
CREATE FOREIGN TABLE ft1_a_child(
	c0 int,
	c1 int,
	c2 int NOT NULL,
	c3 text,
	c4 text,
	c5 text,
	c6 text,
	c7 text default 'ft1',
	c8 text
) SERVER dynamodb_server OPTIONS (partition_key 'c1');
--Testcase 20:
CREATE TABLE ft1 (
	c1 int,
	c2 int,
	c3 text,
	c4 text,
	c5 text,
	c6 text,
	c7 char(10) default 'ft1',
	c8 text,
	spdurl text
) PARTITION BY LIST (spdurl);
ALTER FOREIGN TABLE ft1_a_child DROP COLUMN c0;
--Testcase 21:
CREATE FOREIGN TABLE ft1_a PARTITION OF ft1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 22:
CREATE FOREIGN TABLE ft2_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	cx int,
	c3 text,
	c4 text,
	c5 text,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 text
) SERVER dynamodb_server OPTIONS (partition_key 'c1');
ALTER FOREIGN TABLE ft2_a_child DROP COLUMN cx;
--Testcase 23:
CREATE TABLE ft2 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 text,
	c5 text,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 text,
	spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 24:
CREATE FOREIGN TABLE ft2_a PARTITION OF ft2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 25:
CREATE FOREIGN TABLE ft4_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER dynamodb_server OPTIONS (table_name 'T_3');
--Testcase 26:
CREATE TABLE ft4 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 27:
CREATE FOREIGN TABLE ft4_a PARTITION OF ft4 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 28:
CREATE FOREIGN TABLE ft5_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER dynamodb_server OPTIONS (table_name 'T_4', partition_key 'c1');
--Testcase 29:
CREATE TABLE ft5 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 30:
CREATE FOREIGN TABLE ft5_a PARTITION OF ft5 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 31:
CREATE FOREIGN TABLE ft6_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER dynamodb_server OPTIONS (table_name 'T_4');
--Testcase 32:
CREATE TABLE ft6 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 33:
CREATE FOREIGN TABLE ft6_a PARTITION OF ft6 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 768:
CREATE FOREIGN TABLE ft7_a_child (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text
) SERVER dynamodb_server OPTIONS (table_name 'T_4');
--Testcase 769:
CREATE TABLE ft7 (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 770:
CREATE FOREIGN TABLE ft7_a PARTITION OF ft7 FOR VALUES IN ('/node1/') SERVER spdsrv;

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
-- ALTER SERVER dynamodb_server OPTIONS (
	-- use_remote_estimate 'false',
	-- updatable 'true',
	-- fdw_startup_cost '123.456',
	-- fdw_tuple_cost '0.123',
	-- service 'value',
	-- connect_timeout 'value',
	-- dbname 'value',
	-- hostaddr 'value',
	-- port 'value',
	--client_encoding 'value',
	-- application_name 'value',
	--fallback_application_name 'value',
	-- keepalives 'value',
	-- keepalives_idle 'value',
	-- keepalives_interval 'value',
	-- tcp_user_timeout 'value',
	-- requiressl 'value',
	-- sslcompression 'value',
	-- sslmode 'value',
	-- sslcert 'value',
	-- sslkey 'value',
	-- sslrootcert 'value',
	-- sslcrl 'value',
	--requirepeer 'value',
	-- krbsrvname 'value',
	-- gsslib 'value'
	--replication 'value'
-- );

-- DynamoDB FDW does not support extensions option
-- -- Error, invalid list syntax
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo; bar');

-- -- OK but gets a warning
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo, bar');
-- ALTER SERVER testserver1 OPTIONS (DROP extensions);

-- ALTER USER MAPPING FOR public SERVER dynamodb_server
-- 	OPTIONS (DROP user, DROP password);

-- -- Attempt to add a valid option that's not allowed in a user mapping
-- ALTER USER MAPPING FOR public SERVER dynamodb_server
-- 	OPTIONS (ADD sslmode 'require');

-- -- But we can add valid ones fine
-- ALTER USER MAPPING FOR public SERVER dynamodb_server
-- 	OPTIONS (ADD sslpassword 'dummy');

-- -- Ensure valid options we haven't used in a user mapping yet are
-- -- permitted to check validation.
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslkey 'value', ADD sslcert 'value');

ALTER FOREIGN TABLE ft1_a_child OPTIONS (table_name 'T_1');
ALTER FOREIGN TABLE ft2_a_child OPTIONS (table_name 'T_1');
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c1 OPTIONS (column_name 'C_1');
ALTER FOREIGN TABLE ft2_a_child ALTER COLUMN c1 OPTIONS (column_name 'C_1');
--Testcase 34:
\det+

-- DynamoDB FDW does not support dbname option
-- -- Test that alteration of server options causes reconnection
-- -- Remote's errors might be non-English, so hide them to ensure stable results
-- \set VERBOSITY terse
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
-- ALTER SERVER dynamodb_server OPTIONS (SET dbname 'no such database');
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
-- DO $d$
--     BEGIN
--         EXECUTE $$ALTER SERVER loopback
--             OPTIONS (SET dbname '$$||current_database()||$$')$$;
--     END;
-- $d$;
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again

-- For DynamoDB local, user and password does not have any affect.
-- Therefore, skip these test cases.
-- -- Test that alteration of user mapping options causes reconnection
-- ALTER USER MAPPING FOR public SERVER dynamodb_server
--   OPTIONS (ADD user 'no such user', ADD password 'no such password');
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
-- ALTER USER MAPPING FOR public SERVER dynamodb_server
--   OPTIONS (SET user :DYNAMODB_USER, SET password :DYNAMODB_PASSWORD);
--Testcase 35:
SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
-- \set VERBOSITY default

-- Now we should be able to run ANALYZE.
-- To exercise multiple code paths, we use local stats on ft1
-- and remote-estimate mode on ft2.
-- ANALYZE ft1;
-- ALTER FOREIGN TABLE ft2 OPTIONS (use_remote_estimate 'true');

-- ===================================================================
-- test error case for create publication on foreign table
-- ===================================================================
--Testcase 771:
CREATE PUBLICATION testpub_ftbl FOR TABLE ft1_a_child;  -- should fail

-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias
--Testcase 36:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
--Testcase 37:
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
-- single table with alias - also test that tableoid sort is not pushed to remote side
--Testcase 38:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
--Testcase 39:
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference
--Testcase 40:
EXPLAIN (VERBOSE, COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 41:
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result
--Testcase 42:
SELECT * FROM ft1 WHERE false;
-- with WHERE clause
--Testcase 43:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
--Testcase 44:
SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
-- with FOR UPDATE/SHARE
--Testcase 45:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 46:
SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 47:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
--Testcase 48:
SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
-- aggregate
--Testcase 49:
SELECT COUNT(*) FROM ft1 t1;
-- subquery
--Testcase 50:
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX
--Testcase 51:
SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;
-- used in CTE
--Testcase 52:
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values
--Testcase 53:
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.
SET enable_hashjoin TO false;
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 54:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
--Testcase 55:
SELECT t1.c1, t2."C 1" FROM ft2 t1 JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 56:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
--Testcase 57:
SELECT t1.c1, t2."C 1" FROM ft2 t1 LEFT JOIN "S 1"."T 1" t2 ON (t1.c1 = t2."C 1") OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
--Testcase 58:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 59:
SELECT t1."C 1" FROM "S 1"."T 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
--Testcase 60:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 61:
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins
--Testcase 62:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
--Testcase 63:
SELECT t1."C 1", t2.c1, t3.c1 FROM "S 1"."T 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."C 1") OFFSET 100 LIMIT 10;
RESET enable_hashjoin;
RESET enable_nestloop;

-- Test executing assertion in estimate_path_cost_size() that makes sure that
-- retrieved_rows for foreign rel re-used to cost pre-sorted foreign paths is
-- a sensible value even when the rel has tuples=0
--Testcase 64:
CREATE FOREIGN TABLE ft_empty_a_child (c1 int NOT NULL, c2 text)
  SERVER dynamodb_server OPTIONS (table_name 'loct_empty', partition_key 'c1');
--Testcase 65:
CREATE TABLE ft_empty (c1 int NOT NULL, c2 text, spdurl text)
   PARTITION BY LIST (spdurl);
--Testcase 66:
CREATE FOREIGN TABLE ft_empty_a PARTITION OF ft_empty FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 67:
INSERT INTO ft_empty_a_child
  SELECT id, 'AAA' || to_char(id, 'FM000') FROM generate_series(1, 100) id;
--Testcase 68:
DELETE FROM ft_empty_a_child;
-- ANALYZE ft_empty;
--Testcase 69:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_empty ORDER BY c1;

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
--Testcase 70:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
--Testcase 71:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
--Testcase 72:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NULL;        -- NullTest
--Testcase 73:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NOT NULL;    -- NullTest
-- DynamoDB does not support round and abs function => not push down
--Testcase 74:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
-- DynamoDB does not support - operator => not push down
--Testcase 75:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
-- DynamoDB does not support IS DISTINCT FROM => not push down
--Testcase 77:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
-- DynamoDB FDW only support ANY(ARRAY) with constant value => not push down.
--Testcase 78:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
-- DynamoDB FDW does not support SubscriptingRef => not push down.
--Testcase 79:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef
--Testcase 80:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
--Testcase 81:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- DynamoDB FDW can push down this case
-- parameterized remote path for foreign table
--Testcase 82:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM "S 1"."T 1" a, ft2 b WHERE a."C 1" = 47 AND b.c1 = a.c2;
--Testcase 83:
SELECT * FROM ft2 a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;

-- check both safe and unsafe join conditions
--Testcase 84:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7) ORDER BY a.c1;
--Testcase 85:
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7) ORDER BY a.c1;
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
--Testcase 86:
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5)) ORDER BY c1;
--Testcase 87:
SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5)) ORDER BY c1;
-- we should not push order by clause with volatile expressions or unsafe
-- collations
--Testcase 88:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, random();
--Testcase 89:
EXPLAIN (VERBOSE, COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- user-defined operator/function
--Testcase 90:
CREATE FUNCTION dynamodb_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
--Testcase 91:
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- DynamoDB does not support count and abs function => can not push down
--Testcase 92:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 93:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 94:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 95:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot
--Testcase 96:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = dynamodb_fdw_abs(t1.c2);
--Testcase 97:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = dynamodb_fdw_abs(t1.c2);
--Testcase 98:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 99:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- DynamoDB FDW does not support push-down ORDER BY
--Testcase 100:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 101:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- but let's put them in an extension ...
ALTER EXTENSION dynamodb_fdw ADD FUNCTION dynamodb_fdw_abs(int);
ALTER EXTENSION dynamodb_fdw ADD OPERATOR === (int, int);
-- ALTER SERVER dynamodb_server OPTIONS (ADD extensions 'dynamodb_fdw');

-- ... they can not be shipped because DynamoDB does not support it.
--Testcase 102:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = dynamodb_fdw_abs(t1.c2);
--Testcase 103:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = dynamodb_fdw_abs(t1.c2);
--Testcase 104:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 105:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- and both ORDER BY and LIMIT can not be shipped
--Testcase 106:
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 107:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- DynamoDB FDW does not support pushing down CASE WHEN
-- Test CASE pushdown
-- EXPLAIN (VERBOSE, COSTS OFF)
--Testcase 789:
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;
--Testcase 790:
SELECT c1,c2,c3 FROM ft2 WHERE CASE WHEN c1 > 990 THEN c1 END < 1000 ORDER BY c1;

-- Nested CASE
-- EXPLAIN (VERBOSE, COSTS OFF)
--Testcase 791:
SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;

-- SELECT c1,c2,c3 FROM ft2 WHERE CASE CASE WHEN c2 > 0 THEN c2 END WHEN 100 THEN 601 WHEN c2 THEN c2 ELSE 0 END > 600 ORDER BY c1;

-- CASE arg WHEN
--Testcase 792:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE mod(c1, 4) WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- CASE cannot be pushed down because of unshippable arg clause
--Testcase 793:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c1 > (CASE random()::integer WHEN 0 THEN 1 WHEN 2 THEN 50 ELSE 100 END);

-- these are shippable
--Testcase 794:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c6 WHEN 'foo' THEN true ELSE c3 < 'bar' END;
--Testcase 795:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 WHEN c6 THEN true ELSE c3 < 'bar' END;

-- but this is not because of collation
--Testcase 796:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE CASE c3 COLLATE "C" WHEN c6 THEN true ELSE c3 < 'bar' END;

-- check schema-qualification of regconfig constant
--Testcase 797:
CREATE TEXT SEARCH CONFIGURATION public.custom_search
  (COPY = pg_catalog.english);
--Testcase 798:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
--Testcase 799:
SELECT c1, to_tsvector('custom_search'::regconfig, c3) FROM ft1
WHERE c1 = 642 AND length(to_tsvector('custom_search'::regconfig, c3)) > 0;
--Testcase 800:
DROP TEXT SEARCH CONFIGURATION public.custom_search;
-- ===================================================================
-- JOIN queries
-- ===================================================================
-- Analyze ft4 and ft5 so that we have better statistics. These tables do not
-- have use_remote_estimate set.
-- ANALYZE ft4;
-- ANALYZE ft5;

-- DynamoDB does not support JOIN => not push down
-- join two tables
--Testcase 108:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 109:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join three tables
--Testcase 110:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 111:
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join
--Testcase 112:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 113:
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables
--Testcase 114:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 115:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
--Testcase 116:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
--Testcase 117:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
--Testcase 118:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
--Testcase 119:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
-- right outer join
--Testcase 120:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 121:
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
-- right outer join three tables
--Testcase 122:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 123:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY 1 OFFSET 10 LIMIT 10;
-- full outer join
--Testcase 124:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
--Testcase 125:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
--Testcase 126:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 127:
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 128:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
--Testcase 129:
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
-- b. one of the joining relations is a base relation and the other is a join
-- relation
--Testcase 130:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 131:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries
--Testcase 132:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 133:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries
--Testcase 134:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
--Testcase 135:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."T 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join
--Testcase 136:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
--Testcase 137:
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
-- full outer join three tables
--Testcase 138:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 139:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- full outer join + right outer join
--Testcase 140:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY 1 OFFSET 10 LIMIT 10;
--Testcase 141:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY 1 OFFSET 10 LIMIT 10;
-- right outer join + full outer join
--Testcase 142:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 143:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- full outer join + left outer join
--Testcase 144:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 145:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- left outer join + full outer join
--Testcase 146:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 147:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
SET enable_memoize TO off;
-- right outer join + left outer join
--Testcase 148:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 149:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
RESET enable_memoize;
-- left outer join + right outer join
--Testcase 150:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY 1 OFFSET 10 LIMIT 10;
--Testcase 151:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY 1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows
--Testcase 152:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 153:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set
--Testcase 154:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE dynamodb_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
-- ALTER SERVER loopback OPTIONS (DROP extensions);
-- full outer join + WHERE clause with shippable extensions not set
--Testcase 155:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE dynamodb_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
-- ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
--Testcase 156:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 157:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 158:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
--Testcase 159:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause
--Testcase 160:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 161:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 162:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
--Testcase 163:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE
--Testcase 164:
EXPLAIN (VERBOSE, COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
--Testcase 165:
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference
--Testcase 166:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- SEMI JOIN, not pushed down
--Testcase 167:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 168:
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down
--Testcase 169:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 170:
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- CROSS JOIN can be pushed down
--Testcase 171:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 172:
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.
--Testcase 173:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 174:
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
--Testcase 175:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 176:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
--Testcase 177:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 178:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
--Testcase 179:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 180:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs
--Testcase 181:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
--Testcase 182:
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference
--Testcase 183:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;
--Testcase 184:
SELECT t1."C 1" FROM "S 1"."T 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."C 1" OFFSET 10 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
--Testcase 185:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15 ORDER BY c1;
--Testcase 186:
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15 ORDER BY c1;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
--Testcase 187:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15;
--Testcase 188:
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15 ORDER BY 1;

-- join with nullable side with some columns with null values
--Testcase 189:
UPDATE ft5_a_child SET c3 = null where c1 % 9 = 0;
--Testcase 190:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;
--Testcase 191:
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
-- CREATE TABLE local_tbl (c1 int NOT NULL, c2 int NOT NULL, c3 text, CONSTRAINT local_tbl_pkey PRIMARY KEY (c1));
--Testcase 192:
CREATE FOREIGN TABLE local_tbl_a_child (c1 int, c2 int, c3 text) SERVER dynamodb_server OPTIONS (table_name 'local_tbl', partition_key 'c1');
--Testcase 193:
CREATE TABLE local_tbl (c1 int, c2 int, c3 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 194:
CREATE FOREIGN TABLE local_tbl_a PARTITION OF local_tbl FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 195:
INSERT INTO local_tbl_a_child SELECT id, id % 10, to_char(id, 'FM0000') FROM generate_series(1, 1000) id;
-- ANALYZE local_tbl;
SET enable_nestloop TO false;
SET enable_hashjoin TO false;
--Testcase 196:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 ORDER BY 1 FOR UPDATE;
--Testcase 197:
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 ORDER BY 1 FOR UPDATE;
RESET enable_nestloop;
RESET enable_hashjoin;

-- test that add_paths_with_pathkeys_for_rel() arranges for the epq_path to
-- return columns needed by the parent ForeignScan node
--Testcase 801:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.*, COALESCE(ft1.c3 || ft2.c3, 'foobar') FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100)) ss ON (local_tbl.c1 = ss.c1) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;

-- ALTER SERVER dynamodb_server OPTIONS (DROP extensions);
-- ALTER SERVER dynamodb_server OPTIONS (ADD fdw_startup_cost '10000.0');
--Testcase 802:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM local_tbl LEFT JOIN (SELECT ft1.* FROM ft1 INNER JOIN ft2 ON (ft1.c1 = ft2.c1 AND ft1.c1 < 100 AND ft1.c1 = dynamodb_fdw_abs(ft2.c2))) ss ON (local_tbl.c3 = ss.c3) ORDER BY local_tbl.c1 FOR UPDATE OF local_tbl;
-- ALTER SERVER dynamodb_server OPTIONS (DROP fdw_startup_cost);
-- ALTER SERVER dynamodb_server OPTIONS (ADD extensions 'postgres_fdw');

--Testcase 198:
DROP FOREIGN TABLE local_tbl_a_child;
--Testcase 199:
DROP TABLE local_tbl;

-- check join pushdown in situations where multiple userids are involved
--Testcase 200:
CREATE ROLE regress_view_owner SUPERUSER;
--Testcase 201:
CREATE USER MAPPING FOR regress_view_owner SERVER dynamodb_server 
  OPTIONS (user :DYNAMODB_USER, password :DYNAMODB_PASSWORD);
GRANT SELECT ON ft4 TO regress_view_owner;
GRANT SELECT ON ft5 TO regress_view_owner;

--Testcase 202:
CREATE VIEW v4 AS SELECT * FROM ft4;
--Testcase 203:
CREATE VIEW v5 AS SELECT * FROM ft5;
ALTER VIEW v5 OWNER TO regress_view_owner;
--Testcase 204:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
--Testcase 205:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
ALTER VIEW v4 OWNER TO regress_view_owner;
--Testcase 206:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 207:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

--Testcase 208:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
--Testcase 209:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
ALTER VIEW v4 OWNER TO CURRENT_USER;
--Testcase 210:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
--Testcase 211:
SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
ALTER VIEW v4 OWNER TO regress_view_owner;

-- cleanup
--Testcase 212:
DROP OWNED BY regress_view_owner;
--Testcase 213:
DROP ROLE regress_view_owner;


-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================
-- DynamoDB FDW does not support any aggregate function => not push down
-- Simple aggregates
--Testcase 214:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;
--Testcase 215:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

--Testcase 216:
explain (verbose, costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;
--Testcase 217:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()
--Testcase 218:
explain (verbose, costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query
--Testcase 219:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;
--Testcase 220:
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel
--Testcase 221:
explain (verbose, costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions
--Testcase 222:
explain (verbose, costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;
--Testcase 223:
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are pushed down.
--Testcase 224:
explain (verbose, costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
--Testcase 225:
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
--Testcase 226:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;
--Testcase 227:
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
--Testcase 228:
explain (verbose, costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
--Testcase 229:
explain (verbose, costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
--Testcase 230:
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
--Testcase 231:
explain (verbose, costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);
--Testcase 232:
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability
--Testcase 233:
explain (verbose, costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;
--Testcase 234:
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
--Testcase 235:
explain (verbose, costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
--Testcase 236:
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
--Testcase 237:
explain (verbose, costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
--Testcase 238:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1;
--Testcase 239:
select exists(select 1 from pg_enum), sum(c1) from ft1;

--Testcase 240:
explain (verbose, costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;
--Testcase 241:
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;


-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order
--Testcase 242:
explain (verbose, costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
--Testcase 243:
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC
--Testcase 244:
explain (verbose, costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;
--Testcase 245:
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate
--Testcase 246:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 247:
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
--Testcase 248:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 249:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 250:
explain (verbose, costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 251:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- FILTER within aggregate
--Testcase 252:
explain (verbose, costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
--Testcase 253:
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
--Testcase 254:
explain (verbose, costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;
--Testcase 255:
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query
--Testcase 256:
explain (verbose, costs off)
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 257:
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
-- Inner query is aggregation query
--Testcase 258:
explain (verbose, costs off)
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 259:
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
--Testcase 260:
explain (verbose, costs off)
select sum(c1) filter (where (c1 / c1) * random() <= 1) from ft1 group by c2 order by 1;
--Testcase 261:
explain (verbose, costs off)
select sum(c2) filter (where c2 in (select c2 from ft1 where c2 < 5)) from ft1;

-- Ordered-sets within aggregate
--Testcase 262:
explain (verbose, costs off)
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;
--Testcase 263:
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

-- Using multiple arguments within aggregates
--Testcase 264:
explain (verbose, costs off)
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;
--Testcase 265:
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC
--Testcase 266:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
--Testcase 267:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate
--Testcase 268:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension
alter extension dynamodb_fdw add function least_accum(anyelement, variadic anyarray);
alter extension dynamodb_fdw add aggregate least_agg(variadic items anyarray);
-- alter server loopback options (set extensions 'postgres_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.
--Testcase 269:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;
--Testcase 270:
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension
alter extension dynamodb_fdw drop function least_accum(anyelement, variadic anyarray);
alter extension dynamodb_fdw drop aggregate least_agg(variadic items anyarray);
-- alter server loopback options (set extensions 'postgres_fdw');

-- Not pushed down as we have dropped objects from extension.
--Testcase 271:
explain (verbose, costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup
reset enable_hashagg;
--Testcase 272:
drop aggregate least_agg(variadic items anyarray);
--Testcase 273:
drop function least_accum(anyelement, variadic anyarray);


-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.
--Testcase 274:
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

--Testcase 275:
create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

--Testcase 276:
create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

--Testcase 277:
create operator family my_op_family using btree;

--Testcase 278:
create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

--Testcase 279:
create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.
--Testcase 280:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- This should not be pushed either.
--Testcase 772:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Update local stats on ft2
-- ANALYZE ft2;

-- Add into extension
alter extension dynamodb_fdw add operator class my_op_class using btree;
alter extension dynamodb_fdw add function my_op_cmp(a int, b int);
alter extension dynamodb_fdw add operator family my_op_family using btree;
alter extension dynamodb_fdw add operator public.<^(int, int);
alter extension dynamodb_fdw add operator public.=^(int, int);
alter extension dynamodb_fdw add operator public.>^(int, int);
-- alter server dynamodb_server options (set extensions 'dynamodb_fdw');

-- Now this will be pushed as sort operator is part of the extension.
--Testcase 281:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
--Testcase 282:
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- This should be pushed too.
-- dynamodb_fdw does not support pushdown user-defined operator
--Testcase 773:
explain (verbose, costs off)
select * from ft2 order by c1 using operator(public.<^);

-- Remove from extension
alter extension dynamodb_fdw drop operator class my_op_class using btree;
alter extension dynamodb_fdw drop function my_op_cmp(a int, b int);
alter extension dynamodb_fdw drop operator family my_op_family using btree;
alter extension dynamodb_fdw drop operator public.<^(int, int);
alter extension dynamodb_fdw drop operator public.=^(int, int);
alter extension dynamodb_fdw drop operator public.>^(int, int);
-- alter server dynamodb_server options (set extensions 'dynamodb_fdw');

-- This will not be pushed as sort operator is now removed from the extension.
--Testcase 283:
explain (verbose, costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup
--Testcase 284:
drop operator class my_op_class using btree;
--Testcase 285:
drop function my_op_cmp(a int, b int);
--Testcase 286:
drop operator family my_op_family using btree;
--Testcase 287:
drop operator public.>^(int, int);
--Testcase 288:
drop operator public.=^(int, int);
--Testcase 289:
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
--Testcase 290:
explain (verbose, costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate
--Testcase 291:
explain (verbose, costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;
--Testcase 292:
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
--Testcase 293:
explain (verbose, costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;
--Testcase 294:
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
--Testcase 295:
explain (verbose, costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);
--Testcase 296:
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
--Testcase 297:
explain (verbose, costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;
--Testcase 298:
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization
set enable_hashagg to false;
--Testcase 299:
explain (verbose, costs off)
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1."C 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C 1" < 100 order by 1;
--Testcase 300:
select c2, sum from "S 1"."T 1" t1, lateral (select sum(t2.c1 + t1."C 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."C 1" < 100 order by 1;
reset enable_hashagg;

-- bug #15613: bad plan for foreign table scan with lateral reference
--Testcase 301:
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

--Testcase 302:
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
--Testcase 303:
explain (verbose, costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);
--Testcase 304:
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);


-- Not supported cases
-- Grouping sets
--Testcase 305:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 306:
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 307:
explain (verbose, costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 308:
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 309:
explain (verbose, costs off)
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 310:
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 311:
explain (verbose, costs off)
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;
--Testcase 312:
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
--Testcase 313:
explain (verbose, costs off)
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;
--Testcase 314:
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

-- WindowAgg
--Testcase 315:
explain (verbose, costs off)
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 316:
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 317:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 318:
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 319:
explain (verbose, costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 320:
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;


-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join
--Testcase 321:
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
--Testcase 322:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st1(1, 2);
--Testcase 323:
EXECUTE st1(1, 1);
--Testcase 324:
EXECUTE st1(101, 101);
-- subquery using stable function (can't be sent to remote)
--Testcase 325:
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;
--Testcase 326:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st2(10, 20);
--Testcase 327:
EXECUTE st2(10, 20);
--Testcase 328:
EXECUTE st2(101, 121);
-- subquery using immutable function (can be sent to remote)
--Testcase 329:
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;
--Testcase 330:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st3(10, 20);
--Testcase 331:
EXECUTE st3(10, 20);
--Testcase 332:
EXECUTE st3(20, 30);
-- custom plan should be chosen initially
--Testcase 333:
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;
--Testcase 334:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 335:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 336:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 337:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
--Testcase 338:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
--Testcase 339:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
--Testcase 340:
PREPARE st5(text,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
--Testcase 341:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 342:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 343:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 344:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 345:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 346:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 347:
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
--Testcase 348:
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2 ORDER BY t1.c1;
--Testcase 349:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 350:
PREPARE st7 AS INSERT INTO ft1_a_child (c1,c2,c3) VALUES (1001,101,'foo');
--Testcase 351:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
ALTER TABLE "S 1"."T 1" RENAME TO "T 0";
ALTER FOREIGN TABLE ft1_a_child OPTIONS (SET table_name 'T 0');
--Testcase 352:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st6;
--Testcase 353:
EXECUTE st6;
--Testcase 354:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st7;
ALTER TABLE "S 1"."T 0" RENAME TO "T 1";
ALTER FOREIGN TABLE ft1_a_child OPTIONS (SET table_name 'T_1');

--Testcase 355:
PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 356:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
-- ALTER SERVER loopback OPTIONS (DROP extensions);
--Testcase 357:
EXPLAIN (VERBOSE, COSTS OFF) EXECUTE st8;
--Testcase 358:
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
--Testcase 359:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass ORDER BY 1 LIMIT 1;
--Testcase 360:
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1_a'::regclass ORDER BY 1 LIMIT 1;
--Testcase 361:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 362:
SELECT tableoid::regclass, * FROM ft1 t1 ORDER BY t1.c1 LIMIT 1;
--Testcase 363:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
-- ctid cannot be pushed down, so the result is empty
--Testcase 364:
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 365:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;
--Testcase 366:
SELECT ctid, * FROM ft1 t1 ORDER BY t1.c1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================
--Testcase 367:
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
--Testcase 368:
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
--Testcase 369:
SELECT f_test(100);
--Testcase 370:
DROP FUNCTION f_test(int);

-- -- ===================================================================
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
-- conversion error
-- ===================================================================
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE int;
--Testcase 371:
SELECT * FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8) WHERE x1 = 1;  -- ERROR
--Testcase 372:
SELECT ftx.x1, ft2.c2, ftx.x8 FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
  WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
--Testcase 373:
SELECT ftx.x1, ft2.c2, ftx FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
  WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
--Testcase 374:
SELECT sum(c2), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR
-- ANALYZE ft1; -- ERROR
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE text;

-- ===================================================================
-- local type can be different from remote type in some cases,
-- in particular if similarly-named operators do equivalent things
-- ===================================================================
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE text;
--Testcase 774:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE c8 = 'foo' LIMIT 1;
--Testcase 775:
SELECT * FROM ft1 WHERE c8 = 'foo' LIMIT 1;
--Testcase 776:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM ft1 WHERE 'foo' = c8 LIMIT 1;
--Testcase 777:
SELECT * FROM ft1 WHERE 'foo' = c8 LIMIT 1;
-- we declared c8 to be text locally, but it's still the same type on
-- the remote which will balk if we try to do anything incompatible
-- with that remote type
--Testcase 778:
SELECT * FROM ft1 WHERE c8 LIKE 'foo' LIMIT 1; -- ERROR
--Testcase 779:
SELECT * FROM ft1 WHERE c8::text LIKE 'foo' LIMIT 1; -- ERROR; cast not pushed down
ALTER FOREIGN TABLE ft1_a_child ALTER COLUMN c8 TYPE text;

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;
--Testcase 375:
FETCH c;
SAVEPOINT s;
ERROR OUT;          -- ERROR
ROLLBACK TO s;
--Testcase 376:
FETCH c;
SAVEPOINT s;
--Testcase 377:
SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK TO s;
--Testcase 378:
FETCH c;
--Testcase 379:
SELECT * FROM ft1 ORDER BY c1 LIMIT 1;
COMMIT;

-- ===================================================================
-- test handling of collations
-- ===================================================================
-- create table loct3 (f1 text collate "C" unique, f2 text, f3 varchar(10) unique);
--Testcase 380:
create foreign table loct3_a_child (
	f1 text, 
	f2 text, 
	f3 text
) server dynamodb_server options (table_name 'loct3', partition_key 'f1');
--Testcase 381:
create table loct3 (
	f1 text, 
	f2 text, 
	f3 text,
	spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 382:
CREATE FOREIGN TABLE loct3_a PARTITION OF loct3 FOR VALUES IN ('/node1/') SERVER spdsrv;
-- create foreign table ft3 (f1 text collate "C", f2 text, f3 varchar(10))
--   server loopback options (table_name 'loct3', use_remote_estimate 'true');
--Testcase 383:
create foreign table ft3_a_child (
	f1 text, 
	f2 text, 
	f3 text
) server dynamodb_server options (table_name 'loct3', partition_key 'f1');
--Testcase 384:
create table ft3 (
	f1 text, 
	f2 text, 
	f3 text,
	spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 385:
CREATE FOREIGN TABLE ft3_a PARTITION OF ft3 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- can be sent to remote
--Testcase 386:
explain (verbose, costs off) select * from ft3 where f1 = 'foo';
--Testcase 387:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "C" = 'foo';
--Testcase 388:
explain (verbose, costs off) select * from ft3 where f2 = 'foo';
--Testcase 389:
explain (verbose, costs off) select * from ft3 where f3 = 'foo';
--Testcase 390:
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 and l.f1 = 'foo';
-- can't be sent to remote
--Testcase 391:
explain (verbose, costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';
--Testcase 392:
explain (verbose, costs off) select * from ft3 where f1 = 'foo' COLLATE "C";
--Testcase 393:
explain (verbose, costs off) select * from ft3 where f2 COLLATE "C" = 'foo';
--Testcase 394:
explain (verbose, costs off) select * from ft3 where f2 = 'foo' COLLATE "C";
--Testcase 395:
explain (verbose, costs off) select * from ft3 f, loct3 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================
--Testcase 396:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 ORDER BY c1 LIMIT 20;
--Testcase 397:
INSERT INTO ft2_a_child (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 ORDER BY c1 LIMIT 20;
-- DynamoDB does not support RETURNING in INSERT query => remove RETURNING
--Testcase 398:
INSERT INTO ft2_a_child (c1,c2,c3)
  VALUES (1101,201,'aaa'), (1102,202,'bbb'), (1103,203,'ccc');
--Testcase 399:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1104,204,'ddd'), (1105,205,'eee');
--Testcase 400:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;              -- can be pushed down
--Testcase 401:
UPDATE ft2_a_child SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;
--Testcase 402:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7 RETURNING *;  -- can be pushed down
--Testcase 403:
UPDATE ft2_a_child SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7 RETURNING *;
--Testcase 404:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;                               -- can be pushed down
--Testcase 405:
UPDATE ft2_a_child SET c2 = ft2_a_child.c2 + 500, c3 = ft2_a_child.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 9;
--Testcase 406:
EXPLAIN (verbose, costs off)
  DELETE FROM ft2_a_child WHERE c1 % 10 = 5 RETURNING c1, c4;                               -- can be pushed down
--Testcase 407:
DELETE FROM ft2_a_child WHERE c1 % 10 = 5 RETURNING c1, c4;
--Testcase 408:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child USING ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 2;                -- can be pushed down
--Testcase 409:
DELETE FROM ft2_a_child USING ft1 WHERE ft1.c1 = ft2_a_child.c2 AND ft1.c1 % 10 = 2;
--Testcase 410:
SELECT c1,c2,c3,c4 FROM ft2 ORDER BY c1;
-- DynamoDB does not support RETURNING in INSERT query => remove RETURNING
--Testcase 411:
EXPLAIN (verbose, costs off)
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 412:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1200,999,'foo');
--Testcase 413:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200 RETURNING tableoid::regclass;             -- can be pushed down
--Testcase 414:
UPDATE ft2_a_child SET c3 = 'bar' WHERE c1 = 1200 RETURNING tableoid::regclass;
--Testcase 415:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child WHERE c1 = 1200 RETURNING tableoid::regclass;                       -- can be pushed down
--Testcase 416:
DELETE FROM ft2_a_child WHERE c1 = 1200 RETURNING tableoid::regclass;

-- Test UPDATE/DELETE with RETURNING on a three-table join
--Testcase 417:
INSERT INTO ft2_a_child (c1,c2,c3)
  SELECT id, id - 1200, to_char(id, 'FM00000') FROM generate_series(1201, 1300) id;
--Testcase 418:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c2 = ft4.c1
  RETURNING ft2_a_child, ft2_a_child.*, ft4, ft4.*;       -- can be pushed down
--Testcase 419:
UPDATE ft2_a_child SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c2 = ft4.c1
  RETURNING ft2_a_child, ft2_a_child.*, ft4, ft4.*;
--Testcase 420:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c1 % 10 = 0 AND ft2_a_child.c2 = ft4.c1
  RETURNING 100;                          -- can be pushed down
--Testcase 421:
DELETE FROM ft2_a_child
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 1200 AND ft2_a_child.c1 % 10 = 0 AND ft2_a_child.c2 = ft4.c1
  RETURNING 100;
--Testcase 422:
DELETE FROM ft2_a_child WHERE ft2_a_child.c1 > 1200;

-- Test UPDATE with a MULTIEXPR sub-select
-- (maybe someday this'll be remotely executable, but not today)
--Testcase 423:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;
--Testcase 424:
UPDATE ft2_a_child AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

--Testcase 425:
UPDATE ft2_a_child AS target SET (c2) = (
    SELECT c2 / 10
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

-- Test UPDATE involving a join that can be pushed down,
-- but a SET clause that can't be
--Testcase 780:
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE ft2_a_child d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;
--Testcase 781:
UPDATE ft2_a_child d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;

-- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- user-defined operators/functions
-- ALTER SERVER loopback OPTIONS (DROP extensions);
--Testcase 426:
INSERT INTO ft2_a_child (c1,c2,c3)
  SELECT id, id % 10, to_char(id, 'FM00000') FROM generate_series(2001, 2010) id;
--Testcase 427:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'bar' WHERE dynamodb_fdw_abs(c1) > 2000 RETURNING *;            -- can't be pushed down
--Testcase 428:
UPDATE ft2_a_child SET c3 = 'bar' WHERE dynamodb_fdw_abs(c1) > 2000 RETURNING *;
--Testcase 429:
EXPLAIN (verbose, costs off)
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1
  RETURNING ft2_a_child.*, ft4.*, ft5.*;                                                    -- can't be pushed down
--Testcase 430:
UPDATE ft2_a_child SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 === ft4.c1
  RETURNING ft2_a_child.*, ft4.*, ft5.*;
--Testcase 431:
EXPLAIN (verbose, costs off)
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1
  RETURNING ft2_a_child.c1, ft2_a_child.c2, ft2_a_child.c3;       -- can't be pushed down
--Testcase 432:
DELETE FROM ft2_a_child
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2_a_child.c1 > 2000 AND ft2_a_child.c2 = ft4.c1
  RETURNING ft2_a_child.c1, ft2_a_child.c2, ft2_a_child.c3;
--Testcase 433:
DELETE FROM ft2_a_child WHERE ft2_a_child.c1 > 2000;
-- ALTER SERVER loopback OPTIONS (ADD extensions 'postgres_fdw');

-- Test that trigger on remote table works as expected
--Testcase 434:
CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.c3 = NEW.c3 || '_trig_update';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--Testcase 435:
CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
    ON ft2_a_child FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

-- DynamoDB does not support RETURNING in INSERT query => remove RETURNING
--Testcase 436:
INSERT INTO ft2_a_child (c1,c2,c3) VALUES (1208, 818, 'fff');
--Testcase 437:
INSERT INTO ft2_a_child (c1,c2,c3,c6) VALUES (1218, 818, 'ggg', '(--;');
--Testcase 438:
UPDATE ft2_a_child SET c2 = c2 + 600 WHERE c1 % 10 = 8 AND c1 < 1200 RETURNING *;

-- Test errors thrown on remote side during update
ALTER TABLE "S 1"."T 1" ADD CONSTRAINT c2positive CHECK (c2 >= 0);

--Testcase 439:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12);  -- duplicate key
--Testcase 440:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT DO NOTHING; -- unsupported
--Testcase 441:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO NOTHING; -- unsupported
--Testcase 442:
INSERT INTO ft1_a_child(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO UPDATE SET c3 = 'ffg'; -- unsupported
-- DynamoDB not support constraints
-- --Testcase 443:
-- INSERT INTO ft1_a_child(c1, c2) VALUES(1111, -2);  -- c2positive
-- --Testcase 444:
-- UPDATE ft1_a_child SET c2 = -c2 WHERE c1 = 1;  -- c2positive

-- Test savepoint/rollback behavior
-- DynamoDB FDW does not support transaction, so the savepoint/rollback does not have any effect
--Testcase 445:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 446:
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
begin;
--Testcase 447:
update ft2_a_child set c2 = 42 where c2 = 0;
--Testcase 448:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s1;
--Testcase 449:
update ft2_a_child set c2 = 44 where c2 = 4;
--Testcase 450:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s1;
--Testcase 451:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s2;
--Testcase 452:
update ft2_a_child set c2 = 46 where c2 = 6;
--Testcase 453:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
rollback to savepoint s2;
--Testcase 454:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s2;
--Testcase 455:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s3;
-- DynamoDB not support constraints
-- --Testcase 456:
-- update ft2_a_child set c2 = -2 where c2 = 42 and c1 = 10; -- fail on remote side
rollback to savepoint s3;
--Testcase 457:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s3;
--Testcase 458:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- none of the above is committed yet remotely
--Testcase 459:
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;
commit;
--Testcase 460:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 461:
select c2, count(*) from "S 1"."T 1" where c2 < 500 group by 1 order by 1;

-- VACUUM ANALYZE "S 1"."T 1";

-- Above DMLs add data with c6 as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- FIRST behavior here.
-- ORDER BY DESC NULLS LAST options
--Testcase 462:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;
--Testcase 463:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795  LIMIT 10;
-- ORDER BY DESC NULLS FIRST options
--Testcase 464:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 465:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- ORDER BY ASC NULLS FIRST options
--Testcase 466:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 467:
SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

/*
-- DynamoDB does not support constraint
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

/*
-- DynamoDB FDW does not support WITH CHECK OPTION
-- ===================================================================
-- test WITH CHECK OPTION constraints
-- ===================================================================

CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;

CREATE TABLE base_tbl (a int, b int);
ALTER TABLE base_tbl SET (autovacuum_enabled = 'false');
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON base_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();
CREATE FOREIGN TABLE foreign_tbl (a int, b int)
  SERVER loopback OPTIONS (table_name 'base_tbl');
CREATE VIEW rw_view AS SELECT * FROM foreign_tbl
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
ALTER SERVER dynamodb_server OPTIONS (ADD batch_size '10');
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 15), (0, 5);
INSERT INTO rw_view VALUES (0, 15), (0, 5); -- should fail
SELECT * FROM foreign_tbl;
ALTER SERVER dynamodb_server OPTIONS (DROP batch_size);

DROP FOREIGN TABLE foreign_tbl CASCADE;
DROP TRIGGER row_before_insupd_trigger ON base_tbl;
DROP TABLE base_tbl;

-- test WCO for partitions

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
ALTER SERVER dynamodb_server OPTIONS (ADD batch_size '10');
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rw_view VALUES (0, 15), (0, 5);
INSERT INTO rw_view VALUES (0, 15), (0, 5); -- should fail
SELECT * FROM foreign_tbl;
ALTER SERVER dynamodb_server OPTIONS (DROP batch_size);

DROP FOREIGN TABLE foreign_tbl CASCADE;
DROP TRIGGER row_before_insupd_trigger ON child_tbl;
DROP TABLE parent_tbl CASCADE;

DROP FUNCTION row_before_insupd_trigfunc;
*/

-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- ===================================================================
--Testcase 468:
create foreign table rem1_a_child (id serial, f1 serial, f2 text)
  server dynamodb_server options(table_name 'loct13', partition_key 'id');
--Testcase 469:
create table rem1 (id serial, f1 serial, f2 text, spdurl text) PARTITION BY LIST (spdurl);
--Testcase 470:
CREATE FOREIGN TABLE rem1_a PARTITION OF rem1 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 471:
insert into rem1_a_child(f2) values('hi');
--Testcase 472:
insert into rem1_a_child(f2) values('bye');
--Testcase 473:
select pg_catalog.setval('rem1_a_child_f1_seq', 10, false);
--Testcase 474:
insert into rem1_a_child(f2) values('hi remote');
--Testcase 475:
insert into rem1_a_child(f2) values('bye remote');
--Testcase 476:
select f1, f2 from rem1;

-- ===================================================================
-- test generated columns
-- ===================================================================
--Testcase 477:
create foreign table grem1_a_child (
  id serial,
  a int,
  b int generated always as (a * 2) stored)
  server dynamodb_server options(table_name 'gloc1', partition_key 'id');
--Testcase 478:
create table grem1 (
  id serial,
  a int,
  b int generated always as (a * 2) stored,
  spdurl text
) PARTITION BY LIST (spdurl);
--Testcase 479:
CREATE FOREIGN TABLE grem1_a PARTITION OF grem1 FOR VALUES IN ('/node1/') SERVER spdsrv;
--Testcase 480:
explain (verbose, costs off)
insert into grem1_a_child (a) values (1), (2);
--Testcase 481:
insert into grem1_a_child (a) values (1), (2);
--Testcase 482:
explain (verbose, costs off)
update grem1_a_child set a = 22 where a = 2;
--Testcase 483:
update grem1_a_child set a = 22 where a = 2;
--Testcase 484:
select a, b from grem1;
--Testcase 485:
delete from grem1_a_child;

-- DynamoDB FDW does not support COPY FROM, only keep 1 test case to
-- test error message
-- test copy from
copy grem1_a_child (a) from stdin;
1
2
\.
-- --Testcase 486:
-- select * from grem1;
-- --Testcase 487:
-- delete from grem1_a_child;
-- DynamoDB FDW does not support batch_size option, so this test case should be commented out
-- test batch insert
-- alter server dynamodb_server options (add batch_size '10');
--Testcase 782:
explain (verbose, costs off)
insert into grem1_a_child (a) values (1), (2);
--Testcase 783:
insert into grem1_a_child (a) values (1), (2);
--Testcase 784:
select a, b from grem1;
--Testcase 785:
delete from grem1_a_child;
-- DynamoDB FDW does not support batch_size option, so this test case should be commented out
-- alter server dynamodb_server options (drop batch_size);

-- ===================================================================
-- test local triggers
-- ===================================================================

-- Trigger functions "borrowed" from triggers regress test.
--Testcase 488:
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

--Testcase 489:
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 490:
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 491:
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
--Testcase 492:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 493:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 494:
delete from rem1_a_child;
--Testcase 495:
insert into rem1_a_child (f1, f2) values(1,'insert');
--Testcase 496:
update rem1_a_child set f2  = 'update' where f1 = 1;
--Testcase 497:
update rem1_a_child set f2 = f2 || f2;


-- cleanup
--Testcase 498:
DROP TRIGGER trig_row_before ON rem1_a_child;
--Testcase 499:
DROP TRIGGER trig_row_after ON rem1_a_child;
--Testcase 500:
DROP TRIGGER trig_stmt_before ON rem1_a_child;
--Testcase 501:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

--Testcase 502:
DELETE from rem1_a_child;

-- Test multiple AFTER ROW triggers on a foreign table
--Testcase 503:
CREATE TRIGGER trig_row_after1
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 504:
CREATE TRIGGER trig_row_after2
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 505:
insert into rem1_a_child (f1, f2) values(1,'insert');
--Testcase 506:
update rem1_a_child set f2  = 'update' where f1 = 1;
--Testcase 507:
update rem1_a_child set f2 = f2 || f2;
--Testcase 508:
delete from rem1_a_child;

-- cleanup
--Testcase 509:
DROP TRIGGER trig_row_after1 ON rem1_a_child;
--Testcase 510:
DROP TRIGGER trig_row_after2 ON rem1_a_child;

-- Test WHEN conditions

--Testcase 511:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 512:
CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens
--Testcase 513:
INSERT INTO rem1_a_child (f1, f2) values(1, 'insert');
--Testcase 514:
UPDATE rem1_a_child set f2 = 'test';

-- Insert or update matching: triggers are fired
--Testcase 515:
INSERT INTO rem1_a_child (f1, f2) values(2, 'update');
--Testcase 516:
UPDATE rem1_a_child set f2 = 'update update' where f1 = '2';

--Testcase 517:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 518:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1
--Testcase 519:
DELETE FROM rem1_a_child;

-- cleanup
--Testcase 520:
DROP TRIGGER trig_row_before_insupd ON rem1_a_child;
--Testcase 521:
DROP TRIGGER trig_row_after_insupd ON rem1_a_child;
--Testcase 522:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;
--Testcase 523:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;


-- Test various RETURN statements in BEFORE triggers.

--Testcase 524:
CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

--Testcase 525:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- The new values should have 'triggered' appended
--Testcase 526:
INSERT INTO rem1_a_child (f1, f2) values(1, 'insert');
--Testcase 527:
select f1, f2 from rem1;
-- DynamoDB does not support RETURNING in INSERT query => remove RETURNING
--Testcase 528:
INSERT INTO rem1_a_child (f1, f2) values(2, 'insert');
--Testcase 529:
select f1, f2 from rem1;
--Testcase 530:
UPDATE rem1_a_child set f2 = '';
--Testcase 531:
select f1, f2 from rem1;
--Testcase 532:
UPDATE rem1_a_child set f2 = 'skidoo' RETURNING f2;
--Testcase 533:
select f1, f2 from rem1;

--Testcase 534:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f1 = 10;          -- all columns should be transmitted
--Testcase 535:
UPDATE rem1_a_child set f1 = 10;
--Testcase 536:
select f1, f2 from rem1;

--Testcase 537:
DELETE FROM rem1_a_child;

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger
--Testcase 538:
CREATE TRIGGER trig_row_before_insupd2
BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 539:
INSERT INTO rem1_a_child (f1, f2) values(1, 'insert');
--Testcase 540:
select f1, f2 from rem1;
-- DynamoDB does not support RETURNING in INSERT query => remove RETURNING
--Testcase 541:
INSERT INTO rem1_a_child (f1, f2) values(2, 'insert');
--Testcase 542:
select f1, f2 from rem1 ORDER BY f1;
--Testcase 543:
UPDATE rem1_a_child set f2 = '';
--Testcase 544:
select f1, f2 from rem1 ORDER BY f1;
--Testcase 545:
UPDATE rem1_a_child set f2 = 'skidoo' RETURNING f2;
--Testcase 546:
select f1, f2 from rem1 ORDER BY f1;

--Testcase 547:
DROP TRIGGER trig_row_before_insupd ON rem1_a_child;
--Testcase 548:
DROP TRIGGER trig_row_before_insupd2 ON rem1_a_child;

--Testcase 549:
DELETE from rem1_a_child;

--Testcase 550:
INSERT INTO rem1_a_child (f1, f2) VALUES (1, 'test');

-- Test with a trigger returning NULL
--Testcase 551:
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

--Testcase 552:
CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.
--Testcase 553:
INSERT INTO rem1_a_child (f1, f2) VALUES (2, 'test2');

--Testcase 554:
select f1, f2 from rem1;

--Testcase 555:
UPDATE rem1_a_child SET f2 = 'test2';

--Testcase 556:
select f1, f2 from rem1;

--Testcase 557:
DELETE from rem1_a_child;

--Testcase 558:
select f1, f2 from rem1;

--Testcase 559:
DROP TRIGGER trig_null ON rem1_a_child;
--Testcase 560:
DELETE from rem1_a_child;

-- Test a combination of local and remote triggers
--Testcase 561:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 562:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 563:
CREATE TRIGGER trig_local_before BEFORE INSERT OR UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 564:
INSERT INTO rem1_a_child(f2) VALUES ('test');
--Testcase 565:
UPDATE rem1_a_child SET f2 = 'testo';

-- Test returning a system attribute
-- DynamoDB does not support RETURNING in INSERT query => remove RETURNING
--Testcase 566:
INSERT INTO rem1_a_child(f2) VALUES ('test');

-- cleanup
--Testcase 567:
DROP TRIGGER trig_row_before ON rem1_a_child;
--Testcase 568:
DROP TRIGGER trig_row_after ON rem1_a_child;
--Testcase 569:
DROP TRIGGER trig_local_before ON rem1_a_child;


-- DynamoDB FDW does not support direct modification
-- Test direct foreign table modification functionality
--Testcase 786:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down
--Testcase 787:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child WHERE false;     -- currently can't be pushed down

-- Test with statement-level triggers
--Testcase 570:
CREATE TRIGGER trig_stmt_before
	BEFORE DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 571:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 572:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 573:
DROP TRIGGER trig_stmt_before ON rem1_a_child;

--Testcase 574:
CREATE TRIGGER trig_stmt_after
	AFTER DELETE OR INSERT OR UPDATE ON rem1_a_child
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 575:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 576:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 577:
DROP TRIGGER trig_stmt_after ON rem1_a_child;

-- Test with row-level ON INSERT triggers
--Testcase 578:
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 579:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 580:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 581:
DROP TRIGGER trig_row_before_insert ON rem1_a_child;

--Testcase 582:
CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 583:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 584:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 585:
DROP TRIGGER trig_row_after_insert ON rem1_a_child;

-- Test with row-level ON UPDATE triggers
--Testcase 586:
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 587:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down
--Testcase 588:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 589:
DROP TRIGGER trig_row_before_update ON rem1_a_child;

--Testcase 590:
CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 591:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can't be pushed down
--Testcase 592:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can be pushed down
--Testcase 593:
DROP TRIGGER trig_row_after_update ON rem1_a_child;

-- Test with row-level ON DELETE triggers
--Testcase 594:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 595:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 596:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down
--Testcase 597:
DROP TRIGGER trig_row_before_delete ON rem1_a_child;

--Testcase 598:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 599:
EXPLAIN (verbose, costs off)
UPDATE rem1_a_child set f2 = '';          -- can be pushed down
--Testcase 600:
EXPLAIN (verbose, costs off)
DELETE FROM rem1_a_child;                 -- can't be pushed down
--Testcase 601:
DROP TRIGGER trig_row_after_delete ON rem1_a_child;

-- ===================================================================
-- test inheritance features
-- ===================================================================

--Testcase 602:
CREATE TABLE a (id serial, aa TEXT);
ALTER TABLE a SET (autovacuum_enabled = 'false');
--Testcase 603:
CREATE FOREIGN TABLE b_a_child (bb TEXT) INHERITS (a)
  SERVER dynamodb_server OPTIONS (table_name 'loct', partition_key 'id');
--Testcase 604:
CREATE TABLE b (id integer, aa TEXT, bb TEXT, spdurl text)
   PARTITION BY LIST (spdurl);
--Testcase 605:
CREATE FOREIGN TABLE b_a PARTITION OF b FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 606:
INSERT INTO a(aa) VALUES('aaa');
--Testcase 607:
INSERT INTO a(aa) VALUES('aaaa');
--Testcase 608:
INSERT INTO a(aa) VALUES('aaaaa');

--Testcase 609:
INSERT INTO b_a_child(aa) VALUES('bbb');
--Testcase 610:
INSERT INTO b_a_child(aa) VALUES('bbbb');
--Testcase 611:
INSERT INTO b_a_child(aa) VALUES('bbbbb');

--Testcase 612:
SELECT tableoid::regclass, aa FROM a ORDER BY 1, 2;
--Testcase 613:
SELECT tableoid::regclass, aa, bb FROM b ORDER BY 1, 2, 3;
--Testcase 614:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 615:
UPDATE a SET aa = 'zzzzzz' WHERE aa LIKE 'aaaa%';

--Testcase 616:
SELECT tableoid::regclass, aa FROM a ORDER BY 1, 2;
--Testcase 617:
SELECT tableoid::regclass, aa, bb FROM b ORDER BY 1, 2, 3;
--Testcase 618:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 619:
UPDATE b_a_child SET aa = 'new';

--Testcase 620:
SELECT tableoid::regclass, aa FROM a;
--Testcase 621:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 622:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 623:
UPDATE a SET aa = 'newtoo';

--Testcase 624:
SELECT tableoid::regclass, aa FROM a;
--Testcase 625:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 626:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 627:
DELETE FROM a;

--Testcase 628:
SELECT tableoid::regclass, aa FROM a;
--Testcase 629:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 630:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 631:
DROP TABLE a CASCADE;

-- Check SELECT FOR UPDATE/SHARE with an inherited source table

--Testcase 632:
create table foo (f1 int, f2 int);

--Testcase 633:
create foreign table foo2_a_child (f3 int) inherits (foo)
  server dynamodb_server options (table_name 'loct1');
--Testcase 634:
create table foo2 (f1 int, f2 int, f3 int, spdurl text)
   PARTITION BY LIST (spdurl);
--Testcase 635:
CREATE FOREIGN TABLE foo2_a PARTITION OF foo2 FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 636:
create table bar (f1 int, f2 int);

--Testcase 637:
create foreign table bar2_a_child (f3 int) inherits (bar)
  server dynamodb_server options (table_name 'loct2');
--Testcase 638:
create table bar2 (f3 int, spdurl text)
  PARTITION BY LIST (spdurl);
--Testcase 639:
CREATE FOREIGN TABLE bar2_a PARTITION OF bar2 FOR VALUES IN ('/node1/') SERVER spdsrv;

alter table foo set (autovacuum_enabled = 'false');
alter table bar set (autovacuum_enabled = 'false');

alter foreign table foo2_a_child options (add partition_key 'f1');
alter foreign table bar2_a_child options (add partition_key 'f1');

--Testcase 640:
insert into foo values(1,1);
--Testcase 641:
insert into foo values(3,3);
--Testcase 642:
insert into foo2_a_child values(2,2,2);
--Testcase 643:
insert into foo2_a_child values(4,4,4);
--Testcase 644:
insert into bar values(1,11);
--Testcase 645:
insert into bar values(2,22);
--Testcase 646:
insert into bar values(6,66);
--Testcase 647:
insert into bar2_a_child values(3,33,33);
--Testcase 648:
insert into bar2_a_child values(4,44,44);
--Testcase 649:
insert into bar2_a_child values(7,77,77);

--Testcase 650:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for update;
--Testcase 651:
select * from bar where f1 in (select f1 from foo) for update;

--Testcase 652:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo) for share;
--Testcase 653:
select * from bar where f1 in (select f1 from foo) for share;

-- Now check SELECT FOR UPDATE/SHARE with an inherited source table,
-- where the parent is itself a foreign table
--Testcase 654:
create foreign table foo2child_a_child (f3 int) inherits (foo2_a_child)
  server dynamodb_server options (table_name 'loct4');
--Testcase 655:
create table foo2child (f3 int, spdurl text)
   PARTITION BY LIST (spdurl);
--Testcase 656:
CREATE FOREIGN TABLE foo2child_a PARTITION OF foo2child FOR VALUES IN ('/node1/') SERVER spdsrv;

--Testcase 657:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo2) for share;
--Testcase 658:
select * from bar where f1 in (select f1 from foo2) for share;

--Testcase 659:
drop foreign table foo2child_a_child;
--Testcase 660:
drop table foo2child;

-- And with a local child relation of the foreign table parent
--Testcase 661:
create table foo2child (f3 int) inherits (foo2_a_child);

--Testcase 662:
explain (verbose, costs off)
select * from bar where f1 in (select f1 from foo2) for share;
--Testcase 663:
select * from bar where f1 in (select f1 from foo2) for share;

--Testcase 664:
drop table foo2child;

-- Check UPDATE with inherited target and an inherited source table
--Testcase 665:
explain (verbose, costs off)
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);
--Testcase 666:
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

--Testcase 667:
select tableoid::regclass, * from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery
--Testcase 668:
explain (verbose, costs off)
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;
--Testcase 669:
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

--Testcase 670:
select tableoid::regclass, * from bar order by 1,2;

-- Test forcing the remote server to produce sorted data for a merge join,
-- but the foreign table is an inheritance child.
-- truncate table loct1;
--Testcase 671:
DELETE FROM foo2_a_child;
truncate table only foo;
\set num_rows_foo 2000
--Testcase 672:
insert into foo2_a_child select generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2);
--Testcase 673:
insert into foo select generate_series(1, :num_rows_foo, 2), generate_series(1, :num_rows_foo, 2);
SET enable_hashjoin to false;
SET enable_nestloop to false;
-- alter foreign table foo2 options (use_remote_estimate 'true');
-- --Testcase 674:
-- create index i_loct1_f1 on foo2_a_child(f1);
-- --Testcase 675:
-- create index i_foo_f1 on foo(f1);
-- analyze foo;
-- analyze loct1;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 676:
explain (verbose, costs off)
	select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 677:
select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 678:
explain (verbose, costs off)
	select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 679:
select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
RESET enable_hashjoin;
RESET enable_nestloop;

-- Test that WHERE CURRENT OF is not supported
begin;
declare c cursor for select * from bar where f1 = 7;
--Testcase 680:
fetch from c;
--Testcase 681:
update bar set f2 = null where current of c;
rollback;

--Testcase 682:
explain (verbose, costs off)
delete from foo where f1 < 5 returning *;
--Testcase 683:
delete from foo where f1 < 5 returning *;
--Testcase 684:
explain (verbose, costs off)
update bar set f2 = f2 + 100 returning *;
--Testcase 685:
update bar set f2 = f2 + 100 returning *;

-- Test that UPDATE/DELETE with inherited target works with row-level triggers
--Testcase 686:
CREATE TRIGGER trig_row_before
BEFORE UPDATE OR DELETE ON bar2_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 687:
CREATE TRIGGER trig_row_after
AFTER UPDATE OR DELETE ON bar2_a_child
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 688:
explain (verbose, costs off)
update bar set f2 = f2 + 100;
--Testcase 689:
update bar set f2 = f2 + 100;

--Testcase 690:
explain (verbose, costs off)
delete from bar where f2 < 400;
--Testcase 691:
delete from bar where f2 < 400;

-- cleanup
--Testcase 692:
drop table foo cascade;
--Testcase 693:
drop table bar cascade;

-- Test pushing down UPDATE/DELETE joins to the remote server
--Testcase 694:
create table parent (a int, b text);
-- create foreign table remt1 (a int, b text)
--   server loopback options (table_name 'loct1');
--Testcase 695:
create foreign table remt1_a_child (a int, b text)
  server dynamodb_server options (table_name 'loct11', partition_key 'a');
--Testcase 696:
create table remt1 (a int, b text, spdurl text)
  PARTITION BY LIST (spdurl);
--Testcase 697:
CREATE FOREIGN TABLE remt1_a PARTITION OF remt1 FOR VALUES IN ('/node1/') SERVER spdsrv;
-- create foreign table remt2 (a int, b text)
--   server loopback options (table_name 'loct2');
--Testcase 698:
create foreign table remt2_a_child (a int, b text)
  server dynamodb_server options (table_name 'loct22', partition_key 'a');
--Testcase 699:
create table remt2 (a int, b text, spdurl text)
  PARTITION BY LIST (spdurl);
--Testcase 700:
CREATE FOREIGN TABLE remt2_a PARTITION OF remt2 FOR VALUES IN ('/node1/') SERVER spdsrv;
alter foreign table remt1_a_child inherit parent;

--Testcase 701:
insert into remt1_a_child values (1, 'foo');
--Testcase 702:
insert into remt1_a_child values (2, 'bar');
--Testcase 703:
insert into remt2_a_child values (1, 'foo');
--Testcase 704:
insert into remt2_a_child values (2, 'bar');

-- analyze remt1;
-- analyze remt2;

--Testcase 705:
explain (verbose, costs off)
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a returning *;
--Testcase 706:
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a returning *;
--Testcase 707:
explain (verbose, costs off)
delete from parent using remt2 where parent.a = remt2.a returning parent;
--Testcase 708:
delete from parent using remt2 where parent.a = remt2.a returning parent;

-- cleanup
--Testcase 709:
drop foreign table remt1_a_child;
--Testcase 710:
drop table remt1;
--Testcase 711:
drop foreign table remt2_a_child;
--Testcase 712:
drop table remt2;
--Testcase 713:
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

-- drop table ctrtest;
-- drop table loct1;
-- drop table loct2;

-- DynamoDB FDW does not support COPY FROM
-- -- ===================================================================
-- -- test COPY FROM
-- -- ===================================================================

-- --Testcase 714:
-- create foreign table rem2_a_child (id serial, f1 int, f2 text) server dynamodb_server options(table_name 'loc2', partition_key 'id');
-- --Testcase 715:
-- create table rem2 (id serial, f1 int, f2 text, spdurl text) PARTITION BY LIST (spdurl);
-- --Testcase 716:
-- CREATE FOREIGN TABLE rem2_a PARTITION OF rem2 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- -- Test basic functionality
-- copy rem2_a_child (f1, f2) from stdin;
-- 1	foo
-- 2	bar
-- \.
-- --Testcase 717:
-- select * from rem2;

-- --Testcase 718:
-- delete from rem2_a_child;

-- -- Test check constraints
-- -- alter table loc2 add constraint loc2_f1positive check (f1 >= 0);
-- alter foreign table rem2_a_child add constraint rem2_f1positive check (f1 >= 0);

-- -- check constraint is enforced on the remote side, not locally
-- copy rem2_a_child (f1, f2) from stdin;
-- 1	foo
-- 2	bar
-- \.
-- copy rem2_a_child (f1, f2) from stdin; -- ERROR
-- -1	xyzzy
-- \.
-- --Testcase 719:
-- select * from rem2;

-- alter foreign table rem2_a_child drop constraint rem2_f1positive;
-- -- alter table loc2 drop constraint loc2_f1positive;

-- --Testcase 720:
-- delete from rem2_a_child;

-- -- Test local triggers
-- --Testcase 721:
-- create trigger trig_stmt_before before insert on rem2_a_child
-- 	for each statement execute procedure trigger_func();
-- --Testcase 722:
-- create trigger trig_stmt_after after insert on rem2_a_child
-- 	for each statement execute procedure trigger_func();
-- --Testcase 723:
-- create trigger trig_row_before before insert on rem2_a_child
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- --Testcase 724:
-- create trigger trig_row_after after insert on rem2_a_child
-- 	for each row execute procedure trigger_data(23,'skidoo');

-- copy rem2_a_child (f1, f2) from stdin;
-- 1	foo
-- 2	bar
-- \.
-- --Testcase 725:
-- select * from rem2;

-- --Testcase 726:
-- drop trigger trig_row_before on rem2_a_child;
-- --Testcase 727:
-- drop trigger trig_row_after on rem2_a_child;
-- --Testcase 728:
-- drop trigger trig_stmt_before on rem2_a_child;
-- --Testcase 729:
-- drop trigger trig_stmt_after on rem2_a_child;

-- --Testcase 730:
-- delete from rem2_a_child;

-- --Testcase 731:
-- create trigger trig_row_before_insert before insert on rem2_a_child
-- 	for each row execute procedure trig_row_before_insupdate();

-- -- The new values are concatenated with ' triggered !'
-- copy rem2_a_child (f1, f2) from stdin;
-- 1	foo
-- 2	bar
-- \.
-- --Testcase 732:
-- select * from rem2;

-- --Testcase 733:
-- drop trigger trig_row_before_insert on rem2_a_child;

-- --Testcase 734:
-- delete from rem2_a_child;

-- --Testcase 735:
-- create trigger trig_null before insert on rem2_a_child
-- 	for each row execute procedure trig_null();

-- -- Nothing happens
-- copy rem2_a_child (f1, f2) from stdin;
-- 1	foo
-- 2	bar
-- \.
-- --Testcase 736:
-- select * from rem2;

-- --Testcase 737:
-- drop trigger trig_null on rem2_a_child;

-- --Testcase 738:
-- delete from rem2_a_child;

-- -- Test remote triggers
-- --Testcase 739:
-- create trigger trig_row_before_insert before insert on rem2_a_child
-- 	for each row execute procedure trig_row_before_insupdate();

-- -- The new values are concatenated with ' triggered !'
-- copy rem2_a_child(f1, f2) from stdin;
-- 1	foo
-- 2	bar
-- \.
-- --Testcase 740:
-- select * from rem2;

-- --Testcase 741:
-- drop trigger trig_row_before_insert on rem2_a_child;

-- --Testcase 742:
-- delete from rem2_a_child;

-- --Testcase 743:
-- create trigger trig_null before insert on rem2_a_child
-- 	for each row execute procedure trig_null();

-- -- Nothing happens
-- copy rem2_a_child (f1, f2) from stdin;
-- 1	foo
-- 2	bar
-- \.
-- --Testcase 744:
-- select * from rem2;

-- --Testcase 745:
-- drop trigger trig_null on rem2_a_child;

-- --Testcase 746:
-- delete from rem2_a_child;

-- -- Test a combination of local and remote triggers
-- --Testcase 747:
-- create trigger rem2_trig_row_before before insert on rem2_a_child
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- --Testcase 748:
-- create trigger rem2_trig_row_after after insert on rem2_a_child
-- 	for each row execute procedure trigger_data(23,'skidoo');
-- --Testcase 749:
-- create trigger loc2_trig_row_before_insert before insert on loc2
-- 	for each row execute procedure trig_row_before_insupdate();

-- copy rem2_a_child (f1, f2) from stdin;
-- 1	foo
-- 2	bar
-- \.
-- --Testcase 750:
-- select * from rem2;

-- --Testcase 751:
-- drop trigger rem2_trig_row_before on rem2_a_child;
-- --Testcase 752:
-- drop trigger rem2_trig_row_after on rem2_a_child;
-- --Testcase 753:
-- drop trigger loc2_trig_row_before_insert on rem2_a_child;

-- --Testcase 754:
-- delete from rem2_a_child;

-- -- test COPY FROM with foreign table created in the same transaction
-- begin;
-- --Testcase 755:
-- create foreign table rem3_a_child (f1 int, f2 text)
-- 	server dynamodb_server options(table_name 'loc3');
-- --Testcase 756:
-- create table rem3 (f1 int, f2 text, spdurl text) PARTITION BY LIST (spdurl);
-- --Testcase 757:
-- CREATE FOREIGN TABLE rem3_a PARTITION OF rem3 FOR VALUES IN ('/node1/') SERVER spdsrv;

-- copy rem3_a_child (f1, f2) from stdin;
-- 1	foo
-- 2	bar
-- \.
-- commit;
-- --Testcase 758:
-- select * from rem3;
-- --Testcase 759:
-- drop foreign table rem3_a_child;

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
-- 
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

-- DynamoDB FDW does not support IMPORT FOREIGN SCHEMA
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
-- IMPORT FOREIGN SCHEMA import_source LIMIT TO (t1, nonesuch, t4_part)
--   FROM SERVER loopback INTO import_dest4;
-- \det+ import_dest4.*
-- IMPORT FOREIGN SCHEMA import_source EXCEPT (t1, "x 4", nonesuch, t4_part)
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

-- DynamoDB does not support superuser
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

-- CREATE FOREIGN TABLE pg_temp.ft1_nopw (
-- 	c1 int NOT NULL,
-- 	c2 int NOT NULL,
-- 	c3 text,
-- 	c4 timestamptz,
-- 	c5 timestamp,
-- 	c6 varchar(10),
-- 	c7 char(10) default 'ft1',
-- 	c8 user_enum
-- ) SERVER loopback_nopw OPTIONS (schema_name 'public', table_name 'ft1');

-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- If we add a password to the connstr it'll fail, because we don't allow passwords
-- -- in connstrs only in user mappings.

-- DO $d$
--     BEGIN
--         EXECUTE $$ALTER SERVER loopback_nopw OPTIONS (ADD password 'dummypw')$$;
--     END;
-- $d$;

-- -- If we add a password for our user mapping instead, we should get a different
-- -- error because the password wasn't actually *used* when we run with trust auth.
-- --
-- -- This won't work with installcheck, but neither will most of the FDW checks.

-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password 'dummypw');

-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- Unpriv user cannot make the mapping passwordless
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password_required 'false');


-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- RESET ROLE;

-- -- But the superuser can
-- ALTER USER MAPPING FOR regress_nosuper SERVER loopback_nopw OPTIONS (ADD password_required 'false');

-- SET ROLE regress_nosuper;

-- -- Should finally work now
-- SELECT 1 FROM ft1_nopw LIMIT 1;

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

-- ===================================================================
-- reestablish new connection
-- ===================================================================
-- -- Test case relative with option application_name is not suitable for SQLite FDW.
-- -- Because this option is in libpq of postgres.
-- -- Change application_name of remote connection to special one
-- -- so that we can easily terminate the connection later.
-- ALTER SERVER sqlite_svr OPTIONS (application_name 'fdw_retry_check');
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

-- ===================================================================
-- test connection invalidation cases and postgres_fdw_get_connections function
-- ===================================================================
-- -- Let's ensure to close all the existing cached connections.
-- SELECT 1 FROM dynamodb_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- SELECT server_name FROM dynamodb_fdw_get_connections() ORDER BY 1;
-- -- This test case is for closing the connection in pgfdw_xact_callback
BEGIN;
-- Connection xact depth becomes 1 i.e. the connection is in midst of the xact.
--Testcase 760:
SELECT 1 FROM ft1 LIMIT 1;
--Testcase 788:
SELECT 1 FROM ft7 LIMIT 1;
-- -- List all the existing cached connections. loopback and loopback3 should be
-- -- output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- DynamoDB FDW does not support use_remote_estimate option
-- -- Connection is not closed at the end of the alter statement in
-- -- pgfdw_inval_callback. That's because the connection is in midst of this
-- -- xact, it is just marked as invalid.
-- ALTER SERVER loopback OPTIONS (ADD use_remote_estimate 'off');
-- DROP SERVER loopback3 CASCADE;
-- -- List all the existing cached connections. loopback and loopback3
-- -- should be output as invalid connections. Also the server name for
-- -- loopback3 should be NULL because the server was dropped.
-- SELECT * FROM postgres_fdw_get_connections() ORDER BY 1;
-- The invalid connection gets closed in pgfdw_xact_callback during commit.
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

-- -- try if large batches exceed max number of bind parameters
-- CREATE FOREIGN TABLE ftable ( x int ) SERVER loopback OPTIONS ( table_name 'batch_table', batch_size '100000' );
-- INSERT INTO ftable SELECT * FROM generate_series(1, 70000) i;
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

--Testcase 761:
DROP USER MAPPING FOR public SERVER dynamodb_server;
--Testcase 762:
DROP USER MAPPING FOR CURRENT_USER SERVER spdsrv;
--Testcase 763:
DROP SERVER dynamodb_server CASCADE;
--Testcase 765:
DROP SERVER spdsrv CASCADE;
--Testcase 766:
DROP EXTENSION dynamodb_fdw CASCADE;
--Testcase 767:
DROP EXTENSION pgspider_ext CASCADE;
DROP SCHEMA "S 1" CASCADE;
DROP TABLE ft1;
DROP TABLE ft2;
DROP TABLE ft3;
DROP TABLE ft4;
DROP TABLE ft5;
DROP TABLE ft6;
DROP TABLE ft7;
DROP TABLE loct3;
DROP TABLE ft_empty;
DROP TYPE user_enum;
DROP TABLE rem1;
DROP TABLE grem1;
DROP TABLE foo2;
DROP TABLE bar2;
DROP TABLE b;
DROP FUNCTION trigger_func CASCADE;
DROP FUNCTION trig_row_before_insupdate CASCADE;
DROP FUNCTION trig_null CASCADE;
DROP FUNCTION trigger_data CASCADE;
