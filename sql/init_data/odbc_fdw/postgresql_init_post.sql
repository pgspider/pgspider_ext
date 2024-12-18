-- SET consistant time zones;
SET timezone = 'PST8PDT';
DROP TYPE IF EXISTS user_enum CASCADE;
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
DROP TABLE IF EXISTS "T1" CASCADE;
CREATE TABLE "T1" (
	"C_1" int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10),
	c8 user_enum,
	CONSTRAINT t1_pkey PRIMARY KEY ("C_1")
);

DROP TABLE IF EXISTS "T2" CASCADE;
CREATE TABLE "T2" (
	c1 int NOT NULL,
	c2 text,
	CONSTRAINT t2_pkey PRIMARY KEY (c1)
);
DROP TABLE IF EXISTS "T3" CASCADE;
CREATE TABLE "T3" (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	CONSTRAINT t3_pkey PRIMARY KEY (c1)
);
DROP TABLE IF EXISTS "T4" CASCADE;
CREATE TABLE "T4" (
	c1 int NOT NULL,
	c2 int NOT NULL,
	c3 text,
	CONSTRAINT t4_pkey PRIMARY KEY (c1)
);

DROP TABLE IF EXISTS loct_empty CASCADE;
CREATE TABLE loct_empty (c1 int NOT NULL, c2 text);

-- ===================================================================
-- test WITH CHECK OPTION constraints
-- ===================================================================
DROP TABLE IF EXISTS base_tbl CASCADE;
DROP FUNCTION IF EXISTS row_before_insupd_trigfunc CASCADE;
CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;
CREATE TABLE base_tbl (a int, b int);
ALTER TABLE base_tbl SET (autovacuum_enabled = 'false');
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON base_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

-- test WCO for partitions
DROP TABLE IF EXISTS child_tbl CASCADE;
CREATE TABLE child_tbl (a int, b int, id int);
ALTER TABLE child_tbl SET (autovacuum_enabled = 'false');
CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON child_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();

-- ===================================================================
-- test handling of collations
-- ===================================================================
DROP TABLE IF EXISTS loct3 CASCADE;
create table loct3 (f1 text collate "C" unique, f2 text, f3 varchar(10) unique);

-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- ===================================================================
DROP TABLE IF EXISTS loc1 CASCADE;
create table loc1 (f1 int, f2 text, id serial);
alter table loc1 set (autovacuum_enabled = 'false');

-- ===================================================================
-- test generated columns
-- ===================================================================
DROP TABLE IF EXISTS gloc1 CASCADE;
create table gloc1 (a int, b int);
alter table gloc1 set (autovacuum_enabled = 'false');

-- ===================================================================
-- test inheritance features
-- ===================================================================
DROP TABLE IF EXISTS loct CASCADE;
CREATE TABLE loct (aa TEXT, bb TEXT, id serial primary key);
ALTER TABLE loct SET (autovacuum_enabled = 'false');

-- Check SELECT FOR UPDATE/SHARE with an inherited source table
DROP TABLE IF EXISTS loct1 CASCADE;
DROP TABLE IF EXISTS loct2 CASCADE;
create table loct1 (f1 int, f2 int, f3 int);
create table loct2 (f1 int, f2 int, f3 int);

alter table loct1 set (autovacuum_enabled = 'false');
alter table loct2 set (autovacuum_enabled = 'false');

-- Test pushing down UPDATE/DELETE joins to the remote server
DROP TABLE IF EXISTS parent CASCADE;
DROP TABLE IF EXISTS loct1_2 CASCADE;
DROP TABLE IF EXISTS loct2_2 CASCADE;
create table parent (a int, b text, id int);
create table loct1_2 (a int, b text, id int);
create table loct2_2 (a int, b text, id int);

DROP TABLE IF EXISTS loct4 CASCADE;
create table loct4 (f1 int, f2 int, f3 int);

-- ===================================================================
-- test tuple routing for foreign-table partitions
-- ===================================================================
DROP TABLE IF EXISTS loct1_3 CASCADE;
DROP TABLE IF EXISTS loct2_3 CASCADE;
create table loct1_3 (a int check (a in (1)), b text, id int);
create table loct2_3 (a int check (a in (2)), b text, id int);

-- Test update tuple routing
DROP TABLE IF EXISTS loct_2 CASCADE;
create table loct_2 (a int check (a in (1)), b text, id int);

-- Test copy tuple routing
DROP TABLE IF EXISTS loct1_4 CASCADE;
DROP TABLE IF EXISTS loct2_4 CASCADE;
create table loct1_4 (a int check (a in (1)), b text, id int);
create table loct2_4 (a int check (a in (2)), b text, id int);

-- Test rescan
DROP TABLE IF EXISTS loct1_rescan CASCADE;
CREATE TABLE loct1_rescan (c1 int);
DROP TABLE IF EXISTS loct2_rescan CASCADE;
CREATE TABLE loct2_rescan (c1 int, c2 text);

-- ===================================================================
-- test COPY FROM
-- ===================================================================
DROP TABLE IF EXISTS loc2 CASCADE;
create table loc2 (f1 int, f2 text, id int);
alter table loc2 set (autovacuum_enabled = 'false');
DROP TABLE IF EXISTS loc3 CASCADE;
create table loc3 (f1 int, f2 text, id int);

-- ===================================================================
-- test IMPORT FOREIGN SCHEMA
-- ===================================================================
DROP SCHEMA IF EXISTS import_source CASCADE;
CREATE SCHEMA import_source;

CREATE TABLE import_source.t1 (c1 int, c2 varchar(10) NOT NULL);
CREATE TABLE import_source.t2 (c1 int default 42, c2 varchar(10) NULL, c3 text);
CREATE TABLE import_source.t3 (c1 timestamp, c2 varchar(10));
CREATE TABLE import_source.x_4 (c1 float8, C_2 text, c3 varchar(42));

-- Check case of a type present only on the remote server.
-- We can fake this by dropping the type locally in our transaction.
CREATE TYPE "Colors" AS ENUM ('red', 'green', 'blue');
CREATE TABLE import_source.t5 (c1 int, c2 text collate "C", "Col" "Colors");

-- ===================================================================
-- test partitionwise joins
-- ===================================================================
SET enable_partitionwise_join=on;
DROP TABLE IF EXISTS fprt1 CASCADE;
DROP TABLE IF EXISTS fprt1_p1 CASCADE;
DROP TABLE IF EXISTS fprt1_p2 CASCADE;
CREATE TABLE fprt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
CREATE TABLE fprt1_p1 (LIKE fprt1);
CREATE TABLE fprt1_p2 (LIKE fprt1);
ALTER TABLE fprt1_p1 SET (autovacuum_enabled = 'false');
ALTER TABLE fprt1_p2 SET (autovacuum_enabled = 'false');
INSERT INTO fprt1_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 2) i;
INSERT INTO fprt1_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 2) i;

DROP TABLE IF EXISTS fprt2 CASCADE;
DROP TABLE IF EXISTS fprt2_p1 CASCADE;
DROP TABLE IF EXISTS fprt2_p2 CASCADE;
CREATE TABLE fprt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
CREATE TABLE fprt2_p1 (LIKE fprt2);
CREATE TABLE fprt2_p2 (LIKE fprt2);
ALTER TABLE fprt2_p1 SET (autovacuum_enabled = 'false');
ALTER TABLE fprt2_p2 SET (autovacuum_enabled = 'false');
INSERT INTO fprt2_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 3) i;
INSERT INTO fprt2_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 3) i;

-- ===================================================================
-- test partitionwise aggregates
-- ===================================================================
DROP TABLE IF EXISTS pagg_tab CASCADE;
DROP TABLE IF EXISTS pagg_tab_p1 CASCADE;
DROP TABLE IF EXISTS pagg_tab_p2 CASCADE;
DROP TABLE IF EXISTS pagg_tab_p3 CASCADE;
CREATE TABLE pagg_tab (a int, b int, c text) PARTITION BY RANGE(a);

CREATE TABLE pagg_tab_p1 (LIKE pagg_tab);
CREATE TABLE pagg_tab_p2 (LIKE pagg_tab);
CREATE TABLE pagg_tab_p3 (LIKE pagg_tab);

INSERT INTO pagg_tab_p1 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 10;
INSERT INTO pagg_tab_p2 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 20 and (i % 30) >= 10;
INSERT INTO pagg_tab_p3 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 30 and (i % 30) >= 20;
-- Exit data preparation
\q
