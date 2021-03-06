.mode csv

DROP TABLE IF EXISTS FLOAT4_TBL;
DROP TABLE IF EXISTS FLOAT4_TMP;
DROP TABLE IF EXISTS FLOAT8_TBL;
DROP TABLE IF EXISTS FLOAT8_TMP;
DROP TABLE IF EXISTS INT4_TBL;
DROP TABLE IF EXISTS INT4_TMP;
DROP TABLE IF EXISTS INT4_TMP2;
DROP TABLE IF EXISTS INT8_TBL;
DROP TABLE IF EXISTS INT8_TMP;
DROP TABLE IF EXISTS test_having;
DROP TABLE IF EXISTS onek;
DROP TABLE IF EXISTS tenk1;
DROP TABLE IF EXISTS width_bucket_tbl;

CREATE TABLE FLOAT4_TBL (f1 REAL);
CREATE TABLE FLOAT4_TMP (f1 REAL);
create table test_data (id INTEGER PRIMARY KEY AUTOINCREMENT, bits text);
CREATE TABLE FLOAT8_TBL(f1 DOUBLE PRECISION);
CREATE TABLE FLOAT8_TMP(f1 DOUBLE PRECISION);
CREATE TABLE INT4_TBL(f1 int4);
CREATE TABLE INT4_TMP(f1 int4);
CREATE TABLE INT4_TMP2(a int4, b int4);
CREATE TABLE INT8_TBL(
	q1 int8,
	q2 int8,
	CONSTRAINT t1_pkey PRIMARY KEY (q1, q2)
);
CREATE TABLE INT8_TMP(
	q1 int8,
	q2 int8,
	CONSTRAINT t1_pkey PRIMARY KEY (q1, q2)
);

CREATE TABLE INT2_TBL(f1 int2);
--Testcase 1:
INSERT INTO INT2_TBL(f1) VALUES ('0   ');
--Testcase 2:
INSERT INTO INT2_TBL(f1) VALUES ('  1234 ');
--Testcase 3:
INSERT INTO INT2_TBL(f1) VALUES ('    -1234');
-- largest and smallest values
--Testcase 4:
INSERT INTO INT2_TBL(f1) VALUES ('32767');
--Testcase 5:
INSERT INTO INT2_TBL(f1) VALUES ('-32767');

CREATE TABLE test_having (a int, b int, c char(8), d char);
CREATE TABLE onek (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE onek2 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE tenk2 (
	unique1 	int4,
	unique2 	int4,
	two 	 	int4,
	four 		int4,
	ten			int4,
	twenty 		int4,
	hundred 	int4,
	thousand 	int4,
	twothousand int4,
	fivethous 	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE aggtest (
	a 			int2,
	b			float4
);

CREATE TABLE aggtest_tmp (
	a 			int2,
	b			float4
);

CREATE TABLE regr_test (x float8, y float8);


CREATE TABLE student (
	name 		text,
	age			int4,
	location 	point,
	gpa 		float8
);

CREATE TABLE person (
	name 		text,
	age			int4,
	location 	point
);

CREATE TABLE road (
	name		text,
	thepath 	path
);

create table slt_foo (a text, b int, c int);
create table slt_v (k int);

.separator "\t"
.import /tmp/onek.data onek
.import /tmp/onek.data onek2
.import /tmp/tenk.data tenk1
.import /tmp/agg.data aggtest
.import /tmp/student.data student
.import /tmp/person.data person
.import /tmp/streets.data road

--Testcase 6:
INSERT INTO tenk2 SELECT * FROM tenk1;

CREATE TABLE bitwise_test(
  i2 INT2,
  i4 INT4,
  i8 INT8,
  i INTEGER,
  x INT2,
  y BIT(4)
);

CREATE TABLE bool_test(
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL);

CREATE TABLE bool_test_tmp(
  b1 BOOL,
  b2 BOOL, primary key (b1, b2));

create table minmaxtest(f1 int);

create table agg_t1 (a int, b int, c int, d int, primary key (a, b));
create table agg_t2 (x int, y int, z int, primary key (x, y));
--create table agg_t3 (a int, b int, c int, primary key(a, b) DEFERRABLE);
--create table t1c () inherits (agg_t1);

-- TinyBrace does not support create table ... partition by..
--create table p_t1 (
--  a int,
--  b int,
--  c int,
--  d int,
--  primary key(a,b)
--) partition by list(a);
--create table p_t1_1 partition of p_t1 for values in(1);
--create table p_t1_2 partition of p_t1 for values in(2);

-- multi-arg aggs
create table multi_arg_agg (a int, b int, c text);

create table agg_t0(foo text, bar text);

create table agg_t3(inner_c int);
create table agg_t4(outer_c int);

create table agg_t5(x int);
create table agg_t6(x text);

create table agg_t7(one int);
create table agg_t8(one int, two int);
create table agg_t9(a int4);

create table agg_data_2k(g int);
create table agg_data_20k(g int);
create table agg_group_1(c1 int, c2 numeric, c3 int);
create table agg_group_2(a int, c1 numeric, c2 text, c3 int);
create table agg_group_3(c1 numeric, c2 int4, c3 int);
create table agg_group_4(c1 numeric, c2 text, c3 int);
create table agg_hash_1(c1 int, c2 numeric, c3 int);
create table agg_hash_2(a int, c1 numeric, c2 text, c3 int);
create table agg_hash_3(c1 numeric, c2 int4, c3 int);
create table agg_hash_4(c1 numeric, c2 text, c3 int);

create table agg_t10 (a float8, b float8);
create table agg_t11 (a float4, b float4);
create table agg_t12 (a numeric, b numeric);

create table agg_t13 (f1 int4, f2 int8);
create table agg_t14 (f1 int8, f22 int8);
CREATE TABLE agg_t15 (a text, b int, c int, d text, id int);
CREATE TABLE VARCHAR_TBL(f1 varchar(4));

--Testcase 7:
INSERT INTO VARCHAR_TBL (f1) VALUES ('a');
--Testcase 8:
INSERT INTO VARCHAR_TBL (f1) VALUES ('ab');
--Testcase 9:
INSERT INTO VARCHAR_TBL (f1) VALUES ('abcd');

create table bytea_test_table(v bytea);


CREATE TABLE num_data (id int4, val numeric(210,10), primary key (id));
CREATE TABLE num_exp_add (id1 int4, id2 int4, expected numeric(210,10), primary key (id1, id2));
CREATE TABLE num_exp_sub (id1 int4, id2 int4, expected numeric(210,10), primary key (id1, id2));
CREATE TABLE num_exp_div (id1 int4, id2 int4, expected numeric(210,10), primary key (id1, id2));
CREATE TABLE num_exp_mul (id1 int4, id2 int4, expected numeric(210,10), primary key (id1, id2));
CREATE TABLE num_exp_sqrt (id int4, expected numeric(210,10), primary key (id));
CREATE TABLE num_exp_ln (id int4, expected numeric(210,10), primary key (id));
CREATE TABLE num_exp_log10 (id int4, expected numeric(210,10), primary key (id));
CREATE TABLE num_exp_power_10_ln (id int4, expected numeric(210,10), primary key (id));

CREATE TABLE num_result (id1 int4, id2 int4, result numeric(210,10), primary key (id1, id2));
CREATE TABLE v (id int4, x numeric, val float8, primary key (id));
INSERT INTO v(x) VALUES ('1e340'), ('-1e340');
CREATE TABLE fract_only (id int, val numeric(4,4));
CREATE TABLE ceil_floor_round (a numeric primary key);
CREATE TABLE width_bucket_test (operand_num numeric, operand_f8 float8);
CREATE TABLE num_input_test (n1 numeric);

CREATE TABLE foo (f1 int);

CREATE TABLE J1_TBL (
  i integer,
  j integer,
  t text
);

CREATE TABLE J2_TBL (
  i integer,
  k integer
);

CREATE TABLE t11 (name TEXT, n INTEGER);
CREATE TABLE t21 (name TEXT, n INTEGER);
CREATE TABLE t31 (name TEXT, n INTEGER);
create table x (x1 int, x2 int);
create table y (y1 int, y2 int);

CREATE TABLE t12 (a int, b int);
CREATE TABLE t22 (a int, b int);
CREATE TABLE t32 (x int, y int);

CREATE TABLE tt1 ( tt1_id int4, joincol int4 );
CREATE TABLE tt2 ( tt2_id int4, joincol int4 );
create table tt3(f1 int, f2 text);
create table tt4(f1 int);
create table tt4x(c1 int, c2 int, c3 int);
create table tt5(f1 int, f2 int);
create table tt6(f1 int, f2 int);
create table xx (pkxx int);
create table yy (pkyy int, pkxx int);
create table zt1 (f1 int primary key);
create table zt2 (f2 int primary key);
create table zt3 (f3 int primary key);

create table a1 (i integer);
create table b1 (x integer, y integer);

--TinyBrace does not support create type ... as
--create type mycomptype as (id int, v int);
--create table tidv (idv mycomptype);
--create index on tidv (idv);

create table a2 (
     code char not null,
     primary key (code)
);
create table b2 (
     a char not null,
     num integer not null,
     primary key (a, num)
);
create table c2 (
     name char not null,
     a char,
     primary key (name)
);

create table nt1 (
  id int primary key,
  a1 boolean,
  a2 boolean
);
create table nt2 (
  id int primary key,
  nt1_id int,
  b1 boolean,
  b2 boolean,
  foreign key (nt1_id) references nt1(id)
);
create table nt3 (
  id int primary key,
  nt2_id int,
  c1 boolean,
  foreign key (nt2_id) references nt2(id)
);

CREATE TABLE TEXT_TBL (f1 text);

--Testcase 10:
INSERT INTO TEXT_TBL VALUES ('doh!');
--Testcase 11:
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');

CREATE TABLE a3 (id int PRIMARY KEY, b_id int);
CREATE TABLE b3 (id int PRIMARY KEY, c_id int);
CREATE TABLE c3 (id int PRIMARY KEY);
CREATE TABLE d3 (a int, b int);

create table parent (k int primary key, pd int);
create table child (k int unique, cd int);

CREATE TABLE a4 (id int PRIMARY KEY);
CREATE TABLE b4 (id int PRIMARY KEY, a_id int);

create table innertab (id int8 primary key, dat1 int8);
create table uniquetbl (f1 text unique);

-- Not support create table partition
--create table join_pt1 (a int, b int, c varchar) partition by range(a);
--create table join_pt1p1 partition of join_pt1 for values from (0) to (100) partition by range(b);
--create table join_pt1p2 partition of join_pt1 for values from (100) to (200);
--create table join_pt1p1p1 partition of join_pt1p1 for values from (0) to (100);
--create table join_ut1 (a int, b int, c varchar);

create table fkest (a int, b int, c int unique, primary key(a,b));
create table fkest1 (a int, b int, primary key(a,b) foreign key (a,b) references fkest);

create table j11 (id int primary key);
create table j21 (id int primary key);
create table j31 (id int);

create table j12 (id1 int, id2 int, primary key(id1,id2));
create table j22 (id1 int, id2 int, primary key(id1,id2));
create table j32 (id1 int, id2 int, primary key(id1,id2));

create table inserttest01 (col1 int4, col2 int4 NOT NULL, col3 text default 'testing');


CREATE TABLE update_test (
    i   INTEGER PRIMARY KEY AUTOINCREMENT,
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
);

CREATE TABLE upsert_test (
    a   INT PRIMARY KEY,
    b   TEXT
);

CREATE TABLE width_bucket_tbl (
	id1 float8,
	id2 float8,
	id3 float8,
	id4 int
);

.q
