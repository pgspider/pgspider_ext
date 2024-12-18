#!/bin/sh


# ===================================================================
# Initializing data for Mysql Datasource
# ===================================================================

export MYSQL_PWD="Edb_1234"
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_USER_NAME="edb"

# Below commands must be run first time to create mysql_fdw_post database
# used in regression tests with edb user and Edb_1234 password.

# --connect to mysql with root user
# mysql -u root -p

# --run below
# CREATE DATABASE mysql_fdw_post;
# SET GLOBAL validate_password.policy = LOW;
# SET GLOBAL validate_password.length = 1;
# SET GLOBAL validate_password.mixed_case_count = 0;
# SET GLOBAL validate_password.number_count = 0;
# SET GLOBAL validate_password.special_char_count = 0;
# CREATE USER 'edb'@'localhost' IDENTIFIED BY 'Edb_1234';
# GRANT ALL PRIVILEGES ON mysql_fdw_post.* TO 'edb'@'localhost';
# GRANT SUPER ON *.* TO 'edb'@localhost;

# Set time zone to default time zone of make check PST.
# SET GLOBAL time_zone = '+00:00';
# SET GLOBAL log_bin_trust_function_creators = 1;
# SET GLOBAL local_infile=1;

mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS \`T 0\`;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS \`T 1\`;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS test;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS \`T 2\`;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS \`T 3\`;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS \`T 4\`;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS t1_constraint;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS base_tbl;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS position_data1;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS position_data2;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS table_data;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct_empty;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loc1;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct1;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct2;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct3;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct4;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct5;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct6;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct7;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct8;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct10;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct11;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct12;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct13;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loc2;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loc3;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loc4;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS gloc1;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS gloc1_post14;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS a;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct9;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS child_tbl;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct31;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct41;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct42;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS batch_table;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS tru_rtable;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS tru_rtable2;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS child_local;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS tab_batch_sharded_p1_remote;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS analyze_table;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS ploc1;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS ploc2;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS batch_table_2;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct1_rescan;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_post -e "DROP TABLE IF EXISTS loct2_rescan;"

mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE \`T 0\` (\`C 1\` int PRIMARY KEY, c2 int NOT NULL, c3 text, c4 timestamp, c5 datetime, c6 varchar(10), c7 char(10), c8 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE \`T 1\` (\`C 1\` int PRIMARY KEY, c2 int NOT NULL, c3 text, c4 timestamp, c5 datetime, c6 varchar(10), c7 char(10), c8 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE test (c1 int PRIMARY KEY, c2 int NOT NULL, c3 text, c4 timestamp, c5 timestamp, c6 varchar(10), c7 char(10), c8 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE \`T 2\` (c1 int, c2 text, CONSTRAINT t2_pkey PRIMARY KEY (c1));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE \`T 3\` (c1 int, c2 int NOT NULL, c3 text, CONSTRAINT t3_pkey PRIMARY KEY (c1));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE \`T 4\` (c1 int, c2 int NOT NULL, c3 text, CONSTRAINT t4_pkey PRIMARY KEY (c1));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE base_tbl (id int primary key auto_increment, a int, b int);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct_empty (c1 int PRIMARY KEY NOT NULL, c2 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loc1 (f1 INTEGER, f2 text, id integer primary key auto_increment);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct (id integer primary key auto_increment,aa TEXT, bb TEXT);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct1 (id integer primary key auto_increment, f1 int, f2 int, f3 int);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct2 (id integer primary key auto_increment, f1 int, f2 int, f3 int);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct3 (a int, b text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct4 (a int, b text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct5 (id int primary key auto_increment, a int check (a in (1)), b text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct6 (id int primary key auto_increment, a int check (a in (2)), b text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct7 (a int check (a in (1)), b text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct8 (f1 text, f2 text, f3 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct10 (id int primary key auto_increment, a int check (a in (1)), b text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct11 (id int primary key auto_increment, a int check (a in (3)), b text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct12 (id int primary key auto_increment, a int check (a in (1)), b text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct13 (id int primary key auto_increment, a int check (a in (2)), b text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loc2 (id int primary key auto_increment, f1 int, f2 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loc3 (id int primary key auto_increment, f1 int, f2 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loc4 (id int primary key auto_increment, f1 int, f2 text, CONSTRAINT loc4_f1positive CHECK ((f1 >= 0)));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE gloc1 (id int primary key auto_increment, a int, b int);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE gloc1_post14 (id int primary key auto_increment, a int, b int generated always as (\`a\` * 2) stored);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE a (aa TEXT);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct9 (aa TEXT, bb TEXT);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE child_tbl (id integer primary key auto_increment, a integer, b integer);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct31 (f1 text, f2 text, f3 varchar(10));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct41 (f1 int, f2 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct42 (f1 int, f2 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE t1_constraint (c1 int primary key, c2 int NOT NULL check (c2 >= 0), c3 text, c4 timestamp, c5 timestamp, c6 varchar(10), c7 char(10), c8 text check (c8 IN ('foo','bar', 'buz')));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE position_data1 (c1 INT primary key, c2 INT, c3 CHAR(9), c4 timestamp, c5 timestamp, c6 DECIMAL(10,5), c7 INT, c8 SMALLINT);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE position_data2 (c1 INT primary key, c2 INT, c3 CHAR(9), c4 timestamp, c5 timestamp, c6 DECIMAL(10,5), c7 INT, c8 SMALLINT);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE table_data (i int, b bool);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "INSERT INTO table_data VALUE (1, true);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "INSERT INTO table_data VALUE (2, false);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "INSERT INTO table_data VALUE (null, true);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "INSERT INTO table_data VALUE (null, false);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "INSERT INTO table_data VALUE (3, null);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE batch_table ( x int PRIMARY KEY);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE tru_rtable (id int PRIMARY KEY);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE tru_rtable2 (id int PRIMARY KEY);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE child_local (b text, c numeric, a int PRIMARY KEY);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE tab_batch_sharded_p1_remote (id int PRIMARY KEY, data text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE analyze_table (id int PRIMARY KEY, a text, b bigint);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE ploc1 (f1 int PRIMARY KEY, f2 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE ploc2 (f1 int PRIMARY KEY, f2 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -D $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE batch_table_2 (id int PRIMARY KEY auto_increment, a text, b int);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct1_rescan (c1 int PRIMARY KEY);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_post -e "CREATE TABLE loct2_rescan (c1 int PRIMARY KEY, c2 text);"


# ===================================================================
# Testing for Mysql Datasource
# ===================================================================

MYSQL_ROOT_PASS="Mysql_1234"

mysql -uroot -p$MYSQL_ROOT_PASS -e "SET GLOBAL time_zone = '-8:00';"
mysql -uroot -p$MYSQL_ROOT_PASS -e "SET GLOBAL log_bin_trust_function_creators = 1;"
mysql -uroot -p$MYSQL_ROOT_PASS -e "SET GLOBAL local_infile=1;"

sed -i 's/REGRESS =.*/REGRESS = ported_mysql_fdw/' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/mysql_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/mysql_fdw /' Makefile

make clean
make
make check | tee make_check.out
