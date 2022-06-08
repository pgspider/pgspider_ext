#!/bin/bash

mkdir -p /tmp/jdbc/
rm /tmp/jdbc/*.data
cp sql/init_data/jdbc_fdw/data/*.data /tmp/jdbc/

# this is the test case for jdbc_fdw to test on databases
# before running test, you have to:
# install DBs and download the JDBC DB drivers
# update the db connection info in sql/init_data/griddb_fdw/init.sh, sql/init_data/jdbc_fdw/jdbc_postgres_init.sh and sql/init_data/jdbc_fdw/jdbc_mysql_init.sh
# update the config file in sql/parameters/jdbc_*_parameters.conf
# copy griddb client into pgspider_ext folder.

sed -i 's/REGRESS =.*/REGRESS = ported_jdbc_fdw_griddb ported_jdbc_fdw_mysql ported_jdbc_fdw_postgres /' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/jdbc_fdw contrib\/mysql_fdw contrib\/griddb_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/jdbc_fdw contrib\/mysql_fdw contrib\/griddb_fdw /' Makefile
echo $PWD
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$PWD/sql/init_data/griddb_fdw/griddb/bin

cd sql/init_data/griddb_fdw
chmod +x ./*.sh || true
./init.sh
cd ../../..
./sql/init_data/jdbc_fdw/jdbc_postgres_init.sh --start
./sql/init_data/jdbc_fdw/jdbc_mysql_init.sh

make clean
make install
make check | tee make_check.out
