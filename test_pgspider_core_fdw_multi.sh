#!/bin/sh

sed -i 's/REGRESS =.*/REGRESS = ported_pgspider_core_fdw_multi /' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/sqlite_fdw contrib\/file_fdw contrib\/tinybrace_fdw contrib\/mysql_fdw contrib\/griddb_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/sqlite_fdw contrib\/file_fdw contrib\/tinybrace_fdw contrib\/mysql_fdw contrib\/griddb_fdw /' Makefile

# run setup script
cd sql/init_data/pgspider_core_fdw
./pgspider_core_fdw_init_multi.sh --start
cd ../../..
make clean
make
make check | tee make_check.out
