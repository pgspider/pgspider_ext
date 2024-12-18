#!/bin/sh

sed -i 's/REGRESS =.*/REGRESS = ported_pgspider_core_fdw_postgres_fdw /' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/dblink  /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/dblink /' Makefile
# run setup script
cd sql/init_data/pgspider_core_fdw
./ported_postgres_setup.sh --start
cd ../../..
make clean
make
# mkdir -p results/selectfunc
make check | tee make_check.out
