#!/bin/bash
echo "init data ..."
./sql/init_data/mongodb_fdw/mongodb_init.sh

sed -i 's/REGRESS =.*/REGRESS = ported_mongodb_fdw/' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/mongo_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/mongo_fdw /' Makefile

make clean
make
make check | tee make_check.out
