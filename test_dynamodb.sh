#!/bin/sh

rm -rf init.log || true
cd sql/init_data/dynamodb_fdw
./dynamodb_init.sh > init.log
cd ../../..
sed -i 's/REGRESS =.*/REGRESS = ported_dynamodb_fdw /' Makefile
sed -i 's/temp-install\:.*/temp-install\: EXTRA_INSTALL=contrib\/pgspider_ext contrib\/dynamodb_fdw /' Makefile
sed -i 's/checkprep\:.*/checkprep\: EXTRA_INSTALL=contrib\/pgspider_ext contrib\/dynamodb_fdw /' Makefile

make clean
make
make check | tee make_check.out
