#!/bin/bash

# kill tbserver
pkill tbserver

TB_PATH=/usr/local/tinybrace
DB_PATH=${TB_PATH}/databases
LOG_PATH=${TB_PATH}/log
LIB_PATH=${TB_PATH}/lib
BIN_PATH=${TB_PATH}/bin

# USER=$(whoami)
# sudo -S chown -R ${USER}:${USER} ${DB_PATH}
# sudo chown -R ${USER}:${USER} ${LOG_PATH}
# sudo ln -s /usr/local/tinybrace/lib/libtbclient.so /usr/local/lib/libtbclient.so

rm -rf ${DB_PATH}/test
rm -rf ${DB_PATH}/tinybracefdw_test_post
rm -rf ${DB_PATH}/tinybracefdw_test_core

rm -rf /tmp/*.data

find ./sql/ -name "*.data" -exec cp {} /tmp/ \;

LD_LIBRARY_PATH=${LIB_PATH} ${BIN_PATH}/tbeshell ${DB_PATH}/test.db < sql/init_data/tinybrace_fdw/init.sql

LD_LIBRARY_PATH=${LIB_PATH} ${BIN_PATH}/tbeshell ${DB_PATH}/tinybracefdw_test_post.db < sql/init_data/tinybrace_fdw/init_post.sql

LD_LIBRARY_PATH=${LIB_PATH} ${BIN_PATH}/tbeshell ${DB_PATH}/tinybracefdw_test_core.db < sql/init_data/tinybrace_fdw/init_core.sql

# run tbserver
CURR_PATH=$(pwd)
cd /usr/local/tinybrace
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/tinybrace/lib
bin/tbserver &
cd $CURR_PATH
pwd

sleep 2
sed -i 's/REGRESS =.*/REGRESS = ported_tinybrace_fdw /' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/pgspider_ext contrib\/tinybrace_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/pgspider_ext contrib\/tinybrace_fdw /' Makefile
make clean
make
LD_LIBRARY_PATH=${LIB_PATH}
rm -rf make_check.out
make check | tee make_check.out

pkill tbserver

rm -rf /tmp/*.data

# sudo rm /usr/local/lib/libtbclient.so