#!/bin/bash

# Kill sqlumdash server process
killall -9 sqlumdash

# SQLumDashCS configuration
SLD_HOST="127.0.0.1"
SLD_PORT=12345
SLD_USER="user_sc"
SLD_PASSWORD="testuser_sc"
SLD_DB_NAME1="test_sc.db"
SLD_DB_NAME2="test_sc1.db"

CUR_PATH=$(pwd)
# Path to directory contain client lib, shellcs and server binary

# SQlumDash
#   +-- SQLumDash
#   |     +-- build: contain sqlite/sqlumdashcs lib
#   +-- SQLumDashCS
#         +-- sqlumdash
#         |       +-- bin: contain server binary
#         |       +-- lib: contain lib-client
#         +-- SQLumDashShell: contain shellcs
#

SQLUMDASH_PATH=$HOME/SQLumDash
SLDCS_PATH=$SQLUMDASH_PATH/SQLumDashCS
SLDCS_BIN_PATH=$SLDCS_PATH/sqlumdash/bin
SLDCS_LIB_PATH=$SLDCS_PATH/sqlumdash/lib
SLDCS_CLIENT_SHELL_PATH=$SLDCS_PATH/SQLumDashShell
# Path to SQLumDashCS using by server
SQL_LIB_PATH=$SQLUMDASH_PATH/SQLumDash/build/lib


# FDW configuration
TEST_FILE_NAME1="init_post.sql"
TEST_FILE_NAME2="init.sql"
TEST_FILE_PATH=$CUR_PATH/sql/init_data/sqlumdashcs_fdw

# Export path to refer lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SLDCS_LIB_PATH:$SQL_LIB_PATH

# Create SLD_DB_NAME database create
cd $SLDCS_BIN_PATH
./database_management drop $SLD_DB_NAME1
./database_management drop $SLD_DB_NAME2
./database_management create $SLD_DB_NAME1
./database_management create $SLD_DB_NAME2

# Create user for testing
./user_account_management delete $SLD_USER
./user_account_management add $SLD_USER $SLD_PASSWORD

# Start SQLumDash server (running in background)
./sqlumdash &
sleep 3
# Run client shell to prepare data
cd $TEST_FILE_PATH
cp $TEST_FILE_NAME1 $SLDCS_CLIENT_SHELL_PATH
cp $TEST_FILE_NAME2 $SLDCS_CLIENT_SHELL_PATH
cd $SLDCS_CLIENT_SHELL_PATH
./shellcs  -host $SLD_HOST -port $SLD_PORT -user $SLD_USER -pwd $SLD_PASSWORD -db $SLD_DB_NAME1 < $TEST_FILE_NAME1
./shellcs  -host $SLD_HOST -port $SLD_PORT -user $SLD_USER -pwd $SLD_PASSWORD -db $SLD_DB_NAME2 < $TEST_FILE_NAME2
###### Start to run test ############
sleep 5
cd $CUR_PATH

sed -i 's/REGRESS =.*/REGRESS = ported_sqlumdashcs_fdw/' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/sqlumdashcs_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/sqlumdashcs_fdw /' Makefile
make clean
make
LD_LIBRARY_PATH=$SLDCS_LIB_PATH:$SQL_LIB_PATH
rm -rf make_check.out
make check | tee make_check.out
sleep 2
# Cleanup
killall -9 sqlumdash
cd $SLDCS_BIN_PATH
./database_management drop $SLD_DB_NAME1
./database_management drop $SLD_DB_NAME2
./user_account_management delete $SLD_USER
