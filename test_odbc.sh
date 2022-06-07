#!/bin/sh

MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_USER_NAME="root"
MYSQL_DB_NAME="odbc_fdw_regress"
export PDB_PORT="5444"
export PDB_NAME="odbc_fdw_post"
export MYSQL_PWD="Mysql_1234"
export PGS_SRC_DIR="/home/jenkins/release_FDW/postgresql-13.5"
export PGS_BIN_DIR="/home/jenkins/release_FDW/postgresql-13.5/PGS"
export ODBC_FDW_DIR="/home/jenkins/release_FDW/postgresql-13.5/contrib/odbc_fdw"

CURR_PATH=$(pwd)

if [ "$#" -ne 1 ]; then
    echo "Usage: test.sh --[post | mysql | all]"
    exit
fi

# Postgresql
if [[ ("--post" == $1 ) || ("--all" == $1) ]]
then
    # Init data for PostgreSQL server
    cd $PGS_BIN_DIR/bin

    if ! [ -d "../test_odbc_database" ];
    then
        ./initdb ../test_odbc_database
        sed -i "s~#port = 5432.*~port = $PDB_PORT~g" ../test_odbc_database/postgresql.conf
        ./pg_ctl -D ../test_odbc_database -l /dev/null start
        sleep 2
        ./createdb -p $PDB_PORT $PDB_NAME
    fi
    if ! ./pg_isready -p $PDB_PORT
    then
        echo "Start PostgreSQL"
        ./pg_ctl -D ../test_odbc_database -l /dev/null start
        sleep 2
    fi

    cd $CURR_PATH
    $PGS_BIN_DIR/bin/psql -q -A -t -d $PDB_NAME -p $PDB_PORT -f $CURR_PATH/sql/init_data/odbc_fdw/postgresql_init_post.sql

    sed -i 's/REGRESS =.*/REGRESS = ported_odbc_fdw_postgres /' Makefile
fi

# Mysql
if [[ ("--mysql" == $1 ) || ("--all" == $1) ]]
then
    # Init data for MYSQL server 
    mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D $MYSQL_DB_NAME --local-infile=1 < $CURR_PATH/sql/init_data/odbc_fdw/mysql_init_post.sql

    sed -i 's/REGRESS =.*/REGRESS = ported_odbc_fdw_mysql/' Makefile
fi

if [[ "--all" == $1 ]]
then
    sed -i 's/REGRESS =.*/REGRESS = ported_odbc_fdw_mysql ported_odbc_fdw_postgres /' Makefile
fi

sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/odbc_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/odbc_fdw /' Makefile

make clean
make
make check | tee make_check.out