#!/bin/bash

MYSQL_ROOT_PASS="Mysql_1234"

mysql -u root -p$MYSQL_ROOT_PASS -e "RESTART"

# Wait until restarting mysql service complete
mysql_ready() {
    mysqladmin ping > /dev/null 2>&1
}

counter=0;
maxwait=60
while !(mysql_ready)
do
    let "counter++"
    sleep 1
    if [[ $counter -ge $maxwait ]]
    then
        echo "Fail to wait for mysql server to restart"
        break
    fi
done

mysql -u root -p$MYSQL_ROOT_PASS -e "SET GLOBAL time_zone = '-8:00';"
mysql -u root -p$MYSQL_ROOT_PASS -e "SET GLOBAL log_bin_trust_function_creators = 1;"
mysql -u root -p$MYSQL_ROOT_PASS -e "SET GLOBAL local_infile=1;"
