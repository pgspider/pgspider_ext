TINYBRACE_HOME=/usr/local/tinybrace
POSTGRES_HOME=/home/jenkins/postgres/postgresql-13.5/PGS
CURR_PATH=$(pwd)

if [[ "--start" == $1 ]]
then
  # Start PostgreSQL
  cd ${POSTGRES_HOME}/bin/
  if ! [ -d "../databases" ];
  then
    ./initdb ../databases
    sed -i 's/#port = 5432.*/port = 15432/' ../databases/postgresql.conf
    ./pg_ctl -D ../databases start
    sleep 2
    ./createdb -p 15432 postgres
  fi
  if ! ./pg_isready -p 15432
  then
    echo "Start PostgreSQL"
    ./pg_ctl -D ../databases start
    sleep 2
  fi
  cd $CURR_PATH
  # Start MySQL
  if ! [[ $(systemctl status mysqld.service) == *"active (running)"* ]]
  then
    echo "Start MySQL Server"
    systemctl start mysqld.service
    sleep 2
  fi
  # Stop TinyBrace Server
  if pgrep -x "tbserver" > /dev/null
  then
    echo "Stop TinyBrace Server"
    pkill -9 tbserver
    sleep 2
  fi
  # Initialize data for TinyBrace Server
  $TINYBRACE_HOME/bin/tbeshell $TINYBRACE_HOME/databases/test.db < tiny.dat
  # Start TinyBrace Server
  echo "Start TinyBrace Server"
  cd $TINYBRACE_HOME
  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$TINYBRACE_HOME/lib
  bin/tbserver &
  sleep 3
else
  # Initialize data for TinyBrace Server
  $TINYBRACE_HOME/bin/tbcshell -id=user -pwd=testuser -server=127.0.0.1 -port=5100 -db=test.db < tiny.dat
fi

cd $CURR_PATH

cp pgtest.csv /tmp/

rm /tmp/pgtest.db
sqlite3 /tmp/pgtest.db < sqlite.dat

# SET PASSWORD = PASSWORD('mysql')
mysql -uroot -pMysql_1234 < mysql.dat
 
# postgres should be already started with port=15432
# pg_ctl -o "-p 15432" start -D data

$POSTGRES_HOME/bin/psql -p 15432 postgres -c "create user postgres with encrypted password 'postgres';"
$POSTGRES_HOME/bin/psql -p 15432 postgres -c "grant all privileges on database postgres to postgres;"
$POSTGRES_HOME/bin/psql -p 15432 postgres -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;"
$POSTGRES_HOME/bin/psql postgres -p 15432  -U postgres < post.dat
