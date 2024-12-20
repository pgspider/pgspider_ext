# PGSpider Extension
PGSpider Extension(pgspider_ext) is an extension to construct High-Performance SQL Cluster Engine for distributed big data.
pgspider_ext enables PostgreSQL to access a number of data sources using Foreign Data Wrapper(FDW) and retrieves the distributed data source vertically.  
Code of pgspider_ext is one of extension of PostgreSQL. We call PostgreSQL installed pgspider_ext as **"PGSpider"**.   
Usage of PostgreSQL installed pgspider_ext is the same as PostgreSQL. You can use any client applications such as libpq and psql.

## Features
* Node partitioned table  
    User can get records in multi tables on some data sources by one SQL easily.
	If there are tables with similar schema in each data source, PGSpider can view them as a single virtual table.  
    PGSpider run under Declarative Partitioning feature. Even if tables on data sources does not have a partition key, PGSpider creates a partition key automatically based on child node.  
	For example, tables on data sources have 2 column(i and t).
	Data on node1:
	<pre>
	SELECT * FROM t1_node1;
	  i | t
	----+---
	 10 | a
	 11 | b
	(2 rows)
	</pre>
	Data on node2:
	<pre>
	SELECT * FROM t1_node2;
	  i | t
	----+---
	 20 | c
	 21 | d
	(2 rows)
	</pre>
	If you create a partition table t1 using pgspider_ext from t1_node1 and t1_node2, the partitioned table has 3 columns (i, t and node). x is a column of partition key. You can distinguish data sources by column 'node'.  
	Query on PGSpider:
	<pre>
	  i | t | node
	----+---+-------
	 10 | a | node1
	 11 | b | node1
	 20 | c | node2
	 21 | d | node2
	(4 rows)
	</pre>	

<center><img src="./images/structure.png" width=70%></center>

* Parallel processing  
    When PGSpider executes query, PGSpider expands partitioned table to child tables and fetches results from child nodes in parallel. 

* Pushdown   
    WHERE clause and aggregation functions can be pushed down to child nodes.
    The shippability depends on child FDW.

## How to install pgspider_ext

The current version can work with PostgreSQL 13.15, 15.7, 16.3 and 17.0.

Download PostgreSQL source code.
<pre>
https://ftp.postgresql.org/pub/source/v17.0/postgresql-17.0.tar.gz
</pre>

Decompress PostgreSQL source code. 
<pre>
tar xvf postgresql-17.0.tar.gz
</pre>

Download pgspider_ext source code into "contrib/pgspider_ext" directory.
<pre>
git clone XXX 
</pre>

Build and install PostgreSQL and pgspider_ext.
<pre>
cd postgresql-17.0
./configure
make
sudo make install
cd contrib/pgspider_ext
make 
sudo make install
</pre>

## Usage
For example, there are 3 nodes (1 parent node and 2 child nodes).
PGSpider runs on the parent node. And 2 child nodes are data sources.
Each child node has PostgreSQL. They are accessed by PGSpider.  

Please install PostgreSQL on child nodes and install PostgreSQL FDW into PGSpider. 

Install PostgreSQL FDW 
<pre>
cd ../postgres_fdw
make 
sudo make install
</pre>

### Start PGSpider
You can start PGSpider as same as PostgreSQL. 
<pre>
/usr/local/pgsql
</pre>

Create database cluster and start server.
<pre>
cd /usr/local/pgsql/bin
./initdb -D ~/pgspider_db
./pg_ctl -D ~/pgspider_db start
</pre>

Connect to PGSpider.
<pre>
./psql postgres
</pre>

### Load extension
PGSpider(Parent node)
<pre>
CREATE EXTENSION pgspider_ext;
</pre>

PostgreSQL FDW
<pre>
CREATE EXTENSION postgres_fdw;
</pre>

### Create server
Create PGSpider server.
<pre>
CREATE SERVER spdsrv FOREIGN DATA WRAPPER pgspider_ext;
</pre>

Create servers of child PostgreSQL nodes  
<pre>
CREATE SERVER pgsrv1 FOREIGN DATA WRAPPER postgres_fdw OPTIONS(host '127.0.0.1', port '5433', dbname 'postgres');
CREATE SERVER pgsrv2 FOREIGN DATA WRAPPER postgres_fdw OPTIONS(host '127.0.0.1', port '5434', dbname 'postgres');
</pre>

### Create user mapping
Create user mapping for PGSpider server.
No need to specify options.
<pre>
CREATE USER MAPPING FOR CURRENT_USER SERVER spdsrv;
</pre>

User mapping for PostgreSQL servers.
<pre>
CREATE USER MAPPING FOR CURRENT_USER SERVER pgsrv1 OPTIONS(user 'user', password 'pass');
CREATE USER MAPPING FOR CURRENT_USER SERVER pgsrv2 OPTIONS(user 'user', password 'pass');
</pre>

### Create foreign tables
Create foreign tables of child nodes according to data source FDW usage.
In this example, each PostgreSQL server has table 't1' which has 2 columns ('i' and 't').
You can also create them by using IMPORT FOREIGN SCHEMA if data source FDW supports it.
<pre>
CREATE FOREIGN TABLE t1_pg1_child (i int, t text) SERVER pgsrv1 OPTIONS (table_name 't1');
CREATE FOREIGN TABLE t1_pg2_child (i int, t text) SERVER pgsrv2 OPTIONS (table_name 't1');
</pre>

### Create partition table
Create a partition parent table and partition child tables.
Partition child tables are corresponding to each foreign table created at the previous step.  
You need to declare a partition key column **at the last** in addition to columns of data source table.  

A partition parent table:  
In this example, we define 'node' column as a partition key column.
<pre>
CREATE TABLE t1(i int, t integer, node text) PARTITION BY LIST (node);
</pre>

Partition child tables:
<pre>
CREATE FOREIGN TABLE t1_pg1 PARTITION OF t1 FOR VALUES IN ('node1') SERVER spdsrv;
CREATE FOREIGN TABLE t1_pg2 PARTITION OF t1 FOR VALUES IN ('node2') SERVER spdsrv OPTIONS (child_name 't1_child2');
</pre>

't1_a' is corresponding to the foreign table 't1_pg1'.
PGSpider searches the corresponding foreign table by name having "[table name]_child" by default.
You can specify the name by the 'child_name' option.

### Access a partition table
<pre>
SELECT * FROM t1;
  i | t | node
----+---+-------
 10 | a | node1
 11 | b | node1
 20 | c | node2
 21 | d | node2
(4 rows)
</pre>

## Note
If you want to pushdown aggregate functions, you needs to execute:
<pre>
SET enable_partitionwise_aggregate TO on;
</pre>

We have confirmed that PGSpider can connect to:
- PostgreSQL ([postgres_fdw](https://github.com/postgres/postgres))
- MySQL ([mysql_fdw](https://github.com/pgspider/mysql_fdw))
- DynamoDB ([dynamodb_fdw](https://github.com/pgspider/dynamodb_fdw))
- GridDB ([griddb_fdw](https://github.com/pgspider/griddb_fdw))
- JDBC ([jdbc_fdw](https://github.com/pgspider/jdbc_fdw))
- ODBC ([odbc_fdw](https://github.com/pgspider/odbc_fdw))
- Parquet S3 ([parquet_s3_fdw](https://github.com/pgspider/parquet_s3_fdw))
- SQLite ([sqlite_fdw](https://github.com/pgspider/sqlite_fdw))
- MongoDB ([mongo_fdw](https://github.com/pgspider/mongo_fdw))
- InfluxDB ([influxdb_fdw](https://github.com/pgspider/influxdb_fdw))

Currently, PostgreSQL has a bug which will cause duplicate data when parallel query is executed.
It happens when leader node and worker nodes scan the same data.
We can avoid this bug by disabling leader node from collecting data by executing:
<pre>
SET parallel_leader_participation TO off;
</pre>

## Test execution

PGSpider Extension has several test scripts to execute test for each data source.
| No | Test script file name | Corresponding test files | Remark |
| ----------- | ----------- | ----------- | ----------- |
| 1	| test_dynamodb.sh | ported_dynamodb_fdw.sql | Test for dynamodb_fdw |
| 2	| test_griddb.sh | ported_griddb_fdw.sql | Test for griddb_fdw |
| 3	| test_jdbc.sh | ported_jdbc_fdw_griddb.sql<br>ported_jdbc_fdw_mysql.sql<br>ported_jdbc_fdw_postgres.sql| Test for jdbc_fdw |
| 4	| test_mongodb.sh | ported_mongodb_fdw.sql | Test for mongo_fdw |
| 5	| test_mysql.sh | ported_mysql_fdw.sql | Test for mysql_fdw |
| 6	| test_odbc.sh | ported_odbc_fdw_mysql.sql<br>ported_odbc_fdw_postgres.sql| Test for odbc_fdw |
| 7	| test_parquet_s3_fdw.sh | ported_parquet_s3_fdw_local.sql<br>ported_parquet_s3_fdw_server.sql | Test for parquet_s3_fdw |
| 8	| test_pgspider_core_fdw_parquet_s3.sh | ported_pgspider_core_fdw_import_s3.sql<br>ported_pgspider_core_fdw_parquet_s3_fdw.sql<br>ported_pgspider_core_fdw_parquet_s3_fdw2.sql | Test for pgspider_ext with parquet_s3_fdw, ported from pgspider_core_fdw |
| 9 | test_pgspider_core_fdw_postgres_fdw.sh | ported_pgspider_core_fdw_postgres_fdw.sql | Test for pgspider_ext with postgres_fdw, ported from pgspider_core_fdw |
| 10 | test_postgres.sh | pgspider_ext.sql<br>pgspider_ext2<br>pgspider_ext3 | Test for postgres_fdw |
| 11 | test_sqlite.sh | ported_sqlite_fdw.sql | Test for sqlite_fdw |
| 12 | test_influxdb.sh | ported_influxdb_fdw.sql | Test for influxdb_fdw |

User can execute test script to execute test for corresponding data source.
<pre>
./test_sqlite.sh
</pre>

For some test script, user need to update the paths inside test script or specify the environment variable before execution.
### test_dynamodb.sh
- Export LD_LIBRARY_PATH to the installed folder of AWS C++ SDK (which contains some libaws library).
<pre>
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib64
</pre>
- Update DYNAMODB_ENDPOINT in sql/init_data/dynamodb_fdw/dynamodb_init.sh if necessary.
<pre>
DYNAMODB_ENDPOINT="http://localhost:8000"
</pre>
- Make sure the data in sql/parameters/dynamodb_parameters.conf matches with data of data source.
### test_griddb.sh
- Download and build GridDB's C Client, rename the folder to griddb and put it into pgspider_ext/sql/init_data/griddb_fdw directory.
- Export LD_LIBRARY_PATH to the bin folder of GridDB's C Client.
<pre>
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/jenkins/postgres/postgresql-17.0/contrib/pgspider_ext/sql/init_data/griddb_fdw/griddb/bin
</pre>
- Make sure the data in sql/parameters/griddb_parameters.conf matches with data of data source.
### test_jdbc.sh
- Update paths in sql/init_data/jdbc_fdw/jdbc_mysql_init.sh and sql/init_data/jdbc_fdw/jdbc_postgres_init.sh.
<pre>
export PGS_SRC_DIR="/home/jenkins/postgres/postgresql-17.0/"

export PGS_BIN_DIR="/home/jenkins/postgres/postgresql-17.0/PGS"
export FDW_DIR="/home/jenkins/postgres/postgresql-17.0/contrib/pgspider_ext"
</pre>
- Update DB_DRIVERPATH, DB_DATA in sql/parameters/jdbc_griddb_parameters.conf, sql/parameters/jdbc_mysql_parameters.conf and sql/parameters/jdbc_postgres_parameters.conf.
<pre>
\set DB_DRIVERPATH	'\'/home/jenkins/src/jdbc/gridstore-jdbc-5.5.0.jar\''

\set DB_DATA		'\'/home/jenkins/postgres/postgresql-17.0/contrib/pgspider_ext/sql/init_data/jdbc_fdw/data'
</pre>
- Make sure the data in sql/parameters/jdbc_griddb_parameters.conf, sql/parameters/jdbc_mysql_parameters.conf and sql/parameters/jdbc_postgres_parameters.conf matches with data of data source.
### test_mongodb.sh
- Make sure the data in sql/parameters/mongodb_parameters.conf matches with data of data source.
### test_mysql.sh
- Make sure the data in sql/parameters/mysql_parameters.conf matches with data of data source.
- Open test_mysql.sh and executes the command in comment block manually.
<pre>
# --connect to mysql with root user
# mysql -u root -p

# --run below
# CREATE DATABASE mysql_fdw_post;
# SET GLOBAL validate_password.policy = LOW;
# SET GLOBAL validate_password.length = 1;
# SET GLOBAL validate_password.mixed_case_count = 0;
# SET GLOBAL validate_password.number_count = 0;
# SET GLOBAL validate_password.special_char_count = 0;
# CREATE USER 'edb'@'localhost' IDENTIFIED BY 'edb';
# GRANT ALL PRIVILEGES ON mysql_fdw_post.* TO 'edb'@'localhost';
# GRANT SUPER ON *.* TO 'edb'@localhost;

# Set time zone to default time zone of make check PST.
# SET GLOBAL time_zone = '+00:00';
# SET GLOBAL log_bin_trust_function_creators = 1;
# SET GLOBAL local_infile=1;
</pre>
### test_odbc.sh
- Update paths in test_odbc.sh
<pre>
export PGS_SRC_DIR="/home/jenkins/release_FDW/postgresql-17.0"
export PGS_BIN_DIR="/home/jenkins/release_FDW/postgresql-17.0/PGS"
export ODBC_FDW_DIR="/home/jenkins/release_FDW/postgresql-17.0/contrib/odbc_fdw"
</pre>
- Make sure the data in sql/parameters/odbc_mysql_parameters.conf, sql/parameters/odbc_postgres_parameters.conf matches with data of data source.
### test_parquet_s3_fdw.sh
- Make sure the data in sql/parameters/parquet_s3_local_parameters.conf, sql/parameters/parquet_s3_server_parameters.conf matches with data of data source.
### test_pgspider_core_fdw_parquet_s3.sh
- If do not use docker, comment out the following codes in test_pgspider_core_fdw_parquet_s3.sh
<pre>
# comment out these following code if do not use docker
container_name='minio_server'

if [ ! "$(docker ps -q -f name=^/${container_name}$)" ]; then
    if [ "$(docker ps -aq -f status=exited -f status=created -f name=^/${container_name}$)" ]; then
        # cleanup
        docker rm ${container_name} 
    fi
    # run minio container
   sudo docker run -d --name ${container_name} -it -p 9000:9000 -e "MINIO_ACCESS_KEY=minioadmin" -e "MINIO_SECRET_KEY=minioadmin" -v /tmp/data_s3:/data minio/minio server /data
fi
</pre>
### test_pgspider_core_fdw_postgres_fdw.sh
- Update path in sql/init_data/pgspider_core_fdw/ported_postgres_setup.sh.
<pre>
POSTGRES_HOME=/home/jenkins/postgres/postgresql-17.0/PGS
</pre>
### test_influxdb.sh
- Make sure the data in sql/parameters/influxdb_parameters.conf matches with data of data source.

## Limitations
- PGSpider Extension does not support INSERT, UPDATE, DELETE.

## Contributing
Opening issues and pull requests on GitHub are welcome.

## License
Copyright and license information can be found in the
file [`License`][1] .

[1]: License