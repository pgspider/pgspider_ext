\set ECHO none
\ir sql/parameters/odbc_mysql_parameters.conf
\set ECHO all
show server_version \gset
\i sql/:server_version/ported_odbc_fdw.sql