\set ECHO none
\ir sql/parameters/jdbc_griddb_parameters.conf
\set ECHO all
show server_version \gset
\i sql/:server_version/ported_jdbc_fdw.sql