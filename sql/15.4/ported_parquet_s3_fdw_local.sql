\set ECHO none
\ir sql/parameters/parquet_s3_local_parameters.conf
\set ECHO all
show server_version \gset
\ir sql/:server_version/ported_parquet_s3_fdw.sql