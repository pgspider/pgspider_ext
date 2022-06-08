./sql/init_data/influxdb_fdw/influxdb_init.sh
sed -i 's/REGRESS =.*/REGRESS = ported_influxdb_fdw /' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/pgspider_ext contrib\/influxdb_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/pgspider_ext contrib\/influxdb_fdw /' Makefile
make clean
make
make check | tee make_check.out
