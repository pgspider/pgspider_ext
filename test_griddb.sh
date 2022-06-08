rm -rf make_check.out || true
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/sql/init_data/griddb/bin/
cd sql/init_data/griddb_fdw
chmod +x ./*.sh || true
./init.sh
cd ../../..
sed -i 's/REGRESS =.*/REGRESS = ported_griddb_fdw/' Makefile
sed -i 's/temp-install\:.*/temp-install\: EXTRA_INSTALL=contrib\/pgspider_ext contrib\/griddb_fdw /' Makefile
sed -i 's/checkprep\:.*/checkprep\: EXTRA_INSTALL=contrib\/pgspider_ext contrib\/griddb_fdw /' Makefile
make clean
make
make check | tee make_check.out
