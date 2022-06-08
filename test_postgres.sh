rm -rf make_check.out || true
sed -i 's/REGRESS =.*/REGRESS = pgspider_ext pgspider_ext2 pgspider_ext3/' Makefile
sed -i 's/temp-install\:.*/temp-install\: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext/' Makefile
sed -i 's/checkprep\:.*/checkprep\: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext/' Makefile
make clean
make
make check | tee make_check.out
