rm -rf /tmp/sqlitefdw_test*.db
rm -rf /tmp/*.data
rm -rf /tmp/sqlitefdw_test*.db
cp -a sql/init_data/sqlite_fdw/*.data /tmp/

sqlite3 /tmp/sqlitefdw_test_post.db < sql/init_data/sqlite_fdw/init_post.sql
sqlite3 /tmp/sqlitefdw_test_core.db < sql/init_data/sqlite_fdw/init_core.sql
sqlite3 /tmp/sqlitefdw_test.db < sql/init_data/sqlite_fdw/init.sql
sqlite3 /tmp/sqlitefdw_test_selectfunc.db < sql/init_data/sqlite_fdw/init_selectfunc.sql

sed -i 's/REGRESS =.*/REGRESS = ported_sqlite_fdw/' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/pgspider_ext contrib\/sqlite_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/pgspider_ext contrib\/sqlite_fdw /' Makefile
make clean
make
make check | tee make_check.out
