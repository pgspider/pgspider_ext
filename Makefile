# contrib/pgspider_ext/Makefile

MODULE_big = pgspider_ext
OBJS = pgspider_ext_deparse.o pgspider_ext.o pgspider_ext_option.o $(WIN32RES)
PGFILEDESC = "pgspider_ext - foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir) -I../  -lpq -lm -z defs
SHLIB_LINK = $(libpq)

#LIBS = -lpostgres_fdw

EXTENSION = pgspider_ext
DATA = pgspider_ext--1.0.sql

REGRESS = pgspider_ext pgspider_ext2 pgspider_ext3

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/pgspider_ext/
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

check: temp-install

temp-install: EXTRA_INSTALL=contrib/postgres_fdw contrib/pgspider_ext

checkprep: EXTRA_INSTALL=contrib/postgres_fdw contrib/pgspider_ext

