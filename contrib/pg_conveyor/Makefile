# contrib/pg_conveyor/Makefile

MODULE_big = pg_conveyor
OBJS = \
	$(WIN32RES) \
	pg_conveyor.o

EXTENSION = pg_conveyor
DATA = pg_conveyor--1.0.sql
PGFILEDESC = "pg_conveyor - conveyor belt test"

REGRESS = pg_conveyor

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_conveyor
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
