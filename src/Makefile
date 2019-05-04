# Copyright (C) 2019  Giuseppe Fabio Nicotra <artix2 at gmail dot com>

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
uname_M := $(shell sh -c 'uname -m 2>/dev/null || echo not')
OPTIMIZATION?=-O2
DEPENDENCY_TARGETS=hiredis
NODEPS:=clean distclean

# Default settings
STD=-std=c99 -pedantic -DREDIS_STATIC=''
ifneq (,$(findstring clang,$(CC)))
ifneq (,$(findstring FreeBSD,$(uname_S)))
  STD+=-Wno-c11-extensions
endif
endif
WARN=-Wall -W -Wno-missing-field-initializers
OPT=$(OPTIMIZATION)

PREFIX?=/usr/local
INSTALL_BIN=$(PREFIX)/bin
INSTALL=install

# Default allocator defaults to Jemalloc if it's not an ARM
MALLOC=libc
ifneq ($(uname_M),armv6l)
ifneq ($(uname_M),armv7l)
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
endif
endif
endif

# To get ARM stack traces if Redis crashes we need a special C flag.
ifneq (,$(filter aarch64 armv,$(uname_M)))
        CFLAGS+=-funwind-tables
else
ifneq (,$(findstring armv,$(uname_M)))
        CFLAGS+=-funwind-tables
endif
endif

# Backwards compatibility for selecting an allocator
ifeq ($(USE_TCMALLOC),yes)
	MALLOC=tcmalloc
endif

ifeq ($(USE_TCMALLOC_MINIMAL),yes)
	MALLOC=tcmalloc_minimal
endif

ifeq ($(USE_JEMALLOC),yes)
	MALLOC=jemalloc
endif

ifeq ($(USE_JEMALLOC),no)
	MALLOC=libc
endif

# Override default settings if possible
-include .make-settings

REDIS_CLUSTER_PROXY_CFLAGS=
REDIS_CLUSTER_PROXY_LDFLAGS=

FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS) $(REDIS_CLUSTER_PROXY_CFLAGS)
FINAL_LDFLAGS=$(LDFLAGS) $(REDIS_CLUSTER_PROXY_LDFLAGS) $(DEBUG)
FINAL_LIBS=-lm
DEBUG=-g -ggdb

ifeq ($(uname_S),SunOS)
	# SunOS
        ifneq ($(@@),32bit)
		CFLAGS+= -m64
		LDFLAGS+= -m64
	endif
	DEBUG=-g
	DEBUG_FLAGS=-g
	export CFLAGS LDFLAGS DEBUG DEBUG_FLAGS
	INSTALL=cp -pf
	FINAL_CFLAGS+= -D__EXTENSIONS__ -D_XPG6
	FINAL_LIBS+= -ldl -lnsl -lsocket -lresolv -lpthread -lrt
else
ifeq ($(uname_S),Darwin)
	# Darwin
	FINAL_LIBS+= -ldl
else
ifeq ($(uname_S),AIX)
        # AIX
        FINAL_LDFLAGS+= -Wl,-bexpall
        FINAL_LIBS+=-ldl -pthread -lcrypt -lbsd
else
ifeq ($(uname_S),OpenBSD)
	# OpenBSD
	FINAL_LIBS+= -lpthread
	ifeq ($(USE_BACKTRACE),yes)
	    FINAL_CFLAGS+= -DUSE_BACKTRACE -I/usr/local/include
	    FINAL_LDFLAGS+= -L/usr/local/lib
	    FINAL_LIBS+= -lexecinfo
    	endif

else
ifeq ($(uname_S),FreeBSD)
	# FreeBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),DragonFly)
	# FreeBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
	# All the other OSes (notably Linux)
	FINAL_LDFLAGS+= -rdynamic
	FINAL_LIBS+=-ldl -pthread -lrt
endif
endif
endif
endif
endif
endif
# Include paths to dependencies
FINAL_CFLAGS+= -I../deps/hiredis

ifeq ($(MALLOC),tcmalloc)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc
endif

ifeq ($(MALLOC),tcmalloc_minimal)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc_minimal
endif

ifeq ($(MALLOC),jemalloc)
	DEPENDENCY_TARGETS+= jemalloc
	FINAL_CFLAGS+= -DUSE_JEMALLOC -I../deps/jemalloc/include
	FINAL_LIBS := ../deps/jemalloc/lib/libjemalloc.a $(FINAL_LIBS)
endif

REDIS_CLUSTER_PROXY_CC=$(CC) $(FINAL_CFLAGS)
REDIS_CLUSTER_PROXY_LD=$(CC) $(FINAL_LDFLAGS)
REDIS_CLUSTER_PROXY_INSTALL=$(INSTALL)

REDIS_CLUSTER_PROXY_NAME=redis-cluster-proxy
REDIS_CLUSTER_PROXY_OBJ=adlist.o ae.o anet.o cluster.o commands.o crc16.o dict.o endianconv.o logger.o protocol.o proxy.o rax.o reply_order.o siphash.o sds.o zmalloc.o

Makefile.dep:
	-$(REDIS_CLUSTER_PROXY_CC) -MM *.c > Makefile.dep 2> /dev/null || true

ifeq (0, $(words $(findstring $(MAKECMDGOALS), $(NODEPS))))
-include Makefile.dep
endif

persist-settings: distclean
	echo STD=$(STD) >> .make-settings
	echo WARN=$(WARN) >> .make-settings
	echo OPT=$(OPT) >> .make-settings
	echo MALLOC=$(MALLOC) >> .make-settings
	echo CFLAGS=$(CFLAGS) >> .make-settings
	echo LDFLAGS=$(LDFLAGS) >> .make-settings
	echo REDIS_CLUSTER_PROXY_CFLAGS=$(REDIS_CLUSTER_PROXY_CFLAGS) >> .make-settings
	echo REDIS_CLUSTER_PROXY_LDFLAGS=$(REDIS_CLUSTER_PROXY_LDFLAGS) >> .make-settings
	echo PREV_FINAL_CFLAGS=$(FINAL_CFLAGS) >> .make-settings
	echo PREV_FINAL_LDFLAGS=$(FINAL_LDFLAGS) >> .make-settings
	-(cd ../deps && $(MAKE) $(DEPENDENCY_TARGETS))

.PHONY: persist-settings

# Prerequisites target
.make-prerequisites:
	@touch $@

# Clean everything, persist settings and build dependencies if anything changed
ifneq ($(strip $(PREV_FINAL_CFLAGS)), $(strip $(FINAL_CFLAGS)))
.make-prerequisites: persist-settings
endif

ifneq ($(strip $(PREV_FINAL_LDFLAGS)), $(strip $(FINAL_LDFLAGS)))
.make-prerequisites: persist-settings
endif


# Prerequisites target
.make-prerequisites:
	@touch $@

# redis-cluster-proxy
$(REDIS_CLUSTER_PROXY_NAME): $(REDIS_CLUSTER_PROXY_OBJ)
	$(REDIS_CLUSTER_PROXY_LD) -o $@ $^ ../deps/hiredis/libhiredis.a $(FINAL_LIBS)


all: $(REDIS_CLUSTER_PROXY_NAME)
	@echo ""
	@echo "Done!"
	@echo ""

.PHONY: all

# Because the jemalloc.h header is generated as a part of the jemalloc build,
# building it should complete before building any other object. Instead of
# depending on a single artifact, build all dependencies first.
%.o: %.c .make-prerequisites
	$(REDIS_CLUSTER_PROXY_CC) -c $<

clean:
	rm -rf $(REDIS_CLUSTER_PROXY_NAME) *.o *.gcda *.gcno *.gcov lcov-html Makefile.dep

.PHONY: clean

distclean: clean
	-(cd ../deps && $(MAKE) distclean)
	-(rm -f .make-*)

.PHONY: distclean

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	$(MAKE) CFLAGS="-m32" LDFLAGS="-m32"

valgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc"

helgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc" CFLAGS="-D__ATOMIC_VAR_FORCE_SYNC_MACROS"

test: $(REDIS_CLUSTER_PROXY_NAME)
	@(cd ..; ./runtest)
.PHONY: test
