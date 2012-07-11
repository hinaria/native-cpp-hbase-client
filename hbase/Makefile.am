SUBDIRS = .

ACLOCAL_AMFLAGS = -I m4

BUILT_SOURCES = gen-cpp/hbase/Hbase.cpp

gen-cpp/hbase/Hbase.cpp: hbase.thrift
	mkdir -p gen-cpp/hbase
	$(THRIFT_EXEC_PATH) --gen cpp -out gen-cpp/hbase $<

gen-cpp/hbase/Hbase.h: hbase.thrift
	mkdir -p gen-cpp/hbase
	$(THRIFT_EXEC_PATH) --gen cpp -out gen-cpp/hbase $<

bin_PROGRAMS = hbc
lib_LTLIBRARIES = libhbase.la
noinst_PROGRAMS = connection_pool_test native_testing

libhbase_includedir = $(includedir)/hbase
libhbase_include_HEADERS = BaseHbaseConnection.h ConnectionPool.h Counters.h HbcExtensions.h NativeHbaseClient.h NativeHbaseClient-inl.cpp \
	gen-cpp/hbase/hbase_constants.h gen-cpp/hbase/Hbase.h gen-cpp/hbase/hbase_types.h nhc-config.h
libhbase_la_SOURCES = NativeHbaseClient.cpp \
	gen-cpp/hbase/hbase_constants.cpp gen-cpp/hbase/Hbase.cpp gen-cpp/hbase/hbase_types.cpp
libhbase_la_LIBADD = -lfolly -lthrift
libhbase_la_CPPFLAGS = -Igen-cpp $(THRIFT_CFLAGS)

hbc_SOURCES = hbc.cpp
hbc_LDADD = -lhbase
hbc_CPPFLAGS = -Igen-cpp $(THRIFT_CFLAGS)

TESTS = connection_pool_test

connection_pool_test_SOURCES = ConnectionPoolTest.cpp
connection_pool_test_LDADD = -lfolly

native_testing_SOURCES = testing/NativeTesting.cpp
native_testing_LDADD = -lhbase
native_testing_CPPFLAGS = $(THRIFT_CFLAGS) -Igen-cpp

clean-local:
	rm -rf gen-cpp
