ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir) $(BOOST_CPPFLAGS)

jsoncpp_include_HEADERS = \
  autolink.h \
  config.h \
  features.h \
  forwards.h \
  json_batchallocator.h \
  json.h \
  reader.h \
  value.h \
  writer.h

jsoncpp_includedir = $(includedir)/soa/jsoncpp

lib_LTLIBRARIES = libjsoncpp.la

libjsoncpp_la_LDFLAGS = -avoid-version

libjsoncpp_la_SOURCES = \
  json_reader.cpp \
  json_writer.cpp \
  json_value.cpp

AM_CXXFLAGS = 
AM_CFLAGS = 

CC_TESTS = \
  reader_test

TESTS_ENVIRONMENT = $(abs_top_builddir)/test_driver.sh

TESTS = $(CC_TESTS)

check_PROGRAMS = $(CC_TESTS)

reader_test_SOURCES = testing/reader_test.cc

reader_test_LDADD = \
  libjsoncpp.la \
  $(BOOST_UNIT_TEST_FRAMEWORK_LIB) \
  $(abs_top_builddir)/jml//arch/libarch.la
