ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir)

gc_include_HEADERS = \
  gc.h \
  gc_lock.h \
  rcu_lock.h \
  rcu_protected.h

gc_includedir = $(includedir)/soa/gc

lib_LTLIBRARIES = libgc.la

libgc_la_LDFLAGS = -avoid-version
libgc_la_CXXFLAGS = $(URCU_FLAGS)

libgc_la_SOURCES = \
  gc_lock.cc \
  rcu_lock.cc \
  gc.cc

libgc_la_LIBADD = \
  $(URCU_LIB) \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/utils/libutils.la

AM_CXXFLAGS = 
AM_CFLAGS = 

TEST_LIB = \
  $(BOOST_SYSTEM_LIB) \
  $(BOOST_THREAD_LIB) \
  $(libgc_la_LIBADD) \
  $(V8_LIBS) \
  libgc.la

CC_TESTS = \
  gc_test

TESTS_ENVIRONMENT = $(abs_top_builddir)/test_driver.sh

TESTS = $(CC_TESTS)

check_PROGRAMS = $(CC_TESTS)

gc_test_SOURCES = testing/gc_test.cc
gc_test_CXXFLAGS = $(URCU_FLAGS)
gc_test_LDADD = $(BOOST_UNIT_TEST_FRAMEWORK_LIB) $(TEST_LIB)
