ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir) $(BOOST_CPPFLAGS)

variadic_hash_include_HEADERS = \
  variadic_hash.h

variadic_hash_includedir = $(includedir)/soa/variadic_hash

lib_LTLIBRARIES = libvariadic_hash.la

libvariadic_hash_la_LDFLAGS = -avoid-version
libvariadic_hash_la_CXXFLAGS = $(CITYHASH_FLAGS)
libvariadic_hash_la_LIBADD = $(CITYHASH_LIB)

libvariadic_hash_la_SOURCES = \
  variadic_hash.cc

TESTS_ENVIRONMENT = $(abs_top_builddir)/test_driver.sh

CC_TESTS = variadic_hash_test

TESTS = $(CC_TESTS)

check_PROGRAMS = $(CC_TESTS)

TEST_LIB = \
  $(BOOST_UNIT_TEST_FRAMEWORK_LIB) \
  $(CITYHASH_LIB) \
  libvariadic_hash.la

variadic_hash_test_SOURCES = testing/variadic_hash_test.cc
variadic_hash_test_LDADD = $(TEST_LIB)
