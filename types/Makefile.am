ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir) $(BOOST_CPPFLAGS) $(V8_CPPFLAGS)

sysconf_DATA = date_timezone_spec.csv 

types_include_HEADERS = \
  basic_value_descriptions.h \
  compact_vector_value_description.h \
  date.h \
  id.h \
  json_parsing.h \
  json_printing.h \
  localdate.h \
  periodic_utils.h \
  string.h \
  url.h \
  value_description_fwd.h \
  value_description.h

types_includedir = $(includedir)/soa/types

lib_LTLIBRARIES = libtypes.la libvalue_description.la

libtypes_la_LDFLAGS = -avoid-version
libtypes_la_LIBADD = \
  $(ACE_LIB) \
  $(CITYHASH_LIB) \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/db/libdb.la \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/googleurl/libgoogleurl.la \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(BOOST_SYSTEM_LIB) \
  $(BOOST_REGEX_LIB) \
  $(BOOST_DATE_TIME_LIB)

libtypes_la_SOURCES = \
  date.cc \
  localdate.cc \
  string.cc \
  id.cc \
  url.cc \
  periodic_utils.cc

libvalue_description_la_LDFLAGS = -avoid-version
libvalue_description_la_LIBADD = \
  $(ACE_LIB) \
  $(CITYHASH_LIB) \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/db/libdb.la \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/googleurl/libgoogleurl.la \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(BOOST_SYSTEM_LIB) \
  $(BOOST_REGEX_LIB) \
  $(BOOST_DATE_TIME_LIB) \
  libtypes.la

libvalue_description_la_SOURCES = \
  value_description.cc \
  json_parsing.cc \
  json_printing.cc

AM_CXXFLAGS = 
AM_CFLAGS = 

CC_TESTS = \
  date_test \
  localdate_test \
  id_test \
  string_test \
  json_handling_test \
  value_description_test

TESTS = $(CC_TESTS)

TESTS_ENVIRONMENT = $(abs_top_builddir)/test_driver.sh

check_PROGRAMS = $(CC_TESTS)

date_test_SOURCES = testing/date_test.cc
localdate_test_SOURCES = testing/localdate_test.cc
id_test_SOURCES = testing/id_test.cc
string_test_SOURCES = testing/string_test.cc
json_handling_test_SOURCES = testing/json_handling_test.cc
value_description_test_SOURCES = testing/value_description_test.cc

TEST_LIB = $(BOOST_UNIT_TEST_FRAMEWORK_LIB) \
  $(ACE_LIB) \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/db/libdb.la \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(BOOST_REGEX_LIB) $(ICU_LIBS) \
  $(BOOST_SYSTEM_LIB) \
  $(BOOST_THREAD_LIB) \
  $(BOOST_IOSTREAMS_LIB) \
  libtypes.la \
  libvalue_description.la

date_test_LDADD = $(TEST_LIB)
localdate_test_LDADD = $(TEST_LIB)
id_test_LDADD = $(TEST_LIB)
string_test_LDADD = $(TEST_LIB)
json_handling_test_LDADD = $(TEST_LIB)
value_description_test_LDADD = $(TEST_LIB)

bin_PROGRAMS = id_profile

id_profile_SOURCES = testing/id_profile.cc
id_profile_LDADD = $(TEST_LIB)
