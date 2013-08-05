ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir)

logger_include_HEADERS = \
  callback_output.h \
  cloud_output.h \
  compressing_output.h \
  compressor.h \
  easy_kvp_logger.h \
  file_output.h \
  filter.h \
  json_filter.h \
  kvp_logger_interface.h \
  kvp_logger_mongodb.h \
  kvp_logger_void.h \
  logger.h \
  logger_metrics_interface.h \
  logger_metrics_mongo.h \
  logger_metrics_term.h \
  logger_metrics_void.h \
  log_message_splitter.h \
  multi_output.h \
  publish_output.h \
  remote_input.h \
  remote_output.h \
  rotating_output.h \
  stats_output.h

logger_includedir = $(includedir)/soa/logger

lib_LTLIBRARIES = liblogger.la liblogger_metrics.la

liblogger_la_LDFLAGS = -avoid-version
liblogger_la_CXXFLAGS = $(ZMQ_FLAGS)
liblogger_la_LIBADD = \
  $(BOOST_IOSTREAMS_LIB) \
  $(BOOST_REGEX_LIB) \
  $(BOOST_THREAD_LIB) \
  $(ACE_LIB) \
  $(ZMQ_LIB) \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/soa/service/libservices.la \
  $(abs_top_builddir)/soa/service/libzeromq.la \
  $(abs_top_builddir)/soa/service/libcloud.la \
  $(abs_top_builddir)/soa/gc/libgc.la \
  $(abs_top_builddir)/soa/sigslot/libsigslot.la

liblogger_la_SOURCES = \
  logger.cc \
  remote_output.cc \
  remote_input.cc \
  file_output.cc \
  publish_output.cc \
  filter.cc \
  json_filter.cc \
  stats_output.cc \
  callback_output.cc \
  rotating_output.cc \
  cloud_output.cc \
  compressor.cc \
  compressing_output.cc \
  multi_output.cc 

liblogger_metrics_la_LDFLAGS = -avoid-version
liblogger_metrics_la_CXXFLAGS = $(ZMQ_FLAGS) $(MONGODB_FLAGS)
liblogger_metrics_la_LIBADD = \
  $(MONGODB_LIB) \
  $(abs_top_builddir)/soa/types/libtypes.la \
  $(BOOST_PROGRAM_OPTIONS_LIB) \
  $(BOOST_FILESYSTEM_LIB)

liblogger_metrics_la_SOURCES = \
  kvp_logger_interface.cc \
  kvp_logger_mongodb.cc \
  easy_kvp_logger.cc \
  logger_metrics_interface.cc \
  logger_metrics_mongo.cc \
  logger_metrics_term.cc

PLUGIN_LIBTOOL_FLAGS = \
    -shrext .node \
    -module \
    -shared \
    -avoid-version \
    -rpath $(abs_builddir) \
    -fPIC \
    -Wall \
    -m64 \
    -fdata-sections \
    -ffunction-sections \
    -fno-strict-aliasing \
    -fno-rtti \
    -fno-exceptions

noinst_LTLIBRARIES = logger.la iloggermetricscpp.la

logger_la_LIBADD = \
  $(abs_top_builddir)/soa/js/libjs.la \
  $(abs_top_builddir)/soa/sigslot/libsigslot.la \
  $(NODE_LIB) \
  liblogger.la

logger_la_LDFLAGS = $(PLUGIN_LIBTOOL_FLAGS)
logger_la_CXXFLAGS = $(NODE_FLAGS) $(ZMQ_FLAGS)
logger_la_SOURCES = logger_js.cc filter_js.cc

iloggermetricscpp_la_LIBADD = \
  $(abs_top_builddir)/soa/js/libjs.la \
  $(NODE_LIB) \
  liblogger_metrics.la

iloggermetricscpp_la_LDFLAGS = $(PLUGIN_LIBTOOL_FLAGS)
iloggermetricscpp_la_CXXFLAGS = $(NODE_FLAGS) $(ZMQ_FLAGS)
iloggermetricscpp_la_SOURCES = js/logger_metrics_interface_js.cc

TESTS_ENVIRONMENT = $(abs_top_builddir)/test_driver.sh NODE=$(NODEJS) VOWS=$(VOWS) NODE_LIBS="$(noinst_LTLIBRARIES)"

CC_TESTS = \
  remote_logger_test2 \
  multi_output_logger_test \
  rotating_file_logger_test
#  json_filter_test
#  logger_deadlock_test

TESTS = $(CC_TESTS) $(abs_top_builddir)/runjstest.sh

check_PROGRAMS = $(CC_TESTS)

TEST_LIB = \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/soa/types/libtypes.la \
  $(BOOST_THREAD_LIB) \
  $(BOOST_FILESYSTEM_LIB) \
  $(BOOST_REGEX_LIB) \
  $(BOOST_SYSTEM_LIB) \
  $(BOOST_UNIT_TEST_FRAMEWORK_LIB) \
  liblogger.la

#json_filter_test_SOURCES = testing/json_filter_test.cc
#json_filter_test_LDADD = $(TEST_LIB)

remote_logger_test2_SOURCES = testing/remote_logger_test2.cc
remote_logger_test2_CXXFLAGS = $(ZMQ_FLAGS)
remote_logger_test2_LDADD = $(TEST_LIB)

#logger_deadlock_test_SOURCES = testing/logger_deadlock_test.cc
#logger_deadlock_test_CXXFLAGS = $(ZMQ_FLAGS)
#logger_deadlock_test_LDADD = $(TEST_LIB)

multi_output_logger_test_SOURCES = testing/multi_output_logger_test.cc
multi_output_logger_test_CXXFLAGS = $(ZMQ_FLAGS)
multi_output_logger_test_LDADD = $(TEST_LIB)

rotating_file_logger_test_SOURCES = testing/rotating_file_logger_test.cc
rotating_file_logger_test_CXXFLAGS = $(ZMQ_FLAGS)
rotating_file_logger_test_LDADD = $(TEST_LIB)

node_prefix=$(exec_prefix)/node_modules

install-exec-hook:
	mkdir -p $(node_prefix)
	cp -rf .libs/*.node $(node_prefix)
uninstall-hook:
	for i in $(noinst_LTLIBRARIES); do lib=`echo "$$i" | sed 's/\.la/\.node/g'`; rm $(node_prefix)/$$lib; done
	if find "$(node_prefix)" -maxdepth 0 -empty | read; then rm -rf $(node_prefix); fi
