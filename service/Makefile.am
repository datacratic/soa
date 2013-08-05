ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir) $(BOOST_CPPFLAGS)

services_include_HEADERS = \
  active_endpoint.h \
  async_event_source.h \
  carbon_connector.h \
  chunked_http_endpoint.h \
  connection_handler.h \
  endpoint.h \
  epoller.h \
  http_endpoint.h \
  http_header.h \
  http_monitor.h \
  http_named_endpoint.h \
  http_rest_proxy.h \
  json_codec.h \
  json_endpoint.h \
  loop_monitor.h \
  message_loop.h \
  named_endpoint.h \
  passive_endpoint.h \
  pending_list.h \
  port_range_service.h \
  process_stats.h \
  redis.h \
  rest_proxy.h \
  rest_request_binding.h \
  rest_request_router.h \
  rest_service_endpoint.h \
  rusage.h \
  s3.h \
  service_base.h \
  service_utils.h \
  sftp.h \
  sns.h \
  socket_per_thread.h \
  stat_aggregator.h \
  statsd_connector.h \
  stats_events.h \
  timeout_map.h \
  transport.h \
  typed_message_channel.h \
  xml_helpers.h \
  zmq_endpoint.h \
  zmq_message_router.h \
  zmq_named_pub_sub.h \
  zmq_utils.h \
  zookeeper_configuration_service.h \
  zookeeper.h \
  zmq.hpp

services_includedir = $(includedir)/soa/service

lib_LTLIBRARIES = libopstats.la libzeromq.la libservices.la libcloud.la libredis.la

libopstats_la_LDFLAGS = -avoid-version
libopstats_la_CXXFLAGS = $(NODE_FLAGS)
libopstats_la_LIBADD = \
  $(BOOST_SYSTEM_LIB) \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(abs_top_builddir)/soa/types/libtypes.la \
  $(abs_top_builddir)/jml/utils/libutils.la
 
libopstats_la_SOURCES = \
  statsd_connector.cc \
  carbon_connector.cc \
  stat_aggregator.cc \
  process_stats.cc

libzeromq_la_LDFLAGS = -avoid-version
libzeromq_la_CXXFLAGS = $(ZMQ_FLAGS)
libzeromq_la_LIBADD = \
  $(ZMQ_LIB) \
  $(BOOST_SYSTEM_LIB) \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/utils/libutils.la

libzeromq_la_SOURCES = \
  socket_per_thread.cc \
  zmq_utils.cc

libcloud_la_LDFLAGS = -avoid-version
libcloud_la_CXXFLAGS = $(SSH2_FLAGS)
libcloud_la_LIBADD = \
  $(SSH2_LIB) \
  $(BOOST_FILESYSTEM_LIB) \
  $(abs_top_builddir)/tinyxml2/libtinyxml2.la \
  libservices.la

libcloud_la_SOURCES = \
  sftp.cc \
  s3.cc \
  sns.cc

libservices_la_LDFLAGS = -avoid-version
libservices_la_CXXFLAGS = $(ZMQ_FLAGS)
libservices_la_LIBADD = \
  $(ZOOKEEPER_LIB) \
  $(ZMQ_LIB) \
  $(CURLPP) \
  $(BOOST_THREAD_LIB) \
  $(BOOST_REGEX_LIB) \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/utils/libutils.la \
  libopstats.la \
  libzeromq.la

libservices_la_SOURCES = \
  transport.cc \
  endpoint.cc \
  connection_handler.cc \
  http_endpoint.cc \
  json_endpoint.cc \
  active_endpoint.cc \
  passive_endpoint.cc \
  chunked_http_endpoint.cc \
  epoller.cc \
  http_header.cc \
  port_range_service.cc \
  service_base.cc \
  message_loop.cc \
  loop_monitor.cc \
  named_endpoint.cc \
  zookeeper_configuration_service.cc \
  zmq_endpoint.cc \
  async_event_source.cc \
  rest_service_endpoint.cc \
  http_named_endpoint.cc \
  rest_proxy.cc \
  rest_request_router.cc \
  zookeeper.cc \
  http_rest_proxy.cc

libredis_la_LDFLAGS = -avoid-version
libredis_la_CXXFLAGS = $(HIREDIS_FLAGS)
libredis_la_LIBADD = \
  $(HIREDIS_LIB) \
  $(BOOST_THREAD_LIB) \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(abs_top_builddir)/soa/types/libtypes.la

libredis_la_SOURCES = redis.cc

bin_PROGRAMS = s3_transfer_cmd s3tee s3cp s3_multipart_cmd service_dump

s3_transfer_cmd_LDADD = \
  $(BOOST_SYSTEM_LIB) \
  $(BOOST_FILESYSTEM_LIB) \
  $(BOOST_PROGRAM_OPTIONS_LIB) \
  $(abs_top_builddir)/jml/utils/libutils.la \
  libcloud.la

s3_transfer_cmd_SOURCES = s3_transfer_cmd.cc

s3tee_LDADD = \
  $(BOOST_SYSTEM_LIB) \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(BOOST_PROGRAM_OPTIONS_LIB) \
  libcloud.la

s3tee_SOURCES = s3tee.cc

s3cp_LDADD = \
  $(BOOST_SYSTEM_LIB) \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/soa/types/libtypes.la \
  $(BOOST_PROGRAM_OPTIONS_LIB) \
  libcloud.la

s3cp_SOURCES = s3cp.cc

s3_multipart_cmd_LDADD = \
  $(BOOST_SYSTEM_LIB) \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/soa/types/libtypes.la \
  $(BOOST_PROGRAM_OPTIONS_LIB) \
  libcloud.la

s3_multipart_cmd_SOURCES = s3_multipart_cmd.cc

service_dump_LDADD = \
  $(BOOST_SYSTEM_LIB) \
  $(BOOST_PROGRAM_OPTIONS_LIB) \
  libservices.la

service_dump_SOURCES = service_dump.cc

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

noinst_LTLIBRARIES = opstats.la s3.la services.la

opstats_la_LIBADD = \
  $(abs_top_builddir)/soa/js/libjs.la \
  $(NODE_LIB) \
  libopstats.la

opstats_la_LDFLAGS = $(PLUGIN_LIBTOOL_FLAGS)
opstats_la_CXXFLAGS = $(NODE_FLAGS)
opstats_la_SOURCES = js/opstats_js.cc

s3_la_LIBADD = \
  $(abs_top_builddir)/soa/js/libjs.la \
  libcloud.la

s3_la_LDFLAGS = $(PLUGIN_LIBTOOL_FLAGS)
s3_la_CXXFLAGS = $(NODE_FLAGS)
s3_la_SOURCES = js/s3_js.cc

services_la_LIBADD = \
  $(abs_top_builddir)/soa/js/libjs.la \
  $(abs_top_builddir)/soa/types/libtypes.la \
  $(abs_top_builddir)/soa/sigslot/libsigslot.la \
  libopstats.la \
  libservices.la

services_la_LDFLAGS = $(PLUGIN_LIBTOOL_FLAGS)
services_la_CXXFLAGS = $(NODE_FLAGS)
services_la_SOURCES = js/service_js.cc js/service_base_js.cc

AM_CXXFLAGS = 
AM_CFLAGS = 

CC_TESTS = \
  named_endpoint_test \
  zmq_named_pub_sub_test \
  zmq_endpoint_test \
  message_channel_test \
  rest_service_endpoint_test \
  multiple_service_test \
  zookeeper_test \
  redis_async_test \
  redis_commands_test \
  statsd_connector_test \
  carbon_connector_test \
  endpoint_unit_test \
  test_active_endpoint_nothing_listening \
  test_active_endpoint_not_responding \
  test_endpoint_ping_pong \
  test_endpoint_connection_speed \
  test_endpoint_accept_speed \
  endpoint_periodic_test \
  endpoint_closed_connection_test \
  http_long_header_test \
  service_proxies_test \
  zmq_tcp_bench

TESTS_ENVIRONMENT = $(abs_top_builddir)/test_driver.sh NODE=$(NODEJS) VOWS=$(VOWS) NODE_LIBS="$(noinst_LTLIBRARIES)"

TESTS = $(CC_TESTS) 
#$(abs_top_builddir)/runjstest.sh

check_PROGRAMS = $(CC_TESTS)

TEST_LIB = \
  $(ACE_LIB) \
  $(HIREDIS_LIB) \
  $(ZMQ_LIB) \
  $(BOOST_THREAD_LIB) \
  $(BOOST_SYSTEM_LIB) \
  $(BOOST_UNIT_TEST_FRAMEWORK_LIB) \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/jml/stats/libstats.la \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(abs_top_builddir)/soa/types/libtypes.la \
  libservices.la \
  libzeromq.la \
  libopstats.la \
  libredis.la

named_endpoint_test_SOURCES = testing/named_endpoint_test.cc
named_endpoint_test_CXXFLAGS = $(ZMQ_FLAGS)
named_endpoint_test_LDADD = $(TEST_LIB)

zmq_named_pub_sub_test_SOURCES = testing/zmq_named_pub_sub_test.cc
zmq_named_pub_sub_test_CXXFLAGS = $(ZMQ_FLAGS)
zmq_named_pub_sub_test_LDADD = $(TEST_LIB)

zmq_endpoint_test_SOURCES = testing/zmq_endpoint_test.cc
zmq_endpoint_test_CXXFLAGS = $(ZMQ_FLAGS) 
zmq_endpoint_test_LDADD = $(TEST_LIB)

message_channel_test_SOURCES = testing/message_channel_test.cc
message_channel_test_CXXFLAGS = $(ZMQ_FLAGS)
message_channel_test_LDADD = $(TEST_LIB)

rest_service_endpoint_test_SOURCES = testing/rest_service_endpoint_test.cc
rest_service_endpoint_test_CXXFLAGS = $(ZMQ_FLAGS)
rest_service_endpoint_test_LDADD = $(TEST_LIB)

multiple_service_test_SOURCES = testing/multiple_service_test.cc
multiple_service_test_CXXFLAGS = $(ZMQ_FLAGS)
multiple_service_test_LDADD = $(TEST_LIB)

zookeeper_test_SOURCES = testing/zookeeper_test.cc
zookeeper_test_CXXFLAGS = $(ZOOKEEPER_FLAGS)
zookeeper_test_LDADD = $(TEST_LIB)

redis_async_test_SOURCES = testing/redis_async_test.cc
redis_async_test_CXXFLAGS = $(HIREDIS_FLAGS)
redis_async_test_LDADD = $(TEST_LIB)

redis_commands_test_SOURCES = testing/redis_commands_test.cc
redis_commands_test_CXXFLAGS = $(HIREDIS_FLAGS)
redis_commands_test_LDADD = $(TEST_LIB)

statsd_connector_test_SOURCES = testing/statsd_connector_test.cc
statsd_connector_test_LDADD = $(TEST_LIB)

carbon_connector_test_SOURCES = testing/carbon_connector_test.cc
carbon_connector_test_LDADD = $(TEST_LIB)

endpoint_unit_test_SOURCES = testing/endpoint_unit_test.cc
endpoint_unit_test_LDADD = $(TEST_LIB)

test_active_endpoint_nothing_listening_SOURCES = testing/test_active_endpoint_nothing_listening.cc
test_active_endpoint_nothing_listening_LDADD = $(TEST_LIB)

test_active_endpoint_not_responding_SOURCES = testing/test_active_endpoint_not_responding.cc
test_active_endpoint_not_responding_LDADD = $(TEST_LIB)

test_endpoint_ping_pong_SOURCES = testing/test_endpoint_ping_pong.cc
test_endpoint_ping_pong_LDADD = $(TEST_LIB)

test_endpoint_connection_speed_SOURCES = testing/test_endpoint_connection_speed.cc
test_endpoint_connection_speed_LDADD = $(TEST_LIB)

test_endpoint_accept_speed_SOURCES = testing/test_endpoint_accept_speed.cc
test_endpoint_accept_speed_LDADD = $(TEST_LIB)

endpoint_periodic_test_SOURCES = testing/endpoint_periodic_test.cc
endpoint_periodic_test_LDADD = $(TEST_LIB)

endpoint_closed_connection_test_SOURCES = testing/endpoint_closed_connection_test.cc
endpoint_closed_connection_test_LDADD = $(TEST_LIB)

http_long_header_test_SOURCES = testing/http_long_header_test.cc
http_long_header_test_LDADD = $(TEST_LIB)

service_proxies_test_SOURCES = testing/service_proxies_test.cc
service_proxies_test_CXXFLAGS = $(ZMQ_FLAGS)
service_proxies_test_LDADD = $(TEST_LIB) 

zmq_tcp_bench_SOURCES = testing/tcpsockets.cc testing/zmq_tcp_bench.cc
zmq_tcp_bench_CXXFLAGS = $(ZMQ_FLAGS)
zmq_tcp_bench_LDADD = $(TEST_LIB)

node_prefix=$(exec_prefix)/node_modules

install-exec-hook:
	mkdir -p $(node_prefix)
	cp -rf .libs/*.node $(node_prefix)
uninstall-hook:
	for i in (noinst_LTLIBRARIES); do lib=`echo "$$i" | sed 's/\.la/\.node/g'`; rm $(node_prefix)/$$lib; done
	if find "$(node_prefix)" -maxdepth 0 -empty | read; then rm -rf $(node_prefix); fi
