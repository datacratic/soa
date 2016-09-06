# services makefile
# Jeremy Barnes, 29 May 2012


LIBOPSTATS_SOURCES := \
	statsd_connector.cc carbon_connector.cc stat_aggregator.cc process_stats.cc

LIBOPSTATS_LINK := \
	ACE arch utils boost_thread types

$(eval $(call library,opstats,$(LIBOPSTATS_SOURCES),$(LIBOPSTATS_LINK)))



LIBRECOSET_RUNNERCOMMON_SOURCES := \
	runner_common.cc

LIBRECOSET_RUNNERCOMMON_LINK :=

$(eval $(call library,runner_common,$(LIBRECOSET_RUNNERCOMMON_SOURCES),$(LIBRECOSET_RUNNERCOMMON_LINK)))
$(eval $(call program,runner_helper,runner_common arch))


LIBSERVICES_SOURCES := \
	transport.cc \
	endpoint.cc \
	connection_handler.cc \
	http_endpoint.cc \
	json_endpoint.cc \
	active_endpoint.cc \
	passive_endpoint.cc \
	chunked_http_endpoint.cc \
	epoller.cc \
	epoll_loop.cc \
	http_header.cc \
	port_range_service.cc \
	service_base.cc \
	message_loop.cc \
	loop_monitor.cc \
	named_endpoint.cc \
	async_event_source.cc \
	async_writer_source.cc \
	tcp_client.cc \
	rest_service_endpoint.cc \
	http_named_endpoint.cc \
	rest_request_router.cc \
	rest_request_binding.cc \
	runner.cc \
	curl_wrapper.cc \
	sink.cc \
	openssl_threading.cc \
	http_client.cc \
	http_client_v1.cc \
	http_client_v2.cc \
	http_parsers.cc \
	http_rest_proxy.cc \
	xml_helpers.cc \
	nprobe.cc \
	logs.cc \
	nsq_event_handler.cc \
	event_publisher.cc \
	event_subscriber.cc \
	nsq_client.cc 

LIBSERVICES_LINK := opstats curl boost_regex runner_common ACE arch utils jsoncpp boost_thread types tinyxml2 boost_system value_description crypto

$(eval $(call library,services,$(LIBSERVICES_SOURCES),$(LIBSERVICES_LINK)))
$(eval $(call set_compile_option,runner.cc,-DBIN=\"$(BIN)\"))

$(LIB)/libservices.so: $(BIN)/runner_helper


LIBENDPOINT_SOURCES := \

LIBENDPOINT_LINK := \
	services

$(eval $(call library,endpoint,$(LIBENDPOINT_SOURCES),$(LIBENDPOINT_LINK)))


LIBCLOUD_SOURCES := \
	fs_utils.cc \
	sftp.cc \
	s3.cc \
	sns.cc \
	aws.cc \
	sqs.cc

LIBCLOUD_LINK := crypto++ utils arch types tinyxml2 services ssh2 boost_filesystem value_description


$(eval $(call library,cloud,$(LIBCLOUD_SOURCES),$(LIBCLOUD_LINK)))


LIBREDIS_SOURCES := \
	redis.cc

LIBREDIS_LINK := hiredis utils types boost_thread

$(eval $(call library,redis,$(LIBREDIS_SOURCES),$(LIBREDIS_LINK)))


$(eval $(call program,s3_transfer_cmd,cloud boost_program_options boost_filesystem utils))
$(eval $(call program,s3tee,cloud boost_program_options utils))
$(eval $(call program,s3cp,cloud boost_program_options utils))
$(eval $(call program,s3_multipart_cmd,cloud boost_program_options utils))
$(eval $(call program,syslog_trace,services))
$(eval $(call program,s3cat,cloud boost_program_options utils))
$(eval $(call program,sns_send,cloud boost_program_options utils))

$(eval $(call include_sub_make,service_js,js,service_js.mk))
$(eval $(call include_sub_make,service_testing,testing,service_testing.mk))

