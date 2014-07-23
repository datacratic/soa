#------------------------------------------------------------------------------#
# utils.mk
# Rémi Attab, 26 Jul 2013
# Copyright (c) 2013 Datacratic.  All rights reserved.
#
# Makefile of soa's misc utilities.
#------------------------------------------------------------------------------#


LIB_TEST_UTILS_SOURCES := \
        benchmarks.cc \
        fixtures.cc \
        threaded_test.cc

LIB_TEST_UTILS_LINK := \
	arch utils boost_filesystem boost_thread

$(eval $(call library,test_utils,$(LIB_TEST_UTILS_SOURCES),$(LIB_TEST_UTILS_LINK)))

$(eval $(call library,variadic_hash,variadic_hash.cc,cityhash))

PYTHON_UTILS := \
	__init__.py \
	sdb_utils.py \
	zabbix_utils.py \
	proc_stat_json.py \
	auth_proxy.py \
	config.py \
	rest_connector.py \
	filter_streams.py \
	ordered_set.py

$(eval $(call python_module,python_utils,$(PYTHON_UTILS),))

$(eval $(call include_sub_make,py,,utils_py.mk))
$(eval $(call include_sub_make,testing))

