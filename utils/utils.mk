#------------------------------------------------------------------------------#
# utils.mk
# Rémi Attab, 26 Jul 2013
# Copyright (c) 2013 Datacratic.  All rights reserved.
#
# Makefile of soa's misc utilities.
#------------------------------------------------------------------------------#

LIBUTILS_SOURCES := \
        environment.cc \
        file_functions.cc \
        filter_streams.cc \
        string_functions.cc \
        parse_context.cc \
	configuration.cc \
	csv.cc \
	exc_check.cc \
	exc_assert.cc \
	hex_dump.cc \
	lzma.cc \
	floating_point.cc \
	json_parsing.cc \
	rng.cc \
	hash.cc \
	abort.cc

LIBUTILS_LINK :=	arch boost_iostreams lzma boost_thread cryptopp

$(eval $(call library,utils,$(LIBUTILS_SOURCES),$(LIBUTILS_LINK)))

LIBWORKER_TASK_SOURCES := worker_task.cc
LIBWORKER_TASK_LINK    := arch pthread

$(eval $(call library,worker_task,$(LIBWORKER_TASK_SOURCES),$(LIBWORKER_TASK_LINK)))


LIB_TEST_UTILS_SOURCES := \
        fixtures.cc \
        threaded_test.cc

LIB_TEST_UTILS_LINK := \
	arch utils boost_filesystem boost_thread

$(eval $(call library,test_utils,$(LIB_TEST_UTILS_SOURCES),$(LIB_TEST_UTILS_LINK)))

$(eval $(call library,variadic_hash,variadic_hash.cc,cityhash))

$(eval $(call include_sub_make,testing))
