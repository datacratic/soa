#------------------------------------------------------------------------------#
# testing.mk
# Rémi Attab, 26 Jul 2013
# Copyright (c) 2013 Datacratic.  All rights reserved.
#
# Makefile for the tests of soa's utilities.
#------------------------------------------------------------------------------#

$(eval $(call test,fixture_test,test_utils,boost))
$(eval $(call test,variadic_hash_test,variadic_hash,boost))

$(eval $(call test,parse_context_test,utils arch,boost))
$(eval $(call test,configuration_test,utils arch,boost))
$(eval $(call test,environment_test,utils arch,boost))
$(eval $(call test,compact_vector_test,arch,boost))
$(eval $(call test,circular_buffer_test,arch,boost))
$(eval $(call test,lightweight_hash_test,arch utils,boost))
$(eval $(call test,filter_streams_test,arch utils boost_filesystem boost_system,boost))
$(eval $(call test,csv_parsing_test,arch utils,boost))

$(eval $(call test,worker_task_test,worker_task ACE arch boost_thread pthread,boost manual))
$(eval $(call test,json_parsing_test,utils arch,boost))
