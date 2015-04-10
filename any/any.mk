$(eval $(call library,any,any.cc,value_description))
$(eval $(call include_sub_make,any_testing,testing,any_testing.mk))
