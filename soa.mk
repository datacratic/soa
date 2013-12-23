# SOA makefile

$(eval $(call include_sub_make,math))
$(eval $(call include_sub_make,arch))
$(eval $(call include_sub_make,utils))
$(eval $(call include_sub_make,db))
$(eval $(call include_sub_make,stats))
$(eval $(call include_sub_make,judy))

$(eval $(call include_sub_make,jsoncpp))
$(eval $(call include_sub_make,types))
$(eval $(call include_sub_make,js))
$(eval $(call include_sub_make,sync))
$(eval $(call include_sub_make,sigslot))
$(eval $(call include_sub_make,gc))
$(eval $(call include_sub_make,service))
$(eval $(call include_sub_make,logger))
$(eval $(call include_sub_make,launcher))
