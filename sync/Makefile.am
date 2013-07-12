ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir) $(BOOST_CPPFLAGS)

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

noinst_LTLIBRARIES = sync.la sync_utils.la

sync_la_LIBADD = $(abs_top_builddir)/soa/js/libjs.la
sync_la_LDFLAGS = $(PLUGIN_LIBTOOL_FLAGS)
sync_la_CXXFLAGS = $(NODE_FLAGS)
sync_la_SOURCES = sync_js.cc

sync_utils_la_LIBADD = $(abs_top_builddir)/soa/js/libjs.la
sync_utils_la_LDFLAGS = $(PLUGIN_LIBTOOL_FLAGS)
sync_utils_la_CXXFLAGS = $(NODE_FLAGS)
sync_utils_la_SOURCES = utils_js.cc

TESTS_ENVIRONMENT = $(abs_top_builddir)/test_driver.sh NODE=$(NODEJS) VOWS=$(VOWS) NODE_LIBS="$(noinst_LTLIBRARIES)"

TESTS = $(abs_top_builddir)/runjstest.sh

node_prefix=$(exec_prefix)/node_modules

install-exec-hook:
	mkdir -p $(node_prefix)
	cp -rf .libs/*.node $(node_prefix)
uninstall-hook:
	for i in $(noinst_LTLIBRARIES); do lib=`echo "$$i" | sed 's/\.la/\.node/g'`; rm $(node_prefix)/$$lib; done
	if find "$(node_prefix)" -maxdepth 0 -empty | read; then rm -rf $(node_prefix); fi
