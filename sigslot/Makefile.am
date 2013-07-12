ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir) $(BOOST_CPPFLAGS) $(V8_CPPFLAGS) $(NODE_FLAGS)

sigslot_include_HEADERS = \
  signal.h \
  slot.h \
  slot_impl_js.h \
  slot_js.h

sigslot_includedir = $(includedir)/soa/sigslot

lib_LTLIBRARIES = libsigslot.la

libsigslot_la_LDFLAGS = -avoid-version

libsigslot_la_SOURCES = \
  slot.cc \
  signal.cc \
  slot_js.cc

libsigslot_la_LIBADD = \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/soa/js/libjs.la

#libsigslot_la_CXXFLAGS = $(NODE_FLAGS) $(BOOST_CPPFLAGS)

AM_CXXFLAGS = 
AM_CFLAGS = 

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

noinst_LTLIBRARIES = sigslot.la \
  signal_slot_test_module.la

TESTS_ENVIRONMENT = $(abs_top_builddir)/test_driver.sh NODE=$(NODEJS) VOWS=$(VOWS) NODE_LIBS="$(noinst_LTLIBRARIES)"

TEST_LIB = \
  $(libsigslot_la_LIBADD) \
  $(V8_LIBS) \
  libsigslot.la

sigslot_la_LIBADD = $(TEST_LIB)
sigslot_la_LDFLAGS = $(PLUGIN_LIBTOOL_FLAGS)
sigslot_la_CXXFLAGS = $(NODE_FLAGS) $(BOOST_CPPFLAGS)
sigslot_la_SOURCES = slot_node.cc 

signal_slot_test_module_la_LIBADD = $(TEST_LIB)
signal_slot_test_module_la_LDFLAGS = $(PLUGIN_LIBTOOL_FLAGS)
signal_slot_test_module_la_CXXFLAGS = $(NODE_FLAGS) $(BOOST_CPPFLAGS)
signal_slot_test_module_la_SOURCES = testing/signal_slot_test_module.cc 

CC_TESTS = \
  signal_test \
  slot_test

TESTS = $(CC_TESTS) $(abs_top_builddir)/runjstest.sh

check_PROGRAMS = $(CC_TESTS)

signal_test_SOURCES = testing/signal_test.cc
signal_test_LDADD = $(BOOST_UNIT_TEST_FRAMEWORK_LIB) $(TEST_LIB)

slot_test_SOURCES = testing/slot_test.cc
slot_test_LDADD = $(BOOST_UNIT_TEST_FRAMEWORK_LIB) $(TEST_LIB)

node_prefix=$(exec_prefix)/node_modules

install-exec-hook:
	mkdir -p $(node_prefix)
	cp -rf .libs/*.node $(node_prefix) && rm -rf $(node_prefix)/*_test_module*
uninstall-hook:
	for i in $(noinst_LTLIBRARIES); do lib=`echo "$$i" | sed 's/\.la/\.node/g'`; rm -rf $(node_prefix)/$$lib; done
	if find "$(node_prefix)" -maxdepth 0 -empty | read; then rm -rf $(node_prefix); fi
