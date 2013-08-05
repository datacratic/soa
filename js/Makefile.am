ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir) $(BOOST_CPPFLAGS) $(V8_CPPFLAGS)

js_include_HEADERS = \
  js_call_fwd.h \
  js_call.h \
  js_registry.h \
  js_utils.h \
  js_value_fwd.h \
  js_value.h \
  js_wrapped.h

js_includedir = $(includedir)/soa/js

lib_LTLIBRARIES = libjs.la

libjs_la_LDFLAGS = -avoid-version

libjs_la_SOURCES = \
  js_wrapped.cc \
  js_value.cc \
  js_utils.cc \
  js_registry.cc \
  js_call.cc

libjs_la_LIBADD = \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(abs_top_builddir)/soa/types/libtypes.la \
  $(abs_top_builddir)/jml/arch/libnode_exception_tracing.la \
  $(V8_LIBS)

libjs_la_CXXFLAGS = $(NODE_FLAGS)

AM_CXXFLAGS = 
AM_CFLAGS = 

TEST_PLUGIN_LIBTOOL_FLAGS = \
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

noinst_LTLIBRARIES = v8_inheritance_module.la \
  js_exception_passing_module.la \
  js2json_module.la \
  js_variable_arity_module.la

TESTS_ENVIRONMENT = $(abs_top_builddir)/test_driver.sh NODE=$(NODEJS) VOWS=$(VOWS) NODE_LIBS="$(noinst_LTLIBRARIES)"

TEST_LIB = \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/jml/utils/libutils.la \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(abs_top_builddir)/soa/types/libtypes.la \
  $(abs_top_builddir)/jml/arch/libnode_exception_tracing.la \
  $(V8_LIBS) \
  libjs.la

v8_inheritance_module_la_LIBADD = $(TEST_LIB)
v8_inheritance_module_la_LDFLAGS = $(TEST_PLUGIN_LIBTOOL_FLAGS)
v8_inheritance_module_la_CXXFLAGS = $(NODE_FLAGS)
v8_inheritance_module_la_SOURCES = testing/v8_inheritance_module.cc

js_exception_passing_module_la_LIBADD = $(TEST_LIB)
js_exception_passing_module_la_CXXFLAGS = $(NODE_FLAGS)
js_exception_passing_module_la_LDFLAGS = $(TEST_PLUGIN_LIBTOOL_FLAGS)
js_exception_passing_module_la_SOURCES = testing/js_exception_passing_module.cc

js2json_module_la_LIBADD = $(TEST_LIB)
js2json_module_la_LDFLAGS = $(TEST_PLUGIN_LIBTOOL_FLAGS)
js2json_module_la_CXXFLAGS = $(NODE_FLAGS)
js2json_module_la_SOURCES = testing/js2json_module.cc

js_variable_arity_module_la_LIBADD = $(TEST_LIB)
js_variable_arity_module_la_LDFLAGS = $(TEST_PLUGIN_LIBTOOL_FLAGS) -shrext .node
js_variable_arity_module_la_CXXFLAGS = $(NODE_FLAGS)
js_variable_arity_module_la_SOURCES = testing/js_variable_arity_module.cc

CC_TESTS = \
  js_call_test

TESTS = $(CC_TESTS) $(abs_top_builddir)/runjstest.sh

check_PROGRAMS = $(CC_TESTS)

js_call_test_SOURCES = testing/js_call_test.cc
js_call_test_LDADD = $(libjs_la_LIBADD) libjs.la
