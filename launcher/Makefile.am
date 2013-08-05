ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = \
  -I $(abs_top_builddir) $(BOOST_CPPFLAGS)

bin_PROGRAMS = launcher

launcher_SOURCES = launcher.cc
launcher_LDADD = \
  $(BOOST_PROGRAM_OPTIONS_LIB) \
  $(BOOST_SYSTEM_LIB) \
  $(abs_top_builddir)/jml/arch/libarch.la \
  $(abs_top_builddir)/soa/jsoncpp/libjsoncpp.la \
  $(abs_top_builddir)/soa/service/libservices.la
