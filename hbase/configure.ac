
#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(nhc, 0.1, folly@fb.com)
AC_CONFIG_SRCDIR([NativeHbaseClient.h])
AC_CONFIG_AUX_DIR([build-aux])
AX_PREFIX_CONFIG_H([nhc-config.h], [nhc], [nhc-config.h])
AC_CONFIG_HEADERS([nhc-config.h])
AC_DEFINE([OPEN_SOURCE], [1], [Always defined for the open source release])

AM_INIT_AUTOMAKE([foreign dist-bzip2])

AC_CONFIG_MACRO_DIR([m4])

AC_PROG_INSTALL
AM_PROG_LIBTOOL

AC_LANG([C++])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_CXX_COMPILE_STDCXX_0X

# Checks for libraries.
AC_CHECK_LIB([glog],[openlog],[],[AC_MSG_ERROR(
             [Please install google-glog library])])
AC_CHECK_LIB([folly],[openlog],[],[AC_MSG_ERROR(
             [Please install the Facebook folly library])])
AC_CHECK_LIB([gflags],[getenv],[],[AC_MSG_ERROR(
             [Please install google-gflags library])])
AC_CHECK_LIB([gtest],[getenv],[],[AC_MSG_ERROR(
             [Please install google-gtest library])])
AC_CHECK_LIB([zookeeper_mt],[getenv],[],[AC_MSG_ERROR(
             [Please install zookeeper library])])

# Checks for header files.
AC_CHECK_HEADER(folly/Likely.h, [], [AC_MSG_ERROR(
[Couldn't find folly, please download from https://github.com/facebook/folly]
)], [])
AC_CHECK_HEADER(zookeeper/zookeeper.h, [], [AC_MSG_ERROR(
[Couldn't find zookeeper, please download the C client library
from http://zookeeper.apache.org/releases.html])], [])

PKG_CHECK_MODULES(THRIFT, "thrift", [], [AC_MSG_ERROR(
                [Couldn't find thrift, please download from \
		http://thrift.apache.org/])], [])
			  

AC_PATH_PROG(THRIFT_EXEC_PATH, "thrift")

CXX_FLAGS="-Wall -Wno-sign-compare -Wno-unused-variable -Winvalid-pch -Wno-deprecated -Woverloaded-virtual -Wno-return-type -Werror"
if test "$ac_cv_cxx_compile_cxx0x_cxx" = yes; then
   CXX_FLAGS="-std=c++0x"
fi
if test "$ac_cv_cxx_compile_cxx0x_gxx" = yes; then
   CXX_FLAGS="-std=gnu++0x"
fi

AC_SUBST(AM_CXXFLAGS, '-I../$(top_srcdir)'" $CXX_FLAGS")

AM_CONDITIONAL([HAVE_STD_THREAD], [test "$ac_cv_header_features" = "yes"])
AM_CONDITIONAL([HAVE_X86_64], [test "$build_cpu" = "x86_64"])

# Output
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
