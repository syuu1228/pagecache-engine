#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT(pagecache-engine, 0.1, [syuu@dokukino.com])
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_SRCDIR([src/pagecache_engine.c])
AC_CONFIG_HEADERS([config.h])

AC_CHECK_HEADERS([memcached/engine.h])
AS_IF([test "x$ac_cv_header_memcached_engine_h" = "xno"],
      [AC_MSG_ERROR([You need to install the engine interface to build engines])])

# Checks for programs.
AC_PROG_CXX
AC_PROG_AWK
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB
AC_PROG_LIBTOOL

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h inttypes.h netinet/in.h stddef.h stdlib.h string.h sys/socket.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_CHECK_FUNCS([memset munmap strerror])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
