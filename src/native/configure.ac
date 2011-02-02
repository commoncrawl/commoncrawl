#
# configure.ac for common crawl native code 
#

# Notes: 
# 1. This configure.ac depends on the following environment variables to function correctly:
#    * HADOOP_NATIVE_SRCDIR 
#    * JAVA_HOME
#    * JVM_DATA_MODEL
#    * OS_NAME
#    * OS_ARCH 
#    All these are setup by build.xml. 

#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.
#

AC_INIT([commoncrawl],[1.0.0])

CC_BUILD_ROOT=$BUILD_DIR

AC_SUBST(CC_BUILD_ROOT)

AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/org_commoncrawl_native.h])
AC_CONFIG_AUX_DIR(config)
AC_CONFIG_HEADER([config.h])

AM_INIT_AUTOMAKE([-Wall -Werror foreign])




# Checks for programs.
AC_PROG_CC
AC_PROG_CXX
AC_PROG_LIBTOOL

# Checks for libraries.
dnl Check for '-ldl'
AC_CHECK_LIB([dl], [dlopen])

dnl Check for '-ljvm'
JNI_LDFLAGS=""
JNI_LIB="-ljvm"

if test -e /System/Library/Frameworks/JavaVM.framework ; then
	JNI_LDFLAGS="-export-dynamic -L/System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Libraries"
else
	if test $JAVA_HOME != ""
	then
		JNI_LDFLAGS="-L$JAVA_HOME/jre/lib/$OS_ARCH/server"
	fi
fi
ldflags_bak=$LDFLAGS
LDFLAGS="$LDFLAGS $JNI_LDFLAGS"
AC_CHECK_LIB([jvm], [JNI_GetCreatedJavaVMs])
LDFLAGS=$ldflags_bak
AC_SUBST([JNI_LDFLAGS])
AC_SUBST([JNI_LIB])

# Checks for header files.
dnl Check for Ansi C headers
AC_HEADER_STDC

dnl Check for other standard C headers
AC_CHECK_HEADERS([stdio.h stddef.h], [], AC_MSG_ERROR(Some system headers not found... please ensure their presence on your platform.))

dnl Check for JNI headers
JNI_CPPFLAGS=""
#JNI_CPPFLAGS="arch=x86_64 "

if test $JAVA_HOME != ""; then
  if test -e $JAVA_HOME/../Headers; then
    JNI_CPPFLAGS="$JNI_CPPFLAGS -I$JAVA_HOME/../Headers"
        echo JNI_CPP_FLAGS ARE $JNI_CPPFLAGS
  else
    for dir in `find $JAVA_HOME/include -follow -type d`
      do
        echo Including Java Dir:$dir
        JNI_CPPFLAGS="$JNI_CPPFLAGS -I$dir"
      done
  fi
fi
cppflags_bak=$CPPFLAGS
CPPFLAGS="$CPPFLAGS $JNI_CPPFLAGS"
echo CPPFLAGS ARE:$CPPFLAGS
AC_CHECK_HEADERS([jni.h], [], AC_MSG_ERROR([Native java headers not found. Is \$JAVA_HOME set correctly?]))
CPPFLAGS=$cppflags_bak
AC_SUBST([JNI_CPPFLAGS])



# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST

# Checks for library functions.
AC_CHECK_FUNCS([memset])

AC_CONFIG_FILES([Makefile
		 src/libGoogleURL/Makefile
                 ])
AC_OUTPUT

#
#vim: sw=2: ts=2: noet
#
