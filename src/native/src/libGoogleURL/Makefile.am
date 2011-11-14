# The 'vpath directive' to locate the actual source files 
# vpath %.cc $(GOOGLE_URL_SRC_ROOT)/src

AM_CPPFLAGS = -m$(JVM_DATA_MODEL) @JNI_CPPFLAGS@ -I $(SRC_ROOT)/src/libGoogleURL/googleurl -I $(SRC_ROOT)/src/libGoogleURL/googleurl/icu38.1/includes -I $(SRC_ROOT)/src/libGoogleURL -I $(BUILD_DIR)/src/libGoogleURL
AM_LDFLAGS = @JNI_LDFLAGS@ -m$(JVM_DATA_MODEL)
AM_CFLAGS = -g -Wall -fPIC -O2 -m$(JVM_DATA_MODEL)

pkglib_LTLIBRARIES = libGoogleURL_jni.la
libGoogleURL_jni_la_SOURCES = GoogleURLJNI.cc googleurl/src/gurl.cc googleurl/src/url_canon_etc.cc googleurl/src/url_canon_fileurl.cc googleurl/src/url_canon_host.cc googleurl/src/url_canon_icu.cc googleurl/src/url_canon_internal.cc googleurl/src/url_canon_ip.cc googleurl/src/url_canon_path.cc googleurl/src/url_canon_pathurl.cc googleurl/src/url_canon_query.cc googleurl/src/url_canon_relative.cc googleurl/src/url_canon_stdurl.cc googleurl/src/url_parse.cc googleurl/src/url_parse_file.cc googleurl/src/url_util.cc
libGoogleURL_jni_la_LDFLAGS = @JNI_LDFLAGS@ -m$(JVM_DATA_MODEL) -version-info 1:0:0
libGoogleURL_jni_la_LIBADD  = @LTLIBOBJS@ @JNI_LIB@ -licudata -licui18n -licuio -licutu -licuuc
# libGoogleURL_jni_la_LIBTOOLFLAGS = -dynamic
