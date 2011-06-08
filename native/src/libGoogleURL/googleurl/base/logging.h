// Copyright 2008 Google Inc. All Rights Reserved.

#ifndef BASE_LOGGING_H__
#define BASE_LOGGING_H__

// BEGIN Google Gears changes

#include <assert.h>
#include <string>

// Try to emulate the actual DCHECK macro by ending with a "stream" object
// so you can write DCHECK(condition) << L"Oh noes, condition is false";
#define DCHECK(cond) \
  assert(cond); \
  FakeOStream()

// Used in place of a std::ostream.  We don't want to pull in streaming code
// for logging messages that we don't care about.
class FakeOStream {
 public:
  inline FakeOStream& operator<<(const char* wstr) {
    return *this;
  }
  inline FakeOStream& operator<<(const wchar_t* wstr) {
    return *this;
  }
};
// END Google Gears changes

#endif  // BASE_LOGGING_H__
