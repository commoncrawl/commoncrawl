// Copyright 2007, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef GOOGLEURL_SRC_URL_UTIL_H__
#define GOOGLEURL_SRC_URL_UTIL_H__

#include <string>

#include "googleurl/src/url_parse.h"
#include "googleurl/src/url_canon.h"

namespace url_util {

typedef url_parse::UTF16Char UTF16Char;
typedef url_parse::UTF16String UTF16String;

// Schemes --------------------------------------------------------------------

// Adds an application-defined scheme to the internal list of "standard" URL
// schemes.
void AddStandardScheme(const char* new_scheme);

// Locates the scheme in the given string and places it into |found_scheme|,
// which may be NULL to indicate the caller does not care about the range.
// Returns whether the given |compare| scheme matches the scheme found in the
// input (if any).
bool FindAndCompareScheme(const char* str,
                          int str_len,
                          const char* compare,
                          url_parse::Component* found_scheme);
bool FindAndCompareScheme(const UTF16Char* str,
                          int str_len,
                          const char* compare,
                          url_parse::Component* found_scheme);
inline bool FindAndCompareScheme(const std::string& str,
                                 const char* compare,
                                 url_parse::Component* found_scheme) {
  return FindAndCompareScheme(str.data(), static_cast<int>(str.size()),
                              compare, found_scheme);
}
inline bool FindAndCompareScheme(const UTF16String& str,
                                 const char* compare,
                                 url_parse::Component* found_scheme) {
  return FindAndCompareScheme(str.data(), static_cast<int>(str.size()),
                              compare, found_scheme);
}

// Returns true if the given string corresponds to a known scheme in the
// database.
bool IsStandardScheme(const char* scheme, int scheme_len);
bool IsStandardScheme(const UTF16Char* scheme, int scheme_len);
inline bool IsStandardScheme(const std::string& scheme) {
  return IsStandardScheme(scheme.data(), static_cast<int>(scheme.size()));
}
inline bool IsStandardScheme(const UTF16String& scheme) {
  return IsStandardScheme(scheme.data(), static_cast<int>(scheme.size()));
}

// Returns true if the given string represents a standard URL.
bool IsStandard(const char* spec, int spec_len);
bool IsStandard(const UTF16Char* spec, int spec_len);
inline bool IsStandard(const std::string& spec) {
  return IsStandard(spec.data(), static_cast<int>(spec.size()));
}
inline bool IsStandard(const UTF16String& spec) {
  return IsStandard(spec.data(), static_cast<int>(spec.size()));
}

// URL library wrappers -------------------------------------------------------

// Parses the given spec according to the extracted scheme type. Normal users
// should use the URL object, although this may be useful if performance is
// critical and you don't want to do the heap allocation for the std::string.
//
// Returns true if a valid URL was produced, false if not. On failure, the
// output and parsed structures will still be filled and will be consistent,
// but they will not represent a loadable URL.
bool Canonicalize(const char* spec,
                  int spec_len,
                  url_canon::CanonOutput* output,
                  url_parse::Parsed* output_parsed);
bool Canonicalize(const UTF16Char* spec,
                  int spec_len,
                  url_canon::CanonOutput* output,
                  url_parse::Parsed* output_parsed);

// Resolves a potentially relative URL relative to the given parsed base URL.
// The base MUST be valid. The resulting canonical URL and parsed information
// will be placed in to the given out variables.
//
// The relative need not be relative. If we discover that it's absolute, this
// will produce a canonical version of that URL.
//
// Returns true if the output is valid, false if the input could not produce
// a valid URL.
bool ResolveRelative(const char* base_spec,
                     const url_parse::Parsed& base_parsed,
                     const char* relative,
                     int relative_length,
                     url_canon::CanonOutput* output,
                     url_parse::Parsed* output_parsed);
bool ResolveRelative(const char* base_spec,
                     const url_parse::Parsed& base_parsed,
                     const UTF16Char* relative,
                     int relative_length,
                     url_canon::CanonOutput* output,
                     url_parse::Parsed* output_parsed);

// Replaces components in the given VALID input url. The new canonical URL info
// is written to output and out_parsed.
//
// Returns true if the resulting URL is valid.
bool ReplaceComponents(const char* spec,
                       const url_parse::Parsed& parsed,
                       const url_canon::Replacements<char>& replacements,
                       url_canon::CanonOutput* output,
                       url_parse::Parsed* out_parsed);
bool ReplaceComponents(const char* spec,
                       const url_parse::Parsed& parsed,
                       const url_canon::Replacements<UTF16Char>& replacements,
                       url_canon::CanonOutput* output,
                       url_parse::Parsed* out_parsed);

// String helper functions ----------------------------------------------------

// Compare the lower-case form of the given string against the given ASCII
// string.  This is useful for doing checking if an input string matches some
// token, and it is optimized to avoid intermediate string copies.
//
// The versions of this function that don't take a b_end assume that the b
// string is NULL terminated.
bool LowerCaseEqualsASCII(const char* a_begin,
                          const char* a_end,
                          const char* b);
bool LowerCaseEqualsASCII(const char* a_begin,
                          const char* a_end,
                          const char* b_begin,
                          const char* b_end);
bool LowerCaseEqualsASCII(const UTF16Char* a_begin,
                          const UTF16Char* a_end,
                          const char* b);

}  // namespace url_util

#endif  // GOOGLEURL_SRC_URL_UTIL_H__
