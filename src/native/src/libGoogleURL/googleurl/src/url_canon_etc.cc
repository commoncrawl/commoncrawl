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

// Canonicalizers for random bits that aren't big enough for their own files.

#include <string.h>

#include "googleurl/src/url_canon.h"
#include "googleurl/src/url_canon_internal.h"

namespace url_canon {

namespace {

// Returns true if the given character should be removed from the middle of a
// URL.
inline bool IsRemovableURLWhitespace(int ch) {
  return ch == '\r' || ch == '\n' || ch == '\t';
}

// Backend for RemoveURLWhitespace (see declaration in url_canon.h).
// It sucks that we have to do this, since this takes about 13% of the total URL
// canonicalization time.
template<typename CHAR>
const CHAR* DoRemoveURLWhitespace(const CHAR* input, int input_len,
                                  CanonOutputT<CHAR>* buffer,
                                  int* output_len) {
  // Fast verification that there's nothing that needs removal. This is the 99%
  // case, so we want it to be fast and don't care about impacting the speed
  // when we do find whitespace.
  int found_whitespace = false;
  for (int i = 0; i < input_len; i++) {
    if (!IsRemovableURLWhitespace(input[i]))
      continue;
    found_whitespace = true;
    break;
  }

  if (!found_whitespace) {
    // Didn't find any whitespace, we don't need to do anything. We can just
    // return the input as the output.
    *output_len = input_len;
    return input;
  }

  // Remove the whitespace into the new buffer and return it.
  for (int i = 0; i < input_len; i++) {
    if (!IsRemovableURLWhitespace(input[i]))
      buffer->push_back(input[i]);
  }
  *output_len = buffer->length();
  return buffer->data();
}

// Contains the canonical version of each possible input letter in the scheme
// (basically, lower-cased). The corresponding entry will be 0 if the letter
// is not allowed in a scheme.
const char kSchemeCanonical[0x80] = {
// 00-1f: all are invalid
     0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
     0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
//  ' '   !    "    #    $    %    &    '    (    )    *    +    ,    -    .    /
     0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  '+',  0,  '-', '.',  0,
//   0    1    2    3    4    5    6    7    8    9    :    ;    <    =    >    ?
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',  0 ,  0 ,  0 ,  0 ,  0 ,  0 ,
//   @    A    B    C    D    E    F    G    H    I    J    K    L    M    N    O
     0 , 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
//   P    Q    R    S    T    U    V    W    X    Y    Z    [    \    ]    ^    _
    'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',  0,   0 ,  0,   0 ,  0,
//   `    a    b    c    d    e    f    g    h    i    j    k    l    m    n    o
     0 , 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
//   p    q    r    s    t    u    v    w    x    y    z    {    |    }    ~    
    'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',  0 ,  0 ,  0 ,  0 ,  0 };

// This could be a table lookup as well by setting the high bit for each
// valid character, but it's only called once per URL, and it makes the lookup
// table easier to read not having extra stuff in it.
inline bool IsSchemeFirstChar(unsigned char c) {
  return (c >= 'a' && c < 'z') || (c >= 'A' && c < 'Z');
}

template<typename CHAR, typename UCHAR>
bool DoScheme(const CHAR* spec,
              const url_parse::Component& scheme,
              CanonOutput* output,
              url_parse::Component* out_scheme) {
  if (scheme.len <= 0) {
    // Scheme is unspecified or empty, convert to empty by appending a colon.
    *out_scheme = url_parse::Component(output->length(), 0);
    output->push_back(':');
    return true;
  }

  // The output scheme starts from the current position.
  out_scheme->begin = output->length();

  bool success = true;
  int end = scheme.end();
  for (int i = scheme.begin; i < end; i++) {
    UCHAR ch = static_cast<UCHAR>(spec[i]);
    char replacement = 0;
    if (ch < 0x80) {
      if (i == scheme.begin) {
        // Need to do a special check for the first letter of the scheme.
        if (IsSchemeFirstChar(static_cast<unsigned char>(ch)))
          replacement = kSchemeCanonical[ch];
      } else {
        replacement = kSchemeCanonical[ch];
      }
    }

    if (replacement) {
      output->push_back(replacement);
    } else {
      // Invalid character, store it but mark this scheme as invalid.
      success = false;

      // This will escape the output and also handle encoding issues.
      // Ignore the return value since we already failed.
      AppendUTF8EscapedChar(spec, &i, end, output);
    }
  }

  // The output scheme ends with the the current position, before appending
  // the colon.
  out_scheme->len = output->length() - out_scheme->begin;
  output->push_back(':');
  return success;
}

// The username and password components reference ranges in the corresponding
// *_spec strings. Typically, these specs will be the same (we're
// canonicalizing a single source string), but may be different when
// replacing components.
template<typename CHAR, typename UCHAR>
bool DoUserInfo(const CHAR* username_spec,
                const url_parse::Component& username,
                const CHAR* password_spec,
                const url_parse::Component& password,
                CanonOutput* output,
                url_parse::Component* out_username,
                url_parse::Component* out_password) {
  if (username.len <= 0 && password.len <= 0) {
    // Common case: no user info. We strip empty username/passwords.
    *out_username = url_parse::Component();
    *out_password = url_parse::Component();
    return true;
  }

  // TODO(brettw) bug 735548: we should be checking that the user info
  // characters are allowed in the input.

  // Write the username.
  out_username->begin = output->length();
  if (username.len > 0) {
    AppendInvalidNarrowString(username_spec, username.begin, username.end(),
                              output);
  }
  out_username->len = output->length() - out_username->begin;

  // When there is a password, we need the separator. Note that we strip
  // empty but specified passwords.
  if (password.len > 0) {
    output->push_back(':');
    out_password->begin = output->length();
    AppendInvalidNarrowString(password_spec, password.begin, password.end(),
                              output);
    out_password->len = output->length() - out_password->begin;
  } else {
    *out_password = url_parse::Component();
  }

  output->push_back('@');
  return true;
}

// Helper functions for converting port integers to strings.
inline void WritePortInt(char* output, int output_len, int port) {
  _itoa_s(port, output, output_len, 10);
}
inline void WritePortInt(UTF16Char* output, int output_len, int port) {
  _itow_s(port, output, output_len, 10);
}

// This function will prepend the colon if there will be a port.
template<typename CHAR, typename UCHAR>
bool DoPort(const CHAR* spec,
            const url_parse::Component& port,
            int default_port_for_scheme,
            CanonOutput* output,
            url_parse::Component* out_port) {
  int port_num = url_parse::ParsePort(spec, port);
  if (port_num == url_parse::PORT_UNSPECIFIED ||
      port_num == default_port_for_scheme) {
    *out_port = url_parse::Component();
    return true;  // Leave port empty.
  }

  if (port_num == url_parse::PORT_INVALID) {
    // Invalid port: We'll copy the text from the input so the user can see
    // what the error was, and mark the URL as invalid by returning false.
    output->push_back(':');
    out_port->begin = output->length();
    AppendInvalidNarrowString(spec, port.begin, port.end(), output);
    out_port->len = output->length() - out_port->begin;
    return false;
  }

  // Convert port number back to an integer. Max port value is 5 digits, and
  // the Parsed::ExtractPort will have made sure the integer is in range.
  const int buf_size = 6;
  char buf[buf_size];
  WritePortInt(buf, buf_size, port_num);

  // Append the port number to the output, preceeded by a colon.
  output->push_back(':');
  out_port->begin = output->length();
  for (int i = 0; i < buf_size && buf[i]; i++)
    output->push_back(buf[i]);

  out_port->len = output->length() - out_port->begin;
  return true;
}

// Like ConvertUTF?ToUTF? except this is a UTF-8 -> UTF-8 converter. We'll
// validate the input, and use the unicode replacement character for invalid
// input, and append the result to the output.
bool CopyAndValidateUTF8(const char* input, int input_len,
                         CanonOutput* output) {
  bool success = true;
  for (int i = 0; i < input_len; i++) {
    unsigned code_point;
    success &= ReadUTFChar(input, &i, input_len, &code_point);
    AppendUTF8Value(code_point, output);
  }
  return success;
}

}  // namespace

const char* RemoveURLWhitespace(const char* input, int input_len,
                                CanonOutputT<char>* buffer,
                                int* output_len) {
  return DoRemoveURLWhitespace(input, input_len, buffer, output_len);
}

const UTF16Char* RemoveURLWhitespace(const UTF16Char* input, int input_len,
                                     CanonOutputT<UTF16Char>* buffer,
                                     int* output_len) {
  return DoRemoveURLWhitespace(input, input_len, buffer, output_len);
}

char CanonicalSchemeChar(UTF16Char ch) {
  if (ch >= 0x80)
    return 0;  // Non-ASCII is not supported by schemes.
  return kSchemeCanonical[ch];
}

bool CanonicalizeScheme(const char* spec,
                        const url_parse::Component& scheme,
                        CanonOutput* output,
                        url_parse::Component* out_scheme) {
  return DoScheme<char, unsigned char>(spec, scheme, output, out_scheme);
}

bool CanonicalizeScheme(const UTF16Char* spec,
                        const url_parse::Component& scheme,
                        CanonOutput* output,
                        url_parse::Component* out_scheme) {
  return DoScheme<UTF16Char, UTF16Char>(spec, scheme, output, out_scheme);
}

bool CanonicalizeUserInfo(const char* username_source,
                          const url_parse::Component& username,
                          const char* password_source,
                          const url_parse::Component& password,
                          CanonOutput* output,
                          url_parse::Component* out_username,
                          url_parse::Component* out_password) {
  return DoUserInfo<char, unsigned char>(
      username_source, username, password_source, password,
      output, out_username, out_password);
}

bool CanonicalizeUserInfo(const UTF16Char* username_source,
                          const url_parse::Component& username,
                          const UTF16Char* password_source,
                          const url_parse::Component& password,
                          CanonOutput* output,
                          url_parse::Component* out_username,
                          url_parse::Component* out_password) {
  return DoUserInfo<UTF16Char, UTF16Char>(
      username_source, username, password_source, password,
      output, out_username, out_password);
}

bool CanonicalizePort(const char* spec,
                      const url_parse::Component& port,
                      int default_port_for_scheme,
                      CanonOutput* output,
                      url_parse::Component* out_port) {
  return DoPort<char, unsigned char>(spec, port,
                                     default_port_for_scheme,
                                     output, out_port);
}

bool CanonicalizePort(const UTF16Char* spec,
                      const url_parse::Component& port,
                      int default_port_for_scheme,
                      CanonOutput* output,
                      url_parse::Component* out_port) {
  return DoPort<UTF16Char, UTF16Char>(spec, port, default_port_for_scheme,
                                      output, out_port);
}

// We don't do anything fancy with refs, we just validate that the input is
// valid UTF-8 and return.
bool CanonicalizeRef(const char* spec,
                     const url_parse::Component& ref,
                     CanonOutput* output,
                     url_parse::Component* out_ref) {
  if (ref.len < 0) {
    // Common case of no ref.
    *out_ref = url_parse::Component();
    return true;
  }

  // Append the ref separator. Note that we need to do this even when the ref
  // is empty but present.
  output->push_back('#');
  out_ref->begin = output->length();

  bool success = CopyAndValidateUTF8(&spec[ref.begin], ref.len, output);
  out_ref->len = output->length() - out_ref->begin;
  return success;
}

// 16-bit-character refs need to get converted to UTF-8.
bool CanonicalizeRef(const UTF16Char* spec,
                     const url_parse::Component& ref,
                     CanonOutput* output,
                     url_parse::Component* out_ref) {
  if (ref.len < 0) {
    // Common case of no ref.
    *out_ref = url_parse::Component();
    return true;
  }

  // Append the ref separator. Note that we need to do this even when the ref
  // is empty but present.
  output->push_back('#');
  out_ref->begin = output->length();

  // The handy-dandy conversion function will validate the UTF-16 and convert
  // to UTF-8 for us!
  bool success = ConvertUTF16ToUTF8(&spec[ref.begin], ref.len, output);
  out_ref->len = output->length() - out_ref->begin;
  return success;
}

}  // namespace url_canon
