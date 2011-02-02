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

#include "googleurl/src/url_canon.h"
#include "googleurl/src/url_canon_internal.h"

namespace url_canon {

namespace {

// For reference, here's what IE supports:
// Key: 0 (disallowed: failure if present in the input)
//      + (allowed either escaped or unescaped, and unmodified)
//      U (allowed escaped or unescaped but always unescaped if present in
//         escaped form)
//      E (allowed escaped or unescaped but always escaped if present in
//         unescaped form)
//      % (only allowed escaped in the input, will be unmodified).
//      I left blank alpha numeric characters.
// 
//    00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f
//    -----------------------------------------------
// 0   0  E  E  E  E  E  E  E  E  E  E  E  E  E  E  E
// 1   E  E  E  E  E  E  E  E  E  E  E  E  E  E  E  E
// 2   E  +  E  E  +  E  +  +  +  +  +  +  +  U  U  0
// 3                                 %  %  E  +  E  0  <-- Those are  : ; < = > ?
// 4   %
// 5                                    U  0  U  U  U  <-- Those are  [ \ ] ^ _
// 6   E                                               <-- That's  `
// 7                                    E  E  E  U  E  <-- Those are { | } ~ (UNPRINTABLE)
//
// NOTE: I didn't actually test all the control characters. Some may be
// disallowed in the input, but they are all accepted escaped except for 0.
// I also didn't test if characters affecting HTML parsing are allowed
// unescaped, eg. (") or (#), which would indicate the beginning of the path.
// Surprisingly, space is accepted in the input and always escaped.

// This table lists the canonical version of all characters we allow in the
// input, with 0 indicating it is disallowed. We are more restricive than IE,
// but less restrictive than Firefox, and we only have two modes: either the
// character is allowed and it is unescaped if escaped in the input, or it is
// disallowed and we will prohibit it.
//
// Space is a special case, IE always escapes space, and some sites actually
// use it, so we want to support it. We try to duplicate IE's behavior by treating
// space as valid and unescaping it, and then doing a separate pass at the end of
// canonicalization that looks for spaces. We'll then escape them at that point.
const char kHostCharLookup[0x80] = {
// 00-1f: all are invalid
     0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
     0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,
//  ' '   !    "    #    $    %    &    '    (    )    *    +    ,    -    .    /
//  ' ',  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  '+',  0,  '-', '.',  0, // ORIGINAL 
     0,  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  '+',  0,  '-', '.',  0, // REPLACEMENT - INVALIDATE SPACE - PER SPECIFICATION   
//   0    1    2    3    4    5    6    7    8    9    :    ;    <    =    >    ?
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',  0 ,  0 ,  0 ,  0 ,  0 ,  0 ,
//   @    A    B    C    D    E    F    G    H    I    J    K    L    M    N    O
     0 , 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
//   P    Q    R    S    T    U    V    W    X    Y    Z    [    \    ]    ^    _
    'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '[',  0 , ']',  0 , '_',
//   `    a    b    c    d    e    f    g    h    i    j    k    l    m    n    o
     0 , 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
//   p    q    r    s    t    u    v    w    x    y    z    {    |    }    ~    
    'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',  0 ,  0 ,  0 ,  0 ,  0 };

const int kTempHostBufferLen = 1024;
typedef RawCanonOutputT<char, kTempHostBufferLen> StackBuffer;
typedef RawCanonOutputT<UTF16Char, kTempHostBufferLen> StackBufferW;

// Scans a host name and fills in the output flags according to what we find.
// |has_non_ascii| will be true if there are any non-7-bit characters, and
// |has_escaped| will be true if there is a percent sign.
template<typename CHAR, typename UCHAR>
void ScanHostname(const CHAR* spec, const url_parse::Component& host,
                  bool* has_non_ascii, bool* has_escaped, bool* has_space) {
  int end = host.end();
  *has_non_ascii = false;
  *has_escaped = false;
  *has_space = false;
  for (int i = host.begin; i < end; i++) {
    // This branch is normally taken and will be predicted very well. Testing
    // shows that is is slightly faster to eliminate all the "normal" common
    // characters here and fall through below to find out exactly which one
    // failed.
    if (static_cast<UCHAR>(spec[i]) < 0x80 && spec[i] != '%' && spec[i] != ' ')
      continue;

    if (static_cast<UCHAR>(spec[i]) >= 0x80)
      *has_non_ascii = true;
    else if (spec[i] == '%')
      *has_escaped = true;
    else if (spec[i] == ' ')
      *has_space = true;
  }
}

// Considers the current contents of the output and sees if it looks like an
// IP address. This is called because we canonicalize to the output assuming
// that it's not an IP address, and now need to fix it if we produced one.
//
// The generated hostname is identified by |host|. The output will be fixed
// with a canonical IP address if the host looks like one. Otherwise, there
// will be no change.
void InterpretIPAddress(const url_parse::Component& host,
                        CanonOutput* output) {
  // Canonicalize the IP address in the output to this temporary buffer.
  // IP addresses are small, so this should not cause an allocation.
  RawCanonOutput<64> canon_ip;
  url_parse::Component out_host;  // Unused.
  if (CanonicalizeIPAddress(output->data(), host, &canon_ip, &out_host)) {
    // Looks like an IP address, overwrite the existing host with the newly
    // canonicalized IP address.
    output->set_length(host.begin);
    output->Append(canon_ip.data(), canon_ip.length());
  }
}

// Unescapes all escaped characters in the input, writing the result to
// |*unescaped| and the output length in |*unescaped_len|.
//
// This does validity checking of 7-bit characters based on the above table,
// and allows all characters with the high bit set (UTF-8, hopefully).
//
// Returns true on success. On failure, |*unescaped| and |*unescaped_len|
// will still be consistent & valid, just the contents will be meaningless.
// The caller should return failure in this case.
//
// |*has_non_ascii| will be set according to if there are any non-8-bit
// values in the unescaped output.
bool UnescapeAndValidateHost(const char* src, int src_len,
                             CanonOutput* unescaped, bool* has_non_ascii) {
  bool success = true;
  *has_non_ascii = false;

  for (int i = 0; i < src_len; i++) {
    char ch = static_cast<char>(src[i]);
    if (ch == '%') {
      if (!DecodeEscaped(src, &i, src_len, &ch)) {
        // Invalid escaped character, there is nothing that can make this
        // host valid. We append an escaped percent so the URL looks reasonable
        // and mark as failed.
        AppendEscapedChar('%', unescaped);
        success = false;
        continue;
      }
      // The unescaped character will now be in |ch|.
    }

    if (static_cast<unsigned char>(ch) >= 0x80) {
      // Pass through all high-bit characters so we don't mangle UTF-8. Set the
      // flag so the caller knows it should fix the non-ASCII characters.
      unescaped->push_back(ch);
      *has_non_ascii = true;
    } else {
      // Use the lookup table to canonicalize this ASCII value.
      char replacement = kHostCharLookup[ch];
      if (!replacement) {
        // Invalid character, add it as percent-escaped and mark as failed.
        AppendEscapedChar(ch, unescaped);
        success = false;
      } else {
        // Common case, the given character is valid in a hostname, the lookup
        // table tells us the canonical representation of that character (lower
        // cased).
        unescaped->push_back(replacement);
      }
    }
  }
  return success;
}

// Canonicalizes a host name assuming the input is 7-bit ASCII and requires
// no unescaping. This is the most common case so it should be fast. We convert
// to 8-bit by static_cast (input may be 16-bit) and check for validity.
//
// The return value will be false if there are invalid host characters.
template<typename CHAR>
bool DoSimpleHost(const CHAR* host, int host_len, CanonOutput* output) {
  // First check if the host name is an IP address.
  url_parse::Component out_ip;  // Unused: we compute the size ourselves later.
  if (CanonicalizeIPAddress(host, url_parse::Component(0, host_len),
                            output, &out_ip))
    return true;

  bool success = true;
  for (int i = 0; i < host_len; i++) {
    // Find the replacement character (lower case for letters, the same as the
    // input if no change is required).
    char source = static_cast<char>(host[i]);
    char replacement = kHostCharLookup[source];
    if (!replacement) {
      // Invalid character, add it as percent-escaped and mark as failed.
      AppendEscapedChar(source, output);
      success = false;
    } else {
      // Common case, the given character is valid in a hostname, the lookup
      // table tells us the canonical representation of that character (lower
      // cased).
      output->push_back(replacement);
    }
  }
  return success;
}

// Canonicalizes a host that requires IDN conversion. Returns true on success.
bool DoIDNHost(const UTF16Char* src, int src_len, CanonOutput* output) {
  StackBufferW wide_output;
#if 1  // Google Gears removed this:
  if (!IDNToASCII(src, src_len, &wide_output)) {
#else  // Google Gears added this:
  // Stubbed out since we don't build with the necessary ICU files.
  if (true) {
#endif
    // Some error, give up. This will write some reasonable looking
    // representation of the string to the output.
    AppendInvalidNarrowString(src, 0, src_len, output);
    return false;
  }

  // Now we check the ASCII output like a normal host. This will fail for any
  // invalid characters, including most importantly "%". If somebody does %00
  // as fullwidth, ICU will convert this to ASCII. We don't want to pass this
  // on since it could be interpreted incorrectly.
  //
  // We could unescape at this point, that that could also produce percents
  // or more UTF-8 input, and it gets too complicated. If people want to
  // escape domain names, they will have to use ASCII instead of fullwidth.
  return DoSimpleHost<UTF16Char>(wide_output.data(), wide_output.length(),
                                 output);
}

// 8-bit convert host to its ASCII version: this converts the UTF-8 input to
// UTF-16. The has_escaped flag should be set if the input string requires
// unescaping.
bool DoComplexHost(const char* host, int host_len,
                   bool has_non_ascii, bool has_escaped, CanonOutput* output) {
  // Save the current position in the output. We may write stuff and rewind it
  // below, so we need to know where to rewind to.
  int begin_length = output->length();

  // Points to the UTF-8 data we want to convert. This will either be the
  // input or the unescaped version written to |*output| if necessary.
  const char* utf8_source;
  int utf8_source_len;
  if (has_escaped) {
    // Unescape before converting to UTF-16 for IDN. We write this into the
    // output because it most likely does not require IDNization, and we can
    // save another huge stack buffer. It will be replaced below if it requires
    // IDN. This will also update our non-ASCII flag so we know whether the
    // unescaped input requires IDN.
    if (!UnescapeAndValidateHost(host, host_len, output, &has_non_ascii)) {
      // Error with some escape sequence. We'll call the current output
      // complete. UnescapeAndValidateHost will have written some
      // "reasonable" output.
      return false;
    }

    // Unescaping may have left us with ASCII input, in which case the
    // unescaped version we wrote to output is complete.
    if (!has_non_ascii) {
      // Need to be sure to check for IP addresses in the newly unescaped
      // output. This will fix the output if necessary.
      InterpretIPAddress(url_parse::MakeRange(begin_length, output->length()),
                         output);
      return true;
    }

    // Save the pointer into the data was just converted (it may be appended to
    // other data in the output buffer).
    utf8_source = &output->data()[begin_length];
    utf8_source_len = output->length() - begin_length;
  } else {
    // We don't need to unescape, use input for IDNization later. (We know the
    // input has non-ASCII, or the simple version would have been called
    // instead of us.)
    utf8_source = host;
    utf8_source_len = host_len;
  }

  // Non-ASCII input requires IDN, convert to UTF-16 and do the IDN conversion.
  // Above, we may have used the output to write the unescaped values to, so
  // we have to rewind it to where we started after we convert it to UTF-16.
  StackBufferW utf16;
  if (!ConvertUTF8ToUTF16(utf8_source, utf8_source_len, &utf16)) {
    // In this error case, the input may or may not be the output.
    StackBuffer utf8;
    for (int i = 0; i < utf8_source_len; i++)
      utf8.push_back(utf8_source[i]);
    output->set_length(begin_length);
    AppendInvalidNarrowString(utf8.data(), 0, utf8.length(), output);
    return false;
  }
  output->set_length(begin_length);

  // This will call DoSimpleHost which will do normal ASCII canonicalization
  // and also check for IP addresses in the outpt.
  return DoIDNHost(utf16.data(), utf16.length(), output);
}

// UTF-16 convert host to its ASCII version. The set up is already ready for
// the backend, so we just pass through. The has_escaped flag should be set if
// the input string requires unescaping.
bool DoComplexHost(const UTF16Char* host, int host_len,
                   bool has_non_ascii, bool has_escaped, CanonOutput* output) {
  if (has_escaped) {
    // Yikes, we have escaped characters with wide input. The escaped
    // characters should be interpreted as UTF-8. To solve this problem,
    // we convert to UTF-8, unescape, then convert back to UTF-16 for IDN.
    //
    // We don't bother to optimize the conversion in the ASCII case (which
    // *could* just be a copy) and use the UTF-8 path, because it should be
    // very rare that host names have escaped characters, and it is relatively
    // fast to do the conversion anyway.
    StackBuffer utf8;
    if (!ConvertUTF16ToUTF8(host, host_len, &utf8)) {
      AppendInvalidNarrowString(host, 0, host_len, output);
      return false;
    }

    // Once we convert to UTF-8, we can use the 8-bit version of the complex
    // host handling code above.
    return DoComplexHost(utf8.data(), utf8.length(), has_non_ascii,
                         has_escaped, output);
  }

  // No unescaping necessary, we can safely pass the input to ICU. This
  // function will only get called if we either have escaped or non-ascii
  // input, so it's safe to just use ICU now. Even if the input is ASCII,
  // this function will do the right thing (just slower than we could).
  return DoIDNHost(host, host_len, output);
}

// Takes an otherwise canonicalized hostname in the output buffer starting
// at |host_begin| and ending at the end of |output|. This will do an in-place
// conversion of any spaces to "%20" for IE compatability.
void EscapeSpacesInHost(CanonOutput* output, int host_begin) {
  // First count the number of spaces to see what needs to be done.
  int num_spaces = 0;
  int end = output->length();
  for (int i = host_begin; i < end; i++) {
    if (output->at(i) != ' ') {
    } else {
      num_spaces++;
    }
  }
  if (num_spaces == 0)
    return;  // Common case, nothing to do

  // Resize the buffer so that there's enough room for all the inserted chars.
  // "%20" takes 3 chars, but we delete one for the space we're replacing.
  int num_inserted_characters = num_spaces * 2;
  for (int i = 0; i < num_inserted_characters; i++)
    output->push_back(0);

  // Now do an in-place replacement from the end of the string of all spaces.
  int src = end - 1;
  int dest = src + num_inserted_characters;
  // When src = dest, we're in sync and there are no more spaces.
  while (src >= host_begin && src != dest) {
    char src_char = output->at(src--);
    if (src_char == ' ') {
      output->set(dest--, '0');
      output->set(dest--, '2');
      output->set(dest--, '%');
    } else {
      output->set(dest--, src_char);
    }
  }
}

template<typename CHAR, typename UCHAR>
bool DoHost(const CHAR* spec,
            const url_parse::Component& host,
            CanonOutput* output,
            url_parse::Component* out_host) {
  bool success = true;
  if (host.len <= 0) {
    // Empty hosts don't need anything.
    *out_host = url_parse::Component();
    return true;
  }

  bool has_non_ascii, has_escaped, has_spaces;
  ScanHostname<CHAR, UCHAR>(spec, host, &has_non_ascii, &has_escaped,
                            &has_spaces);

  out_host->begin = output->length();

  if (!has_non_ascii && !has_escaped) {
    success &= DoSimpleHost(&spec[host.begin], host.len, output);

    // Don't look for spaces in the common case that we don't have any.
    if (has_spaces)
      EscapeSpacesInHost(output, out_host->begin);
  } else {
    success &= DoComplexHost(&spec[host.begin], host.len,
                             has_non_ascii, has_escaped, output);
    // We could have had escaped numerals that should now be canonicalized as
    // an IP address. This should be exceedingly rare, it's probably mostly
    // used by scammers.

    // Last, we need to fix up any spaces by escaping them. This must happen
    // after we do everything so spaces get sent through IDN unescaped. We also
    // can't rely on the has_spaces flag we computed above because unescaping
    // could have produced new spaces.
    EscapeSpacesInHost(output, out_host->begin);
  }

  out_host->len = output->length() - out_host->begin;
  return success;
}

}  // namespace

bool CanonicalizeHost(const char* spec,
                      const url_parse::Component& host,
                      CanonOutput* output,
                      url_parse::Component* out_host) {
  return DoHost<char, unsigned char>(spec, host, output, out_host);
}

bool CanonicalizeHost(const UTF16Char* spec,
                      const url_parse::Component& host,
                      CanonOutput* output,
                      url_parse::Component* out_host) {
  return DoHost<UTF16Char, UTF16Char>(spec, host, output, out_host);
}

}  // namespace url_canon
