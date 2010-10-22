/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.commoncrawl.util.shared;

import java.util.LinkedList;
import java.util.List;

/**
 * Utility methods for escaping characters in strings.
 *
 * @author Albert Chern
 */
public class EscapeUtils {
    
    /**
     * A backslash, the character used to begin an escape sequence.
     */
    public static final char ESCAPE = '\\';
    
    /**
     * Given a string and a set of characters to escape, returns an escaped
     * version of the string.
     *
     * @see #ESCAPE
     * @see #unescape
     *
     * @param s             the string to escape characters in
     * @param charsToEscape the set of characters that need to be escaped
     *
     * @return an escaped version of the string
     */
    public static String escape(String s, char[] charsToEscape) {
        StringBuilder buf = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ESCAPE || hasChar(charsToEscape, c)) {
                buf.append(ESCAPE);
            }
            buf.append(c);
        }
        return buf.toString();
    }
    
    /**
     * Given an escaped string returned by {@link #escape} and the original set
     * of characters to escape, returns the original string.
     *
     * @see #ESCAPE
     * @see #escape
     *
     * @param s             the escaped string
     * @param charsToEscape the set of characters that need to be escaped
     *
     * @return the original unescaped string
     */
    public static String unescape(String s, char[] charsToEscape) {
        StringBuilder buf = new StringBuilder(s.length());
        boolean inEscapeSequence = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (inEscapeSequence) {
                if (c != ESCAPE && !hasChar(charsToEscape, c)) {
                    throw new IllegalArgumentException(c +
                        " is not a valid escape sequence character");
                }
                buf.append(c);
                inEscapeSequence = false;
            } else if (hasChar(charsToEscape, c)) {
                throw new IllegalArgumentException(c + " must be escaped");
            } else if (c == ESCAPE) {
                inEscapeSequence = true;
            } else {
                buf.append(c);
            }
        }
        if (inEscapeSequence) {
            throw new IllegalArgumentException("Unterminated escape sequence");
        }
        return buf.toString();
    }
    
    /**
     * Checks if a <tt>char[]</tt> contains a particular character.
     *
     * @param chars  the array to search in
     * @param target the <tt>char</tt> to search for
     *
     * @return <tt>true</tt> if <tt>chars</tt> contains <tt>target</tt>, or
     *         <tt>false</tt> otherwise
     */
    private static boolean hasChar(char[] chars, char target) {
        // The escape set will most likely be small, so just loop through it
        for (char c : chars) {
            if (c == target) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Concatenates multiple strings into one string with each string separated
     * by the separator character and any ocurrences of the separator character
     * in the original strings escaped.
     *
     * @see #split
     *
     * @param separator the separator character
     * @param strings   the strings to concatenate
     *
     * @return the original strings concatenated and separated by
     *         <tt>separator</tt>
     */
    public static String concatenate(char separator, String... strings) {
        char[] charsToEscape = new char[] { separator };
        StringBuilder buf = new StringBuilder();
        for (String s : strings) {
            if (buf.length() > 0) {
                buf.append(',');
            }
            buf.append(escape(s, charsToEscape));
        }
        return buf.toString();
    }
    
    /**
     * Splits a string returned by {@link #concatenate} into its original
     * constituents.
     *
     * @see #concatenate
     *
     * @param separator the character that was originally used in concatenation
     * @param s         the concatenated string to split
     *
     * @return a <tt>String[]</tt> with the original constituents
     */
    public static String[] split(char separator, String s) {
        
        char[] charsToEscape = new char[] { separator };
        List<String> strings = new LinkedList<String>();
        StringBuilder field = new StringBuilder();
        boolean inEscapeSequence = false;
        
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!inEscapeSequence && c == separator) {
                strings.add(unescape(field.toString(), charsToEscape));
                field.setLength(0);
            } else {
                field.append(c);
                inEscapeSequence = (c == ESCAPE && !inEscapeSequence);
            }
        }
        strings.add(unescape(field.toString(), charsToEscape));
        return strings.toArray(new String[strings.size()]);
    }
}
