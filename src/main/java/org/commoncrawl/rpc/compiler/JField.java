/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

package org.commoncrawl.rpc.compiler;

/**
 * A thin wrappper around record field.
 */
public class JField<T> {

  private String name;
  private T      type;
  private int    ordinal;
  private int    modifiers;

  public static class Modifiers {
    public static int KEY       = 1 << 0;
    public static int TRANSIENT = 1 << 1;
  }

  /**
   * Creates a new instance of JField
   */
  public JField(String name, T type, int ordinal, int modifiers) {
    this.type = type;
    this.name = name;
    this.ordinal = ordinal;
    this.modifiers = modifiers;
  }

  String getName() {
    return name;
  }

  T getType() {
    return type;
  }

  int getOrdinal() {
    return ordinal;
  }

  int getModifiers() {
    return modifiers;
  }
}
