package org.commoncrawl.util.shared;

/*
 *    Copyright 2010 - CommonCrawl Foundation
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * File related helpers
 * 
 * @author rana
 * 
 */
public class FileUtils extends org.apache.hadoop.fs.FileUtil {

  public static byte[] readFully(File path) throws IOException {
    byte dataBuffer[] = new byte[(int) path.length()];
    FileInputStream fileInputStream = new FileInputStream(path);
    try {
      int chunkSize = 64 * 1024 * 1024;
      int bytesRemaining = (int) path.length();
      int offset = 0;
      while (bytesRemaining != 0) {
        int bytesToRead = Math.min(bytesRemaining, chunkSize);
        fileInputStream.read(dataBuffer, offset, bytesToRead);
        bytesRemaining -= bytesToRead;
        offset += bytesToRead;
      }
    } finally {
      fileInputStream.close();
    }
    return dataBuffer;
  }

  public static boolean recursivelyDeleteFile(File path) {
    if (path.exists()) {
      if (path.isDirectory()) {
        File[] files = path.listFiles();
        if (files != null) {
          for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
              recursivelyDeleteFile(files[i]);
            } else {
              files[i].delete();
            }
          }
        }
      }
    }
    return (path.delete());
  }

  private static final int ID_MAX       = 100000000;
  private static final int SCALE_FACTOR = 100;

  /**
   * a hacked way to build a hierarchical path for storing a specific file based
   * on a numerical id
   * 
   * @param basePath
   * @param fileId
   * @return
   */
  public static File buildHierarchicalPathForId(File basePath, long fileId) {

    File pathOut = basePath;

    int value = Math.abs((int) (fileId % ID_MAX));
    int divisor = ID_MAX / SCALE_FACTOR;

    while (divisor >= 1) {
      if (divisor != 1) {
        pathOut = new File(pathOut, Integer.toString((value / divisor)));
        value = value % divisor;
        divisor /= SCALE_FACTOR;
      } else {
        pathOut = new File(pathOut, Long.toString(fileId));
        break;
      }
    }
    return pathOut;
  }
}
