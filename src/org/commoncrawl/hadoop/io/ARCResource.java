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

package org.commoncrawl.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * The name and size in bytes of a gzipped ARC file.
 *
 * @author Albert Chern
 */
public class ARCResource implements Writable {
    
    private String name;
    private long size;
    
    public ARCResource(String name, long size) {
        this.name = name;
        this.size = size;
    }
    
    /**
     * Returns the name of this resource.
     */
    public String getName() {
        return name;
    }
    
    /**
     * Returns the size in bytes of this resource.
     */
    public long getSize() {
        return size;
    }
    
    /**
     * @inheritDoc
     */
    public String toString() {
        return name + " " + size;
    }
    
    /**
     * @inheritDoc
     */
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, name);
        out.writeLong(size);
    }
    
    /**
     * @inheritDoc
     */
    public void readFields(DataInput in) throws IOException {
        name = Text.readString(in);
        size = in.readLong();
    }
}
