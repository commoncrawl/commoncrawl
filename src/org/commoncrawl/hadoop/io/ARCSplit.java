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
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

/**
 * A map reduce input split for gzipped ARC files.
 *
 * @author Albert Chern
 */
public class ARCSplit implements InputSplit {
    
    /**
     * The {@link ARCResource}s for this split.
     */
    private ARCResource[] resources;
    
    /**
     * The total size of this split in bytes.
     */
    private long size;
    
    /**
     * The hosts that the resources are located on.
     */
    private String[] hosts;
    
    /**
     * Default constructor for Hadoop.
     */
    public ARCSplit() {}
    
    /**
     * Constructs an <tt>ARCSplit</tt> for {@link ARCResources}.
     * 
     * @param resources the resource identifiers for this split's ARC files
     */
    public ARCSplit(ARCResource[] resources) {
        this(resources, new String[0]);
    }
    
    /**
     * Constructs an <tt>ARCSplit</tt> for {@link ARCResource}s whose locations
     * are known.
     * 
     * @param resources the {@link ARCResource}s for this split's ARC files
     * @param hosts     the hosts that the resources are located on
     */
    public ARCSplit(ARCResource[] resources, String[] hosts) {
        this.resources = resources;
        this.hosts = hosts;
        for (ARCResource resource : resources) {
            size += resource.getSize();
        }
    }

    /**
     * Returns the resources for this split.
     */
    public ARCResource[] getResources() {
        return resources;
    }
    
    /**
     * @inheritDoc
     */
    public long getLength() throws IOException {
        return size;
    }

    /**
     * @inheritDoc
     */
    public String[] getLocations() throws IOException {
        return hosts;
    }

    /**
     * @inheritDoc
     */
    public String toString() {
        return "Resources: " + Arrays.toString(resources) + " Size: " + size +
            " Hosts: " + Arrays.toString(hosts);
    }
    
    /**
     * @inheritDoc
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(resources.length);
        for (ARCResource resource : resources) {
            resource.write(out);
        }
        out.writeLong(size);
        // The hosts are only used on the client side, so don't serialize them
    }

    /**
     * @inheritDoc
     */
    public void readFields(DataInput in) throws IOException {
        int nResources = in.readInt();
        resources = new ARCResource[nResources];
        for (int i = 0; i < nResources; i++) {
            resources[i] = new ARCResource(Text.readString(in), in.readLong());
        }
        size = in.readLong();
        hosts = null;
    }
}
