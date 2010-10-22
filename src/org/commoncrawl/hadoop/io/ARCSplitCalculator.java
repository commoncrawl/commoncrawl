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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

/**
 * Calculates splits based on the desired number of files per split and the
 * desired size of each split.
 *
 * <p> Concrete implementations should override {@link #getARCResources}.
 *
 * @author Albert Chern
 */
public abstract class ARCSplitCalculator implements JobConfigurable {
    
    /**
     * <tt>arc.split.calculator.files.per.split</tt> - the property where the
     * number of files per input split is stored.
     *
     * @see #setFilesPerSplit
     */
    public static final String P_FILES_PER_SPLIT =
        "arc.split.calculator.files.per.split";
    
    /**
     * <tt>arc.split.calculator.mb.per.split</tt> - the property where the desired
     * size in megabytes of the split is stored.
     *
     * @see #setMegabytesPerSplit
     */
    public static final String P_MB_PER_SPLIT =
        "arc.split.calculator.mb.per.split";
    
    /**
     * Sets the desired number of files per input split.
     *
     * <p> Default is 1.
     *
     * @param job           the job to set the number of files per split for
     * @param filesPerSplit the desired number of ARC files per split
     *
     * @see #P_FILES_PER_SPLIT
     */
    public static final void setFilesPerSplit(JobConf job, int filesPerSplit){
        job.setInt(P_FILES_PER_SPLIT, filesPerSplit);
    }
    
    /**
     * Sets the desired number of megabytes per split.
     *
     * <p> New files will be added to a split until the total size of the split
     * exceeds this threshold.  Default is no limit.
     *
     * @param job        the job to set the number of megabytes per split for
     * @param mbPerSplit the desired number of megabytes per split
     */
    public static final void setMegabytesPerSplit(JobConf job, int mbPerSplit) {
        job.setInt(P_MB_PER_SPLIT, mbPerSplit);
    }
    
    private int filesPerSplit;
    private long bytesPerSplit;
    
    /**
     * @inheritDoc
     */
    public final void configure(JobConf job) {
        filesPerSplit = job.getInt(P_FILES_PER_SPLIT, 1);
        bytesPerSplit = job.get(P_MB_PER_SPLIT) == null? Long.MAX_VALUE :
            Long.parseLong(job.get(P_MB_PER_SPLIT)) * 1024 * 1024;
        configureImpl(job);
    }
    
    /**
     * Hook for subclass configuration.
     *
     * @param job the {@link JobConf} of the job
     *
     * @see JobConfigurable#configure
     */
    protected void configureImpl(JobConf job) {}
    
    /**
     * Given a job, returns the {@link ARCResource}s it should process.
     *
     * @param job the job for which to get the {@link ARCResource}s
     *
     * @return the {@link ARCResource}s to process
     *
     * @throws IOException if an IO error occurs
     */
    protected abstract Collection<ARCResource> getARCResources(JobConf job)
        throws IOException;
    
    /**
     * @inheritDoc
     */
    public ARCSplit[] getARCSplits(JobConf job) throws IOException {
        
        List<ARCSplit> splits = new LinkedList<ARCSplit>();
        
        ARCResource[] resources = new ARCResource[filesPerSplit];
        int nResources = 0;
        long length = 0;
        
        for (ARCResource resource : getARCResources(job)) {
            resources[nResources++] = resource;
            length += resource.getSize();
            // When the split is too big, add it
            if (nResources >= filesPerSplit || length >= bytesPerSplit) {
                addSplit(splits, resources, nResources);
                nResources = 0;
                length = 0;
            }
        }
        
        // Add the final split
        addSplit(splits, resources, nResources);
        return splits.toArray(new ARCSplit[splits.size()]);
    }
    
    private void addSplit(List<ARCSplit> splits, ARCResource[] resources, int size) {
        if (size > 0) {
            ARCResource[] copy = new ARCResource[size];
            System.arraycopy(resources, 0, copy, 0, size);
            splits.add(new ARCSplit(copy));
        }
    }
}
