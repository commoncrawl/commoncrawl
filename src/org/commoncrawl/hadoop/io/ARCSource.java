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
import java.io.InputStream;
import org.apache.hadoop.mapred.JobConf;

/**
 * An input source for gzipped ARC files.
 *
 * <p> This interface is a layer that allows gzipped ARC files to be read from
 * different sources, such as a Hadoop FileSystem, an S3 bucket, or an FTP
 * server.
 *
 * @author Albert Chern (Netseer Corp.)
 */
public interface ARCSource {
    
    /**
     * Given a job, returns the {@link ARCSplit}s that it should process.
     * 
     * @param job the job that is being executed
     *
     * @return an array with the {@link ARCSplit}s for this job
     *
     * @throws IOException if an IO error occurs
     */
    public ARCSplit[] getARCSplits(JobConf job) throws IOException;
    
    /**
     * Returns a stream for the given resource (from an {@link ARCSplit}) at the
     * specified byte position, or <tt>null</tt> to signify that the
     * {@link ARCSplitReader} should give up at this point.
     *
     * <p> Different implementations are allowed to implement their own retry
     * code inside this method.  The only supported method of recovery is to
     * return a new stream repositioned at the requested position.  If this is
     * not possible, then the job cannot recover because it might have already
     * processed records read prior to the requested position.
     *
     * @param resource         the resource to read
     * @param streamPosition   the byte position to start at
     * @param lastError        the error that resulted in this call, or
     *                         <tt>null</tt> if this is the first call
     * @param previousFailures the number of previous failures for this resource
     *
     * @return an {@link InputStream} for the resource at the specified byte
     *         position, or <tt>null</tt> if the {@link ARCSplitReader} should
     *         give up at this point
     *
     * @throws Throwable if an error occurs
     */
    public InputStream getStream(String resource, long streamPosition,
        Throwable lastError, int previousFailures) throws Throwable;
}
