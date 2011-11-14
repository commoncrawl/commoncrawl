package org.commoncrawl.query;

public interface RemoteQueryCompletionCallback {
    void queryComplete(Query request,long resultCount);
    void queryFailed(Query request,String reason);
}
