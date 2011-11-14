package org.commoncrawl.query;

import java.util.Vector;

import org.apache.hadoop.fs.Path;

public class QueryResult<KeyType,ValueType> {
  
  Vector<QueryResultRecord<KeyType,ValueType>> results = new Vector<QueryResultRecord<KeyType,ValueType>>(); 
  int                       pageNumber = 0;                         
  long                      totalRecordCount = 0;
  Path                      srcFilePath = null;
  
  public QueryResult() { 
    results = new Vector<QueryResultRecord<KeyType,ValueType>>();
  }
  
  public int  getPageNumber() { return pageNumber; }
  public void setPageNumber(int pageNumber) { this.pageNumber = pageNumber; }
  
  public long  getTotalRecordCount() { return totalRecordCount; }
  public void  setTotalRecordCount(long totalRecordCount) { this.totalRecordCount = totalRecordCount; }

  public int getResultCount() { return results.size(); }
  public QueryResultRecord<KeyType,ValueType> getResultAt(int index) { return results.get(index); }
  public Vector<QueryResultRecord<KeyType,ValueType>> getResults() { return results; }
  
  
  public Path getSrcPath() { return srcFilePath; }
  public void setSrcPath(Path path) { srcFilePath = path; }
}
