package org.commoncrawl.query;

public class QueryResultRecord<KeyType,ValueType> {

  private KeyType _key;
  private ValueType _value;

  
  public QueryResultRecord(KeyType key,ValueType value) { 
    _key = key;
    _value = value;
  }
  
  public KeyType getKey(){ return _key; }
  public ValueType getValue() { return _value; }
}