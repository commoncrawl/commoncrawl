package org.commoncrawl.util.shared;

import java.util.Map;
import java.util.TreeMap;

/**
 * The beginnings of a very basic Histogram class
 * @author rana
 *
 * @param <KeyType>
 */
public class Histogram<KeyType> {
    
  private TreeMap<KeyType,Integer> _histogramData = new TreeMap<KeyType,Integer>();
  
  public void addSample(KeyType keyType) { 
    Integer existingCount = _histogramData.get(keyType);
    if (existingCount != null) { 
      _histogramData.put(keyType,existingCount.intValue() + 1);
    }
    else { 
      _histogramData.put(keyType,1);
    }
  }
  
  public String toString() { 
    StringBuffer buffer = new StringBuffer();
    for (Map.Entry<KeyType,Integer> entry : _histogramData.entrySet()) { 
      buffer.append(entry.getKey().toString());
      buffer.append(":");
      for (int i=0;i<entry.getValue().intValue();++i) { 
        buffer.append("#");
      }
      buffer.append("\n");
    }
    return buffer.toString();
  }
}
