package org.commoncrawl.util.shared;

/**
 * Basic Smoothed Average class
 * 
 * @author rana
 * 
 */
public class SmoothedAverage {

  private double _alpha;
  private int    _observationCount;
  private double _smoothedValue;

  public SmoothedAverage(double alpha) {
    _alpha = alpha;
    _observationCount = 0;
    _smoothedValue = 0;
  }

  public synchronized double addSample(double sampleValue) {
    if (_observationCount++ == 0) {
      _smoothedValue = sampleValue;
    } else {
      _smoothedValue = (_alpha * sampleValue) + ((1 - _alpha) * _smoothedValue);
    }
    return _smoothedValue;
  }

  public synchronized double getAverage() {
    return _smoothedValue;
  }

}
