package org.commoncrawl.util.shared;

import java.util.LinkedList;

/**
 * A basic moving average class
 * 
 * @author rana
 * 
 */
public class MovingAverage {

  private LinkedList<Double> _samples = new LinkedList<Double>();
  private int                _sampleSizeMax;
  private double             _average;

  public MovingAverage(int sampleSize) {
    _sampleSizeMax = sampleSize;
    _average = 0;
  }

  public synchronized double addSample(double sampleValue) {
    if (_samples.size() == _sampleSizeMax) {
      Double oldValue = _samples.removeFirst();
      oldValue = sampleValue;
      _samples.addLast(oldValue);
    } else {
      _samples.addLast(sampleValue);
    }
    // now compute new average value ...
    _average = 0;
    for (double value : _samples) {
      _average += value;
    }
    _average /= (double) _samples.size();

    return _average;
  }

  public synchronized double getAverage() {
    return _average;
  }
}
