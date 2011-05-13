package org.commoncrawl.util.shared;
/**
 * A placeholder for tuples 
 * 
 * A lot more to do here ... 
 * 
 * @author rana
 *
 */
public class Tuples { 
  
  /**
   * A basic Pair Tuple 
   * @author rana
   *
   */
  public static class Pair<T1,T2> { 
    public T1 e0;
    public T2 e1;
    
    public Pair(T1 e0,T2 e1) { 
      this.e0 = e0;
      this.e1 = e1;
    }
  }
  
  public static class Triple<T1,T2,T3> extends Pair<T1,T2> { 
    public T3 e2;
    
    public Triple(T1 e0,T2 e1,T3 e2) {
      super(e0,e1);
      this.e2 = e2;
    }    
  }

  public static class Quad<T1,T2,T3,T4> extends Triple<T1,T2,T3> { 
    public T4 e3;
    
    public Quad(T1 e0,T2 e1,T3 e2,T4 e3) {
      super(e0,e1,e2);
      this.e3 = e3;
    }    
  }
  
}