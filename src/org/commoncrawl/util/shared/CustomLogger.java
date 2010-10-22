package org.commoncrawl.util.shared;

import java.util.Enumeration;

import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.HierarchyEventListener;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.spi.LoggerRepository;

/**
 * Hacked implementation of CustomLogger using log4j framework
 * 
 * @author rana
 *
 */
public class CustomLogger extends Category {

  public CustomLogger(String name) {
    
    super(name);

    repository = new LoggerRepository() {

      public void addHierarchyEventListener(HierarchyEventListener listener) {        
      }

      public void emitNoAppenderWarning(Category cat) {
      }

      public Logger exists(String name) {
        return null;
      }

      public void fireAddAppenderEvent(Category logger, Appender appender) {
      }

      public Enumeration getCurrentCategories() {
        return null;
      }

      public Enumeration getCurrentLoggers() {
        return null;
      }

      public Logger getLogger(String name) {
        return null;
      }

      public Logger getLogger(String name, LoggerFactory factory) {
        return null;
      }

      public Logger getRootLogger() {
        return null;
      }

      public Level getThreshold() {
        return null;
      }

      public boolean isDisabled(int level) {
        return false;
      }

      public void resetConfiguration() {
      }

      public void setThreshold(Level level) {
      }

      public void setThreshold(String val) {
      }

      public void shutdown() {
      } 
    };
    
    this.setLevel(Level.ALL);
  }
  
}
