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

package org.commoncrawl.async;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Vector;

public final class TimerRegistry {

  private EventLoop _eventLoop;

  public TimerRegistry(EventLoop eventLoop) {
    _eventLoop = eventLoop;
  }

  public void setTimer(Timer t) {
    synchronized (t) {
      if (!t.isArmed()) {
        t.arm();

        synchronized (this) {
          _active.add(t);
          _sort = true;
        }
        // wakeup event loop if current thread != event loop thread
        // and this timer is the timer with the earliest fire time ...
        if (Thread.currentThread() != _eventLoop.getEventThread()) {
          _eventLoop.wakeup();
        }
      }
    }
  }

  public void cancelTimer(Timer t) {
    synchronized (t) {
      if (t.isArmed()) {
        t.disarm();
        synchronized (this) {
          _sort = true;
        }
      }
    }
  }

  // fire the timer
  long fireTimers() {

    long currentTime = System.currentTimeMillis();

    LinkedList<Timer> fireList = new LinkedList<Timer>();

    int fired = 0;

    synchronized (this) {

      if (_sort) {
        // long timeStart = System.currentTimeMillis();
        Collections.sort(_active);
        // long timeEnd = System.currentTimeMillis();
        // _sortTime.addSample((double)timeEnd-timeStart);
        _sort = false;
      }

      // first pass ... fire all timers in list (that need to be fired)
      for (int i = 0; i < _active.size(); ++i) {

        Timer t = _active.get(i);

        if (t.isArmed() && t.getNextFireTime() <= currentTime) {
          // add to fire list ...
          fireList.add(t);
        } else {
          break;
        }
      }
    }

    // now in an unblocked manner ... iterate list and fire timers ...
    for (Timer t : fireList) {

      synchronized (t) {
        if (t.isArmed() && t.getNextFireTime() <= currentTime) {
          t.fire();
          fired++;
        }
      }
    }

    synchronized (this) {

      // now, second pass... walk entire array
      int activeCount = _active.size();

      for (int i = 0; i < activeCount; ++i) {
        Timer t = _active.elementAt(i);

        // if not armed, remove from list ...
        if (!t.isArmed()) {
          _active.remove(i--);
          // reduce total count
          activeCount--;
        }
      }

      // now if something fired, or sort flag is true ...
      if (fired != 0 || _sort) {
        Collections.sort(_active);
        _sort = false;
      }
      // return the next fire time
      return (_active.size() != 0) ? _active.get(0).getNextFireTime() : 0;
    }
  }

  private Vector<Timer> _active = new Vector<Timer>();
  private boolean       _sort   = false;
}
