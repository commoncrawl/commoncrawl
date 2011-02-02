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

public final class Timer implements Comparable<Timer> {
	
	private long _delay;
	private boolean _periodic;
	private long _nextFireTime = 0;
	
	public static interface Callback { 
		public void timerFired(Timer timer);
	};
	
	private Callback _callback;
	
	public Timer(long delay,boolean periodic,Callback callback)  {
		_delay = delay;
		_periodic = periodic;
		_callback = callback;
	}
	
	public long     getDelay() { return _delay; }
	public boolean  isPeriodic() { return _periodic; }
	public boolean  isArmed() { return _nextFireTime != 0; }
	
	long	 getNextFireTime() { return _nextFireTime; }
	
	void     fire() { 
		if (_periodic) {
			// rearm
			arm();
		}
		else { 
			disarm();
		}
		_callback.timerFired(this); 
	}
	
	void   arm() { _nextFireTime = System.currentTimeMillis() + _delay; }
	void 	 disarm() { _nextFireTime = 0; }
	public void rearm() { arm(); }
	
	//@Override
	public int compareTo(Timer t) {
		if (_nextFireTime < t._nextFireTime)
			return -1;
		else if (_nextFireTime > t._nextFireTime)
			return 1;
		else 
			return 0;
	}
	
}
