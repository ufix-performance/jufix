package jufix;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;

abstract class Locker {
    
    public static final int SPIN_TIME = 20;
    
    private ReentrantLock mutex;
    private Condition cond; 

    private long  startTimestamp;
    private int elapseTime;

    private int pWait;

    protected abstract boolean hasEvent();

    public Locker() {
	mutex = new ReentrantLock();
	cond = mutex.newCondition();             

	startTimestamp = Util.getCurrentTimestamp();
	elapseTime = 0;
    }
    
    private static void waitOnCond(long timeMS, Condition cond) {
	try {
	    cond.await(timeMS, TimeUnit.MILLISECONDS);
	} catch (InterruptedException ie) {
	}
    }

    private int elapse(long timestamp) {
	return (int) (timestamp - startTimestamp);
    }

    public void wait(int timeMS, boolean forceWait) {
	if (forceWait) {
	    mutex.lock();
	    waitOnCond(timeMS, cond);
	    mutex.unlock();
	    return;
	}
	
	if (hasEvent()) return;
	startTimestamp = Util.getCurrentTimestamp();
	pWait = 1;
	
	mutex.lock();
	
	while (!hasEvent()) {
	    long timestamp = Util.getCurrentTimestamp();
	    elapseTime = elapse(timestamp);
	    if (elapseTime > SPIN_TIME) break;
	    Thread.yield();
	}
    
	if (!hasEvent()) {
	    waitOnCond(timeMS, cond);
	}
	
	mutex.unlock();
	elapseTime = 0;
	pWait = 0;
    }

    public void signal() {
	if (pWait == 1) {
	    if (elapseTime < SPIN_TIME) {
		long timestamp = Util.getCurrentTimestamp();
		if (elapse(timestamp) < SPIN_TIME) return;
	    }
	    boolean code;
	    while (true) {
		if (pWait == 0) break;
		code = mutex.tryLock();
		if (code) {
		    cond.signal();
		    mutex.unlock();
		    break;
		}
		Thread.yield();
	    }
	} 
    }
}
