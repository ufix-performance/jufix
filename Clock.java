package jufix;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

class Clock {
    
    private static final int NUM_TIMESTAMP = 8;
    
    private byte[][] utcTime;
    private long[] timestamp;
    int currentIndex;
    
    private DateFormat df;

    public Clock() {
	currentIndex = -1;
	utcTime = new byte[NUM_TIMESTAMP][32];
	timestamp = new long[NUM_TIMESTAMP];
	df = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSS");
    }

    public void updateUTCTime() {
	long ts = System.currentTimeMillis();
	int newIndex = currentIndex + 1;
	if (newIndex == NUM_TIMESTAMP) newIndex = 0;

	timestamp[newIndex] = ts;
	
        String time = df.format(ts);
	Util.convertToBytes(time, utcTime[newIndex], 0); 
	currentIndex = newIndex;
  }
    
    public byte[] getUTCTime() {
	return utcTime[currentIndex];
    }

    public long  getTimestamp() {
	return timestamp[currentIndex]/1000;
    }
}
