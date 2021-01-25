package jufix;

import java.io.File;

import java.util.Date;
import java.text.SimpleDateFormat;

public class Util {
    
    private static SimpleDateFormat sdfTime;
    private static SimpleDateFormat sdfDateTime;

    private static final String FIX_TIME_FORMAT = "HH:mm:ss.SSSSSS";
    private static final String FIX_DATE_TIME_FORMAT = "yyyyMMdd-HH:mm:ss.SSS";

    static {
	sdfTime = new SimpleDateFormat(FIX_TIME_FORMAT);
	sdfDateTime = new SimpleDateFormat(FIX_DATE_TIME_FORMAT);
    }
    
    private static final boolean isCharEnd(byte val) {
	return (val == 0 || val == Constants.SOH);
    }

    public static final byte convertITOC(int val) {
	return (byte) (val + 48);
    }

    public static final int convertCTOI(byte val) {
	return (val - 48);
    }
    
    public static final int convertITOA(int value, byte[] buf, int offset) {
	int pow = 10;
	int digits = 1;
	
	while (pow <= value) {
	    pow *= 10;
	    digits++;
	}

	for (int i = digits - 1; i >= 0; i--) {
	    int newValue = value/10;
	    buf[offset + i] = convertITOC(value - newValue*10);
	    value = newValue;
	}
	return digits;
    }
    
    public static final int convertITOA(int value, byte[] buf, int offset, int bufEnd) {
	int pos = bufEnd - 1;
	while (true) {
	    buf[pos] = convertITOC(value % 10);
	    value /= 10;
	    if (value == 0) break;
	    pos--;
	}
	return bufEnd - pos;
    }
    
    public static final int convertATOI(byte[] buf, int offset) {
	int val = 0;
	int pos = offset;
	while (true) {
	    if(buf[pos] < (byte) '0' || buf[pos] > (byte) '9') break;
	    val = val*10 + convertCTOI(buf[pos++]);
	}
	return val;
    }

    public static final int convertATOI(byte[] buf, int offset, int len) {
	int val = 0;
	for (int i = 0; i < len; i++) {
	    val = val*10 + convertCTOI(buf[offset + i]);
	}
	return val;
    }
    
    public static final int convertLTOA(long value, byte[] buf, int offset) {
	long pow = 10;
	int digits = 1;
	
	while (pow <= value) {
	    pow *= 10;
	    digits++;
	}
	
	for (int i = digits - 1; i >= 0; i--) {
	    long newValue = value/10;
	    buf[offset + i] = convertITOC((int) (value - newValue*10));
	    value = newValue;
	}
	return digits;
    }
    
    public static final int convertLTOA(long value, byte[] buf, int offset, int bufEnd) {
	int pos = bufEnd - 1;
	while (true) {
	    buf[pos] = convertITOC((int) (value % 10));
	    value /= 10;
	    if (value == 0) break;
	    pos--;
	}
	return bufEnd - pos;
    }

    public static final long convertATOL(byte[] buf, int offset, int len) {
	long val = 0;
	for (int i = 0; i < len; i++) {
	    val = val*10 + convertCTOI(buf[offset++]);
	}
	return val;
    }

    public static final long convertATOL(byte[] buf, int offset) {
	long val = 0;
	int pos = offset;
	while (true) {
	    if(buf[pos] < (byte) '0' || buf[pos] > (byte) '9') break;
	    val = val*10 + convertCTOI(buf[pos++]);
	}
	return val;
    }

    public static final long getCurrentTimestamp() {
	return System.nanoTime()/1000;
    }
    
    public static final void sleep(int ms) {
	try {
	    Thread.sleep(ms);
	} catch (InterruptedException ie) {
	}
    }

    public static final int convertToBytes(long val, byte[] buf, int offset) {
	for (int i = Constants.LONG_BYTES - 1; i >= 0; i--) {
	    buf[offset + i] = (byte)(val & 0xFF);
	    val >>= 8;
	}
	return Constants.LONG_BYTES;
    }
    
    public static final int convertToBytes(int val, byte[] buf, int offset) {
        for (int i = Constants.INT_BYTES - 1; i >= 0; i--) {
            buf[offset + i] = (byte)(val & 0xFF);
            val >>= 8;
        }
        return Constants.INT_BYTES;
    }

    public static final int convertToBytes(String val, byte[] buf, int offset) {
	int length = val.length();
	for (int i = 0; i < length; i++) {
	    buf[offset + i] = (byte) val.charAt(i);
	}
	return length;
    }

    public static long convertToLong(byte[] val, int offset) {
	long result = 0;
	
	for (int i = 0; i < Constants.LONG_BYTES; i++) {
	    result <<= 8;
	    result |= (val[offset + i] & 0xFF);
	}

	return result;
    }

    public static int convertToInt(byte[] val, int offset) {
        int result = 0;

        for (int i = 0; i < Constants.INT_BYTES; i++) {
            result <<= 8;
            result |= (val[offset + i] & 0xFF);
        }
	
        return result;
    }    
    
    public static final String convertToString(byte[] val, int offset) {
	int end = offset;
        while (!isCharEnd(val[end])) {
            end++;
        }
	return new String(val, offset, end - offset);
    }

    public static final void dirCreate(String dirName) {
	File file = new File(dirName);
	if (file.exists()) return;
	if (file.mkdirs()) {
	    return;
	} else {
	    System.out.println("Couldn't create directory " + dirName);
	    System.exit(0);
	}
    }

    public static final String formatTime() {
	return sdfTime.format(new Date());
    }

    public static final String formatDateTime() {
        return sdfDateTime.format(new Date());
    }

    public static final boolean isSessionMsg(String msgType) {
	if (msgType.length() != 1) return false;
	char firstChar = msgType.charAt(0);
	return ((firstChar >= Constants.MSG_TYPE_HEARTBEAT.charAt(0) && firstChar <= Constants.MSG_TYPE_LOGOUT.charAt(0)) ||
		firstChar == Constants.MSG_TYPE_LOGON.charAt(0) ||
		firstChar == Constants.MSG_TYPE_XML_NON_FIX.charAt(0));
    }
    
    public static final int strSize(byte[] str, int offset) {
	int end = offset;
	while (!isCharEnd(str[end])) {
	    end++;
	}
	return end - offset;
    }
    
    public static final int seek(byte[] data, int dataPos, int dataLength, 
				 String pattern, int patternLength) {
	int pos = dataPos;
	int prevPos = dataPos;
	int endPos = dataPos + dataLength;

	while (true) {
	    for (int i = 0; i < patternLength; i++) {
		if (pos == endPos) return -1;
		if (data[pos] == (byte) pattern.charAt(i)) {
		    pos++;
		} else {
		    pos++;
		    prevPos = pos;
		    break;
		}
	    }
	    if (prevPos != pos) {
		return prevPos;
	    }
	}
    }

    public static final boolean match(byte[] srcVal, int srcValOffset, String destVal) {
	int pos = 0;
	int len = destVal.length();

	while (pos < len) {
	    byte destValCur = (byte) destVal.charAt(pos);
	    int srcIndex = srcValOffset + pos;

	    if (srcVal[srcIndex] != destValCur) {
		if (srcVal[srcIndex] == 0 && destValCur == Constants.SOH) {
		    return true;
		}
		if (srcVal[srcIndex] == Constants.SOH && destValCur == 0) {
		    return true;
		}
		return false;
	    }

	    if (srcVal[srcIndex] == 0 || srcVal[srcIndex] == Constants.SOH) {
		break;
	    }
	    pos++;
	}
	return true;
    }

    public static final int getQueueIndex(long seq, int qSize) {
	return (int) (seq%qSize);
    }
}