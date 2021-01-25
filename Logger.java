package jufix;

import java.io.File;

public abstract class Logger {
  
    protected abstract void dataMsgSent(WMessage msg);
    protected abstract void dataMsgRecv(RMessage msg);
    
    protected FileHandler logger;
    
    public Logger() {    
	reset();
    }
    
    public void reset() {
	logger = null;
    }

    public void setLogFile(FileHandler logger) {
	if (this.logger == null) {
	    this.logger = logger;
	}
    }
    
    public void fatal(String msg) {
	String time = Util.formatTime();
	logger.printf("%s   FATAL:  %s\n", time, msg);
	
	StackTraceElement[] traces = Thread.currentThread().getStackTrace();
	
	for (int i = 0; i < traces.length; i++) {
	    logger.printf("%s\n", traces[i]);
	}
	logger.flush();
	Util.sleep(1000);
	logger.close();
	System.exit(-1);
    }

    public void error(String msg) {
	String time =  Util.formatTime();
	logger.printf("%s   ERROR:  %s\n", time, msg);
	logger.flush();
  }

    public void warn(String msg) {
	String time =  Util.formatTime();
        logger.printf("%s   WARN:  %s\n", time, msg); 
	logger.flush();
  }

    public void info(String msg) {
	String time =  Util.formatTime();
        logger.printf("%s   INFO:  %s\n", time, msg);     
        logger.flush();
    }

    public void sessionMsgSent(WMessage msg) {
	byte[] buffer = msg.getData();
	int dataPos = msg.getDataStartPos();
	int dataLength = msg.getDataLength();
	String data = new String(buffer, dataPos, dataLength);
	String time = Util.formatTime();
	
	logger.printf("%s  SS ->   %s\n", time, data);
	logger.flush();
    }

    public void sessionMsgRecv(RMessage msg) {
	byte[] buffer = msg.getData();
	int dataPos = msg.getDataStartPos();
	int dataLength = msg.getDataLength();
	
	String data = new String(buffer, dataPos, dataLength);     
	String time = Util.formatTime();
	
	logger.printf("%s  SS <-   %s\n", time, data);
	logger.flush();
    }
    
    public void msgError(byte[] msg, int dataStartPos, int dataLength, String error) {
	String time = Util.formatTime();
	String data = new String(msg, dataStartPos, dataLength);
	logger.printf("%s   ERROR:  %s. ERROR msg: %s\n", time, data, error);
	logger.flush();
    }
}
