package jufix;

public class SimpleLogger extends Logger {
  
    public SimpleLogger() {
    }
    
    protected void dataMsgSent(WMessage msg) {
	byte[] buffer = msg.getData();        
	int dataPos = msg.getDataStartPos();
	int dataLength = msg.getDataLength();
	String data = new String(buffer, dataPos, dataLength);
	String time = Util.formatTime();

	logger.printf("%s  DT ->   %s\n", time, data);
	logger.flush();
    }

    protected void dataMsgRecv(RMessage msg) {
	byte[] buffer = msg.getData();
	int dataPos = msg.getDataStartPos();
	int dataLength = msg.getDataLength();

	String data = new String(buffer, dataPos, dataLength);
	String time = Util.formatTime();

	logger.printf("%s  DT <-   %s\n", time, data);
	logger.flush();
    }
}

