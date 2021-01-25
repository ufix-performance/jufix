package jufix;

import java.io.InputStream;
import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.InetSocketAddress;

public class SocketServer implements Runnable {
    
    private Thread threadServer;

    private Logger sLogger;
    
    private Session[] sessions;
    private int numSessions;
    
    private String socketAddr;
    private int socketPort;
    
    public SocketServer(int maxNumSessions) {
	sessions = new Session[maxNumSessions];
	numSessions = 0;
	sLogger = new SimpleLogger();
	socketAddr = null;
    }
    
    private int isFullMsg(byte[] buf, int offset, int len) {
	int msgBodyLengthPos = Util.seek(buf, offset, len, "9=", 2);
	if (msgBodyLengthPos < 0) return 0;
       
	msgBodyLengthPos += 2;
	int pos = msgBodyLengthPos;
	while (pos < offset + len) {
	    if (buf[++pos] == Constants.SOH) break;
	}
	if (pos >= offset + len) return 0;
	
	int bodyLength = Util.convertATOI(buf, msgBodyLengthPos);
	
	int msgLen = pos - offset + 1 + bodyLength + Constants.CHECKSUM_SIZE;
	
	if (len < msgLen) {
	    return 0;
	}
	return msgLen;
    }
  
    private Session findSession(byte[] msg, int senderCompIDPos, int targetCompIDPos) {
        for (int i = 0; i < numSessions; i++) {  
            if (sessions[i].match(msg, senderCompIDPos, msg, targetCompIDPos)) {        
                return sessions[i];
            }
        }
        return null;
    }

    public void setSocketAddr(String addr) {  
	socketAddr = addr;
    }

    public void setSocketPort(int port) {
	socketPort = port;
    }      
    
    public void addSession(Session session) {  
	sessions[numSessions++] = session;
    }

    public void run() {
	String logFileName;
	SocketAddress socketAddress;

	if (socketAddr == null) {
	    logFileName = String.format("server_log_%s:%d", "0.0.0.0", socketPort);
	    socketAddress = new InetSocketAddress(socketPort);
	} else {
	    logFileName = String.format("server_log_%s:%d", socketAddr, socketPort);
	    socketAddress = new InetSocketAddress(socketAddr, socketPort);
	}
	sLogger.setLogFile(new FileHandler(logFileName, 'a', false)); 
	
	ServerSocket socketServer = null;
		
	try {
	    socketServer =  new ServerSocket();
	    socketServer.bind(socketAddress);
	} catch (IOException ie) {
	    String log = String.format("Coudln't init server on socket address: %s", ie.getMessage());
	    sLogger.fatal(log);
	}

	while(true) {
	    try {
		Socket socketClient = socketServer.accept();
		InetSocketAddress clientAddress = (InetSocketAddress) socketClient.getRemoteSocketAddress();
		String ip = clientAddress.getAddress().toString().split("/")[1];
		int port = clientAddress.getPort();
		String log = String.format("New conection accepted from %s:%d", 
					   ip, port);
		sLogger.info(log);
		SocketClientHandler handler = new SocketClientHandler(socketClient);
		handler.start();
	    } catch (IOException ie) {
		String log = String.format("Exception: %s", ie.getMessage());
		sLogger.error(log);
	    }
	}
    }
    
    public void start() {
	new Thread(this, "Server-" + socketAddr + "-" + socketPort).start();
    }

    private class SocketClientHandler extends Thread {
	
	private Socket socket;
	private InputStream is;

	private SocketClientHandler(Socket socket) throws IOException {
	    this.socket = socket;
	    is = socket.getInputStream();
	}
	
	private void close() throws IOException {
	    is.close();
	    socket.close();
	}

	public void run() {
	    byte[] buf = new byte[8192];
	    int end = 0;
	    int msgLen;
	    
	    while(true) {
		int readSize;
		try {
		    readSize = is.read(buf, end, 8192 - end);
		    if (readSize < 0) {
			Util.sleep(1);
			continue;
		    }
		} catch (Exception e) {
		    String log = String.format("Reading socket exception %s", e.getMessage());
		    sLogger.error(log);
		    return;
		}
		end += readSize;

		msgLen = isFullMsg(buf, 0, end);
		if (msgLen > 0) {		    
		    String log = String.format("Recv msg: %s", new String(buf, 0, msgLen));
		    sLogger.info(log);
		    break;
		}
	    }
	    
	    int senderCompIDPos = Util.seek(buf, 0, msgLen, "49=", 3);
	    if (senderCompIDPos < 0) {
		sLogger.error("Incorrect format msg: Missing SenderCompID");
		return;
	    }
	    senderCompIDPos += 3;
	    
	    int targetCompIDPos = Util.seek(buf, 0, msgLen, "56=", 3);
	    if (targetCompIDPos < 0) {
		sLogger.error("Incorrect format msg: Missing TargetCompID");
		return;
	    }
	    targetCompIDPos += 3;
	    	    
	    Session session = findSession(buf, targetCompIDPos, senderCompIDPos);
	    
	    if (session == null) {
		String senderCompID = Util.convertToString(buf, senderCompIDPos);
		String targetCompID = Util.convertToString(buf, targetCompIDPos);
		String log = String.format("Session SenderCompID %s, TargetCompID %s not found", 
					   senderCompID, targetCompID);
		sLogger.error(log);
		return;
	    }

	    try {
		SocketConnector sConnector = new SocketConnector(buf, 0, end, socket, session);
		sConnector.start();
		sConnector.keepUpdateUTCTime();
		sConnector.join();
		close();
	    } catch (Exception e) {
		String log = String.format("Exception: %s", e.getMessage());
		sLogger.error(log);
            }
	}
          
    }
}
