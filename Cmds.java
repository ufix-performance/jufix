package jufix;

class Cmds {
    public static final int MAX_CMDS = 1024;
    
    private byte[] cmds;
    private int head;
    private int tail;
    private Object lock;

    public Cmds() {
	lock = new Object();
	cmds = new byte[Constants.CMD_BLOCK_SIZE*MAX_CMDS];
	head = 0;
	tail = 0;
    }

    public void addCmd(byte[] cmd) {
	synchronized(lock) {
	    System.arraycopy(cmd, 0, cmds, tail*Constants.CMD_BLOCK_SIZE, Constants.CMD_BLOCK_SIZE);
	    int newTail = tail + 1;
	    tail = (newTail == MAX_CMDS)?0:newTail;
	}
    }
    
    public byte[] getNextCmd(byte[] cmd) {
	if (head == tail) return null;
	System.arraycopy(cmds, head*Constants.CMD_BLOCK_SIZE, cmd, 0, Constants.CMD_BLOCK_SIZE);
	head++;
	if (head == MAX_CMDS) head = 0;
	return cmd;
    }
}
