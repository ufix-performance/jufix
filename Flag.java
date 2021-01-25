package jufix;

class Flag {
    private int val;

    public Flag(int val) {
	this.val = val;
    }

    public void setVal(int val) {
	this.val = val;
    }
    
    public int getVal() {
	return val;
    }

    public void increase() {
	val++;
    }
}