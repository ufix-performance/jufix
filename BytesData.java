package jufix;

class BytesData {
    
    private byte[] data;
    private int pos;
    private int end;
    
    public void setData(byte[] data, int pos, int len) {
	this.data = data;
	this.pos = pos;
	this.end = pos + len;
    }

    public int readTag() {
        int tag = 0;
        while (data[pos] != Constants.EQUAL) {
            int digit = Util.convertCTOI(data[pos]);
            if (digit > 9 || digit < 0) {
                return 0;
            }
            tag = tag*10 + digit;

            pos++;
            if (pos >= end) return 0;
        }
        return tag;
    }
    
    public int readValue() {
        	
	while (pos < end && data[pos] != Constants.SOH) {
            pos++;
	}

        return 1;
    }
    
    public int readRGValue(int rgTag, RGroupsDictionary rgDictionary) {
        if (readValue() == 0) return 0;
	
	int lastRGPos;

	while (true) {
	    
	    lastRGPos = pos;
            pos++;

	    if (pos >= end) return 0;
	    int tag = readTag();
	    if (!rgDictionary.isTagInRG(rgTag, tag)) {
		break;
	    }
	    if (readValue() == 0) return 0;
	}

	pos = lastRGPos;
	return 1;
    }

    public void next() {
	pos++;
    }

    public boolean isEnd() {
	return pos >= end;
    }

    public int getPos() {
	return pos;
    }
}