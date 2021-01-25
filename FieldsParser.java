package jufix;

class FieldsParser {
  
    private RGroupsDictionary rgDictionary;
    
    private int fieldsSize;
    private FieldInfo[] fields;

    private long counter;
    
    private BytesData bData;

    public FieldsParser(int size) {
	fieldsSize = size;
	counter = 0;
	fields = new FieldInfo[fieldsSize];
	for (int i = 0; i < fieldsSize; i++) {
	    fields[i] = new FieldInfo();
	}
	bData = new BytesData();
    }
    
    public void setRGDictionary(RGroupsDictionary dictionary) {
	rgDictionary = dictionary;
    }

    public int parse(byte[] data, int pos, int dataLen) {
	counter++;
	bData.setData(data, pos, dataLen);
	
        while (true) {                                                                                           
	    if (bData.isEnd()) return 1;                                                                            
	    int tag = bData.readTag();
	    if (tag == 0) return 0;
	    bData.next();

	    int index = tag%fieldsSize;
	    fields[index].counter = counter;
	    fields[index].data = data;
	    fields[index].tag = tag;
	    fields[index].valStartPos = bData.getPos();
	    
	    int valueCode;
	    
	    if (rgDictionary == null || rgDictionary.isNormal(tag)) {
		valueCode = bData.readValue();
	    } else {
		valueCode = bData.readRGValue(tag, rgDictionary);
	    }
	    if (valueCode == 0) return 0;
	    fields[index].valLength = bData.getPos() - fields[index].valStartPos;
	    bData.next();
	}
    }
  
    public int parseGroups(byte[] data, int pos, int dataLen, FieldInfo[] infos) {
	bData.setData(data, pos, dataLen);
	int startPos = bData.getPos();
	if (bData.readValue() == 0) return 0;
	
	int numGroups = Integer.parseInt(new String(data, startPos, bData.getPos() - startPos));
	
	bData.next();
	
	int tagStart = 0;
	int numGroupsRead = 0;
        
	int endValPos = 0;

	while (true) {
	    if (bData.isEnd()) {
		if (numGroupsRead == numGroups) {
		    infos[numGroupsRead - 1].valLength = endValPos - infos[numGroupsRead - 1].valLength;
		    return numGroups;
		} else {
		    return 0;
		}
	    }
	    
	    int tagPos = bData.getPos();
	    int tag = bData.readTag();
	    if (tag == 0) return 0;
	    if (tagStart == 0) tagStart = tag;
	    
	    bData.next();
	    
	    if (tag == tagStart) {
		infos[numGroupsRead].valLength = tagPos;
		infos[numGroupsRead].valStartPos = tagPos;
	    	    
		if (numGroupsRead > 0) {
		    infos[numGroupsRead - 1].valLength = endValPos - infos[numGroupsRead - 1].valLength;
		}
		numGroupsRead++;
	    }
	    
	    int valueCode;

	    if (rgDictionary == null || rgDictionary.isNormal(tag)) {
		valueCode = bData.readValue();
	    } else {
		valueCode = bData.readRGValue(tag, rgDictionary);
	    }
	    if (valueCode == 0) return 0;
	    endValPos = pos++;
	}
    }
    
    public int parseGroup(byte[] data, int pos, int dataLen, FieldInfo[] infos) {
	int index = 0;
	        
	bData.setData(data, pos, dataLen);

	while (true) {
	    if (bData.isEnd()) break;
	    
	    int tag = bData.readTag();
	    if (tag == 0) return 0;
	    
	    infos[index].data = data;
	    infos[index].tag = tag;
	    infos[index].valStartPos = ++pos;
	    
	    int valueCode;

	    if (rgDictionary == null || rgDictionary.isNormal(tag)) {
		valueCode = bData.readValue();
	    } else {
		valueCode = bData.readRGValue(tag, rgDictionary);
	    }
	    if (valueCode == 0) return 0;

	    infos[index].valLength = bData.getPos() - infos[index].valStartPos;  
	    bData.next();
	    index++;
	}
	return index;
    }
    

    public FieldInfo getField(int tag) {
	int index = tag%fieldsSize;
	if (fields[index].counter != counter) {
	    return null;
	}
	if (fields[index].tag != tag) {
	    return null;
	}
	return fields[index];
    }
}
