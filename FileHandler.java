package jufix;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.IOException;

class FileHandler {
    
    private File file;
    private FileOutputStream fos;
    private PrintWriter pw;
    private RandomAccessFile rf;

    public FileHandler(String name, char mode, boolean isBinary) {
	this.file = new File(name);
	setMode(mode, isBinary);
    } 
    
    public FileHandler(File file, char mode, boolean isBinary) {
	this.file = file;
	setMode(mode, isBinary);
    }
    
    private void setMode(char mode, boolean isBinary) {
	boolean append = (mode == 'a')?true:false;
			  
	try {
	    if (mode == 'a' || mode == 'w') {
		fos = new FileOutputStream(file, append);
		if (!isBinary) {
		    pw = new PrintWriter(fos);
		}
	    } else if (mode == 'r') {
		rf = new RandomAccessFile(file, "r");
	    }
	} catch (FileNotFoundException fe) {
	    System.out.println("Couldn't open writer for file " + file.getAbsolutePath());
	    fe.printStackTrace();
	    System.exit(-1);
	}
    }

    public void printf(String format, Object ... msgs) {
	pw.printf(format, msgs);
    }
    
    public void write(byte[] bytes, int offset, int length) {
	try {
	    fos.write(bytes, offset, length);
	} catch (IOException ie) {
        }
    }

    public void flush() {
	try {
	    if (pw != null) {
		pw.flush();
	    } else {
		fos.flush();
	    }
	} catch (IOException ie) {
        }
    }

    public long getFileSize() {
	return file.length();
    }

    public void seek(long pos) {
	try {
	    rf.seek(pos);
	} catch (IOException ie) {
        }
    }
    
    public int read(byte[] bytes, int offset, int length) {
	try {
	    return rf.read(bytes, offset, length);
	} catch (IOException ie) {
	    return 0;
        }
    }

    public void close() {
	try {
	    if (pw != null) {
		pw.close();
	    } 
	    if (fos != null) {
		fos.close();
	    }
	    if (rf != null) {
		rf.close();
	    }
	} catch (IOException ie) {
	}
    }
}