package bigdata;

import java.io.File;
import java.util.ArrayList;

public class DirectoryReader {
	
	String directoryPath;
	ArrayList <String> fileList;
	public DirectoryReader (String directoryPath) {
		this.directoryPath = directoryPath;
		this.fileList = new ArrayList<String>();
		String [] stringFileList = new File(directoryPath).list(); 	
		for (int i=0; i<stringFileList.length;i++) {
			if (stringFileList[i].endsWith(".csv")) {
				fileList.add(stringFileList[i]);
			}
		} 
	}
	
	public ArrayList <String> getFileList () {
		return fileList;
	}
}
