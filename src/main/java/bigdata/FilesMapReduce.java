package bigdata;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FilesMapReduce {

	// CLASS MAPPER
		public static class FilesMapper extends Mapper<Object, Text, Text, CleanWritable>{
			// map function
			private static final String[] CATEGORIES = {"VETERAN","SENIOR","JUNIOR","MINIME","CADET","ESPOIR","CADETTE","BENJAMIN","HANDISPORT"};
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				// split lines to get different informations
				
				String[] parts = value.toString().split(";");
				String range = parts[0];
				String time = "";
				String category = "";
				String name = "";
				String team = "";
				String newKey = "";
				boolean canBeWritten = true;
				int afterName = 0;
				FileSplit file = (FileSplit) context.getInputSplit();
				String[] fileNameSplit = file.getPath().getName().toString().split("/");
				String fileName = fileNameSplit[fileNameSplit.length - 1];
				fileName = fileName.substring(0,fileName.length() - 5);
				
				// We check if the lastname and the firstname are together or not 
				if (areTogetherLastFirst(parts[0])) {
						name = parts[1].toUpperCase();
						afterName = 2;
				}
				else {
						name = parts[1].toUpperCase() + " " + parts[2].toUpperCase();
						afterName = 3;
				}
				for (int i = afterName; i < parts.length; i++) {
					 if (isCategory(parts[i].toUpperCase())) {
						 category = parts[i].toUpperCase();
					 }
					else if (isTime(parts[parts.length - 2])) {
						time = adaptTime(parts[i]);
					}
					else {
						team = parts[i].toUpperCase(); 
					}
				} 
				
				if (isKey(fileName)) {
					newKey = generateKey(fileName);
				}
				String [] elements = {newKey,time,category,name,team};
				canBeWritten = isNoOneEmpty(elements);
				if (canBeWritten) {
					CleanWritable clean = new CleanWritable(time,category,range,name,team); 
					context.write(new Text(newKey), clean);
				}
			}
			
			// Auxiliar functions
			
			public boolean areTogetherLastFirst(String name) {
				if (name.split(" ").length > 1) {
					return true;
				}
				return false;
			}
			
			public boolean isCategory(String category) {
				 Pattern pattern;
				 Matcher matcher;
				 for (String validCategory: CATEGORIES) {
					 pattern = Pattern.compile(validCategory);
					 matcher = pattern.matcher(category);
					 if (matcher.find()) {
						 return true;
					 }
				 }
				 return false;
			}
			
			public boolean isTime(String time) {
				if(time.matches("d{2,2}\\S\\d{2,2}")) {
					return true;
				}
				else if (time.matches("\\d{1,2}\\S\\d{2,2}\\S\\d{2,2}\\S")) {
					return true;
				}
				else if (time.matches("\\d{1,2}\\S\\d{2,2}\\S\\S\\d{2,2}\\S")) {
					return true;
				}
				else if (time.matches("\\d{1,2}\\S\\d{2,2}\\S\\d{2,2}")) {
					return true;
				}
				else if (time.matches("\\d{1,2}\\S\\S\\d{2,2}\\S\\d{2,2}")) {
					return true;
				}
				else if (time.matches("d{2,2}\\S\\d{2,2}\\S")) {
					return true;
				}
				return false;
			}
			
			public String adaptTime(String time) {
				time = time.replaceAll("[^\\d]",":");
				time = time.split("\\.")[0];
				if (time.charAt(time.length() - 1) == ':') {
					time = time.substring(0, time.length() -2);
				}
				time = time.replace("::",":");
				String[] timeSplit = time.split(":");
				if (timeSplit.length == 1) {
					time = "00:" + timeSplit[0] + ":" + "00";
				}
				else if (timeSplit.length == 2) {
					time = "00:" + time;
				}
				else if (timeSplit.length == 3) {
					if (timeSplit[0].length() == 1) {
						time = "0" + time;
					}
				}
				return time;
			}
			
			public boolean isKey(String fileName) {
				if (fileName.matches("\\d{1,2}\\S{2,30}\\d{1,2}\\S{0,2}")) {
					return true;
				}
				return false;
			}
			
			public String generateKey(String key) {
				String years = "";
				String city = "";
				String distance = "";
				if (key.matches("\\d{2,2}\\S{2,30}\\d{2,2}")) {
					years = key.substring(0,1);
					city = key.substring(2,key.length() - 3);
					distance = key.substring(key.length() - 2,key.length() - 1);
				}
				else if (key.matches("\\d{1,1}\\S{2,30}\\d{2,2}")) {
					years = key.substring(0,0);
					city = key.substring(2,key.length() - 3);
					distance = key.substring(key.length() - 2,key.length() - 1);
				}
				else if (key.matches("\\d{1,1}\\S{2,30}\\d{1,1}")) {
					years = key.substring(0,0);
					city = key.substring(1,key.length() - 2);
					distance = key.substring(key.length() - 1,key.length() - 1);
				}
				else if (key.matches("\\d{2,2}\\S{2,30}\\d{1,1}")) {
					years = key.substring(0,1);
					city = key.substring(2,key.length() - 2);
					distance = key.substring(key.length() - 1,key.length() - 1);
				}
				
				return city + ";" + years + ";" + distance + ";";
 			}
			
			public boolean isNoOneEmpty(String[] elements) {
				for (String el: elements) {
					if (el.isEmpty()) {
						return false;
					}
				}
				return true;
			}
			
	  }
	  
		// CLASS REDUCER
		public static class FilesReducer extends Reducer<Text,CleanWritable,Text,CleanWritable> {
			public void reduce(Text key, Iterable<CleanWritable> values, Context context) throws IOException, InterruptedException {
				for (CleanWritable line: values) {
					context.write(key, line);
				}
				
			}
		}
}
