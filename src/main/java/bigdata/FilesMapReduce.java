package bigdata;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FilesMapReduce {

	// CLASS MAPPER
		public static class FilesMapper extends Mapper<Object, Text, Text, CleanWritable>{
			public static String fileName = "";
			
			// map function
			private static final String[] CATEGORIES = {"VETERAN","SENIOR","JUNIOR","MINIME","CADET","ESPOIR","CADETTE","BENJAMIN","HANDISPORT"};
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				setup(context);
				// split lines to get different informations
				String[] parts = value.toString().split(";");
				String range;
				if (parts.length > 0) {
					 range = parts[0];
				}
				else {
					range = "";
				}
				String time = "";
				String category = "";
				String name = "";
				String team = "";
				String newKey = "";
				boolean canBeWritten = true;
				int afterName = 0;
				if (parts.length > 1) {
					
					// We check if the lastname and the firstname are together or not 
					
					if (areTogetherLastFirst(parts[1])) {
							name = parts[1].toUpperCase();
							afterName = 2;
					}
					else {
						if (parts.length > 2) {
								name = parts[1].toUpperCase() + " " + parts[2].toUpperCase();
						}
								afterName = 3;
						
					}
					
					// All the other elements of the line go in the filters
					for (int i = afterName; i < parts.length; i++) {
						if (!parts[i].isEmpty()) {
							parts[i] = parts[i].trim();
							 if (isCategory(parts[i].toUpperCase())) {
								 category = parts[i].toUpperCase();
							 }
							else if (isTime(parts[i])) {
								time = adaptTime(parts[i]);
							}
							else {
								team = parts[i].toUpperCase(); 
							}
						}
					} 
					
					if (isKey(fileName)) {
						newKey = generateKey(fileName);
					}
					String [] elements = {newKey,time,category,name};
					canBeWritten = isNoOneEmpty(elements);
					if (canBeWritten) {
						CleanWritable clean = new CleanWritable(time,category,range,name,team); 
						context.write(new Text(newKey), clean);
					}
				}
			}
			
			/***************** Auxiliar functions *****************/
			
			// Filters
			
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
				else if (time.matches("d{2,2}\\S\\S\\d{2,2}\\S")) {
					return true;
				}
				return false;
			}
			
			public boolean isKey(String fileName) {
				if (fileName.matches("\\d{2,2}\\S{2,30}\\d{2,2}")) {
					return true;
				}
				else if(fileName.matches("\\d{2,2}\\S{2,30}")) {
					return true;
				}
				return false;
			}
			
			public boolean isNoOneEmpty(String[] elements) {
				for (String el: elements) {
					if (el.isEmpty()) {
						return false;
					}
				}
				return true;
			}
			
			// Functions to adapt elements
			
			public String adaptTime(String time) {
				time = time.replaceAll("[^\\d]",":");
				time = time.split("\\.")[0];
				if (time.length() > 1 && time.charAt(time.length() - 1) == ':') {
					time = time.substring(0, time.length() -1);
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
			
			
			
			public String generateKey(String key) {
				String years = "";
				String city = "";
				String distance = "";
				if (key.matches("\\d{2,2}\\S{2,30}\\d{2,2}")) {
					years = key.substring(0,2);
					city = key.substring(2,key.length() - 2);
					distance = key.substring(key.length() - 2,key.length());
				}
				else if (fileName.matches("\\d{2,2}\\S{2,30}")) {
					years = key.substring(0,2);
					city = key.substring(2,key.length() - 2);
					distance = key.substring(key.length() - 2,key.length());
				}
				
				return city + ";" + years + ";" + distance + ";";
 			}
			
			/****************** Reflection function ******************/
			@Override
			protected void setup(Context context) throws IOException,
			        InterruptedException {
			    InputSplit split = context.getInputSplit();
			    Class<? extends InputSplit> splitClass = split.getClass();

			    FileSplit fileSplit = null;
			    if (splitClass.equals(FileSplit.class)) {
			        fileSplit = (FileSplit) split;
			        String[] fileNameSplit = fileSplit.getPath().getName().toString().split("/");
			        fileName = fileNameSplit[fileNameSplit.length - 1];
			        if (fileName.endsWith(".csv")) {
			        	fileName = fileName.substring(0,fileName.length() - 4);
			        }
			    } else if (splitClass.getName().equals(
			            "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
			    

			        try {
			            Method getInputSplitMethod = splitClass
			                    .getDeclaredMethod("getInputSplit");
			            getInputSplitMethod.setAccessible(true);
			            fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
			            String[] fileNameSplit = fileSplit.getPath().getName().toString().split("/");
				        fileName = fileNameSplit[fileNameSplit.length - 1];
				        if (fileName.endsWith(".csv")) {
				        	fileName = fileName.substring(0,fileName.length() - 4);
				        }
			            
			        } catch (Exception e) {
			            throw new IOException(e);
			        }

			    }
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
