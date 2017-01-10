package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SumUpMapReduce {

	// CLASS MAPPER
		public static class SumUpMapper extends Mapper<Object, Text, Text, SumUpWritable>{
			// map function
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				// split lines to get different informations
				String[] parts = value.toString().split(";");
				Text raceKey = new Text("");
				
				// TO DOOOOOO : TREAT SPEED
				double speed = 0;
				//if(parts[2].charAt(0) >= '0' || parts[5].charAt(0) <= '9') {
					//speed = 
				//}
				String category = "";
				
				
				// First name and last name are not in the same part
				if(parts.length == 9) {
					category = treatCategory(parts[8]);
					raceKey = generateKey(parts[2], category);
					context.write(raceKey, new SumUpWritable(category, parts[2], "1", parts[7], 
							parts[7], parts[7], Double.toString(speed)));
			
				} 
				// First name and last name are in the same part
				else if(parts.length == 8) {
					// Handle weird category place
					if(parts[5].charAt(0) >= '0' || parts[5].charAt(0) <= '9') {
						category = treatCategory(parts[5]);
						raceKey = generateKey(parts[2], category);	
						context.write(raceKey, new SumUpWritable(category, parts[2], "1", parts[6], 
								parts[6], parts[6], Double.toString(speed)));
					} 
					// Category is at the right place
					else {
						category = treatCategory(parts[7]);
						raceKey = generateKey(parts[2], category);
						context.write(raceKey, new SumUpWritable(category, parts[2], "1", parts[6], 
								parts[6], parts[6], Double.toString(speed)));		
					}
				}
			}
			
			// Key generator
			public Text generateKey(String distance, String category) {
				String raceKey = distance + ";" + category;
				Text toReturn = new Text(raceKey);
				return toReturn;
			}
			
			public String treatCategory(String category) {
				String toReturn = "";
				if(category.charAt(0) >= '0' || category.charAt(0) <= '9') {
					String[] categoryParts = category.split(" ");
					if(categoryParts.length == 2) {	
						toReturn = categoryParts[1];
					} else if(categoryParts.length == 3) {
						toReturn = categoryParts[1] + categoryParts[2];
					} else if(categoryParts.length == 4) {
						toReturn = categoryParts[1] + categoryParts[2] + categoryParts[3];
					}
				return toReturn;
				} else {
					return category;
				}
			}
		}
	  
	  
		// CLASS REDUCER
		public static class SumUpReducer extends Reducer<Text,SumUpWritable,Text,SumUpWritable> {
			public void reduce(Text key, Iterable<SumUpWritable> values, Context context) throws IOException, InterruptedException {
				String[] keyParts = key.toString().split(";");
				String category = keyParts[0];
				String distance = keyParts[1];
				
				long nbPax = 0;
				long minTimeInSeconds = 10*3600;
				long maxTimeInSeconds = 0;
				long avgTimeInSeconds = 0;
				double speedSum = 0;
				
				for(SumUpWritable value : values) {
					nbPax++;
					avgTimeInSeconds += Long.parseLong(value.avgTime);
					speedSum += Double.parseDouble(value.avgSpeed);
					if(minTimeInSeconds > Long.parseLong(value.minTime)) {
						minTimeInSeconds = Long.parseLong(value.minTime);
					} 
					if(maxTimeInSeconds > Long.parseLong(value.maxTime)) {
						maxTimeInSeconds = Long.parseLong(value.maxTime);
					}
				}
				double avgSpeed = speedSum / nbPax;
				
				SumUpWritable toReturn = new SumUpWritable(category, distance, Long.toString(nbPax), 
						Long.toString(minTimeInSeconds), Long.toString(maxTimeInSeconds), 
						Long.toString(avgTimeInSeconds), Double.toString(avgSpeed));
				
				context.write(key, toReturn);
			}
		}
}
