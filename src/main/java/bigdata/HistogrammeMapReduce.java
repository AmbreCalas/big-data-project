package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class HistogrammeMapReduce {
	private final static int SPEED1 = 5;
	private final static int SPEED2 = 10;
	private final static int SPEED3 = 15;
	private final static int SPEED4 = 20;
	private final static String[] CATEGORIES = {"VETERAN", "SENIOR", "JUNIOR", "CADET", "ESPOIR", "MINIME", "CADETTE", "BENJAMIN", "HANDISPORT"};
		
	// FIRST MAPPER
	public static class HistoMapper extends
			Mapper<Object, Text, Text, HistogrammeWritable> {
		
		// map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] parts = value.toString().split(";");
			if (parts.length > 3) {
				String stringKey = getKey(parts);
				String category = getCategory(parts[4]);
				if(!stringKey.equals("") && !category.equals("")) {
					context.write(new Text(stringKey), new HistogrammeWritable(
							category, parts[2], parts[3], "1"));
				}
			}
		}
		
		public String getCategory(String dirtyCategory) {
			String[] catParts = dirtyCategory.split(" ");
			for(String part: catParts) {
				for(String cat: CATEGORIES) {
					if(part.equals(cat)) {
						return part;
					}
				}
			}
			return "";
		}
		
		public String getKey(String[] parts) {
			String timeKeyPart = getTimeKeyPart(parts[2], parts[3]);
			if(!timeKeyPart.equals("")) {
				return getCategory(parts[4]) + ";" + parts[2] + ";" + timeKeyPart;
			}
			return "";
		}
		
		public String getTimeKeyPart(String stringDist, String time) {
			int distance = 0;
			try {
				distance = Integer.parseInt(stringDist);
			} catch (Exception e) {
				return "";
			}
			int timeInSeconds = timeInSeconds(time);
			
			int timeLimit1 = (int) ((float) distance / SPEED1) * 60; // 5*60/5 = 60 
			int timeLimit2 = (int) ((float) distance / SPEED2) * 60; // 5*60/10 = 30
			int timeLimit3 = (int) ((float) distance / SPEED3) * 60; // 5*60/15 = 20
			int timeLimit4 = (int) ((float) distance / SPEED4) * 60; // 5*60/20 = 15
			
			if(timeInSeconds > (timeLimit1 * 60)) {
				return timeLimit1 + "..+";
			} else if(timeInSeconds > (timeLimit2 * 60)) {
				return Integer.toString(timeLimit2) + ".." + Integer.toString(timeLimit1);
			} else if(timeInSeconds > (timeLimit3 * 60)) {
				return Integer.toString(timeLimit3) + ".." + Integer.toString(timeLimit2);				
			} else if(timeInSeconds > (timeLimit4 * 60)) {
				return Integer.toString(timeLimit4) + ".." + Integer.toString(timeLimit3);				
			} else if(timeInSeconds < (timeLimit4 * 60)) {
				return  "0.." + Integer.toString(timeLimit4);				
			}
			return "";
		}
		
		public int timeInSeconds(String time) {
			String[] timeParts = time.split(":");
			int hours = Integer.parseInt(timeParts[0]);
			int minutes = Integer.parseInt(timeParts[1]);
			int seconds = Integer.parseInt(timeParts[2]);
			return hours*60*60 + minutes*60 + seconds;
		}
	}

	// FIRST REDUCER
	public static class HistoReducer extends
			Reducer<Text, HistogrammeWritable, NullWritable, HistogrammeWritable> {

		public void reduce(Text key, Iterable<HistogrammeWritable> values,
				Context context) throws IOException, InterruptedException {
			
			String[] parts = key.toString().split(";");
			String distance = parts[0];
			String category = parts[1];
			String time = parts[2];
			
			String[] timeParts = time.split("\\.\\.");
			if(timeParts.length > 0) {
				if(timeParts[0].equals("0")) {
					context.write(null, new HistogrammeWritable(" "," ", " ", " "));
				}
			}
			
			int count = 0;
			for (HistogrammeWritable value : values) {
				count += Integer.parseInt(value.nbPax);
			}
			context.write(null, new HistogrammeWritable(distance,
					category, time, Integer.toString(count)));
		}
	}
}
