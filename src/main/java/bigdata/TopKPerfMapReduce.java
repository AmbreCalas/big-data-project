package bigdata;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopKPerfMapReduce {

	// CLASS MAPPER
		public static class TopKPerfMapper extends Mapper<Object, Text, Text, TopPerfDistanceWritable>{
			protected int k = 0;
			protected int top = 0;
			
			// map function
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				setUp(context);
				
				String[] parts = value.toString().split(";");
				if(parts.length > 6) {
					if(top == 1) {
						context.write(new Text(parts[2]), new TopPerfDistanceWritable(parts[2], parts[3], parts[6]));
					} else if(top == 2) {
						context.write(new Text(parts[2]), new TopPerfDistanceCatWritable(parts[2], parts[3], parts[6], getCategory(parts[4])));
					}
				}
			}
			
			public String getCategory(String dirtyCategory) {
				String[] catParts = dirtyCategory.split(" ");
				boolean isFirst = true;
				String result = "";
				for(String part : catParts) {
					if(!isFirst) {
						result += part + " ";
					} else {
						isFirst = false;
					}
				}
				return result;
			}
			
			public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				String kValue = conf.get("kValue");
				k = Integer.parseInt(kValue);

				String whichTop = conf.get("whichTop");
				top = Integer.parseInt(whichTop);
			}
		}
	  
	  
		// CLASS REDUCER
		public static class TopKPerfReducer extends Reducer<Text,TopPerfDistanceWritable,Text,TopPerfDistanceWritable> {
			protected int k = 0;
			protected int top = 0;
			
			public void reduce(Text key, Iterable<TopPerfDistanceWritable> values, Context context) throws IOException, InterruptedException {
				setUp(context);
				
				
				TreeMap<Long, TopPerfDistanceWritable> sortedPerfs = new TreeMap<Long, TopPerfDistanceWritable>();
				for(TopPerfDistanceWritable value : values) {
					TopPerfDistanceWritable perf = new TopPerfDistanceWritable();
					if(top == 1) {
						perf = new TopPerfDistanceWritable(value.distance, value.perf, value.pax);
					} else if(top == 2) {
						perf = new TopPerfDistanceCatWritable(value.distance, value.perf, value.pax, ((TopPerfDistanceCatWritable) value).category);
					}
					
					sortedPerfs.put(timeInSeconds(perf.perf), perf);
					if (sortedPerfs.size() > k) {
						sortedPerfs.remove(sortedPerfs.lastKey());
					}
				}
				
				long position = 1;
				for(Map.Entry<Long, TopPerfDistanceWritable> perf : sortedPerfs.entrySet()) {
					if(top == 1) {
						context.write(new Text(perf.getValue().distance + " - " + Long.toString(position)), perf.getValue());
					} else if(top == 2) {
						context.write(new Text("(" + perf.getValue().distance + ", " + ((TopPerfDistanceCatWritable) perf.getValue()).category + ") - " + Long.toString(position)), perf.getValue());
					}
					position++;
				}
			}
			
			public long timeInSeconds(String time) {
				String myTime = time.trim();
				String[] timeParts = myTime.split(":");
				long hours = Long.parseLong(timeParts[0]);
				long minutes = Long.parseLong(timeParts[1]);
				long seconds = Long.parseLong(timeParts[2]);
				return hours*60*60 + minutes*60 + seconds;
			}
			
			public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				String kValue = conf.get("kValue");
				k = Integer.parseInt(kValue);

				String whichTop = conf.get("whichTop");
				top = Integer.parseInt(whichTop);
			}
		}
}
