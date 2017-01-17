package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopKPopMapReduce {

	// FIRST CLASS MAPPER
		public static class TopKPopFirstMapper extends Mapper<Object, Text, Text, TopRaceWritable>{
			protected int k = 0;
			protected int top = 0;
			
			// map function
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				
				String[] parts = value.toString().split(";");
				if(top == 1) {
					context.write(new Text(getRaceKey(parts)), new TopRaceWritable(parts[0], parts[1], parts[2], "1"));
				} else if(top == 2) {
					context.write(new Text(getRaceKeyCategory(parts)), new TopRaceAgeWritable(parts[0], parts[1], parts[2], "1", getCategory(parts[4])));
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
			
			public String getRaceKeyCategory(String[] parts) {
				return parts[0] + ";" + parts[1] + ";" + parts[2] + ";" + getCategory(parts[4]);
			}
			
			public String getRaceKey(String[] parts) {
				return parts[0] + ";" + parts[1] + ";" + parts[2];
			}
			

			public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				String kValue = conf.get("kValue");
				k = Integer.parseInt(kValue);

				String whichTop = conf.get("whichTop");
				top = Integer.parseInt(whichTop);
			}
		}
	  
	  
		// FIRST CLASS REDUCER
		public static class TopKPopFirstReducer extends Reducer<Text,TopRaceWritable,Text,TopRaceWritable> {
			int k = 0;
			int top = 0;
			
			public void reduce(Text key, Iterable<TopRaceWritable> values, Context context) throws IOException, InterruptedException {
				int count = 0;
				String[] raceKeyParts = key.toString().split(";");
				for(TopRaceWritable value : values) {
					count += Integer.parseInt(value.nbPax);
				}
				if(top == 1) {
					context.write(key, new TopRaceWritable(raceKeyParts[0], raceKeyParts[1], raceKeyParts[2], Integer.toString(count)));
				} else if(top == 2) {
					context.write(key, new TopRaceAgeWritable(raceKeyParts[0], raceKeyParts[1], raceKeyParts[2], Integer.toString(count), raceKeyParts[3]));
				}
			}
			
			public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				String kValue = conf.get("kValue");
				k = Integer.parseInt(kValue);

				String whichTop = conf.get("whichTop");
				top = Integer.parseInt(whichTop);
			}
		}
		
		// SECOND CLASS MAPPER
				public static class TopKPopSecondMapper extends Mapper<Text, TopRaceWritable, Text, TopRaceWritable>{
					protected int k = 0;
					protected int top = 0;
					
					// map function
					public void map(Text key, TopRaceWritable value, Context context) throws IOException, InterruptedException {
						context.write(new Text(value.distance), value);
					}

					public void setUp(Context context) throws IOException, InterruptedException {
						Configuration conf = context.getConfiguration();
						String kValue = conf.get("kValue");
						k = Integer.parseInt(kValue);

						String whichTop = conf.get("whichTop");
						top = Integer.parseInt(whichTop);
					}
				}
			  
			  
				// SECOND CLASS REDUCER
				public static class TopKPopSecondReducer extends Reducer<Text,TopRaceWritable,Text,TopRaceWritable> {
					protected int k = 0;
					protected int top = 0;
					
					public void reduce(Text key, Iterable<TopRaceWritable> values, Context context) throws IOException, InterruptedException {
						int count = 0;
						SortedMapWritable sortedRaces = new SortedMapWritable ();
						Text raceKey = new Text("");
						for(TopRaceWritable value : values) {
							raceKey.set(value.getKey());
							sortedRaces.put(value, raceKey);
							if (sortedRaces.size() > k) {
								sortedRaces.remove(sortedRaces.firstKey());
							}
						}
						for(WritableComparable<TopRaceWritable> race : sortedRaces.keySet()) {
							context.write(new Text(((TopRaceWritable) race).getKey()), (TopRaceWritable) race);
						}
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
