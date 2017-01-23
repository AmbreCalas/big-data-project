package bigdata;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
				setUp(context);
				
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
		public static class TopKPopFirstReducer extends Reducer<Text,TopRaceWritable,NullWritable,TopRaceWritable> {
			int k = 0;
			int top = 0;
			
			public void reduce(Text key, Iterable<TopRaceWritable> values, Context context) throws IOException, InterruptedException {
				setUp(context);
				
				int count = 0;
				String[] raceKeyParts = key.toString().split(";");
				for(TopRaceWritable value : values) {
					count += Integer.parseInt(value.nbPax);
				}
				if(top == 1) {
					context.write(null, new TopRaceWritable(raceKeyParts[0], raceKeyParts[1], raceKeyParts[2], Integer.toString(count)));
				} else if(top == 2) {
					context.write(null, new TopRaceAgeWritable(raceKeyParts[0], raceKeyParts[1], raceKeyParts[2], Integer.toString(count), raceKeyParts[3]));
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
				public static class TopKPopSecondMapper extends Mapper<Object, Text, Text, TopRaceWritable>{
					protected int k = 0;
					protected int top = 0;
					
					// map function
					public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
						setUp(context);
						
						String[] parts = value.toString().split(";");
						
						if(parts.length == 3) {
							String city = parts[0];
							String year = parts[1];
							String[] popParts = parts[2].split(" : ");
							String distance = popParts[0];
							String nbPax = popParts[1];

							context.write(new Text(distance), new TopRaceWritable(city, year, distance, nbPax));
						} else if(parts.length == 4) {
							

							String city = parts[0];
							String year = parts[1];
							String distance = parts[2];
							String[] popParts = parts[3].split(" : ");
							String category = popParts[0];
							String nbPax = popParts[1];
							
							context.write(new Text(distance), new TopRaceAgeWritable(city, year, distance, nbPax, category));
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
			  
			  
				// SECOND CLASS REDUCER
				public static class TopKPopSecondReducer extends Reducer<Text,TopRaceWritable,Text,TopRaceWritable> {
					protected int k = 0;
					protected int top = 0;
					
					public void reduce(Text key, Iterable<TopRaceWritable> values, Context context) throws IOException, InterruptedException {
						setUp(context);
						int count = 0;
						//SortedMapWritable sortedRaces = new SortedMapWritable();
						TreeSet<TopRaceWritable> sortedRaces = new TreeSet<TopRaceWritable>();
						//Text raceKey = new Text("");
						int isAdded = 0;
						for(TopRaceWritable value : values) {
							//raceKey.set(value.getKey());
							//sortedRaces.put(value, raceKey);
							if(sortedRaces.add(value)) {
								isAdded++;
							}
							/*if (sortedRaces.size() > k) {
								sortedRaces.remove(sortedRaces.firstKey());
							}*/
							if (sortedRaces.size() > k) {
								sortedRaces.remove(sortedRaces.first());
							}
						}
						long position = 1;
						//for(WritableComparable<TopRaceWritable> race : sortedRaces.keySet()) {
						for(WritableComparable<TopRaceWritable> race : sortedRaces) {
							TopRaceWritable thisRace = (TopRaceWritable) race;
							context.write(new Text(isAdded + " - " + thisRace.distance + " - " + Long.toString(position)), thisRace);
							position++;
						}
					}
					
					
					

					public void setUp(Context context) throws IOException, InterruptedException {
						// Get conf values
						Configuration conf = context.getConfiguration();
						
						// Get k value for the top k
						String kValue = conf.get("kValue");
						k = Integer.parseInt(kValue);

						// Get whichTop value for the option
						String whichTop = conf.get("whichTop");
						top = Integer.parseInt(whichTop);
						
						// Delete middlepath
						String middlePath = conf.get("middlePath");
						File file = new File(middlePath);
						/*if (file.exists() && file.canWrite())
						{
							file.delete();
						}*/
					}
			}
}
