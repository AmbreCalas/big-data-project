package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Iterables;

public class TP5 {
	// CLASS MAPPER
		public static class MyMapper extends Mapper<Object, Text, NullWritable, SortedMapWritable>{
			protected int k = 0;		       
			SortedMapWritable sortedCities = new SortedMapWritable();
			
			// map function
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

				// split lines to get cities informations
				String[] parts = value.toString().split(",");
				// if the city is valid
				if(isValid(parts)) {
					String population = parts[4];
					// check if population is not empty and if it's not the first line
					if (!population.isEmpty() && !isPopText(population)) {
						CityWritable city = new CityWritable(Integer.parseInt(population), generateKey(parts).toString());
						//Writable cityName = new Writable(generateKey(parts));
						sortedCities.put(city, generateKey(parts));
						
						if (sortedCities.size() > k) {
							sortedCities.remove(sortedCities.firstKey());
						}
					}
				}
			}

			// Key generator
			public Text generateKey(String[] parts) {
				String key = parts[0] + "," + parts[1];
				Text toReturn = new Text(key);
				return toReturn;
			}
			
			// Check if a line corresponds to a valid city
			public boolean isValid(String[] parts) {
				// If the line is long enough
				if (parts.length < 4) {
					return false;
				} 
				// If there is no empty field
				else if(parts[0].isEmpty() || parts[1].isEmpty()) {
					return false;
				} 
				return true;
			}
			
			// Check if a string corresponds to "pop text" (Population), to erase first line
			public boolean isPopText(String population) {
				if(population.equals("Population"))
					return true;
				return false;
			}
			
			public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				String kValue = conf.get("kValue");
				k = Integer.parseInt(kValue);
		
			}
			
			public void cleanUp(Context context) throws IOException, InterruptedException {
				context.write(NullWritable.get(), sortedCities);
			}
		}
	  
	  
		// CLASS REDUCER
		public static class MyReducer extends Reducer<NullWritable,SortedMapWritable,IntWritable,Writable> {
			int k = 0;
			public void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
				SortedMapWritable sortedCities = new SortedMapWritable();
				for(SortedMapWritable smw : values) {
					//sortedCity.put(smw.keySet());
					//for( )
				}
				//context.write(key, values[0]);				
			}
			
			public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				String kValue = conf.get("kValue");
				k = Integer.parseInt(kValue);
			}
		}
	  
	  
	  
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
		conf.set("kValue", args[1]);
	    Job job = Job.getInstance(conf, "TP5");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP5.class);
	    job.setMapperClass(MyMapper.class);
	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(Writable.class);
	    job.setReducerClass(MyReducer.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(RandomPointInputFormat.class);  
	    FileOutputFormat.setOutputPath(job, new Path(args[0])); 
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
	}