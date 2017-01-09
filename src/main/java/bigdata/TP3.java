package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// TP3
public class TP3 {
	
	// CLASS MAPPER
	public static class TP3Mapper extends Mapper<Object, Text, Text, IntWritable>{
		// map function
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// split lines to get cities informations
			String[] parts = value.toString().split(",");
			// if the city is valid
			if(isValid(parts)) {
				String population = parts[4];
				// check if population is not empty and if it's not the first line
				if (!population.isEmpty() && !isPopText(population)) {
					// Add a city to nb_cities counter
					context.getCounter("WBC", "nb_cities").increment(1);
					// Give this result to the reducer
					context.write(generateKey(parts), new IntWritable(Integer.parseInt(population)));
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
	}
  
  
	// CLASS REDUCER
	public static class TP3Reducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// Add a city to nb_pop counter
			context.getCounter("WBC", "nb_pop").increment(1);
			IntWritable maximum = new IntWritable(0);
			System.out.println();
			// get maximum population value for this city
			for(IntWritable value : values) {
				if(value.compareTo(maximum) == 1) {
					maximum = value;
				}
			}
			// Add population to total_pop counter
			context.getCounter("WBC", "total_pop").increment(maximum.get());
			// commit the city with the right population
			context.write(key, maximum);
		}
	}
  
  
	// MAIN
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "TP3");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP3.class);
	    job.setMapperClass(TP3Mapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(TP3Reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
