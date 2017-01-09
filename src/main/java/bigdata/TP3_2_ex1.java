package bigdata;

import java.io.IOException;
import java.util.Collection;

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
public class TP3_2_ex1 {
	
	// CLASS FIRST MAPPER
	public static class FirstMapper extends Mapper<Object, Text, Text, IntWritable>{
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
			String key = parts[0] + "," + parts[1] + ",";
			Text toReturn = new Text(key);
			return toReturn;
		}
		
		// Check if a line corresponds to a valid city
		public boolean isValid(String[] parts) {
			// If the line is long enough
			if (parts.length < 3) {
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
  
  
	// CLASS FIRST REDUCER
	public static class FirstReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// Add a city to nb_pop counter
			context.getCounter("WBC", "nb_pop").increment(1);
			IntWritable maximum = new IntWritable(0);
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
  
	
	
	
	// CLASS SECOND MAPPER
	public static class SecondMapper extends Mapper<Object, Text, Text, IntWritable>{
			// map function
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				// split lines to get cities informations
				String[] parts = value.toString().split(",");
				String population = parts[2].trim();
				
				context.write(generateKey(population), new IntWritable(Integer.parseInt(population)));
			}

			// Key generator
			public Text generateKey(String population) {
				int intPop = Integer.parseInt(population);
				int logPop = (int) Math.log10(intPop);
				Text toReturn = new Text(Integer.toString(logPop));
				return toReturn;
			}
			
		}
	  
	
	// CLASS COMBINER
	
	public static class Combiner extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int valuesLength = 0;
			for(IntWritable value : values) {
				valuesLength++;
			}
			IntWritable partialSum = new IntWritable(valuesLength);
			context.write(key, partialSum);
		}
	}
	  
	// CLASS SECOND REDUCER
	public static class SecondReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				// Get log value
				int lineInt = Integer.parseInt(key.toString());
				// Set line name
				double inf = Math.pow(10, lineInt);
				double sup = Math.pow(10, lineInt + 1);
				String lineName = Integer.toString((int) inf) + ".." + Integer.toString((int) sup);
				Text finalKey = new Text(lineName);
				
				int intNbCities  = 0;
				for(IntWritable value : values) {
					intNbCities += value.get();
				}
				IntWritable nbCities = new IntWritable(intNbCities);
			
				context.write(finalKey, nbCities);
			}
		}
		
		
		
		
	
	public static void firstJob(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "TP3_2_ex1");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP3_2.class);
	    job.setMapperClass(FirstMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setReducerClass(FirstReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.waitForCompletion(true);
	}
	
	public static void secondJob(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "TP3_2_ex1");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP3_2.class);
	    job.setMapperClass(SecondMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    // We had the combiner here
	    job.setCombinerClass(Combiner.class);
	    job.setReducerClass(SecondReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class); 
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
  
	// MAIN
	public static void main(String[] args) throws Exception {
		String middlePath = "/users/acalas001/TP3_2_ex1";
		firstJob(args[0], middlePath);
	    secondJob(middlePath, args[1]);
	    
	}
}
