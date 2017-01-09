package bigdata;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// TP3
public class TP3_2 {
	
	public static int maxPop = 100000000;
	
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
	public static class SecondMapper extends Mapper<Object, Text, Text, ComputationWritable>{
			private String discretizationParam = "";
			
			public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				discretizationParam = conf.get("discretizationParam");
			}
			
			// map function
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				setUp(context);
				// split lines to get cities informations
				String[] parts = value.toString().split(",");
				String population = parts[2].trim();
				
				int intPopulation = Integer.parseInt(population);
				
				context.write(generateKey(population, context), new ComputationWritable(intPopulation, 1, intPopulation, intPopulation));
			}

			// Key generator
			public Text generateKey(String population, Context context) {
				int intPop = Integer.parseInt(population); // 250
				int logPop = (int) Math.log10(intPop); // 2
				
				// There is no discretization parameter : make log10 treatment
				if(discretizationParam.equals("null")) {
					Text toReturn = new Text(Integer.toString(logPop));
					return toReturn;
					
				} else { 
					int intDiscrParam = Integer.parseInt(discretizationParam); // 3
					
					int power10 = (int) Math.pow(10, logPop);  // 100
					int power10sup = (int) Math.pow(10, logPop + 1);  // 1000
					
					// separate new classes
					int i;
					double[] subClasses = new double[intDiscrParam + 1]; // length = 3
					subClasses[0] = power10; // 100
					String currentClass = "";
					
					for(i = 1; i <= intDiscrParam; i ++) {
						subClasses[i] = ((double) ((double) power10sup - (double) power10) / intDiscrParam) + subClasses[i-1];
						if(intPop >= (int) subClasses[i-1] && intPop < (int) subClasses[i]) {
							currentClass = Integer.toString((int) subClasses[i-1]) + "." + Integer.toString((int) subClasses[i]); // 100.400
							break;
						}
					}
				
					String strToReturn = Integer.toString(logPop) + "." + currentClass;
					Text toReturn = new Text(strToReturn);
					return toReturn;
				}
			}
			
		}
	  
	
	// CLASS COMBINER
	
	public static class Combiner extends Reducer<Text,ComputationWritable,Text,ComputationWritable> {
		public void reduce(Text key, Iterable<ComputationWritable> values, Context context) throws IOException, InterruptedException {
			
			int valuesCount = 0;
			int valuesSum = 0;
			int valuesMin = maxPop;
			int valuesMax = 0;
			for(ComputationWritable value : values) {
				valuesCount += value.count;
				valuesSum += value.sum;
				if(value.max > valuesMax) valuesMax = value.max;
				if(value.min < valuesMin) valuesMin = value.min;				
			}
			ComputationWritable partialSum = new ComputationWritable(valuesSum, valuesCount, valuesMin, valuesMax);
			context.write(key, partialSum);
		}
	}
	  
	// CLASS SECOND REDUCER
	public static class SecondReducer extends Reducer<Text,ComputationWritable,Text,DisplayWritable> {
			public boolean isFirst= true;
			
			public void reduce(Text key, Iterable<ComputationWritable> values, Context context) throws IOException, InterruptedException {
				if(isFirst) {
					setUp(context);
					isFirst = false;
				}
				
				double inf = 0;
				double sup = 0; 
				String lineName = "";
				String stringKey = key.toString();
				
				String[] logTab = stringKey.split("\\.");
				if(logTab.length > 1) { // par exemple pour discr = 3 et pop = 250, key = 2.100.400 (populations allant de 100 à 400)
					
					int firstPart = Integer.parseInt(logTab[0]); // 2 
					inf = Integer.parseInt(logTab[1]); // 100 
					sup = Integer.parseInt(logTab[2]); // 400 
					
				} else {
				// Get log value
					int lineInt = Integer.parseInt(key.toString());// Set line name
					inf = Math.pow(10, lineInt);
					sup = Math.pow(10, lineInt + 1);
				}
				
				lineName = Integer.toString((int) inf) + ".." + Integer.toString((int) sup);
				Text finalKey = new Text(lineName);
				
				
				int valuesCount = 0;
				int valuesSum = 0;
				int valuesMin = maxPop;
				int valuesMax = 0;
				for(ComputationWritable value : values) {
					valuesCount += value.count;
					valuesSum += value.sum;
					if(value.max > valuesMax) valuesMax = value.max;
					if(value.min < valuesMin) valuesMin = value.min;				
				}
				double avg = (double) valuesSum / (double) valuesCount;
				DisplayWritable totalResult = new DisplayWritable(Integer.toString(valuesCount), Double.toString(avg), Integer.toString(valuesMin), Integer.toString(valuesMax));
			
				context.write(finalKey, totalResult);
			}
			
			
			public void setUp(Context context) throws IOException, InterruptedException {
				DisplayWritable toWrite = new DisplayWritable("Class     Count", "Average", "Min", "Max");
				context.write(null, toWrite);
			}
		}
		
	
	public static int randomInt(int min, int max) {
		return min + (int)(Math.random() * ((max - min) + 1));
	}
		
		
	
	public static void firstJob(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "TP3_2");
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
	
	public static void secondJob(String input, String output, String discretizationParam) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("discretizationParam", discretizationParam);
	    Job job = Job.getInstance(conf, "TP3_2");
	    job.setJobSetupCleanupNeeded(true);
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TP3_2.class);
	    job.setMapperClass(SecondMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(ComputationWritable.class);
	    // We had the combiner here
	    job.setCombinerClass(Combiner.class);
	    job.setReducerClass(SecondReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DisplayWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class); 
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
  
	// MAIN
	public static void main(String[] args) throws Exception {
		String middlePath = "/users/acalas001/tp3_2." + randomInt(0,100);
		int argsLength = args.length;
		firstJob(args[0], middlePath);
		if(argsLength > 2) {
			secondJob(middlePath, args[1], args[2]);
		} else {
			secondJob(middlePath, args[1], "null");
		}
	    
	}
}
