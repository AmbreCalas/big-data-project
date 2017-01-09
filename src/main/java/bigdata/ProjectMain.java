package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigdata.SumUpMapReduce.SumUpMapper;
import bigdata.SumUpMapReduce.SumUpReducer;
import bigdata.TopKPerfMapReduce.TopKPerfMapper;
import bigdata.TopKPerfMapReduce.TopKPerfReducer;
import bigdata.TopKPopMapReduce.TopKPopMapper;
import bigdata.TopKPopMapReduce.TopKPopReducer;

public class ProjectMain {
	private static String inputFile;
	private static String outputFile;
	private static String kValue;
	
	
	private static void sumUpTreatment() throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "SumUpMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(SumUpMapReduce.class);
	    job.setMapperClass(SumUpMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(SumUpWritable.class);
	    job.setReducerClass(SumUpReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(SumUpWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(RandomPointInputFormat.class);  
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static void topKPopTreatment(String whichTop) throws Exception {
		Configuration conf = new Configuration();		
		conf.set("kValue", kValue);		
		conf.set("whichTop", whichTop);
	    Job job = Job.getInstance(conf, "TopKPopMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TopKPopMapReduce.class);
	    job.setMapperClass(TopKPopMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(TopRaceWritable.class);
	    job.setReducerClass(TopKPopReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(TopRaceWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(RandomPointInputFormat.class);  
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	
	private static void topKPerfTreatment(String whichTop) throws Exception {
		Configuration conf = new Configuration();			
		conf.set("kValue", kValue);			
		conf.set("whichTop", whichTop);
	    Job job = Job.getInstance(conf, "topKPerfMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TopKPerfMapReduce.class);
	    job.setMapperClass(TopKPerfMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(TopPerfDistanceWritable.class);
	    job.setReducerClass(TopKPerfReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(TopPerfDistanceWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setInputFormatClass(RandomPointInputFormat.class);  
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	

	public static void main(String[] args) throws Exception {
		inputFile = args[0];
		outputFile = args[1];
		
		// Not enough arguments
		if(args.length < 3) {
			System.out.println("You need at least 3 arguments");
		} 
		// Sum up option
		else if(args[2].equals("1")) {
			sumUpTreatment();
		} 
		// Top k option
		else if(args[2].equals("2")) {
			// Not enough arguments
			if(args.length < 5) {
				System.out.println("You need at least one more argument for this option");	
			} 
			// Get k 
			else {
				kValue = args[4];
				// Top k distance
				if(args[3].equals("1")) {
					topKPopTreatment("1");
				}
	
				// Top k distance and category
				else if(args[3].equals("2")) {
					topKPopTreatment("1");
				} 
				// Top k performance distance
				else if(args[3].equals("3")) {
					topKPerfTreatment("1");
				} 
				// Top k performance category and distance
				else if(args[3].equals("4")) {
					topKPerfTreatment("2");
				} 
				// No more options
				else {
					System.out.println("Fourth argument must be 1 or 2");
				}
			}
		} 
		// Prediction option
		else if(args[2].equals("3")) {
			// TODOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO
		} 
		// No more options
		else {
			System.out.println("Third argument must be 1, 2 or 3");
		}
	}
}
