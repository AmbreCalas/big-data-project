package bigdata;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import bigdata.FilesMapReduce.FilesMapper;
import bigdata.FilesMapReduce.FilesReducer;
import bigdata.HistogrammeMapReduce.HistoMapper;
import bigdata.HistogrammeMapReduce.HistoReducer;
import bigdata.PredictionMapReduce.PredictionCombiner;
import bigdata.PredictionMapReduce.PredictionMapper;
import bigdata.PredictionMapReduce.PredictionReducer;
import bigdata.TopKClubMapReduce.TopKClubCombiner;
import bigdata.TopKClubMapReduce.TopKClubMapper;
import bigdata.TopKClubMapReduce.TopKClubReducer;
import bigdata.TopKPerfMapReduce.TopKPerfMapper;
import bigdata.TopKPerfMapReduce.TopKPerfReducer;
import bigdata.TopKPopMapReduce.TopKPopFirstMapper;
import bigdata.TopKPopMapReduce.TopKPopFirstReducer;
import bigdata.TopKPopMapReduce.TopKPopSecondMapper;
import bigdata.TopKPopMapReduce.TopKPopSecondReducer;

public class ProjectMain {
	private static String inputFile;
	private static String outputFile;
	private static String kValue;
	
	
	private static void filesTreatment() throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "FilesMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(FilesMapReduce.class);
	    job.setMapperClass(FilesMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(CleanWritable.class);
	    job.setReducerClass(FilesReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(CleanWritable.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);  
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    
	    job.setNumReduceTasks(1);
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static void topKPopTreatment(String whichTop) throws Exception {
		String middlePath = generateRandomFile("topkpop");
		topKPopFirstJob(inputFile, middlePath, whichTop);
		topKPopSecondJob(middlePath, outputFile, whichTop);
	}
	
	private static void topKPopFirstJob(String input, String output, String whichTop) throws Exception{
		Configuration conf = new Configuration();	
		conf.set("kValue", kValue);		
		conf.set("whichTop", whichTop);
	    Job job = Job.getInstance(conf, "TopKPopMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TopKPopMapReduce.class);
	    job.setMapperClass(TopKPopFirstMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    if(whichTop == "1") {
		    job.setMapOutputValueClass(TopRaceWritable.class);	    	
	    } else if (whichTop == "2") {
		    job.setMapOutputValueClass(TopRaceAgeWritable.class);	    	
	    }
	    job.setReducerClass(TopKPopFirstReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
	    if(whichTop == "1") {
		    job.setOutputValueClass(TopRaceWritable.class);    	
	    } else if (whichTop == "2") {
		    job.setOutputValueClass(TopRaceAgeWritable.class);    	
	    }
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.waitForCompletion(true);
	}
	
	private static void topKPopSecondJob(String input, String output, String whichTop) throws Exception{
		Configuration conf = new Configuration();	
		conf.set("kValue", kValue);		
		conf.set("whichTop", whichTop);
		conf.set("middlePath", input);	
	    Job job = Job.getInstance(conf, "TopKPopMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TopKPopMapReduce.class);
	    job.setMapperClass(TopKPopSecondMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    if(whichTop == "1") {
		    job.setMapOutputValueClass(TopRaceWritable.class);	    	
	    } else if (whichTop == "2") {
		    job.setMapOutputValueClass(TopRaceAgeWritable.class);	    	
	    }
	    job.setReducerClass(TopKPopSecondReducer.class);
	    job.setOutputKeyClass(Text.class);
	    if(whichTop == "1") {
		    job.setOutputValueClass(TopRaceWritable.class);    	
	    } else if (whichTop == "2") {
		    job.setOutputValueClass(TopRaceAgeWritable.class);    	
	    }
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	public static String generateRandomFile(String part) {
		Random rand = new Random();
	    int randomNum = rand.nextInt((99 - 1) + 1) + 1;
		return "/users/acalas001/" + part + randomNum;
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
	    if(whichTop == "1") {
		    job.setMapOutputValueClass(TopPerfDistanceWritable.class);    	
	    } else if (whichTop == "2") {
		    job.setMapOutputValueClass(TopPerfDistanceCatWritable.class);	    	
	    }
	    job.setReducerClass(TopKPerfReducer.class);
	    job.setOutputKeyClass(Text.class);
	    if(whichTop == "1") {
		    job.setOutputValueClass(TopPerfDistanceWritable.class); 	
	    } else if (whichTop == "2") {
		    job.setOutputValueClass(TopPerfDistanceCatWritable.class);    	
	    }
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	
	private static void topKClubTreatment() throws Exception {
		Configuration conf = new Configuration();			
		conf.set("kValue", kValue);
	    Job job = Job.getInstance(conf, "topKClubMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(TopKClubMapReduce.class);
	    job.setMapperClass(TopKClubMapper.class);
	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TopClubWritable.class);
	    job.setCombinerClass(TopKClubCombiner.class);
	    job.setReducerClass(TopKClubReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(TopClubWritable.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	private static void histogrammeTreatment() throws Exception {
		Configuration conf = new Configuration();	
	    Job job = Job.getInstance(conf, "TopKPopMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(HistogrammeMapReduce.class);
	    job.setMapperClass(HistoMapper.class);
	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HistogrammeWritable.class);	    	
	    job.setReducerClass(HistoReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(HistogrammeWritable.class);    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	
	private static void predictionTreatment(String category, String previousDistance, String previousTime, String futureDistance) throws Exception {
		Configuration conf = new Configuration();	
		conf.set("category", category);
		conf.set("previousDistance", previousDistance);
		conf.set("previousTime", previousTime);
		conf.set("futureDistance", futureDistance);
	    Job job = Job.getInstance(conf, "PredictionMapReduce");
	    job.setNumReduceTasks(1);
	    job.setJarByClass(PredictionMapReduce.class);
	    job.setMapperClass(PredictionMapper.class);
	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PredictionWritable.class);	  
	    job.setCombinerClass(PredictionCombiner.class);  	
	    job.setReducerClass(PredictionReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(PredictionWritable.class);    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(inputFile));
	    FileOutputFormat.setOutputPath(job, new Path(outputFile));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	// MAPPER clé: category;distance valeur: category;distance;temps
	// COMBINER si distance = distance1 récupère place temps1 par rapport à ses collègues et je réécris juste les distance2
	// REDUCER prends le temps correspondant à la place 
	
	public static void main(String[] args) throws Exception {
		inputFile = args[0];
		outputFile = args[1];
		
		// Not enough arguments
		if(args.length < 3) {
			System.out.println("You need at least 3 arguments");
		} 
		// Sum up option
		else if(args[2].equals("1")) {
			filesTreatment();
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
					topKPopTreatment("2");
				} 
				// Top k performance distance
				else if(args[3].equals("3")) {
					topKPerfTreatment("1");
				} 
				// Top k performance category and distance
				else if(args[3].equals("4")) {
					topKPerfTreatment("2");
				} 
				// Top k most active club
				else if(args[3].equals("5")) {
					topKClubTreatment();
				}
				// No more options
				else {
					System.out.println("Fourth argument must be between 1 and 5");
				}
			}
		} 
		// Prediction option
		else if(args[2].equals("3")) {
			// Not enough arguments
			if (args.length < 4) {
				System.out.println("You need at least one more argument for this option");
			}
			// Get option
			else {
				// Histogramme
				if (args[3].equals("1")) {
					histogrammeTreatment();
				}

				// Prediction
				else if (args[3].equals("2")) {
					if (args.length < 8) {
						System.out.println("You need at least one more argument for this option");
					}
					else {
						predictionTreatment(args[4], args[5], args[6], args[7]);
					}
				}
			}
		}
		// No more options
		else {
			System.out.println("Third argument must be between 1 and 3");
		}
	}
}
