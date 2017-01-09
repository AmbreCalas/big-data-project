package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Iterables;

public class TP4 {
	  static int totalCount=0;
	  static int innerCount=0;
	  
	  public static class MyMapper
	  extends Mapper<LongWritable, Point2DWritable, LongWritable, DoubleWritable>{
		  int nbPoints = 0; 
		  public void map(LongWritable key, Point2DWritable value, Context context
				  ) throws IOException, InterruptedException {
				  setUp(context);
			  
			  totalCount++;
			  if(Math.pow(value.getX(), 2) + Math.pow(value.getY(), 2) <= 1) {
				  innerCount++;
			  }
			  if(totalCount == nbPoints) {				    
				  double finalResult = (double) (((double) innerCount) / totalCount) * 4;
				  DoubleWritable result = new DoubleWritable(finalResult);
				  context.write((LongWritable) key, result);
			  }
		  }
		  
		  public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				nbPoints = Integer.parseInt(conf.get("nbPoints"));
			}
	  } 
  
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	conf.set("nbMappers", args[1]);
	conf.set("nbPoints", args[2]);
    Job job = Job.getInstance(conf, "TP4");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP4.class);
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(RandomPointInputFormat.class);  
    FileOutputFormat.setOutputPath(job, new Path(args[0])); 
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
