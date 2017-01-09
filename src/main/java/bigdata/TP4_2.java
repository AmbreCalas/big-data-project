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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Iterables;

public class TP4_2 {
  public static class MyMapper
       extends Mapper<LongWritable, Point2DWritable, LongWritable, Point2DWritable>{
      public void map(LongWritable key, Point2DWritable value, Context context
              ) throws IOException, InterruptedException {
          context.write((LongWritable) key, value);
      }
  }
  
  public static class MyReducer
       extends Reducer<LongWritable,Point2DWritable,LongWritable,Point2DWritable> {
    public void reduce(LongWritable key, Iterable<Point2DWritable> values,
                       Context context) throws IOException, InterruptedException {
    	for(Point2DWritable value : values) {
    		context.write(key, value);
    	}
    }
  }
  
  
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	conf.set("nbMappers", args[1]);
	conf.set("nbPoints", args[2]);
    Job job = Job.getInstance(conf, "TP4_2");
    job.setNumReduceTasks(1);
    job.setJarByClass(TP4_2.class);
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Point2DWritable.class);
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(RandomPointInputFormat.class);  
    FileOutputFormat.setOutputPath(job, new Path(args[0])); 
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
