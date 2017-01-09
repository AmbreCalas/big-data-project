package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopKPerfMapReduce {

	// CLASS MAPPER
		public static class TopKPerfMapper extends Mapper<Object, Text, Text, TopPerfDistanceWritable>{
			// map function
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				//context.write(generateKey(parts), new IntWritable(Integer.parseInt(population)));
			}
		}
	  
	  
		// CLASS REDUCER
		public static class TopKPerfReducer extends Reducer<Text,TopPerfDistanceWritable,Text,TopPerfDistanceWritable> {
			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				//context.write(key, maximum);
			}
		}
}
