package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopKPopMapReduce {

	// CLASS MAPPER
		public static class TopKPopMapper extends Mapper<Object, Text, Text, TopRaceWritable>{
			// map function
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				//context.write(generateKey(parts), new IntWritable(Integer.parseInt(population)));
			}
		}
	  
	  
		// CLASS REDUCER
		public static class TopKPopReducer extends Reducer<Text,TopRaceWritable,Text,TopRaceWritable> {
			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				//context.write(key, maximum);
			}
		}
}
