package bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopKPerfMapReduce {

	// CLASS MAPPER
		public static class TopKPerfMapper extends Mapper<Object, Text, Text, TopPerfDistanceWritable>{
			protected int k = 0;
			protected int top = 0;
			
			// map function
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				setUp(context);
				
				String[] parts = value.toString().split(";");
				if(parts.length > 6) {
					if(top == 1) {
						context.write(new Text(parts[2]), new TopPerfDistanceWritable(parts[2], parts[3], parts[6]));
					} else if(top == 2) {
						context.write(new Text(parts[2]), new TopPerfDistanceCatWritable(parts[2], parts[3], parts[6], parts[4]));
					}
				}
			}
			
			public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				String kValue = conf.get("kValue");
				k = Integer.parseInt(kValue);

				String whichTop = conf.get("whichTop");
				top = Integer.parseInt(whichTop);
			}
		}
	  
	  
		// CLASS REDUCER
		public static class TopKPerfReducer extends Reducer<Text,TopPerfDistanceWritable,Text,TopPerfDistanceWritable> {
			protected int k = 0;
			protected int top = 0;
			
			public void reduce(Text key, Iterable<TopPerfDistanceWritable> values, Context context) throws IOException, InterruptedException {
				setUp(context);
				
				//context.write(key, maximum);
			}
			
			public void setUp(Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				String kValue = conf.get("kValue");
				k = Integer.parseInt(kValue);

				String whichTop = conf.get("whichTop");
				top = Integer.parseInt(whichTop);
			}
		}
}
