package bigdata;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopKClubMapReduce {

	// CLASS FIRST MAPPER
	public static class TopKClubFirstMapper extends
			Mapper<Object, Text, Text, TopClubWritable> {
		protected int k = 0;

		// map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			setUp(context);

			String[] parts = value.toString().split(";");
			if (parts.length > 8) {
				if(!isEmpty(parts[8])) {
					context.write(new Text(parts[8]), new TopClubWritable(parts[8],	"1"));
				}
			}
		}

		public boolean isEmpty(String toTreat) {
			String[] parts = toTreat.split(" ");
			for(String part: parts) {
				if(!part.equals("")) {
					return false;
				}
			}
			return true;
		}
		
		public void setUp(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String kValue = conf.get("kValue");
			k = Integer.parseInt(kValue);
		}
	}

	// CLASS FIRST REDUCER
	public static class TopKClubFirstReducer extends
			Reducer<Text, TopClubWritable, NullWritable, TopClubWritable> {

		public void reduce(Text key, Iterable<TopClubWritable> values,
				Context context) throws IOException, InterruptedException {
			String clubName = "";
			int size = 0;
			boolean isFirst = true;
			for (TopClubWritable value : values) {
				if (isFirst) {
					clubName = value.clubName;
					isFirst = false;
				}
				size++;
			}
			context.write(null,
					new TopClubWritable(clubName, Integer.toString(size)));
		}
	}

	// CLASS SECOND MAPPER
	public static class TopKClubSecondMapper extends
			Mapper<Object, Text, Text, TopClubWritable> {

		// map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] parts = value.toString().split(" : ");
			context.write(new Text(" "), new TopClubWritable(parts[0], parts[1]));
		}
	}

	// CLASS SECOND REDUCER
	public static class TopKClubSecondReducer extends
			Reducer<Text, TopClubWritable, Text, TopClubWritable> {
		protected int k = 0;

		public void reduce(Text key, Iterable<TopClubWritable> values,
				Context context) throws IOException, InterruptedException {
			setUp(context);
			TreeMap<Integer, TopClubWritable> sortedClubs = new TreeMap<Integer, TopClubWritable>();
			for(TopClubWritable value : values) {
				TopClubWritable club = new TopClubWritable(value.clubName, value.activity);
				sortedClubs.put(Integer.parseInt(club.activity), club);
				if (sortedClubs.size() > k) {
					sortedClubs.remove(sortedClubs.firstKey());
				}
			}
			
			long position = 1;
			for(Map.Entry<Integer, TopClubWritable> club : sortedClubs.descendingMap().entrySet()) {
				context.write(new Text(Long.toString(position)), club.getValue());
				position++;
			}
		}

		public void setUp(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String kValue = conf.get("kValue");
			k = Integer.parseInt(kValue);
			

			// Delete middlepath
			String middlePath = conf.get("middlePath");
			File file = new File(middlePath);
			if (file.exists() && file.canWrite())
			{
				file.delete();
			}
		}
	}
}
