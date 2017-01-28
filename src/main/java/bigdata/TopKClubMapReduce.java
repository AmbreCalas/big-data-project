package bigdata;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class TopKClubMapReduce {

	// CLASS MAPPER
	public static class TopKClubMapper extends
			Mapper<Object, Text, Text, TopClubWritable> {

		// map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

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
	}

	// CLASS COMBINER
	public static class TopKClubCombiner extends
			Reducer<Text, TopClubWritable, Text, TopClubWritable> {

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
			context.write(new Text(" "),
					new TopClubWritable(clubName, Integer.toString(size)));
		}
	}
	
	// CLASS REDUCER
	public static class TopKClubReducer extends
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
		}
	}
}
