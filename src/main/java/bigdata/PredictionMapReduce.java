package bigdata;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class PredictionMapReduce {
	private final static String[] CATEGORIES = { "VETERAN", "SENIOR", "JUNIOR",
			"CADET", "ESPOIR", "MINIME", "CADETTE", "BENJAMIN", "HANDISPORT" };
	private static String category;	
	private static String previousDistance;
	private static String previousTime;
	private static String futureDistance;
	private static int pourcentage;

	// CLASS MAPPER
	public static class PredictionMapper extends
			Mapper<Object, Text, Text, PredictionWritable> {

		// map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			setUp(context);

			String[] parts = value.toString().split(";");
			
			String lineCategory = getCategory(parts[4]);
			
			if (parts.length > 4) {
				if(lineCategory.equals(category)) {
					context.write(new Text(getKey(parts)), new PredictionWritable(
							getCategory(parts[4]), parts[2], parts[3]));
				}
			}
		}

		public String getCategory(String dirtyCategory) {
			String[] catParts = dirtyCategory.split(" ");
			for (String part : catParts) {
				for (String cat : CATEGORIES) {
					if (part.equals(cat)) {
						return part;
					}
				}
			}
			return "";
		}

		public String getKey(String[] parts) {
			return getCategory(parts[4]) + parts[2];
		}

		public void setUp(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			category = conf.get("category");
			previousDistance = conf.get("previousDistance");
			previousTime = conf.get("previousTime");
			futureDistance = conf.get("futureDistance");
		}
	}

	// CLASS COMBINER
	public static class PredictionCombiner extends
			Reducer<Text, PredictionWritable, Text, PredictionWritable> {

		public void reduce(Text key, Iterable<TopClubWritable> values,
				Context context) throws IOException, InterruptedException {
			
			String[] parts = key.toString().split(";");
			String distance = parts[0].split(" : ")[0];
			/*String clubName = "";
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
					new TopClubWritable(clubName, Integer.toString(size)));*/
		}
	}

	// CLASS REDUCER
	public static class PredictionReducer extends
			Reducer<Text, PredictionWritable, NullWritable, PredictionWritable> {

		public void reduce(Text key, Iterable<TopClubWritable> values,
				Context context) throws IOException, InterruptedException {
			/*TreeMap<Integer, TopClubWritable> sortedClubs = new TreeMap<Integer, TopClubWritable>();
			for (TopClubWritable value : values) {
				TopClubWritable club = new TopClubWritable(value.clubName,
						value.activity);
				sortedClubs.put(Integer.parseInt(club.activity), club);
				if (sortedClubs.size() > k) {
					sortedClubs.remove(sortedClubs.firstKey());
				}
			}

			long position = 1;
			for (Map.Entry<Integer, TopClubWritable> club : sortedClubs
					.descendingMap().entrySet()) {
				context.write(new Text(Long.toString(position)),
						club.getValue());
				position++;
			}*/
		}
	}

}
