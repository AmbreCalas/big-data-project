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
	private static String globalPourcentage; 

	// CLASS MAPPER
	public static class PredictionMapper extends
			Mapper<Object, Text, Text, PredictionWritable> {
		private static String category;	
		

		// map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			setUp(context);

			String[] parts = value.toString().split(";");
			
			
			if (parts.length > 5) {
				String lineCategory = getCategory(parts[4]);
				if(lineCategory.equalsIgnoreCase(category)) {
					context.write(new Text(getKey(lineCategory, parts[2])), new PredictionWritable(
							lineCategory, parts[2], parts[3], ""));
				}
			}
		}
		
		public String getKey(String category, String distance) {
			return category + ";" + distance;
		}

		public String getCategory(String dirtyCategory) {
			String[] catParts = dirtyCategory.split(" ");
			for (String part : catParts) {
				for (String cat : CATEGORIES) {
					if (part.equalsIgnoreCase(cat)) {
						return part;
					}
				}
			}
			return "";
		}


		public void setUp(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			category = conf.get("category");
		}
		
	}

	// CLASS COMBINER
	public static class PredictionCombiner extends
			Reducer<Text, PredictionWritable, Text, PredictionWritable> {
		private static String previousDistance;
		private static String previousTime;
		private static String futureDistance;

		public void reduce(Text key, Iterable<PredictionWritable> values,
				Context context) throws IOException, InterruptedException {
			setUp(context);

			String[] parts = key.toString().split(";");
			String category = parts[0];
			String distance = parts[1];

			if (distance.equals(previousDistance)) {
				int totalCount = 0;
				int fasterCount = 0;

				for (PredictionWritable value : values) {
					totalCount++;
					if (timeInSeconds(value.time) >= timeInSeconds(previousTime)) {
						fasterCount++;
					}
				}

				float thisPourcentage = (float) ((((float) fasterCount) / totalCount) * 100);
				context.write(new Text(category), new PredictionWritable(category, distance, "", Float.toString(thisPourcentage)));

			} 

			if (distance.equals(futureDistance)) {
				for (PredictionWritable value : values) {
					context.write(new Text(category), value);
				}
			}

		}

		public long timeInSeconds(String time) {
			String myTime = time.trim();
			String[] timeParts = myTime.split(":");
			long hours = Long.parseLong(timeParts[0]);
			long minutes = Long.parseLong(timeParts[1]);
			long seconds = Long.parseLong(timeParts[2]);
			return hours * 60 * 60 + minutes * 60 + seconds;
		}

		public void setUp(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			previousDistance = conf.get("previousDistance");
			previousTime = conf.get("previousTime");
			futureDistance = conf.get("futureDistance");
		}
	}

	// CLASS REDUCER
	public static class PredictionReducer extends
			Reducer<Text, PredictionWritable, NullWritable, PredictionWritable> {
		private static String category;	
		private static String previousDistance;
		private static String previousTime;
		private static String futureDistance;


		public void reduce(Text key, Iterable<PredictionWritable> values,
				Context context) throws IOException, InterruptedException {
			setUp(context);
			
			float thisPourcentage = 0;
			int totalCount = 0; 
			
			TreeMap<Double, PredictionWritable> sortedPerfs = new TreeMap<Double, PredictionWritable>();
			
			int i = 0;
			for(PredictionWritable value : values) {
				if(value.distance.equals(previousDistance)) {
					thisPourcentage = Float.parseFloat(value.pourcentage);
				}
				if(value.distance.equals(futureDistance)) {
					PredictionWritable perf = new PredictionWritable(value.category, value.distance, value.time, Float.toString(thisPourcentage));
					
					long timeInSeconds = timeInSeconds(perf.time);
					if(timeInSeconds != -1) {
						String id = Integer.toString(i);
						double finalTime = Double.parseDouble(timeInSeconds + "." + id);
						sortedPerfs.put(finalTime, perf);
						totalCount++;
					}
				}
				i++;
			}
			
			float finalPlace = (float) (((float) totalCount * thisPourcentage) / 100);
			String stringPlace = Integer.toString((int) finalPlace);
		
			
			int position = 1;
			for(Map.Entry<Double, PredictionWritable> perf : sortedPerfs.descendingMap().entrySet()) {
				if(Integer.toString(position).equals(stringPlace)) {
					context.write(null, new PredictionWritable(category, futureDistance, perf.getValue().time, Float.toString(thisPourcentage)));
					break;
				}
				position++;
			}
		}
		
		public long timeInSeconds(String time) {
			String myTime = time.trim();
			String[] timeParts = myTime.split(":");
			if(timeParts.length > 2) {
				long hours = Long.parseLong(timeParts[0]);
				long minutes = Long.parseLong(timeParts[1]);
				long seconds = Long.parseLong(timeParts[2]);
				return hours*60*60 + minutes*60 + seconds;
			} 
			return -1;
		}
		
		public void setUp(Context context) {			
			Configuration conf = context.getConfiguration();
			category = conf.get("category");
			previousDistance = conf.get("previousDistance");
			previousTime = conf.get("previousTime");
			futureDistance = conf.get("futureDistance");
		}
	}

}
