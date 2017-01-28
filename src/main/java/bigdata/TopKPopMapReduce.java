package bigdata;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKPopMapReduce {
	private final static String[] CATEGORIES = {"VETERAN", "SENIOR", "JUNIOR", "CADET", "ESPOIR", "MINIME", "CADETTE", "BENJAMIN", "HANDISPORT"};


	// FIRST CLASS MAPPER
	public static class TopKPopFirstMapper extends
			Mapper<Object, Text, Text, TopRaceWritable> {
		protected int k = 0;
		protected int top = 0;

		// map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			setUp(context);

			String[] parts = value.toString().split(";");
			if (top == 1) {
				if (parts.length > 3 && !getCategory(parts[4]).equals("")) {
					context.write(new Text(getRaceKey(parts)),
							new TopRaceWritable(parts[0], parts[1], parts[2],
									"1"));
				}
			} else if (top == 2) {
				if (parts.length > 4 && !getCategory(parts[4]).equals("")) {
					context.write(new Text(getRaceKeyCategory(parts)),
							new TopRaceAgeWritable(parts[0], parts[1],
									parts[2], "1", getCategory(parts[4])));
				}
			}
		}

		public String getCategory(String dirtyCategory) {
			String[] catParts = dirtyCategory.split(" ");
			for(String part: catParts) {
				for(String cat: CATEGORIES) {
					if(part.equals(cat)) {
						return part;
					}
				}
			}
			return "";
		}
		
		public boolean isPlaceNumber(String toTreat) {
			if (toTreat.charAt(0) < '0' && toTreat.charAt(0) > '9') {
				return false;
			}
			return true;
		}

		public String getRaceKeyCategory(String[] parts) {
			return parts[0] + ";" + parts[1] + ";" + parts[2] + ";"
					+ getCategory(parts[4]);
		}

		public String getRaceKey(String[] parts) {
			return parts[0] + ";" + parts[1] + ";" + parts[2];
		}

		public void setUp(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String kValue = conf.get("kValue");
			k = Integer.parseInt(kValue);

			String whichTop = conf.get("whichTop");
			top = Integer.parseInt(whichTop);
		}
	}

	// FIRST CLASS REDUCER
	public static class TopKPopFirstReducer extends
			Reducer<Text, TopRaceWritable, NullWritable, TopRaceWritable> {
		int k = 0;
		int top = 0;

		public void reduce(Text key, Iterable<TopRaceWritable> values,
				Context context) throws IOException, InterruptedException {
			setUp(context);

			int count = 0;
			String[] raceKeyParts = key.toString().split(";");
			for (TopRaceWritable value : values) {
				count += Integer.parseInt(value.nbPax);
			}
			if (top == 1) {
				if (raceKeyParts.length > 2) {
					context.write(
							null,
							new TopRaceWritable(raceKeyParts[0],
									raceKeyParts[1], raceKeyParts[2], Integer
											.toString(count)));
				}
			} else if (top == 2) {
				if (raceKeyParts.length > 3) {
					context.write(
							null,
							new TopRaceAgeWritable(raceKeyParts[0],
									raceKeyParts[1], raceKeyParts[2], Integer
											.toString(count), raceKeyParts[3]));
				}
			}
		}

		public void setUp(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String kValue = conf.get("kValue");
			k = Integer.parseInt(kValue);

			String whichTop = conf.get("whichTop");
			top = Integer.parseInt(whichTop);
		}
	}

	// SECOND CLASS MAPPER
	public static class TopKPopSecondMapper extends
			Mapper<Object, Text, Text, TopRaceWritable> {
		protected int k = 0;
		protected int top = 0;

		// map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			setUp(context);

			String[] parts = value.toString().split(";");

			if (parts.length == 3) {
				String city = parts[0];
				String year = parts[1];
				String[] popParts = parts[2].split(" : ");
				String distance = popParts[0];
				String nbPax = popParts[1];

				context.write(new Text(distance), new TopRaceWritable(city,
						year, distance, nbPax));
			} else if (parts.length == 4) {

				String city = parts[0];
				String year = parts[1];
				String distance = parts[2];
				String[] popParts = parts[3].split(" : ");
				String category = popParts[0];
				String nbPax = popParts[1];

				context.write(new Text(distance + ";" + category),
						new TopRaceAgeWritable(city, year, distance, nbPax,
								category));
			}
		}

		public void setUp(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String kValue = conf.get("kValue");
			k = Integer.parseInt(kValue);

			String whichTop = conf.get("whichTop");
			top = Integer.parseInt(whichTop);
		}
	}

	// SECOND CLASS REDUCER
	public static class TopKPopSecondReducer extends
			Reducer<Text, TopRaceWritable, Text, TopRaceWritable> {
		protected int k = 0;
		protected int top = 0;

		public void reduce(Text key, Iterable<TopRaceWritable> values,
				Context context) throws IOException, InterruptedException {
			setUp(context);
			TreeMap<Integer, TopRaceWritable> sortedRaces = new TreeMap<Integer, TopRaceWritable>();
			for (TopRaceWritable value : values) {
				TopRaceWritable race = new TopRaceWritable();
				if (top == 1) {
					race = new TopRaceWritable(value.city, value.year,
							value.distance, value.nbPax);
				} else if (top == 2) {
					race = new TopRaceAgeWritable(value.city, value.year,
							value.distance, value.nbPax,
							((TopRaceAgeWritable) value).category);
				}
				sortedRaces.put(Integer.parseInt(race.nbPax), race);
				if (sortedRaces.size() > k) {
					sortedRaces.remove(sortedRaces.firstKey());
				}
			}

			long position = 1;
			for (Map.Entry<Integer, TopRaceWritable> race : sortedRaces
					.descendingMap().entrySet()) {
				if (top == 1) {
					context.write(new Text(race.getValue().distance + " - "
							+ Long.toString(position)), race.getValue());
				} else if (top == 2) {
					context.write(new Text("(" + race.getValue().distance
							+ ", "
							+ ((TopRaceAgeWritable) race.getValue()).category
							+ ") - " + Long.toString(position)),
							race.getValue());
				}
				position++;
			}
		}

		public void setUp(Context context) throws IOException,
				InterruptedException {
			// Get conf values
			Configuration conf = context.getConfiguration();

			// Get k value for the top k
			String kValue = conf.get("kValue");
			k = Integer.parseInt(kValue);

			// Get whichTop value for the option
			String whichTop = conf.get("whichTop");
			top = Integer.parseInt(whichTop);

			// Delete middlepath
			String middlePath = conf.get("middlePath");
			File file = new File(middlePath);
			if (file.exists() && file.canWrite()) {
				file.delete();
			}
		}
	}
}