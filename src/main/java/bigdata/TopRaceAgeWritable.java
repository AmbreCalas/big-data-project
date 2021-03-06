package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class TopRaceAgeWritable extends TopRaceWritable {
	public String category;
	
	public TopRaceAgeWritable() {
		super();
	}
	
	public TopRaceAgeWritable(String city, String year, String distance, String nbPax, String category) {
		super(city, year, distance, nbPax);
		this.category = category;
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		category = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		WritableUtils.writeString(out, category);
	}
	public String getKey () {
		return super.getKey() +";" + this.category;
	}
	
	@Override
	public String toString() {
		return super.toStringAddedValue(category);
	}
	

}
