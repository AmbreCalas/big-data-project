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
	
	public int compareTo(TopRaceAgeWritable otherTop) {
		int comparedKeys = super.compareTo(otherTop);
		if (comparedKeys == 0) {
			return this.category.compareTo(otherTop.category);
		}
		else {
			return comparedKeys;
		}
	}
	
	public int compareTo(Object o) {
		TopRaceAgeWritable otherTop = (TopRaceAgeWritable) o;
		int comparedKeys = super.compareTo(otherTop);
		if (comparedKeys == 0) {
			return this.category.compareTo(otherTop.category);
		}
		else {
			return comparedKeys;
		}
	}
	
	@Override
	public boolean equals(Object otherTop) {
		if(otherTop instanceof TopRaceAgeWritable) {
			return this.getKey().equals(((TopRaceAgeWritable) otherTop).getKey());
		}
		return false;
	}

}
