package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class TopRaceWritable implements Writable{
	public String city;
	public String year;
	public String distance;
	public String nbPax;
	
	public TopRaceWritable() {}
	
	public TopRaceWritable(String city, String year, String distance, String nbPax) {
		this.city = city;
		this.year = year;
		this.distance = distance;
		this.nbPax = nbPax;
	}
	
	public void readFields(DataInput in) throws IOException {
		city = WritableUtils.readString(in);
		year = WritableUtils.readString(in);
		distance = WritableUtils.readString(in);
		nbPax = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, city);
		WritableUtils.writeString(out, year);
		WritableUtils.writeString(out, distance);
		WritableUtils.writeString(out, nbPax);
	}
	
	public String getKey () {
		return this.city + ";" + this.year + ";" + this.distance;
	}
	@Override
	public String toString() {
		return city + ";" + year + ";" + distance + " : " + nbPax;
	}
	
	public String toStringAddedValue(String addedValue) {
		return city + ";" + year + ";" + distance + ";" + addedValue + " : " + nbPax;
	}
	
}
