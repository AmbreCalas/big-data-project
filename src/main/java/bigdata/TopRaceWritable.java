package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class TopRaceWritable implements WritableComparable{
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

	public int compareTo(TopRaceWritable otherTop) {
		if(Integer.parseInt(this.nbPax) > Integer.parseInt(otherTop.nbPax)) {
			return 1;
		} else if(Integer.parseInt(this.nbPax) < Integer.parseInt(otherTop.nbPax)) {
			return -1;
		} else {
			return compareRace(this, otherTop);
		}
	}
	
	public int compareRace(TopRaceWritable thisTop, TopRaceWritable otherTop) {
		int comparedCities = thisTop.city.compareTo(otherTop.city);
		if(comparedCities == 0) {
			int comparedYear = thisTop.year.compareTo(otherTop.year);
			if(comparedYear == 0) {
				return thisTop.distance.compareTo(otherTop.distance);
			} else {
				return comparedYear;
			}
		} else {
			return comparedCities;
		}
	}
	
	@Override
	public boolean equals(Object otherTop) {
		if(otherTop instanceof TopRaceWritable) {
			return this.getKey().equals(((TopRaceWritable) otherTop).getKey());
		}
		return false;
	}

	public int compareTo(Object o) {
		TopRaceWritable otherTop = (TopRaceWritable) o;
		if(Integer.parseInt(this.nbPax) > Integer.parseInt(((TopRaceWritable) otherTop).nbPax)) {
			return 1;
		} else if(Integer.parseInt(this.nbPax) < Integer.parseInt(otherTop.nbPax)) {
			return -1;
		} else {
			return compareRace(this, otherTop);
		}
	}
	
}
