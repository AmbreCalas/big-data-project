package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SumUpWritable implements Writable {

	public String city;
	public String raceId;
	public String distance;
	public String time;
	public String category;
	public String range;
	public String name;
	public String club;
	
	public SumUpWritable(String city, String raceId, String distance, String time, String category, String range, String name, String club) {
		this.city = city;
		this.raceId = raceId;
		this.distance = distance;
		this.time = time;
		this.category = category;
		this.range = range;
		this.name = name;
		this.club = club;
	}
	
	public void readFields(DataInput in) throws IOException {
		city = WritableUtils.readString(in);
		raceId = WritableUtils.readString(in);
		distance = WritableUtils.readString(in);
		time = WritableUtils.readString(in);
		category = WritableUtils.readString(in);
		range = WritableUtils.readString(in);
		name = WritableUtils.readString(in);
		club = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, city);
		WritableUtils.writeString(out, raceId);
		WritableUtils.writeString(out, distance);
		WritableUtils.writeString(out, time);
		WritableUtils.writeString(out, category);
		WritableUtils.writeString(out, range);
		WritableUtils.writeString(out, club);
	}
	
	@Override
	public String toString() {
		return city + ";" + raceId + ";" + distance + ";" + time + ";" + category + ";" + range + ";" + club;
	}

}
