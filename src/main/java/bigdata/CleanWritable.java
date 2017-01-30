package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class CleanWritable implements Writable{
	String time;
	String category;
	String range;
	String name;
	String team;
	
	public CleanWritable () {}
	
	public CleanWritable (String time, String category, String range, String name, String team) {
		this.time = time;
		this.category = category;
		this.range = range;
		this.name = name;
		this.team = team;
	}
	
	public void readFields(DataInput in) throws IOException {
		time = WritableUtils.readString(in);
		category = WritableUtils.readString(in);
		range = WritableUtils.readString(in);
		name = WritableUtils.readString(in);
		team = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, time);
		WritableUtils.writeString(out, category);
		WritableUtils.writeString(out, range);
		WritableUtils.writeString(out, name);
		WritableUtils.writeString(out, team);	
	}

	@Override
	public String toString() {
		return time + ";" + category + ";" + range + ";" + name + ";" + team + ";";
	}
}
