package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SumUpWritable implements Writable {

	public String age;
	public String distance;
	public String nbPax;
	public String minTime;
	public String maxTime;
	public String avgTime;
	public String avgSpeed;
	
	public void readFields(DataInput in) throws IOException {
		age = WritableUtils.readString(in);
		distance = WritableUtils.readString(in);
		nbPax = WritableUtils.readString(in);
		minTime = WritableUtils.readString(in);
		maxTime = WritableUtils.readString(in);
		avgTime = WritableUtils.readString(in);
		avgSpeed = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, age);
		WritableUtils.writeString(out, distance);
		WritableUtils.writeString(out, nbPax);
		WritableUtils.writeString(out, minTime);
		WritableUtils.writeString(out, maxTime);
		WritableUtils.writeString(out, avgTime);
		WritableUtils.writeString(out, avgSpeed);
	}
	
	@Override
	public String toString() {
		return "(" + age + "," + distance + ") -> " + nbPax + " " + minTime + " " + maxTime + " " + avgTime + " " + avgSpeed;
	}

}
