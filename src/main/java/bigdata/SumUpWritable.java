package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SumUpWritable implements Writable {

	public String category;
	public String distance;
	public String nbPax;
	public String minTime;
	public String maxTime;
	public String avgTime;
	public String avgSpeed;
	
	public SumUpWritable(String category, String distance, String nbPax, String minTime, String maxTime, String avgTime, String avgSpeed) {
		this.category = category;
		this.distance = distance;
		this.nbPax = nbPax;
		this.minTime = minTime;
		this.maxTime = maxTime;
		this.avgTime = avgTime;
		this.avgSpeed = avgSpeed;
	}
	
	public void readFields(DataInput in) throws IOException {
		category = WritableUtils.readString(in);
		distance = WritableUtils.readString(in);
		nbPax = WritableUtils.readString(in);
		minTime = WritableUtils.readString(in);
		maxTime = WritableUtils.readString(in);
		avgTime = WritableUtils.readString(in);
		avgSpeed = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, category);
		WritableUtils.writeString(out, distance);
		WritableUtils.writeString(out, nbPax);
		WritableUtils.writeString(out, minTime);
		WritableUtils.writeString(out, maxTime);
		WritableUtils.writeString(out, avgTime);
		WritableUtils.writeString(out, avgSpeed);
	}
	
	@Override
	public String toString() {
		return "(" + category + "," + distance + ") -> " + nbPax + " " + minTime + " " + maxTime + " " + avgTime + " " + avgSpeed;
	}

}
