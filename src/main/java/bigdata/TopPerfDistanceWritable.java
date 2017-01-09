package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TopPerfDistanceWritable implements Writable{
	private String distance;	
	private String perf;

	public void readFields(DataInput in) throws IOException {
		distance = WritableUtils.readString(in);
		perf = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, distance);
		WritableUtils.writeString(out, perf);
	}
	
	@Override
	public String toString() {
		return distance + " -> " + perf;
	}
	
	public String addableToString(String addedParam) {
		return "(" + distance + ", " + addedParam + ") -> " + perf;
	}

}
