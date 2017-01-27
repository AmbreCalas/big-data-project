package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TopPerfDistanceWritable implements Writable{
	public String distance;	
	public String perf;
	public String pax;
	
	public TopPerfDistanceWritable() {};
	
	public TopPerfDistanceWritable(String distance, String perf, String pax) {
		this.distance = distance;
		this.perf = perf;
		this.pax = pax;
	}

	public void readFields(DataInput in) throws IOException {
		distance = WritableUtils.readString(in);
		perf = WritableUtils.readString(in);
		pax = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, distance);
		WritableUtils.writeString(out, perf);
		WritableUtils.writeString(out, pax);
	}
	
	@Override
	public String toString() {
		return distance + " : " + perf + ";" + pax;
	}
	
	public String addableToString(String addedParam) {
		return distance + ";" + addedParam + " : " + perf + ";" + pax;
	}

}
