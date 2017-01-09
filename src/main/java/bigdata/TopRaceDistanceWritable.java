package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class TopRaceDistanceWritable extends TopRaceWritable {
	private String distance;

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		distance = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		WritableUtils.writeString(out, distance);
	}
	
	@Override
	public String toString() {
		return super.toString(distance);
	}
	
}
