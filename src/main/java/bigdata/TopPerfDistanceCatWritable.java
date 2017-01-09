package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class TopPerfDistanceCatWritable extends TopPerfDistanceWritable {
	private String age;

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		age = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		WritableUtils.writeString(out, age);
	}
	
	@Override
	public String toString() {
		return super.addableToString(age);
	}
	

}
