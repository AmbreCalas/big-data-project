package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ComputationWritable implements Writable{
	public int sum;
	public int count;
	public int min;
	public int max;
	
	public ComputationWritable() {
	}
	
	public ComputationWritable(int sum, int count, int min, int max) {
		this.sum = sum;
		this.count = count;
		this.min = min;
		this.max = max;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(sum);
		out.writeInt(count);
		out.writeInt(min);
		out.writeInt(max);
	}

	public void readFields(DataInput in) throws IOException {
		sum = in.readInt();
		count = in.readInt();
		min = in.readInt();
		max = in.readInt();
	}

}
