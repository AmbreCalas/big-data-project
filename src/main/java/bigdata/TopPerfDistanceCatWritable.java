package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class TopPerfDistanceCatWritable extends TopPerfDistanceWritable {
	private String category;
	
	public TopPerfDistanceCatWritable() {
		super();
	};
	
	public TopPerfDistanceCatWritable(String distance, String perf, String pax, String category) {
		super(distance, perf, pax);
		this.category = category;
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		category = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		WritableUtils.writeString(out, category);
	}
	
	@Override
	public String toString() {
		return super.addableToString(category);
	}
	

}
