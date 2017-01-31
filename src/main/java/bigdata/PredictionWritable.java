package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class PredictionWritable implements Writable {
	public String category;
	public String distance;	
	public String time;
	public String pourcentage;
	
	public PredictionWritable() {};
	
	public PredictionWritable(String category, String distance, String time, String pourcentage) {
		this.category = category;
		this.distance = distance;
		this.time = time;
		this.pourcentage = pourcentage;
	}

	public void readFields(DataInput in) throws IOException {
		category = WritableUtils.readString(in);
		distance = WritableUtils.readString(in);
		time = WritableUtils.readString(in);
		pourcentage = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, category);
		WritableUtils.writeString(out, distance);
		WritableUtils.writeString(out, time);
		WritableUtils.writeString(out, pourcentage);
	}
	
	@Override
	public String toString() {	
		return pourcentage + ";" + category + ";" + distance + " : " + time;
	}
}
