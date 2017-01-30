package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class HistogrammeWritable implements Writable {
	public String category;
	public String distance;	
	public String time;
	public String nbPax;
	
	public HistogrammeWritable() {};
	
	public HistogrammeWritable(String category, String distance, String time, String nbPax) {
		this.category = category;
		this.distance = distance;
		this.time = time;
		this.nbPax = nbPax;
	}

	public void readFields(DataInput in) throws IOException {
		category = WritableUtils.readString(in);
		distance = WritableUtils.readString(in);
		time = WritableUtils.readString(in);
		nbPax = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, category);
		WritableUtils.writeString(out, distance);
		WritableUtils.writeString(out, time);
		WritableUtils.writeString(out, nbPax);
	}
	
	@Override
	public String toString() {
		if(category.equals(" ") || distance.equals(" ") || time.equals(" ")) {
			return " ";
		}
		return "(" + category + ", " + distance + ", " + time + ") : " + nbPax;
	}
}
