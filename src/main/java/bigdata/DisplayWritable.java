package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class DisplayWritable implements Writable{
	public String count;
	public String avg;
	public String min;
	public String max;
	
	public DisplayWritable() {
	}
	
	public DisplayWritable(String count, String avg, String min, String max) {
		this.avg = avg;
		this.count = count;
		this.min = min;
		this.max = max;
	}
	
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, count);
		WritableUtils.writeString(out, avg);
		WritableUtils.writeString(out, min);
		WritableUtils.writeString(out, max);
		
	}

	public void readFields(DataInput in) throws IOException {
		count = WritableUtils.readString(in);
		avg = WritableUtils.readString(in);
		min = WritableUtils.readString(in);
		max = WritableUtils.readString(in);
	}


    @Override
    public String toString() {
        return count + "     " + avg + "     " + min + "     " + max;
    }

}
