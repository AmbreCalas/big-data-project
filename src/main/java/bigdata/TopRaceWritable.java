package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TopRaceWritable implements Writable{
	private String raceName;
	private String nbPax;
	
	public void readFields(DataInput in) throws IOException {
		raceName = WritableUtils.readString(in);
		nbPax = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, raceName);
		WritableUtils.writeString(out, nbPax);
	}
	
	public String toString(String addedParam) {
		return "(" + raceName + ", " + addedParam + ") -> " + nbPax;
	}

}
