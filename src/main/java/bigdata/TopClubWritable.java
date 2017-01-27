package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TopClubWritable implements Writable{
	public String clubName;	
	public String activity;
	
	public TopClubWritable() {};
	
	public TopClubWritable(String clubName, String activity) {
		this.clubName = clubName;
		this.activity = activity;
	}

	public void readFields(DataInput in) throws IOException {
		clubName = WritableUtils.readString(in);
		activity = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, clubName);
		WritableUtils.writeString(out, activity);
	}
	
	@Override
	public String toString() {
		return clubName + " : " + activity;
	}

}
