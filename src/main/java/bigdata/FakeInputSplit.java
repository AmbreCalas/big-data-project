package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable {
	protected long nbPoints;
	protected long splitOffset;
	
	public FakeInputSplit() {
		this.nbPoints = 0;
		this.splitOffset = 0;		
	}
	
	public FakeInputSplit(long nbPoints, long offset) {
		this.nbPoints = nbPoints;
		this.splitOffset = offset;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return nbPoints;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}
	
	public long getOffset() {
		return splitOffset;
	}
	
	public long getNbPoints() {
		return nbPoints;
	}

	public void readFields(DataInput in) throws IOException {
		nbPoints = in.readLong();
		splitOffset = in.readLong();		
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(nbPoints);
		out.writeLong(splitOffset);
	}

}
