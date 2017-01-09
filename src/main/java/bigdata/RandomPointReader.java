package bigdata;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointReader extends RecordReader<Object, Point2DWritable> {
	
	protected long splitCount;
	//protected static Point2DWritable currentValue;
	protected FakeInputSplit split;

	public RandomPointReader() {
	}
	
	@Override
	public void close() throws IOException {
		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		LongWritable toReturn = new LongWritable(splitCount + split.getOffset());
		return toReturn;
	}

	@Override
	public Point2DWritable getCurrentValue() throws IOException, InterruptedException {
		return random2DPoint();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) ((float) split.getNbPoints())/splitCount;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.split = (FakeInputSplit) split;
		this.splitCount = 0;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		splitCount++;
		if(splitCount == split.getNbPoints()) {
			return false;
		}
		return true;
	}
	
	private Point2DWritable random2DPoint() {
		double randomX = Math.random();
		double randomY = Math.random();
		Point2DWritable point = new Point2DWritable(randomX, randomY);
		return point;
	}

}
