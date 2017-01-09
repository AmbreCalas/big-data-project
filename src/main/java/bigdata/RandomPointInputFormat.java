package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointInputFormat extends InputFormat<Object, Point2DWritable>{

	private static long nbMappers;
	private static long nbPoints;
	
	
	public long getNbMappers() {
		return nbMappers;
	}
	
	public static void setNbMappers(int nb) {
		nbMappers = nb;
	}
	
	public long getNbPoints() {
		return nbPoints;
	}
	
	public static void setNbPoints(int nb) {
		nbPoints = nb;
	}
	
	@Override
	public RecordReader<Object, Point2DWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		RandomPointReader reader = new RandomPointReader();
		return reader;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		nbMappers = Integer.parseInt(conf.get("nbMappers"));
		nbPoints = Integer.parseInt(conf.get("nbPoints"));
		
		long div = (long) nbPoints / nbMappers;
		long rest = (long) nbPoints % nbMappers;
        List<InputSplit> splits = new ArrayList<InputSplit>();
    	FakeInputSplit split;
        for(int i = 0; i < nbMappers; i++) {
        	if(i == nbMappers - 1) {
        		long total = div + rest;
        		split = new FakeInputSplit(total, i * div);
        	} else {
        		split = new FakeInputSplit(div, i * div);
        	}
        	splits.add(split);
        }
		return splits;
	}

}
