package bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CityWritable implements WritableComparable<CityWritable> {
	public double population;
	public String cityName;
	
	public CityWritable(double population, String cityName) {
		this.population = population;
		this.cityName = cityName;
	}
	
	public void readFields(DataInput in) throws IOException {
		population = in.readDouble();
		cityName = WritableUtils.readString(in); 
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(population);
		WritableUtils.writeString(out, cityName);
	}

	public int compareTo(CityWritable toCompare) {
		if(this.population > toCompare.population) {
			return 1;
		} else if (this.population < toCompare.population) {
			return -1;
		} else {
			return this.cityName.compareTo(toCompare.cityName);
		}
	}

}
