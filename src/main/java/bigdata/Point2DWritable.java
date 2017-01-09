package bigdata;

import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point2DWritable implements Writable {
	protected Point2D.Double point2dDouble;
	
	public Point2DWritable() {
		point2dDouble = new Point2D.Double(0L, 0L);
	}
	
	public Point2DWritable(double x, double y) {
		point2dDouble = new Point2D.Double(x, y);
	}
	
	public double getX() {
		return point2dDouble.getX();
	}
	
	public double getY() {
		return point2dDouble.getY();
	}
	
	public void setX(double x) {
		point2dDouble.setLocation(x, point2dDouble.getY());
	}
	
	public void setY(double y) {
		point2dDouble.setLocation(point2dDouble.getX(), y);
	}
	
	public void setXAndY(double x, double y) {
		point2dDouble.setLocation(x, y);
	}

	public void readFields(DataInput in) throws IOException {
		point2dDouble.x = in.readDouble();
		point2dDouble.y = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(point2dDouble.getX());
		out.writeDouble(point2dDouble.getY());
	}
	
	@Override
    public String toString() {
        return point2dDouble.toString();
    }

}
