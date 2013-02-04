import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.hadoop.io.WritableComparable;




public class Point implements WritableComparable<Point> {
   	private RealVector vector;
   	private int dim;
	   	
   	public Point(){}
   	
   	public Point(RealVector v){
   		this.vector = v;
   		this.dim = v.getDimension();
   	}
   	
	public Point(String s){
	   	String [] coords = s.split("\\s");
   		this.dim = coords.length;
   		this.vector = new ArrayRealVector(this.dim);
   		
   		for(int i = 0; i < this.dim;i++){
   			Double d = new Double(coords[i]);
   			this.vector.setEntry(i, d);
   		}
   	}
   	
    public RealVector getVector(){
    	return vector;
    }

    public int getDim() {
		return dim;
	}

	public double getDistance(Point that){
    	return vector.getDistance(that.getVector());
    }

    @Override
    public int compareTo(Point that) {
    	if(vector.equals(that.getVector())) return 0;  // return 0 if equal
    	
    	Integer thatDim = that.getDim();
    	Integer thisDim = dim;
    	
    	if(!thisDim.equals(thatDim)) return thisDim.compareTo(thatDim) ;
    	
    	// if sizes are equal, just lexicographically compare coordinates
    	for(int i = 0; i < thisDim;i++){
    		Double thisC = this.getVector().getEntry(i);
    		Double thatC = that.getVector().getEntry(i);     
    		if(!thisC.equals(thatC)) return thisC.compareTo(thatC);
    	}        	
    	// If all co-ordinates are equal, the points are equal
    	return 0;       	
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int l = dim;
        out.writeInt(l);
        for(int i = 0; i < l;i++){
        	out.writeDouble(vector.getEntry(i));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int l = in.readInt();
        RealVector v = new ArrayRealVector(l);
        for(int i = 0; i < l;i++){
        	Double d = in.readDouble();
        	v.setEntry(i,d);
        }
        this.vector = v;
        this.dim = l;            		
    }

    @Override
    public String toString() {
    	String out = "";
        for(int i=0; i < dim;i++){
        	Double d = vector.getEntry(i);
        	out += d.toString() + " ";
        }
        return out;
    }
}