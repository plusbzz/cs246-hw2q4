import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.hadoop.io.WritableComparable;




public class Point implements WritableComparable<Point> {
   	private RealVector vector = null;
   	private int dim = 0;
	private double cost = 0.0;
	
   	public Point(){}
   	
   	public Point(RealVector v){
   		this.vector = v;
   		this.dim = v.getDimension();
   	}
   	
   	public Point(double d){
   		this.cost = d;
   	}

	public Point(String s){
	   	String [] coords = s.split("\\s");
	   	if(coords[0].equals("NULL")){
	   		this.cost = new Double(coords[1]);
	   	} else {
	   		this.dim = coords.length;
	   		this.vector = new ArrayRealVector(this.dim);
	   		
	   		for(int i = 0; i < this.dim;i++){
	   			Double d = new Double(coords[i]);
	   			this.vector.setEntry(i, d);
	   		}
	   	}
   	}
	
    public double getCost() {
		return cost;
	}

	public void setCost(double cost) {
		this.cost = cost;
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
    	int thatDim = that.getDim();
    	int thisDim = dim;
    	
    	if(thisDim != thatDim || thisDim == 0 || thatDim == 0) return thisDim - thatDim ;
    	if(vector.equals(that.getVector())) return 0;  // return 0 if equal
    	

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
    	if(dim == 0){
    		out.writeInt(0);
    		out.writeDouble(cost);
    		
    	} else {
	        out.writeInt(dim);
	        for(int i = 0; i < dim;i++){
	        	out.writeDouble(vector.getEntry(i));
	        }
    	}
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dim = in.readInt();
        if(dim == 0){
        	this.cost = in.readDouble();
        } else {
	        RealVector v = new ArrayRealVector(dim);
	        for(int i = 0; i < dim;i++){
	        	Double d = in.readDouble();
	        	v.setEntry(i,d);
	        }
	        this.vector = v;
        }
    }

    @Override
    public String toString() {
    	String out = "";
    	if(dim > 0){
	        for(int i=0; i < dim;i++){
	        	Double d = vector.getEntry(i);
	        	out += d.toString() + " ";
	        }
    	} else {
    		Double d = this.cost;
    		out = "NULL " + d.toString();
    	}
        return out;
    }
}