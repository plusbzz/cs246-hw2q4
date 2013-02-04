import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
        
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class KMeans {
   public static class Map extends Mapper<LongWritable, Text, Point, Point> {
	    public static Point nullPoint = new Point();
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        List<Point> centroids = new ArrayList<Point>();
	        
	        // Read the centroid file set in the configuration
	        String currentLine;
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	        Path cFile = new Path(context.getConfiguration().get("centroids"));
	        DataInputStream d = new DataInputStream(fs.open(cFile));
	        BufferedReader reader = new BufferedReader(new InputStreamReader(d)); 
	        while ((currentLine = reader.readLine()) != null) {
	        	centroids.add(new Point(currentLine));
			}
	        
	        // Calculate minimum distance of each point in chunk to centroids to get best centroid	
	        Point x = new Point(value.toString());
	        double cost = 0;
	        double minDistance = Double.MAX_VALUE;
	        Point bestCentroid = x;
	        for(Point c: centroids){
	        	if(c.getDim() == 0) continue;  // skip the null point
	        	double dist = x.getDistance(c);	        	
	        	if(dist < minDistance){
	        		minDistance = dist;
	        		bestCentroid = c;	
	        	}
	        }
	        cost += (minDistance*minDistance);
	        context.write(bestCentroid, x);
	        Point costPoint = new Point(cost);
	        context.write(nullPoint,costPoint);
	    }
   } 
	        
   public static class Reduce extends Reducer<Point, Point, Point, Text> {
	   private static Text txt = new Text("");
	   public void reduce(Point key, Iterable<Point> values, Context context) 
			   throws IOException, InterruptedException {
		   
			// Input (centroid, [points])
			int count = 0;
			Point centroid = key;
			int dim = centroid.getDim();
			if(dim == 0){
				Double cost = 0.0;
				for(Point x: values){
					cost += x.getCost();
				}				
				context.write(centroid,new Text(cost.toString()));
			} else {
				// calculate mean of values to get new centroid
				RealVector newCentroidVector = new ArrayRealVector(dim);
				for(Point x: values){
					newCentroidVector = newCentroidVector.add(x.getVector());
					count++;
				}
				newCentroidVector = newCentroidVector.mapDivide(count);
				Point newCentroid = new Point(newCentroidVector);
				context.write(newCentroid,txt);
			}
	    }
   }
        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("centroids","/user/rbandyopadhyay/c2.txt");
	    int maxIter = 20;
	    for(int i = 0; i < maxIter; i++){
	    	String iterStr = Integer.toString(i);
		    // Run one iteration
		    Job job = new Job(conf, "kmeans_"+ iterStr);
		    job.setJarByClass(KMeans.class);
	
		    job.setMapperClass(Map.class);
		    job.setReducerClass(Reduce.class);
		       
		    job.setMapOutputKeyClass(Point.class);
		    job.setMapOutputValueClass(Point.class);
		    
		    job.setOutputKeyClass(Point.class);
		    job.setOutputValueClass(Text.class);
		    
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		        
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]+ "_" + iterStr));
	        
		    job.waitForCompletion(true);
	    
		    // merge centroid file and set new file location
		    FileSystem fs = FileSystem.get(conf);
		    FileUtil.copyMerge(fs, new Path(args[1]+ "_" + iterStr), fs, new Path("/user/rbandyopadhyay/c_"+iterStr+".txt"), false, conf, "");
		    conf.set("centroids","/user/rbandyopadhyay/c_"+iterStr+".txt");
	    }
	 }
        
}
