package Project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.*;
import java.util.*;
import java.net.*;
import java.text.*;
import java.lang.Math.*;

public class Kmeans {

	public static HashMap<Integer, Integer> counter = new HashMap<>();
	public static HashMap<Integer, String>centroids = new HashMap<>();

	public static class PointMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                	try {
                    	FileSystem fs = FileSystem.get(context.getConfiguration());
                    	Path getFilePath = new Path(cacheFiles[0].toString());
                    	BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                    	String value;
                    	centroids = new HashMap<>();
                    	int count = 0;
                    	while ((value = reader.readLine()) != null) {
                    		count+=1;
                        	centroids.put(count, value);
                        	counter.put(count,0);
                    	}
                    }   
            	    catch (Exception ex) {
                        System.out.println("Unable to read the file");
                        System.exit(1);
                    }
	    	    }
            }
        }
                              
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] point = value.toString().split(",");
			double pX = Double.parseDouble(point[0]);
			double pY = Double.parseDouble(point[1]);
			double dist;
			int cid = 1000;
			double leastDist = Double.MAX_VALUE;
			try {
				//System.out.println(centroids.keySet());
				for (Integer centroidId: centroids.keySet()) {
					String cxy = centroids.get(centroidId);
					cxy=cxy.trim();
					double cX = Double.parseDouble(cxy.split(",")[0]);
					double cY = Double.parseDouble(cxy.split(",")[1]);
					dist = Math.sqrt(Math.pow((pX - cX),2) + Math.pow((pY - cY),2));
					//System.out.println(cX + " " + cY + " " + dist);
					if (dist < leastDist) {
						leastDist = dist;
						cid = centroidId;
					}
				}
				context.write(new IntWritable(cid),new Text(Double.toString(pX) + "," + Double.toString(pY)));
			}
			catch (Exception ex) {
				ex.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public static class CustomCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
				String b = key.toString();
				int count = counter.get(Integer.parseInt(b));
				double countX = 0.0;
				double countY = 0.0;
				for (Text val : values) {
					count+=1;
					String[] val_ = val.toString().split(",");
					double pX = Double.parseDouble(val_[0]);
					double pY = Double.parseDouble(val_[1]);
					countX+=pX;
					countY+=pY;
					
				}
				counter.put(Integer.parseInt(b),count);
				int c = count;
				context.write(key, new Text(Double.toString(countX) + "," + Double.toString(countY) + "," + Integer.toString(c)));
				//System.out.println("OUTPUT COMBINER " + " " + countX + " " + countY + " " + c);
		}
	}
	
	public static class CustomReducer extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				int totalCount = 0;
				double totalCountX = 0.0;
				double totalCountY = 0.0;
				DecimalFormat df = new DecimalFormat("#.##");
				for (Text val:values) {
					String[] val_ = val.toString().split(",");
					//System.out.println("INPUT REDUCER " + val);
					int tCount = Integer.parseInt(val_[2]);
					double countX = Double.parseDouble(val_[0]);
					double countY = Double.parseDouble(val_[1]);
					totalCountX+=countX;
					totalCountY+=countY;
					totalCount+=tCount; 
				}
				int ct = totalCount;
				System.out.println(totalCountX + " " + totalCountY + " " + totalCount);
				double new_cx = Double.parseDouble(df.format(totalCountX / ct));
				double new_cy = Double.parseDouble(df.format(totalCountY / ct));
				context.write(new Text(""), new Text(Double.toString(new_cx) + "," + Double.toString(new_cy)));
    		}
			catch (Exception ex) {
				ex.printStackTrace();
				System.exit(1);
			}
		}
    }
    
    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	FileSystem.get(conf).delete(new Path(args[2]), true);
    	URI centroids = new URI(args[0]);
    	for (int i = 0; i < 6; i++) {
    		System.out.println("\n\n\n Iteration" + (i+1) + "\n\n\n"); 
    		Path output = new Path(args[2]+i);
    		Job job = Job.getInstance(conf, "Kmeans");
        	job.setJarByClass(Kmeans.class);
        	job.setMapperClass(Kmeans.PointMapper.class);
        	job.setCombinerClass(Kmeans.CustomCombiner.class);
        	job.setReducerClass(Kmeans.CustomReducer.class);
        	job.setOutputKeyClass(IntWritable.class);
        	job.setOutputValueClass(Text.class);
        	job.setNumReduceTasks(1);
        	job.addCacheFile(centroids);
        	FileInputFormat.addInputPath(job, new Path(args[1]));
        	job.setOutputFormatClass(TextOutputFormat.class);
        	FileOutputFormat.setOutputPath(job, new Path(args[2]+i));
        	job.waitForCompletion(true);
        	FileSystem fs = FileSystem.get(conf);
        	BufferedReader br1 = new BufferedReader(new InputStreamReader(fs.open(new Path(centroids))));
        	BufferedReader br2 = new BufferedReader(new InputStreamReader(fs.open(new Path(output.toString() + "/part-r-00000" ))));
        	ArrayList<String> presentCentroids = new ArrayList<String>();
        	ArrayList<String> previousCentroids = new ArrayList<String>();
        	while (br1.readLine()!=null) 
        		presentCentroids.add(br1.readLine());
        	while (br2.readLine()!=null) 
        		previousCentroids.add(br2.readLine());
        	if (presentCentroids.containsAll(previousCentroids))
        		break;
        	centroids = new URI(output + "/part-r-00000");
        }
    }
}
    

