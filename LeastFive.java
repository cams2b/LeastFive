import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class LeastFive {
	
	
	public static class LeastFiveMapper 
		extends Mapper<Object, Text, Text, LongWritable>{
		
	    private final static LongWritable one = new LongWritable(1);
	    private Text word = new Text();
	    private TreeMap<String, Long> tmap;// = new HashMap<String, Long>(); 
	    
	    
	    @Override
	    public void setup(Context context) throws IOException, 
	                                     InterruptedException 
	    { 
	        tmap = new TreeMap<String, Long>(); 
	    } 
		
	    
	    @Override
	    public void map(Object key, Text value, Context context
	    		) throws IOException, InterruptedException {
	    	
	    	// split input data
	    	String[] tokens = value.toString().split(" ");
	    	
	    	// iterate through the words
	    	for(int i = 0; i < tokens.length; i++) {
	    		
	    		// if hmap contains the value.
	    		if(tmap.get(tokens[i]) != null) {
	    			
	    			Long val = tmap.get(tokens[i]);
	    			// update 
	    			val = val + 1;
	    			tmap.put(tokens[i], val);
	    			
	    		} else {
	    			// add values.
	    			Long curr = (long) 1;
	    			tmap.put(tokens[i], curr);
	    			
	    		}
	    		
	    	} // end of for loop
	    	
	    	for(Map.Entry<String, Long> entry : tmap.entrySet()) {
	    		
	    		// retrieve value from hash map
	    		long count = entry.getValue();
	    		String name = entry.getKey();
	    		context.write(new Text (name), new LongWritable(count));
	    		
	    	}
	    	
	    	
	    	
	    	
	    }
	    
		
	} // End of mapper.
	
	
	public static class LeastFiveReducer 
		extends Reducer<Text, LongWritable, Text, LongWritable> {
		private TreeMap<Long, Stack<String>> tmap2;// = new TreeMap<Long, String>(); 
		
		@Override
	    public void setup(Context context) throws IOException, 
	                                     InterruptedException 
	    { 
	        tmap2 = new TreeMap<Long, Stack<String>>(); 
	    } 

		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, 
							Context context) throws IOException, InterruptedException {
			
			String name = key.toString();
			long count = 0;
			for(LongWritable val: values) {
				// count all values.
				count += val.get();
			}
			
			// make negative for least 5
			//count = -count;
			if(tmap2.get(count) == null) {
				Stack<String> curr = new Stack<String>();
				tmap2.put(count, curr);
				//tmap2.get(count).push(name);
				
				
				
			}
			
			
			tmap2.get(count).push(name);
			
			
			//tmap2.put(count, name);
			
			
			if(tmap2.size() > 5) {
				tmap2.remove(tmap2.lastKey());
				System.out.println("HERE");
			}
			
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			
			// sort values.
			
			while(tmap2.size() > 5) {
				tmap2.remove(tmap2.lastKey());
			}
			
			int counter = 0;
			/*
			while(counter < 5) {
				
				Long curr = tmap2.firstKey();
				if(tmap2.get(curr).empty() == false) {
					
					String name = tmap2.get(curr).pop();
					context.write(new Text(name), new LongWritable(curr));
					
				} else {
					tmap2.remove(tmap2.firstKey());
				}
				counter++;
				
			}
			*/
			
			for (Entry<Long, Stack<String>> entry : tmap2.entrySet()) {
				
				long count = entry.getKey();
				//count = -count;
				//String name = entry.getValue();
				Stack<String> curr = entry.getValue();
				
				// write 
				while(curr.empty() == false && counter < 5) {
					String name = curr.pop();
					context.write(new Text(name), new LongWritable(count));
					counter++;
				}
				//context.write(new Text(name), new LongWritable(count));
			
				
				
				
			}
			
			
		}
		
		
		
		
	} // End of reducer
	
	
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length < 2) {
			
			// if arguments are not provided 
			System.err.println("Error: please provide two paths");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Least 5");
		job.setJarByClass(LeastFive.class);
		
		job.setMapperClass(LeastFiveMapper.class);
		job.setReducerClass(LeastFiveReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
	}
	
	

}

