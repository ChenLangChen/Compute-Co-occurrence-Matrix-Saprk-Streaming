package Assignment2.TaskA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Task1_Pairs {
	public static final Logger LOG = Logger.getLogger(Task1_Pairs.class);
	
	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, IntWritable>{
		
		private final static IntWritable one = new IntWritable(1);
				
		public void map(Object key, Text value, Context context) 
				 throws IOException,InterruptedException{
			// Convert doc to String
			String doc = value.toString();
			// Cleaning the doc, removing punctuation and making it case-insensitive 
			// and splitting doc into multiple lines
			String[] cleaned_doc = doc.replaceAll("[^a-zA-Z \n]", "").toLowerCase().split("\\n");
			
			for (String line : cleaned_doc){	            
				line = line.trim();
				// Tokenize the line
	            ArrayList<String> tokenized_line = new ArrayList<>();
	            tokenized_line.addAll(Arrays.asList(line.split("\\s+")));
	            System.out.println(tokenized_line);

	            // Loop through tokenized_line, construct a pair with all neighbors of the current token.
	            for (int i=0; i<tokenized_line.size(); i++){
	                // Find the neighbors for the current token; Loop through tokenized_line but skip the current token
	                for (int j=0;j<tokenized_line.size(); j++ ){
	                    if (i!=j){
	                    	// Retrieving key & val to construct word_pair	                 
	                    	String keyString = tokenized_line.get(i);
	                    	String valString = tokenized_line.get(j);                	
	                    	String pairString = keyString+ " : " + valString;
	                    	Text word_pair = new Text(pairString); 
	                    	// Emit(Pair(w,u),count)
	                    	context.write(word_pair, one);	                                      
	                    }
	                }
	            }
			}			
		}		
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		
		public void reduce (Text key, Iterable<IntWritable> values, Context context)
		throws IOException,InterruptedException{
			
			LOG.setLevel(Level.DEBUG);
			LOG.debug("Key: " + key);
			
			int sum = 0;
			for(IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");      
		job.setJarByClass(Task1_Pairs.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	

}
