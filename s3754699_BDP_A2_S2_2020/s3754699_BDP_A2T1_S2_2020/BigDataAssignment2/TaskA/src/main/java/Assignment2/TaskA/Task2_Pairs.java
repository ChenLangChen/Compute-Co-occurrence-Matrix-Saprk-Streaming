package Assignment2.TaskA;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;



public class Task2_Pairs {
	public static class Occurrence_Count_Pair implements Writable, WritableComparable<Occurrence_Count_Pair> 
	{
		private String current_word = new String(); // Natural key
		private String neighbour = new String();    // Secondary key		
		
		public String getCurrent_word() {
			return current_word;
		}
		public String getNeighbour() {
			return neighbour;
		}

		public void setCurrent_word(String current_word) {
			this.current_word = current_word;
		}
		public void setNeighbour(String neighbour) {
			this.neighbour = neighbour;
		}

		@Override
		public int compareTo(Occurrence_Count_Pair pair) {
			// Natural key
			int compareValue = this.current_word.compareTo(pair.getCurrent_word());
			// Secondary key
			if(compareValue==0) {
				compareValue = this.neighbour.compareTo(pair.getNeighbour());
			}
			return compareValue;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			current_word = in.readUTF(); // readLine()???
			neighbour = in.readUTF();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(current_word);
			out.writeUTF(neighbour);
			
		}		
	}

	public static final Logger LOG = Logger.getLogger(Task2_Pairs.class);
	
	public static class TokenizerMapper 
	extends Mapper<Object, Text, Occurrence_Count_Pair, DoubleWritable>{
		
		private final static DoubleWritable one = new DoubleWritable(1);
				
		public void map(Object key, Text value, Context context) 
				 throws IOException,InterruptedException{
			// Convert doc to String
			String doc = value.toString();
			// Cleaning the doc, removing punctuation and making a case-insensitive 
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
	                    	
	                    	Occurrence_Count_Pair pair = new Occurrence_Count_Pair();
	                    	pair.setCurrent_word(keyString);
	                    	pair.setNeighbour(valString);
	                    
	                    	// A generic pair (dog, *)
	                    	Occurrence_Count_Pair all_Pair = new Occurrence_Count_Pair();
	                    	all_Pair.setCurrent_word(keyString);
	                    	all_Pair.setNeighbour("*");
	                    	context.write(pair, one);
	                    	context.write(all_Pair, one);
	                    }
	                }
	            }
			}			
		}		
	}
	
	// Partitioner class
	public static class occurrencePartitioner extends Partitioner<Occurrence_Count_Pair, DoubleWritable>{

		@Override
		public int getPartition(Occurrence_Count_Pair key, DoubleWritable value, int numPartitions) {			
			LOG.setLevel(Level.DEBUG);			
			// Use the first word for partition
			Integer assigned_reducer = Math.abs(key.getCurrent_word().hashCode()%numPartitions); 
			LOG.debug(key.getCurrent_word() + " going to reducer " + assigned_reducer);
			return assigned_reducer;					
		}		
	}
	
	public static class IntSumReducer extends Reducer<Occurrence_Count_Pair, DoubleWritable, Text, DoubleWritable>{
		private DoubleWritable result = new DoubleWritable();
		private Double sum_for_all = new Double(0.0);
			
		public void reduce (Occurrence_Count_Pair key, Iterable<DoubleWritable> values, Context context)
		throws IOException,InterruptedException{	
			
			LOG.setLevel(Level.DEBUG);
			LOG.debug("Handling Key: " + key.getCurrent_word() + " : " + key.getNeighbour());	
			LOG.debug("Values: " + values);
			// If the key is e.g.("dog", "*")
			if (key.getNeighbour().equals("*")) {
				// Update the sum_for_all for each current_word
				sum_for_all = 0.0;
				for (DoubleWritable val : values) {
					sum_for_all += val.get();
				}
				LOG.debug("sum for all: " + sum_for_all);				
				result.set(sum_for_all);
				Text the_key = new Text (key.getCurrent_word() + " : " + key.getNeighbour());
				// Emit (("dog", "*"),count)
				context.write(the_key, result);
			}
			// If the key is e.g.("dog", "cute")
			else {
				// Keep the sum_for_all from ("dog", "*")
				LOG.debug("sum for all: " + sum_for_all);
				Double individual_sum = 0.0;	
				for (DoubleWritable val : values) {
					individual_sum += val.get();
				}
				// Relative co-occurrence
				Double relative_percentage = individual_sum / sum_for_all;
				result.set(relative_percentage);
				Text the_key = new Text (key.getCurrent_word() + " : " + key.getNeighbour());
				// Emit (("dog", "cute"),relative co-occurrence)
				LOG.debug(the_key + " : " + result);
				context.write(the_key, result);			
			}
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");      
		job.setJarByClass(Task2_Pairs.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setPartitionerClass(occurrencePartitioner.class);
		job.setNumReduceTasks(3);	
		job.setReducerClass(IntSumReducer.class);		
		// Mapper and reducer have different output types 
		job.setMapOutputKeyClass(Occurrence_Count_Pair.class);
		job.setMapOutputValueClass(DoubleWritable.class);  
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	

}