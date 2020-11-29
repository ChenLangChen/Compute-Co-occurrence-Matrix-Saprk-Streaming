package Assignment2.TaskA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class Task2_Strips {
	public static final Logger LOG = Logger.getLogger(Task2_Strips.class);
	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, MapWritable>{			
		public void map(Object key, Text value, Context context) 
				 throws IOException,InterruptedException{
			// HashMap in HashMap. For example, {'cat': {'eat':3, 'meow': 5, 'sleep': 2},
			//                                   'dog': {'bark':2, 'park':5, 'friend': 3}}
			HashMap<String, HashMap<String, Double>> hashMap = new HashMap<String, HashMap<String, Double>>();		
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
	            	//Checking if the current token already exists
	            	String currentToken = tokenized_line.get(i);
	            	if (! hashMap.containsKey(currentToken)) {
						hashMap.put( currentToken, new HashMap<String, Double>());
					}
	            	
	                // Find the neighbors for the current token; Loop through tokenized_line but skip the current token
	                for (int j=0;j<tokenized_line.size(); j++ ){
	                    if (i!=j){	                   	
	                    	String currentNeighbor = tokenized_line.get(j);
	                    	// Retrieving the HashMap for the current Token
	                    	HashMap<String, Double> current_HashMap = hashMap.get(currentToken);
	                    	if (! current_HashMap.containsKey(currentNeighbor)) {
								current_HashMap.put(currentNeighbor, 1.0);
							}
	                    	else {
								current_HashMap.put(currentNeighbor, current_HashMap.get(currentNeighbor)+1.0);
							}              	                    		                    	                    	                                     
	                    }
	                }	                 
	            }
			}
						
			for (Map.Entry<String, HashMap<String, Double>> itemEntry : hashMap.entrySet()) {				
				Map<Writable, Writable> map = new MapWritable();	
				Text wordText = new Text(itemEntry.getKey());
				HashMap<String, Double> wordHashMap = itemEntry.getValue();
				// Adding all items to map
				for(Map.Entry<String, Double> item : wordHashMap.entrySet()) {
					Text neighborText = new Text(item.getKey());
					DoubleWritable freq = new DoubleWritable(item.getValue());
					map.put(neighborText, freq);		
				}
				// Emit word and the corresponding map. e.g. 'cat': {'eat':3, 'meow': 5, 'sleep': 2}
				context.write(wordText, (MapWritable) map);
			}			
		}		
	}
	
	public static class IntSumReducer extends Reducer<Text, MapWritable, Text, MapWritable>{
		public void reduce (Text key, Iterable<MapWritable> values, Context context)
		throws IOException,InterruptedException{
			// Create a Map for the current word
			Map<Writable, Writable> word_Map = new MapWritable();	
			LOG.setLevel(Level.DEBUG);
			
			for(MapWritable hm : values) {
				for (Map.Entry<Writable, Writable> itemEntry: hm.entrySet()) {
					Text currentNeighbor = (Text) itemEntry.getKey();
					DoubleWritable currentNeightborCount = (DoubleWritable) itemEntry.getValue();
					if (! word_Map.containsKey(currentNeighbor)) {
						word_Map.put(currentNeighbor, currentNeightborCount);					
					}
					else {
						// Retrieve the values and put it to word_Map
						Double currentNeighborCount = currentNeightborCount.get();
						DoubleWritable originalCount = (DoubleWritable) word_Map.get(currentNeighbor);
						Double originalCountDouble = originalCount.get();
						word_Map.put(currentNeighbor, new DoubleWritable(currentNeighborCount + originalCountDouble));
					}
				}		 
			}
			
			// Sum up
			Double sum_Count = 0.0;
			for(Map.Entry<Writable, Writable> itemEntry : word_Map.entrySet()) {
				DoubleWritable valueIntWritable = (DoubleWritable) itemEntry.getValue();
				sum_Count = sum_Count + valueIntWritable.get();
			}
			LOG.debug("Sum: " + sum_Count);
			// sum is made final, so it wouldn't change in the future
			final Double sum_static = sum_Count;
			// Now we have the sum of the current word			
			for(Map.Entry<Writable, Writable> itemEntry : word_Map.entrySet()) {
				DoubleWritable valueIntWritable = (DoubleWritable) itemEntry.getValue();
				// TODO 
				Double valDouble = valueIntWritable.get();
				Double rel_freq = valDouble/sum_static; 			
				// Update the count to relative frequency  
				Text new_key = (Text) itemEntry.getKey();
				DoubleWritable new_val = new DoubleWritable(rel_freq);
				word_Map.put(new_key, new_val);
				LOG.debug("Current Neighbor: " + new_key);
				LOG.debug("Count: " + valDouble);
				LOG.debug("Relative Freq AFTER calculation: " + new_val);
			}	
			context.write(key, (MapWritable) word_Map);			
		}		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Task2_Strips.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
