package streaming.lab9

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.HashMap
import org.apache.spark.SparkContext
import java.util.logging.Level
import org.apache.log4j.Level
import org.apache.spark.streaming.Time
import scala.collection.mutable.ArrayBuffer




object NetworkWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }
 
    //StreamingExamples.setStreamingLogLevels()
    
    
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(10))
 
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    
    // Tokenize the text, removing non-alphabetic token
    val words = lines.flatMap(_.split("\\s+"))
    val cleaned_words = words.map(x=>x.replaceAll("[^a-zA-Z0-9]", "")) 
    // Removing ""
    val cleaned_wordss = cleaned_words.filter(x=>x.length()>0) 
    
    
    //================================ Task A
    // Count the words one by one
    val wordCounts = cleaned_wordss.map(x => (x, 1)).reduceByKey(_ + _)    
    wordCounts.print()
    
    // Saving the output to HDFS for TaskA (wordCount)
    wordCounts.foreachRDD{(rdd,Time) => 
      val rdd_count=rdd.count() 
      // Preventing empty rdd from writing to output folder
      if (rdd_count>0){
        // Creating output path
        val dir_name = args(1)+"/"+Time.toString()
        rdd.saveAsTextFile(dir_name)
      }
      }
    
    //================================== Task B
    // Filter out short words (length<5), then count.
    val long_words = cleaned_wordss.filter(x=>x.length()>4)
    val long_word_counts = long_words.map(x => (x, 1)).reduceByKey(_ + _)
    // Saving the output to HDFS 
    long_word_counts.foreachRDD{(rdd,Time)=>
        val rdd_count=rdd.count()
        if (rdd_count>0){
          // Creating output path
          val dir_name = args(2)+"/"+Time.toString()
          rdd.saveAsTextFile(dir_name)
        }
    }
    
    //==================================== Task C
       
    lines.foreachRDD{(text_rdd,Time)=>
      // Making sure to not process empty rdd
      if(!text_rdd.isEmpty()){       
        // Initiate the co_occurrence matrix
        val coMatrix = new ArrayBuffer[(String, Int)]()        
        // Split the doc into lines
        val lines_arr = text_rdd.map(x=>x.split("\\n")).collect()
        // Loop through each line
        lines_arr.foreach{       
          line_arr =>  
            // Tokenize each line then clean it
            val tokenised_line = line_arr.flatMap(x=>x.split("\\s+"))
            val cleaned_line = tokenised_line.map(x=>x.replaceAll("[^a-zA-Z0-9]", ""))
            // Removing ""
            val cleaned_words = cleaned_line.filter(x=>x.length()>0) 
            
            for(i <- 0 to cleaned_words.length-1){
              for(j <- 0 to cleaned_words.length-1){
                if(i!=j){
                  coMatrix += (( cleaned_words(i)+" # "+cleaned_words(j),1))
                }         
              }
            }
            // Convert the coMatrix to rdd
            val rddMatrix = ssc.sparkContext.parallelize(coMatrix)
            // Reduce
            val matrix = rddMatrix.reduceByKey(_ + _)
            // Creating the output path  
            val dir_name = args(3)+"/"+Time.toString()
            matrix.saveAsTextFile(dir_name) 
            
        }
           
      }
           
    }
    
       
    ssc.start()
    ssc.awaitTermination()
  }
}











