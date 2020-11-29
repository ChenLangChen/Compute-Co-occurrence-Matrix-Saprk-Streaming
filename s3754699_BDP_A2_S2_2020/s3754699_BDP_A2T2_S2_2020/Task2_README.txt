1. Make a folder named "Spark_Input" in /user/s3754699/
2. Upload lab9-0.0.1-SNAPSHOT-jar-with-dependencies.jar to HDFS
3. Copy the jar file from HDFS to EMR
hadoop fs -copyToLocal /user/s3754699/lab9-0.0.1-SNAPSHOT-jar-with-dependencies.jar ~/
4. Run the jar file 
spark-submit --class streaming.lab9.NetworkWordCount --master yarn --deploy-mode client lab9-0.0.1-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/s3754699/Spark_Input hdfs:///user/s3754699/Out_Spark_A hdfs:///user/s3754699/Out_Spark_B hdfs:///user/s3754699/Out_Spark_C
5. Upload the text one by one to Spark_Input, with an time interval of at least 10 seconds 
6. Output of task A is in Out_Spark_A, task B in Out_Spark_B, task C in Out_Spark_C