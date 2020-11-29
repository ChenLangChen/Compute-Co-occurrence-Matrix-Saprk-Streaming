How to run with the standalone jar

1. Make a folder "inputdata" in /user/s3754699, upload the 3 text files into inputdata
2. Upload TaskA-0.0.1-SNAPSHOT.jar to HDFS /user/s3754699
3. Copy the jar file from HDFS to EMR
hadoop fs -copyToLocal /user/s3754699/TaskA-0.0.1-SNAPSHOT.jar ~/
4. Run the jar
- Task1.1 Pairs: 
hadoop jar TaskA-0.0.1-SNAPSHOT.jar Assignment2.TaskA.Task1_Pairs /user/s3754699/inputdata /user/s3754699/output_Task1_Pairs

- Task1.1 Strips: 
hadoop jar TaskA-0.0.1-SNAPSHOT.jar Assignment2.TaskA.Task1_Strips /user/s3754699/inputdata /user/s3754699/output_Task1_Strips

- Task1.2 Pairs: 
hadoop jar TaskA-0.0.1-SNAPSHOT.jar Assignment2.TaskA.Task2_Pairs /user/s3754699/inputdata /user/s3754699/output_Task2_Pairs

- Task1.2 Strips: 
hadoop jar TaskA-0.0.1-SNAPSHOT.jar Assignment2.TaskA.Task2_Strips /user/s3754699/inputdata /user/s3754699/output_Task2_Strips









