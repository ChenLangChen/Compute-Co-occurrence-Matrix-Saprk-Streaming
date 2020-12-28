# Task 1 – Compute Co-occurrence Matrix

## Task 1.1 
Implement both “pairs approach” and “strips approach” to compute the co-occurrence matrix where the word-pair frequency is maintained. The context of a word is defined as the words in the same line.

## Task 1.2 
Implement both “pairs approach” and “strips approach” to compute the co-occurrence matrix where the word-pair relative frequency is maintained. The context of a word is defined as the words in the same line. Note “pairs approach” should avoid the memory bottleneck issue.

# Task 2 – Spark Streaming

Develop code in a Scala Maven project to monitor a folder in HDFS in real time such that any new file in the folder will be processed (in this assignment, you are required to load “3littlepigs”, “Melbourne” and “RMIT” files in the folder under monitoring in sequence order; note must wait for at least 10 seconds between two files). For each RDD in the stream, the following subtasks are performed concurrently:
- (a) Count the word frequency and save the output in HDFS.
Note, for each word, make sure space (" "), comma (","), semicolon (";"), colon (":"), period ("."), apostrophe (“’”), quotation marks (“””), exclamation (“!”), question mark (“?”), and brackets ("[", “{”, “(”, “<”,"]", “)”, “}”,”>” ) are trimmed.
- (b) Filter out the short words (i.e., < 5 characters) and save the output in HDFS.
- (c) Count the co-occurrence of words in each RDD where the context is the same line; and save the
output in HDFS.
