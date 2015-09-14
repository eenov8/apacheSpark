# Download the required Jar from https://drive.google.com/file/d/0B70R5CJksVCnTTNLc3pxaENZWjg/view?usp=sharing

# bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.0 
# --jars /home/edureka/Desktop/downloads/spark-streaming-kafka-assembly_2.10-1.5.0.jar 
# /home/anurag/Desktop/spark/kafka_spark_wc.py localhost:9092 test

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_spark_wc.py <broker_list> <topic>")
        exit(-1)

    #create SparkContext which will run on local machine as master
	#since one thread will be listening to the incoming stream, it will be a good idea to have at least two threads,
	#one to listen to the stream and another one that will process the same.
	sc = SparkContext(appName="PythonKafkaWordCount")
	
	#Create a streaming Spark Context that will poll the source every 2 seconds to create a DStream.
    ssc = StreamingContext(sc, 2)

	#read the command line argument to create the brokers list and the topic that we need to listen to.
	#If we want to listen multiple topics we need to create multiple streams for the same as in next line.
    brokers, topic = sys.argv[1:]
	
	#Create a Dstream from kafkaUtils library that comes along with the included jar.
	#Create multiple such streams if we have more that one topic to be read.
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
	
	#Now comes in the standard code that will read the line, split it on spaces to get the words and then finally reduce by the keys
	#to generate the word count.
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    
	#pprint will print the first 10 entries.
	counts.pprint()

	#Start the streaming context and await manual interruption.
    ssc.start()
    ssc.awaitTermination()