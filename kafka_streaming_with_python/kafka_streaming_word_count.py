# Download the required Jar from https://drive.google.com/file/d/0B70R5CJksVCnTTNLc3pxaENZWjg/view?usp=sharing
#bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.0 --jars /home/edureka/Desktop/downloads/spark-streaming-kafka-assembly_2.10-1.5.0.jar /home/anurag/Desktop/spark/kafka_spark_wc.py localhost:9092 test

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_spark_wc.py <broker_list> <topic>")
        exit(-1)

    sc = SparkContext(appName="PythonKafkaWordCount")
    ssc = StreamingContext(sc, 2)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()