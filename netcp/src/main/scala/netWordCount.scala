import java.util.HashMap

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object netWordCount {
  def main(args: Array[String]) {    

	val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
	val ssc = new StreamingContext(conf, Seconds(1))
	val lines = ssc.socketTextStream("localhost", 9999)
	val words = lines.flatMap(_.split(" "))
	import org.apache.spark.streaming.StreamingContext._
	// Count each word in each batch
	val pairs = words.map(word => (word, 1))
	val wordCounts = pairs.reduceByKey(_ + _)

	// Print the first ten elements of each RDD generated in this DStream to the console
	wordCounts.print()
	println("***********************************OutPut of the nc wordcount**********************")
//reducedByKey.collect.foreach(println)
	ssc.start()             // Start the computation
	ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
