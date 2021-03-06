package roose.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

object RecoverableStatefulWordCount {

  // Update the cumulative count using mapWithState
  // This will give a DStream made of state (which is the cumulative count of the words)
  val mappingFunc: (String, Option[Int], State[Int]) => (String, Int) =
  ( word: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }


  def createContext(ip: String, port: Int, checkpointDirectory: String)
    : StreamingContext = {

    val sparkConf = new SparkConf().setAppName("RecoverableStatefulWordCount")
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(checkpointDirectory)

    // Initial state RDD for mapWithState operation
    val initialRDD: RDD[(String, Int)] = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    // Create a socket stream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    val lines = ssc.socketTextStream(ip, port)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)

    val stateDstream = wordCounts.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))
    stateDstream.print()

    ssc
  }

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(s"Your arguments were ${args.mkString("[", ", ", "]")}")
      System.err.println(
        """
          |Usage: RecoverableNetworkWordCount <hostname> <port> <checkpoint-directory>
          |     <hostname> and <port> describe the TCP server that Spark
          |     Streaming would connect to receive data. <checkpoint-directory> directory to
          |     HDFS-compatible file system which checkpoint data.
          |
          |In local mode, <master> should be 'local[n]' with n > 1
          |And <checkpoint-directory> must be absolute paths
        """.stripMargin
      )
      System.exit(1)
    }

    val Array(ip, port, checkpointDirectory) = args
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(ip, port.toInt, checkpointDirectory))
    ssc.start()
    ssc.awaitTermination()
  }
}
