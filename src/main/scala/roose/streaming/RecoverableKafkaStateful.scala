package roose.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import roose.streaming.RecoverableStatefulWordCount.{createContext, mappingFunc}

object RecoverableKafkaStateful {

  // Update the cumulative count using mapWithState
  // This will give a DStream made of state (which is the cumulative count of the words)
  val mappingFunc: (String, Option[Int], State[Int]) => (String, Int) =
  ( word: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }

  def createContext(brokers: String, groupId: String, topics: String, checkpointDirectory: String)
    : StreamingContext = {

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("RecoverableKafkaStateful")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest"
    )
    // Initial state RDD for mapWithState operation
    val initialRDD = ssc.sparkContext.parallelize(List[(String, Int)]())

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    val stateDstream = wordCounts.mapWithState(
      StateSpec.function(mappingFunc).initialState(initialRDD))
    stateDstream.print()

    ssc
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics> <checkpoint-directory>
           |  <brokers> is a list of one or more Kafka brokers
           |  <groupId> is a consumer group name to consume from topics
           |  <topics> is a list of one or more kafka topics to consume from
           |  <checkpoint-directory> is HDFS directory which checkpoint data
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, groupId, topics, checkpointDirectory) = args

    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => createContext(brokers, groupId, topics, checkpointDirectory))
    ssc.start()
    ssc.awaitTermination()
  }
}
