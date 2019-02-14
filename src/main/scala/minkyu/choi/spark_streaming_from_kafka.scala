package minkyu.choi

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object spark_streaming_from_kafka {

  def main(args: Array[String]): Unit = {

    // Spark Configuration

    val conf = new SparkConf()
      .setAppName("twitter-streaming")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    // val ssc = new StreamingContext(conf, Seconds(1))


    // Kafka Configuration

    val topics = List("twitter").toSet // Your topic name

    val kafkaParams = Map(
      "bootstrap.servers" -> "", // Your server
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer-group", // Your consumer group
      "auto.offset.reset" -> "earliest")

    // Getting the Data from Kafka into Dstream Object
    // Getting streaming data from Kafka and send it to the Spark and create Dstream RDD

    val kafka_stream_Dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    // Sample Transformation : converting all characters to lower cases

    val lower_Dstream = kafka_stream_Dstream.map(record => record.value().toString.toLowerCase)



    // Print out the result in the console
    // lower_Dstream.print()


    // Save it to HDFS from Dstream Object
    /**
    lower_Dstream.foreachRDD(eachRdd => {
      eachRdd.saveAsTextFile("hdfs://") Your HDFS address 
    })
    */

    // Converting Dstream RDD to Data Frame

    lower_Dstream.foreachRDD(rddRaw => {
      val spark = SparkSession.builder.config(rddRaw.sparkContext.getConf).getOrCreate()
      val cols = List("created_at", "text")
      val df = spark.read.json(rddRaw).select(cols.head, cols.tail: _*)
      df.write.mode(SaveMode.Append).format("csv").save("hdfs://") Your HDFS address 
      df.show()

    })

    ssc.start()
    ssc.awaitTermination()
  }

}