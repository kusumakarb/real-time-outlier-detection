package outliers

import java.util
import java.util.Properties
import javax.inject.{Inject, Singleton}

//import netscape.javascript.JSObject
import play.api.libs.json.{JsValue, Json, JsObject}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.types.{StructField, StructType, _}


object OutlierDetectionApp extends Serializable{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(AppConf.APP_NAME)
      .master(AppConf.SPARK_MASTER)
      .getOrCreate()
    /*spark.conf.set("spark.executor.memory", "4g")
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.conf.set("spark.default.parallelism", "1")*/

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        //println("Query made progressssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss: " + event.progress)
        val queryProgress: String = event.progress.json
        println(queryProgress)

        val  producerProps = new Properties()
        producerProps.put("bootstrap.servers", AppConf.KAFKA_SERVER)
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val consumerProps = new Properties()
        consumerProps.put("bootstrap.servers", AppConf.KAFKA_SERVER)

        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        consumerProps.put("group.id", "something")
        consumerProps.put("auto.offset.reset", "latest")


        //lagConsumer is to get the latest offset from the Kafka topic partition
        val lagConsumer = new KafkaConsumer[String, String](consumerProps)
        import org.apache.kafka.common.TopicPartition
        val inputPartition = new TopicPartition("input", 0)
        lagConsumer.assign(util.Collections.singleton(inputPartition))
        lagConsumer.seekToEnd(util.Collections.singleton(inputPartition))
        println(lagConsumer.position(inputPartition))
        val latestOffset = lagConsumer.position(inputPartition)
        val latestOffsetJson: JsValue = Json.obj(
          "latestOffset" -> latestOffset
        )
        val queryProgressJson: JsValue = Json.parse(queryProgress)
        val queryProgressJsObject: JsObject = queryProgressJson.as[JsObject] + ("latestOffset",latestOffsetJson)

        //the producer Kafka producer writes the query progress to test topic
        val producer = new KafkaProducer[String, String](producerProps)
        val TOPIC="test"

        println("Streaming Query made progressssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss: " + queryProgressJsObject.toString())
        val record = new ProducerRecord(TOPIC, "key", queryProgressJsObject.toString())
        producer.send(record)
        producer.close()
        lagConsumer.close()

      }
    })
    //spark.conf.set("spark.extraListeners", "QueryProgressTracker")

    import spark.implicits._

    // Read from Input kafka topic
    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConf.KAFKA_SERVER)
      .option("subscribe", AppConf.INPUT_TOPIC)
      .option("failOnDataLoss", "false")
      .option("startingOffsets", "latest") // equivalent of auto.offset.reset which is not allowed here
      .option("maxOffsetsPerTrigger", 10000)
      .load()

//    val listener = new QueryProgressTracker()
//    spark.streams.addListener(listener)


    // Deserialize data to the original format
    val schemaString = "unique_transact_id card_num processing_flag trans_amt trans_time card_type merchant_key mcc_desc " +
      "category_name city zip chrgbck_amt chargeback_cat chargeback_res"


    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    val df: DataFrame = ds1.selectExpr("cast (value as string) as json")
      .select(from_json($"json", schema).as("data"))
      .select("data.*")

    println(df.isStreaming)


    // Outlier logic
    val outlierUpperWarning = Outlier(df, "trans_amt", "Upper_Warning", 3.0, 5.5)
    val predictedDf1: DataFrame = outlierUpperWarning.predict(df)
    val outlierUpperAction = Outlier(predictedDf1, "trans_amt", "Upper_Action", 4.0, 6.5)
    val predictedDf2 = outlierUpperAction.predict(predictedDf1)


    // Writing data to the "Output" Kafka topic for downstream systems
    val predictedDf3 = predictedDf2.select(to_json(struct($"*")).as("value")).toDF("value")


    val query = predictedDf3
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConf.KAFKA_SERVER)
      .option("topic", AppConf.PROCESSED_TOPIC)
      .option("checkpointLocation", "/tmp/kafka/output")
      .start()

    // For debugging purpose

/*        val query = predictedDf2
          .writeStream
          .outputMode("append")
          .queryName("table")
          .format("console")
          .start()*/

    query.awaitTermination()
  }

}
