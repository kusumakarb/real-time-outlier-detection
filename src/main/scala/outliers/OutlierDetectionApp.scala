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
import org.apache.kafka.common.TopicPartition


object OutlierDetectionApp extends Serializable{

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName(AppConf.APP_NAME)
      .master(AppConf.SPARK_MASTER)
      .getOrCreate()

    val listener = new EventCollector()
    spark.streams.addListener(listener)


    val schemaString = "unique_transact_id card_num processing_flag trans_amt trans_time card_type merchant_key mcc_desc " +
      "category_name city zip chrgbck_amt chargeback_cat chargeback_res"


    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)


    import spark.implicits._

    // Read from Input kafka topic
    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConf.KAFKA_SERVER)
      .option("subscribe", AppConf.INPUT_TOPIC)
      .option("startingOffsets", "latest") // equivalent of auto.offset.reset which is not allowed here
      .option("maxOffsetsPerTrigger", 10000)
      .load()


    // Deserialize data to the original format

    val df: DataFrame = ds1.selectExpr("cast (value as string) as json")
      .select(from_json($"json", schema).as("data"))
      .select("data.*")

    println(df.isStreaming)


    // Outlier logic
    val outlierUpperWarning = Outlier(df, "trans_amt", "Upper_Warning", 7.0, 5.5)
    val predictedDf1: DataFrame = outlierUpperWarning.predict(df)
    val outlierUpperAction = Outlier(predictedDf1, "trans_amt", "Upper_Action", 10.0, 7)
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

    query.awaitTermination()
  }

}
