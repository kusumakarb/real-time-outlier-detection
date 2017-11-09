package outliers

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import play.api.libs.json.{JsObject, JsValue, Json}

class EventCollector extends StreamingQueryListener{
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        val queryProgress: String = event.progress.json
        println(queryProgress)

        val producerProps = new Properties()
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

        val inputPartition = new TopicPartition("input", 0)
        lagConsumer.assign(util.Collections.singleton(inputPartition))
        lagConsumer.seekToEnd(util.Collections.singleton(inputPartition))
        println(lagConsumer.position(inputPartition))
        val latestOffset = lagConsumer.position(inputPartition)
        val latestOffsetJson: JsValue = Json.obj(
          "latestOffset" -> latestOffset
        )
        val queryProgressJson: JsValue = Json.parse(queryProgress)
        val queryProgressJsObject: JsObject = queryProgressJson.as[JsObject] + ("latestOffset", latestOffsetJson)

        //the producer Kafka producer writes the query progress to test topic
        val producer = new KafkaProducer[String, String](producerProps)
        val TOPIC = "test"


        val record = new ProducerRecord(TOPIC, "key", queryProgressJsObject.toString())
        producer.send(record)
        producer.close()
        lagConsumer.close()
      }
}


