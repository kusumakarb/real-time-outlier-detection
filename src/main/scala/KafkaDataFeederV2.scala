package barclays

import java.io.InputStream
import java.util.Properties
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import org.apache.kafka.clients.producer._

object KafkaDataFeederV2 {
  var input: InputStream = _
  val prop: Properties = new Properties
  input = getClass.getResourceAsStream("/generator.properties")
  prop.load(input)
  val concurrency = prop.getProperty("concurrency").toInt
  val throttle = prop.getProperty("throttle").toInt

  val  props = new Properties()
  props.put("bootstrap.servers", AppConf.KAFKA_SERVER)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)
  val TOPIC = AppConf.INPUT_TOPIC

  def main(args: Array[String]): Unit = {
    val runnableTask = new Runnable {
      override def run(): Unit = {
        val transactionData = new ProducerRecord(TOPIC, "key", new TransactionDetails().toString)
        producer.send(transactionData)
      }
    }

    val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(concurrency)
    var i: Int = 1
    while (i <= concurrency) {
      executorService.scheduleAtFixedRate(runnableTask, 0, throttle, TimeUnit.SECONDS)
      i += 1
    }
  }
}
