package outliers

import java.io.InputStream
import java.util.Properties
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

//case class TransDetail(unique_transact_id:String, card_num:String, processing_flag:String, trans_amt:String, trans_time:String, trans_date:String, card_type:String, merchant_key:String, mcc_desc:String, category_name:String, city:String, zip:String, chrgbck_amt:String, chargeback_cat:String, chargeback_res:String)

object KafkaDataFeeder {
  val conf = new SparkConf().setAppName("Barclays").setMaster("local[2]")
  val sc = new SparkContext(conf)

  var input: InputStream = null
  val prop: Properties = new Properties
  input = getClass.getResourceAsStream("/generator.properties")
  prop.load(input)
  val concurrency = prop.getProperty("concurrency").toInt
  val throttle = prop.getProperty("throttle").toInt

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(AppConf.APP_NAME)
      .master(AppConf.SPARK_MASTER)
      .getOrCreate()

    val runnableTask = new Runnable {
      override def run(): Unit = {
        val mapper = new ObjectMapper()
        val json = mapper.writeValueAsString(new TransactionDetails)
        //println(json)

        import spark.sqlContext.implicits._
        val ds = sc.parallelize(Seq(json)).toDF("value")

        ds.write
          .format("kafka")
          .option("kafka.bootstrap.servers", AppConf.KAFKA_SERVER)
          .option("topic", AppConf.INPUT_TOPIC)
          //.option("topic", "vizTopic")
          .save()
      }
    }

    val executorService: ScheduledExecutorService = Executors.newScheduledThreadPool(concurrency)
    var i: Int = 1
    while (i <= concurrency) {
      executorService.scheduleAtFixedRate(runnableTask, 0, throttle, TimeUnit.SECONDS)
      i += 1;
    }
  }
}
