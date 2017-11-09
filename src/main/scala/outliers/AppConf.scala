package outliers

object AppConf {

  // Spark appName
  val APP_NAME = "OutlierDetection"

  // Kafka Related Configs
  val INPUT_TOPIC = "input"
  val PROCESSED_TOPIC = "output"
  val GROUP_ID = "outlier_detection"


  //Local System
//  val SPARK_MASTER = "local"
//  val KAFKA_SERVER = "localhost:9092"
//  val ZOOKEEPER_SERVER = "localhost:2181"

  //AWS
  val SPARK_MASTER = "spark://kafka01.app.scienaptic.com:7077"
  val KAFKA_SERVER = "kafka01.app.scienaptic.com:2181,kafka02.app.scienaptic.com:9092,kafka03.app.scienaptic.com:9092"
  val ZOOKEEPER_SERVER = "kafka01.app.scienaptic.com:2181"

  //Local Server
  //val SPARK_MASTER = "spark://192.168.0.31:7077"
  //val KAFKA_SERVER = "192.168.0.31:9092,192.168.0.32:9092,192.168.0.35:9092"
  //val ZOOKEEPER_SERVER = "192.168.0.31:9092"



}
