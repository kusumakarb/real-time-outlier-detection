package barclays

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory


case class Outlier(mean: Double,
                   std: Double,
                   colName: String,
                   var outputCol: String,
                   threshold: Double) extends Serializable {

  def predict(data: DataFrame) =
    data.withColumn(outputCol, when(col(colName) > threshold, 1)
      .otherwise(0))
}


object Outlier extends Serializable {

  def apply(data: DataFrame, inputColName: String, outputColName: String, meanCoeff: Double, stdCoeff: Double) = {
    require(data.columns.contains(inputColName), "Column name not valid")

    // TODO : Put logic to calculate mean and stdDev
    val mean: Double = 10.0
    val stdDev: Double = 1.5

    val threshold = meanCoeff * mean + stdCoeff * stdDev

    System.out.println("Calculated threshold  = " + threshold + " for colName=" + outputColName)
    new Outlier(mean, stdDev, inputColName, outputColName, threshold)
  }

}



