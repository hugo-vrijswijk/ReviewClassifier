import org.apache.spark.sql.{Row, SparkSession}


/**
  * Created by hugod on 08-Dec-16.
  */
object ClassifierTrainer {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Classifier")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val rows = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("header", true)
      .load("C:/Users/hugod/OneDrive/School/Jaar 3/Data Analysis and Data Mining/datasets/Kaggle/labeledTrainData.tsv")

    val rdd = rows.select("review", "sentiment").rdd
    val filteredRdd = rdd.map(row => (cleanString(row.getAs[String]("review")), intToBool(row.getAs[String]("sentiment")))).filter(_._1.nonEmpty)

  }

  def cleanString(entry: String): String = entry.replaceAll("<.*?>", "")

  def intToBool(num: String): Boolean = num match {
    case "1" => true
    case _ => false
  }

}