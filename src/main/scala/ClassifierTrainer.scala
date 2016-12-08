import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession


/**
  * Created by hugod on 08-Dec-16.
  */
object ClassifierTrainer {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Classifier")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sc = spark.sparkContext

    val result = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .load("C:/Users/hugod/OneDrive/School/Jaar 3/Data Analysis and Data Mining/datasets/Kaggle/labeledTrainData.tsv")

    val data = MLUtils.loadLibSVMFile(sc, "hdfs://labeledTrainData.tsv")
    print(data.collect().head)
  }
}