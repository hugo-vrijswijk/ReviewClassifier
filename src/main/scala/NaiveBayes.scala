import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.SparkSession

import scala.tools.jline_embedded.console.ConsoleReader

/**
  * Created by hugod on 21-Jan-17.
  */
object NaiveBayes extends App {
  val spark = SparkSession
    .builder()
    .appName("Classifier")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._

  val rows = spark.read.format("com.databricks.spark.csv")
    .option("delimiter", "\t")
    .option("quote", "\"")
    .option("header", true)
    .load("C:/Users/hugod/OneDrive/School/Jaar 3/Data Analysis and Data Mining/datasets/Kaggle/labeledTrainData.tsv")

  val rdd = rows.select("review", "sentiment").rdd

  val naiveBayesAndDictionaries = createNaiveBayesModel()

  def console() = {
    println("Enter 'q' to quit\n" + "Type a review, or CSV::filepath to read a CSV and classify the reviews in it")

    val consoleReader = new ConsoleReader()
    while ( {
      consoleReader.readLine("Input> ") match {
        case s if s == "q" => false
        case file if file.startsWith("csv::") => classifyCSV(file)
        case review if review.nonEmpty => classifyReview(review)
        case _ => true
      }
    }) {}

    spark.stop()
  }

  def classifyReview(review: String) = ???

  def classifyCSV(directory: String) = ???

  def createNaiveBayesModel(): NaiveBayesAndDictionaries = {
    // Read CSV
    val rows = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("header", true)
      .load("C:/Users/hugod/OneDrive/School/Jaar 3/Data Analysis and Data Mining/datasets/Kaggle/labeledTrainData.tsv")

    val reviewsRdd = rows.rdd

    // Create dictionary term => id
    // And id => term
    val terms = reviewsRdd

  }
}

case class NaiveBayesAndDictionaries(model: NaiveBayesModel,
                                     termDictionary: Dictionary,
                                     labelDictionary: Dictionary,
                                     idfs: Map[String, Double])