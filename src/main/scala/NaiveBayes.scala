import org.apache.spark.ml.feature.Tokenizer
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

  val naiveBayesAndDictionaries = createNaiveBayesModel()

  console()

  def console() = {
    println("Enter 'q' to quit\n" + "Type a review, or CSV::filepath to read a CSV and classify the reviews in it")

    val consoleReader = new ConsoleReader()
    while ( {
      consoleReader.readLine("Input> ") match {
        case s if s == "q" => false
        case review if review.nonEmpty => classifyReview(review)
        case file if file.startsWith("csv::") => classifyCSV(file.replace("csv::", ""))
        case _ => true
      }
    }) {}

    spark.stop()
  }

  def classifyReview(review: String) = ???

  def classifyCSV(directory: String) = ???

  def createNaiveBayesModel() = {
    // Read CSV
    val rows = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("header", true)
      .load("C:/Users/hugod/OneDrive/School/Jaar 3/Data Analysis and Data Mining/datasets/Kaggle/labeledTrainData.tsv")
      .rdd

    // Tokenize all reviews and neatly wrap them in a Review object
    val tokenizedReviews = ReviewTokenizer.parseAll(rows)
    val bayes = new NaiveBayesModel()
    // All words used in all reviews
    val terms = tokenizedReviews.flatMap(_.terms).distinct().collect().sortBy(identity)
    val termsDictionary = new Dictionary(terms)
    //    val termDict = new Dictionary(terms)

  }
}

//
//case class NaiveBayesAndDictionaries(model: NaiveBayesModel,
//                                     termDictionary: Dictionary,
//                                     labelDictionary: Dictionary,
//                                     idfs: Map[String, Double])