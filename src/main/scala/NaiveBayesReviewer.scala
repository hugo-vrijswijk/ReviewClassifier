import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.StdIn


/**
  * Created by Hugo van Rijswijk
  */
object NaiveBayesReviewer extends App {
  val spark = SparkSession
    .builder()
    .appName("Classifier")
    .master("local[4]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  System.setProperty("hadoop.home.dir", "C:/Program Files (x86)/Hadoop")

  import spark.implicits._

  val beforeLearning = System.currentTimeMillis()
  val bayesAndVectorizerModel = createNaiveBayesModel()
  logActivityTime("\nTotal of learning", beforeLearning)

  testAccuracy(bayesAndVectorizerModel)
  console(bayesAndVectorizerModel)

  def console(naiveBayes: BayesAndVectorizerModel): Unit = {
    println("\n\nEnter 'q' to quit\n" + "Type a review, or CSV::filepath to read a CSV and classify the reviews in it\n")

    while ( {
      StdIn.readLine("Input> ") match {
        case s if s == "q" => false
        case review if review.nonEmpty => classifyReview(naiveBayes, review)
          true
        case file if file.startsWith("csv::") => classifyCSV(naiveBayes, file.replace("csv::", ""))
          true
        case _ => true
      }
    }) {}

    spark.stop()
  }

  def classifyReview(bayesAndVectorizerModel: BayesAndVectorizerModel, review: String): Unit = {
    val tokenizedReview = ReviewTokenizer.tokenizeText(review)
    val predicted = vectorizeReview(bayesAndVectorizerModel, tokenizedReview)

    println("Review classified as: " + predicted)
  }

  def vectorizeReview(bayesAndVectorizerModel: BayesAndVectorizerModel, review: Seq[String]): Double = {
    val dataset = spark.createDataset(Seq(review)).withColumnRenamed("value", "words")

    val hashingTF = bayesAndVectorizerModel.hashingTF
    val bayesModel = bayesAndVectorizerModel.bayesModel

    val result = idfDataset(dataset, hashingTF)
    bayesModel.transform(result).head().getAs[Double]("predictedSentiment")
  }

  def testAccuracy(bayesAndVectorizerModel: BayesAndVectorizerModel): Unit = {
    val beforeAccuracyTest = System.currentTimeMillis()
    println("\nTesting accuracy...")
    val testData = bayesAndVectorizerModel.testData
    val bayesModel = bayesAndVectorizerModel.bayesModel
    val hashingTF = bayesAndVectorizerModel.hashingTF
    val tokenizedReviews = ReviewTokenizer.parseAll(testData)

    val vectorizedData = idfDataset(tokenizedReviews, hashingTF)

    val predictedDataset = bayesModel
      .setFeaturesCol("features")
      .setPredictionCol("predictedSentiment")
      .transform(vectorizedData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("sentiment")
      .setPredictionCol("predictedSentiment")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictedDataset) * 100

    logActivityTime("Accuracy test", beforeAccuracyTest)
    println(s"Test set accuracy: $accuracy%")
  }

  def classifyCSV(naiveBayesAndDictionaries: BayesAndVectorizerModel, directory: String) = ???

  def createNaiveBayesModel(): BayesAndVectorizerModel = {
    // Read CSV
    val beforeReadingCsv = System.currentTimeMillis()
    println("\nReading CSV...")
    val trainData = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("header", value = true)
      .load("src/main/resources/labeledTrainData.tsv")

    logActivityTime("Reading CSV", beforeReadingCsv)

    val testData = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("header", value = true)
      .load("src/main/resources/labeledTestData.tsv")

    // Split into training data and test data
    println("\nTokenizing reviews...")
    val beforeTokenizing = System.currentTimeMillis()

    // Tokenize all reviews and wrap them in a Review object
    val tokenizedReviews = ReviewTokenizer.parseAll(trainData)

    logActivityTime("Tokenizing reviews", beforeTokenizing)

    println("\nVectorizing reviews...")
    val beforeVectorizing = System.currentTimeMillis()

    val hashingTF = new HashingTF()
      .setInputCol("words").setBinary(true)
      .setOutputCol("rawFeatures")

    val vectorizedData = idfDataset(tokenizedReviews, hashingTF)

    logActivityTime("Vectorizing reviews", beforeVectorizing)

    println("\nTraining model...")
    val beforeTrainingModel = System.currentTimeMillis()

    val naiveBayesModel = new NaiveBayes()
      .setSmoothing(0.6)
      .fit(vectorizedData.select("sentiment", "features").withColumnRenamed("sentiment", "label"))

    logActivityTime("Training model", beforeTrainingModel)

    BayesAndVectorizerModel(naiveBayesModel, hashingTF, testData)
  }

  def idfDataset(tokenizedData: Dataset[_], hashingVectorizer: HashingTF): Dataset[_] = {
    val tfData = hashingVectorizer.transform(tokenizedData)

    val idf = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfModel = idf.fit(tfData)
    idfModel.transform(tfData)
  }

  def logActivityTime(activity: String, startTime: Long): Unit =
    println(activity + s" took: ${System.currentTimeMillis() - startTime} ms")
}

case class BayesAndVectorizerModel(bayesModel: NaiveBayesModel, hashingTF: HashingTF, testData: Dataset[Row])