import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
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
    val dataset = spark.createDataset(Seq(review))

    val transformedDf = bayesAndVectorizerModel.vectorModel.setInputCol("value").transform(dataset)

    val newVector = bayesAndVectorizerModel.model.setFeaturesCol("features").setPredictionCol("predictionCol").transform(transformedDf)
    newVector.head().getAs[Double]("predictionCol")
  }

  def testAccuracy(bayesAndVectorizerModel: BayesAndVectorizerModel): Unit = {
    val beforeAccuracyTest = System.currentTimeMillis()
    println("\nTesting accuracy...")
    val testData = bayesAndVectorizerModel.testData
    val countVectorizerModel = bayesAndVectorizerModel.vectorModel

    val tokenizedReviews = ReviewTokenizer.parseAll(testData)
    val vectorizedDataset = countVectorizerModel
      .setInputCol("words")
      .setOutputCol("features")
      .transform(tokenizedReviews)

    val predictedDataset = bayesAndVectorizerModel.model
      .setFeaturesCol("features")
      .setPredictionCol("predictedSentiment")
      .setRawPredictionCol("confidence")
      .transform(vectorizedDataset)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("sentiment")
      .setPredictionCol("predictedSentiment")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictedDataset) * 100

    logActivityTime("Accuracy test", beforeAccuracyTest)
    println("Test set accuracy: " + Math.round(accuracy) + "%")
  }

  def classifyCSV(naiveBayesAndDictionaries: BayesAndVectorizerModel, directory: String) = ???

  def createNaiveBayesModel(): BayesAndVectorizerModel = {
    // Read CSV
    val beforeReadingCsv = System.currentTimeMillis()
    println("\nReading CSV...")
    val rows = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("header", value = true)
      .load("src/main/resources/labeledTrainData.tsv")

    logActivityTime("Reading CSV", beforeReadingCsv)

    // Split into training data and test data
    val Array(trainData, testData) = rows.randomSplit(Array(0.75, 0.25))

    println("\nTokenizing reviews...")
    val beforeTokenizing = System.currentTimeMillis()

    // Tokenize all reviews and wrap them in a Review object
    val tokenizedReviews = ReviewTokenizer.parseAll(trainData)

    logActivityTime("Tokenizing reviews", beforeTokenizing)

    println("\nVectorizing reviews...")
    val beforeVectorizing = System.currentTimeMillis()

    val countVectorizer = new CountVectorizer()
      .setBinary(true)
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(2)
      .setMinDF(2)
      .fit(tokenizedReviews)

    val countVectorModel = countVectorizer.transform(tokenizedReviews)
    logActivityTime("Vectorizing reviews", beforeVectorizing)

    val vectorizedData = countVectorModel.select("id", "sentiment", "features")

    println("\nTraining model...")
    val beforeTrainingModel = System.currentTimeMillis()

    val naiveBayesModel = new NaiveBayes()
      .setModelType("bernoulli")
      .fit(vectorizedData.select("sentiment", "features").withColumnRenamed("sentiment", "label"))

    logActivityTime("Training model", beforeTrainingModel)

    BayesAndVectorizerModel(naiveBayesModel, countVectorizer, testData)
  }

  def logActivityTime(activity: String, startTime: Long): Unit =
    println(activity + s" took: ${System.currentTimeMillis() - startTime} ms")
}

case class BayesAndVectorizerModel(model: NaiveBayesModel, vectorModel: CountVectorizerModel, testData: Dataset[Row])