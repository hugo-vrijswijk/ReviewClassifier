import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql._
import scala.io.StdIn


/**
  * Created by hugod on 21-Jan-17.
  */
object NaiveBayesReviewer extends App {
  val spark = SparkSession
    .builder()
    .appName("Classifier")
    .master("local[4]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val rows = spark.read.format("com.databricks.spark.csv")
    .option("delimiter", "\t")
    .option("quote", "\"")
    .option("header", true)
    .load("C:/Users/hugod/OneDrive/School/Jaar 3/Data Analysis and Data Mining/datasets/Kaggle/labeledTrainData.tsv")

  val beforeLearning = System.currentTimeMillis()
  val bayesAndVectorizerModel = createNaiveBayesModel()
  println("\n\nLearning took: " + (System.currentTimeMillis() - beforeLearning) + "ms\n")
  val beforeAccuracyTest = System.currentTimeMillis()

  testAccuracy(bayesAndVectorizerModel)
  println("Accuracy test took: " + (System.currentTimeMillis() - beforeAccuracyTest) + "ms\n")

  console(bayesAndVectorizerModel)

  def console(naiveBayes: BayesAndVectorizerModel): Unit = {
    println("Enter 'q' to quit\n" + "Type a review, or CSV::filepath to read a CSV and classify the reviews in it\n")

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
    //    val vector = Vectors.fromML(transformedDf.head().getAs[org.apache.spark.ml.linalg.Vector]("features"))
    //
    val newVector = bayesAndVectorizerModel.model.setFeaturesCol("features").setPredictionCol("predictionCol").transform(transformedDf)
    newVector.head().getAs[Double]("predictionCol")
  }

  def testAccuracy(bayesAndVectorizerModel: BayesAndVectorizerModel): Unit = {
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
    println("Test set accuracy: " + Math.round(accuracy) + "%")

    //
    //    val newTestData = testData.rdd.zipWithIndex().filter(_._2 < 1000)
    //    val size = newTestData.count()
    //    //noinspection ComparingUnrelatedTypes
    //    val predictedCorrect = newTestData.map(_._1).map(row => {
    //      val id = row.getAs[String]("id")
    //      val reviewText = ReviewTokenizer.tokenizeText(row.getAs[String]("review"))
    //      val sentiment = ReviewTokenizer.extractSentiment(row)
    //      val predictedSentiment = vectorizeReview(bayesAndVectorizerModel, reviewText)
    //
    //      ClassifiedReview(id, reviewText, sentiment, predictedSentiment)
    //    })
    //      .filter(review => review.actualSentiment != null && review.actualSentiment == review.predictedSentiment)
    //      .count()
    //
    //    val newAccuracy = predictedCorrect.toDouble / size.toDouble
    //    println("Accuracy: " + newAccuracy)
  }

  def classifyCSV(naiveBayesAndDictionaries: BayesAndVectorizerModel, directory: String) = ???

  def createNaiveBayesModel(): BayesAndVectorizerModel = {
    // Read CSV
    val rows = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("header", true)
      .load("C:/Users/hugod/OneDrive/School/Jaar 3/Data Analysis and Data Mining/datasets/Kaggle/labeledTrainData.tsv")

    val Array(trainData, testData) = rows.randomSplit(Array(0.75, 0.25))

    // Tokenize all reviews and neatly wrap them in a Review object
    val tokenizedReviews = ReviewTokenizer.parseAll(trainData)

    val countVectorizer = new CountVectorizer()
      .setBinary(true)
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(2)
      .setMinDF(2)
      .fit(tokenizedReviews)

    val countVectorModel = countVectorizer.transform(tokenizedReviews)
    val vectorizedData = countVectorModel.select("id", "sentiment", "features")

    val naiveBayesModel = new NaiveBayes()
      .setModelType("bernoulli")
      .fit(vectorizedData.select("sentiment", "features").withColumnRenamed("sentiment", "label"))

    BayesAndVectorizerModel(naiveBayesModel, countVectorizer, testData)
  }
}

case class BayesAndVectorizerModel(model: NaiveBayesModel, vectorModel: CountVectorizerModel, testData: Dataset[Row])