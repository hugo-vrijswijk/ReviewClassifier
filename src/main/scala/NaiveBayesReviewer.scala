import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.StdIn
import scala.util.Try


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

  testAccuracy(bayesAndVectorizerModel, bayesAndVectorizerModel.testData)
  console(bayesAndVectorizerModel)

  def console(naiveBayes: BayesAndVectorizerModel): Unit = {
    println("\n\nEnter 'q' to quit\n" + "Type a review, or tsv::filepath to read a TSV and classify the reviews in it\n")

    while ( {
      StdIn.readLine("Input> ") match {
        case s if s == "q" => false
        case file if file.startsWith("tsv::") => classifyCSV(naiveBayes, file.replaceFirst("tsv::", ""))
          true
        case entry if entry.nonEmpty =>
          if (entry.startsWith("tsv::")) classifyCSV(naiveBayes, entry.replaceFirst("tsv::", ""))
          else classifyReview(naiveBayes, entry)
          true
        case _ => true
      }
    }) {}

    spark.stop()
  }

  def classifyReview(bayesAndVectorizerModel: BayesAndVectorizerModel, review: String): Unit = {
    val tokenizedReview = ReviewTokenizer.tokenizeText(review)
    val reviewDataset = spark.createDataset(Seq(NonClassifiedReview(tokenizedReview)))

    val result = predictReviews(bayesAndVectorizerModel, reviewDataset).head()

    val prediction = result.getAs[Double]("predictedSentiment") match {
      case 1.0 => "positive"
      case 0.0 => "negative"
    }

    println(s"Review classified as: $prediction\n" +
      s"Confidence [negative, positive]: ${result.getAs[Double]("probability")}")
  }

  def classifyCSV(naiveBayesAndDictionaries: BayesAndVectorizerModel, directory: String): Unit = {
    val rows = readFile(directory)
    if (Try(rows.col("sentiment")).isSuccess) {
      println("File has sentiment. Testing accuracy of Naive Bayes model on file")
      testAccuracy(bayesAndVectorizerModel, rows)
    } else {
      print("File has no sentiment and cannot be classified")
    }
  }

  def testAccuracy(bayesAndVectorizerModel: BayesAndVectorizerModel, testData: Dataset[Row]): Unit = {
    val beforeAccuracyTest = System.currentTimeMillis()
    println("\nTesting accuracy...")

    val tokenizedReviews = ReviewTokenizer.parseAll(testData)

    val predictedDataset = predictReviews(bayesAndVectorizerModel, tokenizedReviews)

    val prEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("sentiment")
      .setRawPredictionCol("predictedSentiment")
      .setMetricName("areaUnderPR")
    val rocEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("sentiment")
      .setRawPredictionCol("predictedSentiment")
      .setMetricName("areaUnderROC")

    val prAccuracy = Math.round(prEvaluator.evaluate(predictedDataset) * 100)
    val rocAccuracy = Math.round(rocEvaluator.evaluate(predictedDataset) * 100)

    logActivityTime("Accuracy test", beforeAccuracyTest)
    println(s"Test set accuracy (area under PR): $prAccuracy%")
    println(s"Test set accuracy (area under ROC): $rocAccuracy%")
  }

  def predictReviews(bayesAndVectorizerModel: BayesAndVectorizerModel, tokenizedReviews: Dataset[_]): Dataset[Row] = {
    val cvModel = bayesAndVectorizerModel.cvModel
    val tfData = cvModel.transform(tokenizedReviews)
    val idfModel = bayesAndVectorizerModel.idfModel
    val vectorizedData = idfModel.transform(tfData)

    bayesAndVectorizerModel.bayesModel
      .setFeaturesCol("features")
      .setPredictionCol("predictedSentiment")
      .transform(vectorizedData)
  }

  def createNaiveBayesModel(): BayesAndVectorizerModel = {
    // Read CSV
    val beforeReadingCsv = System.currentTimeMillis()
    println("\nReading CSV...")

    val trainData = readFile("src/main/resources/labeledTrainData.tsv")
    val testData = readFile("src/main/resources/labeledTestData.tsv")
    logActivityTime("Reading CSV", beforeReadingCsv)


    // Split into training data and test data
    println("\nTokenizing reviews...")
    val beforeTokenizing = System.currentTimeMillis()

    // Tokenize all reviews and wrap them in a Review object
    val tokenizedReviews = ReviewTokenizer.parseAll(trainData)

    logActivityTime("Tokenizing reviews", beforeTokenizing)

    println("\nCalculating term-frequency reviews...")
    val beforeCv = System.currentTimeMillis()

    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      
      .fit(tokenizedReviews)
    val tfData = cvModel.transform(tokenizedReviews)
    logActivityTime("\nCalculating term-frequency", beforeCv)

    val beforeIdf = System.currentTimeMillis()
    println("Calculating IDF...")
    val idfModel = new IDF()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .fit(tfData)
    val vectorizedData = idfModel.transform(tfData)

    logActivityTime("\nCalculating inverse document frequency", beforeIdf)

    println("\nTraining model...")
    val beforeTrainingModel = System.currentTimeMillis()

    val naiveBayesModel = new NaiveBayes()
      .setSmoothing(0.85)
      .fit(vectorizedData.select("sentiment", "features").withColumnRenamed("sentiment", "label"))

    logActivityTime("Training model", beforeTrainingModel)

    BayesAndVectorizerModel(naiveBayesModel, cvModel, idfModel, testData)
  }

  def readFile(filePath: String): Dataset[Row] = {
    spark.read
      .option("sep", "\t")
      .option("quote", "\"")
      .option("header", value = true)
      .csv(filePath)
  }

  def writeFile(filePath: String, dataset: Dataset[String]): Unit = {
    dataset.write.text(filePath)
  }

  def logActivityTime(activity: String, startTime: Long): Unit =
    println(activity + s" took: ${System.currentTimeMillis() - startTime} ms")
}

case class BayesAndVectorizerModel(bayesModel: NaiveBayesModel, cvModel: CountVectorizerModel, idfModel: IDFModel, testData: Dataset[Row])