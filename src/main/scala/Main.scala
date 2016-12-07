import java.io.File

import net.tixxit.delimited.{DelimitedFormat, DelimitedParser}
import org.apache.spark
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Hugo van Rijswijk on 01-Dec-16.
  */
object Main extends App {
  val sc = SparkContext.getOrCreate()
  val stream = getClass.getResourceAsStream("/labeledTrainData.tsv")
  val myFormat: DelimitedFormat = DelimitedFormat(
    separator = "\t",
    quote = "\"\""
  )
  val parser = DelimitedParser(myFormat)

  val rowsEither = parser.parseFile(new File("C:/Users/hugod/OneDrive/School/Jaar 3/Data Analysis and Data Mining/Workspace/Classifier/src/main/resources/labeledTrainData.tsv"))

  // Find error and throw it
  rowsEither.collectFirst { case Left(x) => throw x }

  val rows = rowsEither.collect { case Right(x) => x }

  val cleanedRows = rows.map(_(2).replaceAll("<.*?>", ""))


  val model = new NaiveBayes()


  def countWords(text: String): Map[String, Int] =
    clean(text).split("[ ,!.]+").groupBy(identity).mapValues(_.length)

  def clean(text: String): String = text.replaceAll("<.*?>", "").toLowerCase




  //Example
  val conf = new SparkConf().setAppName("NaiveBayesExample")
  val sc = new SparkContext(conf)
  // $example on$
  // Load and parse the data file.
  val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")

  // Split data into training (60%) and test (40%).
  val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

  val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

  val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
  val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

  // Save and load model
  model.save(sc, "target/tmp/myNaiveBayesModel")
  val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
  // $example off$
}
