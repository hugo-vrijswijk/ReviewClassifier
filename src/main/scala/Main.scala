import java.io.File

import net.tixxit.delimited.{DelimitedFormat, DelimitedParser}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Hugo van Rijswijk on 01-Dec-16.
  */
object Main {
  def main(args: Array[String]) {
    val logFile = "C:/Users/hugod/Documents/spark-2.0.2-bin-hadoop2.7/README.md"
    // Should be some file on your system
    val conf = new SparkConf().setAppName("Classifier").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc = new SparkContext(conf)
    val stream = getClass.getResourceAsStream("/labeledTrainData.tsv")
    val myFormat: DelimitedFormat = DelimitedFormat(
      separator = "\t",
      quote = "\"\""
    )
    val parser = DelimitedParser(myFormat)

    val rowsEither = parser.parseFile(new File("C:/Users/hugod/OneDrive/School/Jaar 3/Data Analysis and Data Mining/datasets/Kaggle/labeledTrainData.tsv"))

    // Find error and throw it
    rowsEither.collectFirst { case Left(x) => throw x }

    val rows = rowsEither.collect { case Right(x) => x }

    val cleanedRows = rows.map(_ (2).replaceAll("<.*?>", ""))

    def countWords(text: String): Map[String, Int] =
      clean(text).split("[ ,!.]+").groupBy(identity).mapValues(_.length)

    def clean(text: String): String = text.replaceAll("<.*?>", "").toLowerCase


    // $example on$
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "src/main/resources/labeledTrainData.tsv")

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
}