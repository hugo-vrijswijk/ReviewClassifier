import java.io.File

import net.tixxit.delimited.{DelimitedFormat, DelimitedParser}
import org.apache.spark.ml.classification.NaiveBayes


/**
  * Created by Hugo van Rijswijk on 01-Dec-16.
  */
object Main extends App {
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
}
